{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.ReactorSpec
-- Description : Native/WASM byte-identity tests for the cage reactor.
module Cardano.MPFS.Client.Cage.ReactorSpec
    ( spec
    ) where

import Cardano.Crypto.Hash (hashFromBytes)
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx (mkBasicTx)
import Cardano.Ledger.Api.Tx.Body (mkBasicTxBody)
import Cardano.Ledger.Api.Tx.Out (datumTxOutL, mkBasicTxOut)
import Cardano.Ledger.Api.Tx.Wits (mkBasicTxWits)
import Cardano.Ledger.BaseTypes (Network (Testnet))
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.Ledger.Credential
    ( Credential (KeyHashObj)
    , StakeReference (StakeRefNull)
    )
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Keys (KeyHash (..))
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MaryValue (..)
    , MultiAsset (..)
    , PolicyID (..)
    )
import Cardano.MPFS.Cage.Ledger (Coin (..), ConwayEra, TxOut)
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (LegacyRejectRefundRequiresTopUp)
    )
import Cardano.MPFS.Client.Cage.Reactor (runCageEnvelope)
import Cardano.MPFS.Client.Cage.Reject
    ( RefundPlan (..)
    , preflightLegacyExactRefund
    )
import Cardano.Tx.Ledger (ConwayTx)
import Control.Monad (forM)
import Data.Aeson (Value, encode, object, (.=))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Functor (($>))
import Data.List (isInfixOf)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text.Encoding qualified as T
import Lens.Micro ((&), (.~))
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class (toBuiltinData)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck
    ( Property
    , conjoin
    , counterexample
    , ioProperty
    , property
    , withMaxSuccess
    , (===)
    )

spec :: Spec
spec = describe "runCageEnvelope byte identity" $ do
    it "matches the wasm reactor for fixed cage envelopes"
        $ property
        $ withMaxSuccess 1 byteIdentityProperty
    it
        "preflight-refuses min-UTxO top-up on legacy exact-refund validator"
        $ do
            let plan =
                    RefundPlan
                        { refundRawCoin = Coin 731_158
                        , refundMinCoin = Coin 849_070
                        , refundFinalCoin = Coin 849_070
                        }
            preflightLegacyExactRefund True [plan]
                `shouldBe` Left
                    ( LegacyRejectRefundRequiresTopUp
                        "legacy exact-refund validator cannot accept \
                        \min-UTxO refund top-up: raw refund 731158, \
                        \min refund 849070, final refund 849070"
                    )
            preflightLegacyExactRefund False [plan] `shouldBe` Right ()
    describe "decode op" $ do
        it "decodes a witnessed request tx_out, preserving old and new" $ do
            let out = BSC.unpack (runCageEnvelope decodeRequestEnvelope)
            out `shouldSatisfy` isInfixOf "decoded: "
            out `shouldSatisfy` isInfixOf "\"datum\":\"request\""
            out `shouldSatisfy` isInfixOf "\"operation\":\"update\""
            out `shouldSatisfy` isInfixOf "\"old\":\"6f6c64\""
            out `shouldSatisfy` isInfixOf "\"new\":\"6e6577\""
        it "decodes a witnessed state tx_out, recovering the token id" $ do
            let out = BSC.unpack (runCageEnvelope decodeStateEnvelope)
            out `shouldSatisfy` isInfixOf "decoded: "
            out `shouldSatisfy` isInfixOf "\"datum\":\"state\""
            out `shouldSatisfy` isInfixOf "\"token_id\":\"746f6b\""
        it "fails closed on a tx_out that carries no cage datum" $ do
            let out = BSC.unpack (runCageEnvelope decodeNoDatumEnvelope)
            out `shouldSatisfy` isInfixOf "decode_error: "
        it "rejects an unknown op" $ do
            let out = BSC.unpack (runCageEnvelope unknownOpEnvelope)
            out `shouldSatisfy` isInfixOf "unknown_op: "

byteIdentityProperty :: Property
byteIdentityProperty =
    ioProperty $ do
        wasm <- lookupEnv "MPFS_CAGE_REACTOR_WASM"
        case wasm of
            Nothing ->
                expectationFailure
                    "MPFS_CAGE_REACTOR_WASM is not set"
                    $> counterexample "missing wasm path" False
            Just wasmPath -> do
                results <- forM fixedEnvelopes $ \envelope -> do
                    let nativeOut = runCageEnvelope envelope
                    wasmOut <- runWasmReactor wasmPath envelope
                    pure
                        $ counterexample
                            ( "native: "
                                <> BSC.unpack nativeOut
                                <> "\nwasm: "
                                <> BSC.unpack wasmOut
                            )
                        $ wasmOut === nativeOut
                pure (conjoin results)

runWasmReactor :: FilePath -> ByteString -> IO ByteString
runWasmReactor wasmPath input = do
    runner <- lookupEnv "MPFS_CAGE_REACTOR_WASM_RUNNER"
    let runnerPath = fromMaybe "wasmtime" runner
    (code, stdout, stderr) <-
        readProcessWithExitCode
            runnerPath
            [wasmPath]
            (BSC.unpack input)
    case code of
        ExitSuccess -> pure (BSC.pack stdout)
        ExitFailure n ->
            expectationFailure
                ( "wasm reactor exited "
                    <> show n
                    <> ": "
                    <> stderr
                )
                $> ""

fixedEnvelopes :: [ByteString]
fixedEnvelopes =
    [ bootEnvelope
    , assembleEnvelope
    , integerToByteStringEnvelope
    , bitwiseConversionsEnvelope
    , endEnvelope
    , requestInsertEnvelope
    , requestUpdateEnvelope
    , requestDeleteEnvelope
    , updateEnvelope
    , retractEnvelope
    , rejectEnvelope
    , decodeRequestEnvelope
    , decodeStateEnvelope
    , decodeNoDatumEnvelope
    ]

bootEnvelope :: ByteString
bootEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("boot" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

assembleEnvelope :: ByteString
assembleEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("assemble" :: Text)
            , "unsigned_tx" .= hexText unsignedTx
            , "witness_set" .= hexText emptyWitnessSet
            ]

integerToByteStringEnvelope :: ByteString
integerToByteStringEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("self_test_integer_to_byte_string" :: Text)
            ]

bitwiseConversionsEnvelope :: ByteString
bitwiseConversionsEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("self_test_bitwise_conversions" :: Text)
            ]

endEnvelope :: ByteString
endEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("end" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

requestInsertEnvelope :: ByteString
requestInsertEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("request_insert" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

requestUpdateEnvelope :: ByteString
requestUpdateEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("request_update" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

requestDeleteEnvelope :: ByteString
requestDeleteEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("request_delete" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

updateEnvelope :: ByteString
updateEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("update" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

retractEnvelope :: ByteString
retractEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("retract" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

rejectEnvelope :: ByteString
rejectEnvelope =
    encodeStrict
        $ object
            [ "op" .= ("reject" :: Text)
            , "trusted_root" .= hexText (BSC.replicate 32 '\NUL')
            , "cage_config" .= cageConfigValue
            , "wallet_policy" .= object []
            , "facts" .= object []
            ]

-- | A @decode@ envelope over a witnessed request tx_out whose inline
-- datum is an @update@ request, exercising the non-lossy old/new fields.
decodeRequestEnvelope :: ByteString
decodeRequestEnvelope = decodeEnvelope (serializeTxOut requestTxOut)

-- | A @decode@ envelope over a witnessed state tx_out carrying the cage
-- token, exercising token-id recovery from the value.
decodeStateEnvelope :: ByteString
decodeStateEnvelope = decodeEnvelope (serializeTxOut stateTxOut)

-- | A @decode@ envelope over an output with no inline cage datum, which
-- must fail closed.
decodeNoDatumEnvelope :: ByteString
decodeNoDatumEnvelope = decodeEnvelope (serializeTxOut bareTxOut)

unknownOpEnvelope :: ByteString
unknownOpEnvelope =
    encodeStrict $ object ["op" .= ("not_a_real_op" :: Text)]

decodeEnvelope :: ByteString -> ByteString
decodeEnvelope txOutBytes =
    encodeStrict
        $ object
            [ "op" .= ("decode" :: Text)
            , "tx_out" .= hexText txOutBytes
            ]

serializeTxOut :: TxOut ConwayEra -> ByteString
serializeTxOut = serialize' (natVersion @11)

-- | A request output: ADA only plus an inline @update@ request datum.
requestTxOut :: TxOut ConwayEra
requestTxOut =
    mkBasicTxOut sampleAddr (MaryValue (Coin 2_000_000) mempty)
        & datumTxOutL .~ inlineDatum requestDatum
  where
    requestDatum =
        RequestDatum
            OnChainRequest
                { requestToken =
                    OnChainTokenId (BuiltinByteString "tok")
                , requestOwner =
                    BuiltinByteString (BS.replicate 28 7)
                , requestKey = "key"
                , requestValue = OpUpdate "old" "new"
                , requestFee = 1_500_000
                , requestSubmittedAt = 1_700_000_000_000
                }

-- | A state output carrying the cage token plus an inline state datum.
stateTxOut :: TxOut ConwayEra
stateTxOut =
    mkBasicTxOut sampleAddr stateValue
        & datumTxOutL .~ inlineDatum stateDatum
  where
    stateDatum =
        StateDatum
            OnChainTokenState
                { stateOwner = BuiltinByteString (BS.replicate 28 0)
                , stateRoot = OnChainRoot (BS.replicate 32 0)
                , stateMaxFee = 1_000_000
                , stateProcessTime = 60_000
                , stateRetractTime = 30_000
                , stateStakeScript = Nothing
                }
    stateValue =
        MaryValue
            (Coin 2_000_000)
            ( MultiAsset
                ( Map.singleton
                    samplePolicy
                    (Map.singleton (AssetName (SBS.toShort "tok")) 1)
                )
            )

-- | An output with no inline datum; the decode op must fail closed.
bareTxOut :: TxOut ConwayEra
bareTxOut =
    mkBasicTxOut sampleAddr (MaryValue (Coin 2_000_000) mempty)

inlineDatum :: CageDatum -> Datum ConwayEra
inlineDatum datum =
    let BuiltinData plutusData = toBuiltinData datum
    in  Datum (dataToBinaryData (Data plutusData))

sampleAddr :: Addr
sampleAddr =
    case hashFromBytes (BS.replicate 28 0) of
        Just h -> Addr Testnet (KeyHashObj (KeyHash h)) StakeRefNull
        Nothing -> error "sampleAddr: invalid key hash"

samplePolicy :: PolicyID
samplePolicy =
    case hashFromBytes (BS.replicate 28 1) of
        Just h -> PolicyID (ScriptHash h)
        Nothing -> error "samplePolicy: invalid script hash"

cageConfigValue :: Value
cageConfigValue =
    object
        [ "cage_script_bytes" .= ("00" :: Text)
        , "request_script_bytes" .= ("00" :: Text)
        , "default_process_time" .= (300000 :: Integer)
        , "default_retract_time" .= (600000 :: Integer)
        , "default_tip" .= (1000000 :: Integer)
        , "network" .= ("preprod" :: Text)
        ]

unsignedTx :: ByteString
unsignedTx =
    serialize'
        (natVersion @11)
        (mkBasicTx mkBasicTxBody :: ConwayTx)

emptyWitnessSet :: ByteString
emptyWitnessSet =
    serialize' (natVersion @11) (mkBasicTxWits @ConwayEra)

hexText :: ByteString -> Text
hexText = T.decodeUtf8 . B16.encode

encodeStrict :: Value -> ByteString
encodeStrict = BSL.toStrict . encode
