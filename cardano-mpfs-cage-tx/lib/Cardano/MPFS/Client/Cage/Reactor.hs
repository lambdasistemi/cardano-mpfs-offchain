{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Reactor
-- Description : Target-agnostic cage transaction reactor.
--
-- Pure stdin-to-stdout core for the browser-facing cage helper. The
-- function decodes a JSON envelope, verifies facts before building a
-- transaction, and renders a stable one-line verdict with 'show' for
-- error payloads so native and WASM output bytes stay identical.
module Cardano.MPFS.Client.Cage.Reactor
    ( runCageEnvelope
    ) where

import Cardano.Ledger.Alonzo.Scripts
    ( fromPlutusScript
    , mkPlutusScript
    )
import Cardano.Ledger.Api.Tx (addrTxWitsL, witsTxL)
import Cardano.Ledger.BaseTypes
    ( Network (..)
    )
import Cardano.Ledger.Binary
    ( Annotator
    , Decoder
    , decCBOR
    , decodeFullAnnotator
    , natVersion
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Core
    ( TxWits
    , hashScript
    )
import Cardano.Ledger.Hashes (ScriptHash)
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.Plutus.Language
    ( Language (PlutusV3)
    , Plutus (..)
    , PlutusBinary (..)
    )
import Cardano.MPFS.Cage.Ledger (ConwayEra)
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.BuildError (BuildError)
import Cardano.MPFS.Client.Cage.Config (CageConfig (..))
import Cardano.MPFS.Client.Cage.End (endCageTx)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Cage.Request
    ( requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Serialize (serializeCageTx)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify
    ( VerifiedBootFacts
    , VerifiedEndFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestUpdateFacts
    , VerifyError
    , verifyBootFacts
    , verifyEndFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    )
import Cardano.Slotting.Slot (SlotNo (..))
import Cardano.Tx.Ledger (ConwayTx)
import Data.Aeson
    ( FromJSON
    , Object
    , Result (..)
    , Value
    , eitherDecodeStrict
    , fromJSON
    , withObject
    , (.!=)
    , (.:)
    , (.:?)
    )
import Data.Aeson.Types
    ( Parser
    , parseEither
    )
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short
    ( ShortByteString
    , toShort
    )
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Word (Word64)
import Lens.Micro
    ( (%~)
    , (&)
    , (^.)
    )

-- The facts types are decoded through 'fromJSON' in the op branch; we
-- only need their instances in scope.
import Cardano.MPFS.API.Types.Facts ()

-- | Decode an envelope and render the cage reactor verdict as bytes.
-- Every recognized failure mode maps to a tagged line.
runCageEnvelope :: ByteString -> ByteString
runCageEnvelope input =
    T.encodeUtf8 $ case eitherDecodeStrict input of
        Left err -> "bad_envelope: " <> T.pack err
        Right value -> case parseEither dispatch value of
            Left err -> "bad_envelope: " <> T.pack err
            Right verdict -> verdict

dispatch :: Value -> Parser Text
dispatch = withObject "Envelope" $ \o -> do
    op <- o .: "op"
    case op :: Text of
        "boot" -> dispatchBoot o
        "assemble" -> do
            unsignedTx <- o .: "unsigned_tx"
            witnessSet <- o .: "witness_set"
            pure (assembleTx unsignedTx witnessSet)
        "request_insert" -> dispatchRequestInsert o
        "request_update" -> dispatchRequestUpdate o
        "request_delete" -> pure ("unknown_op: " <> op)
        "retract" -> pure ("unknown_op: " <> op)
        "reject" -> pure ("unknown_op: " <> op)
        "end" -> dispatchEnd o
        _ -> pure ("unknown_op: " <> op)

dispatchBoot :: Object -> Parser Text
dispatchBoot o = do
    tr <- TrustedRoot <$> o .: "trusted_root"
    facts <- o .: "facts"
    case runVerified (verifyBootFacts tr) facts of
        Left verdict -> pure verdict
        Right verified -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            policyValue <- o .:? "wallet_policy"
            policy <- parseWalletPolicyMaybe policyValue
            pure (buildBoot cfg policy verified)

dispatchEnd :: Object -> Parser Text
dispatchEnd o = do
    tr <- TrustedRoot <$> o .: "trusted_root"
    cfg <- o .: "cage_config" >>= parseCageConfig
    facts <- o .: "facts"
    case runVerified (verifyEndFacts cfg tr) facts of
        Left verdict -> pure verdict
        Right verified -> do
            policyValue <- o .:? "wallet_policy"
            policy <- parseWalletPolicyMaybe policyValue
            pure (buildEnd cfg policy verified)

dispatchRequestInsert :: Object -> Parser Text
dispatchRequestInsert o = do
    tr <- TrustedRoot <$> o .: "trusted_root"
    facts <- o .: "facts"
    case runVerified (verifyRequestInsertFacts tr) facts of
        Left verdict -> pure verdict
        Right verified -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            policyValue <- o .:? "wallet_policy"
            policy <- parseWalletPolicyMaybe policyValue
            pure (buildRequestInsert cfg policy verified)

dispatchRequestUpdate :: Object -> Parser Text
dispatchRequestUpdate o = do
    tr <- TrustedRoot <$> o .: "trusted_root"
    facts <- o .: "facts"
    case runVerified (verifyRequestUpdateFacts tr) facts of
        Left verdict -> pure verdict
        Right verified -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            policyValue <- o .:? "wallet_policy"
            policy <- parseWalletPolicyMaybe policyValue
            pure (buildRequestUpdate cfg policy verified)

runVerified
    :: (FromJSON facts)
    => (facts -> Either VerifyError verified)
    -> Value
    -> Either Text verified
runVerified verify facts =
    case fromJSON facts of
        Error err -> Left ("bad_facts: " <> T.pack err)
        Success decoded -> case verify decoded of
            Left err -> Left ("verify_error: " <> T.pack (show err))
            Right verified -> Right verified

buildBoot
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Text
buildBoot cfg policy verified =
    case bootCageTx cfg policy verified of
        Left err -> renderBuildError err
        Right tx -> "cage_tx: " <> renderHex (serializeCageTx tx)

buildEnd
    :: CageConfig
    -> WalletPolicy
    -> VerifiedEndFacts
    -> Text
buildEnd cfg policy verified =
    case endCageTx cfg policy verified of
        Left err -> renderBuildError err
        Right tx -> "cage_tx: " <> renderHex (serializeCageTx tx)

buildRequestInsert
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRequestInsertFacts
    -> Text
buildRequestInsert cfg policy verified =
    case requestInsertCageTx cfg policy verified of
        Left err -> renderBuildError err
        Right tx -> "cage_tx: " <> renderHex (serializeCageTx tx)

buildRequestUpdate
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRequestUpdateFacts
    -> Text
buildRequestUpdate cfg policy verified =
    case requestUpdateCageTx cfg policy verified of
        Left err -> renderBuildError err
        Right tx -> "cage_tx: " <> renderHex (serializeCageTx tx)

assembleTx :: Text -> Text -> Text
assembleTx unsignedTxHex witnessSetHex =
    case assembleTxBytes unsignedTxHex witnessSetHex of
        Left err -> "assemble_error: " <> err
        Right signedTx -> "signed_tx: " <> renderHex signedTx

assembleTxBytes :: Text -> Text -> Either Text ByteString
assembleTxBytes unsignedTxHex witnessSetHex = do
    unsignedTxBytes <- decodeHex "unsigned_tx" unsignedTxHex
    witnessSetBytes <- decodeHex "witness_set" witnessSetHex
    tx <- decodeTx unsignedTxBytes
    witnessSet <- decodeWitnessSet witnessSetBytes
    pure
        $ serializeCageTx
        $ tx
        & witsTxL . addrTxWitsL
            %~ Set.union (witnessSet ^. addrTxWitsL)

decodeTx :: ByteString -> Either Text ConwayTx
decodeTx =
    decodeCbor
        "Conway transaction"
        (decCBOR :: forall s. Decoder s (Annotator ConwayTx))

decodeWitnessSet :: ByteString -> Either Text (TxWits ConwayEra)
decodeWitnessSet =
    decodeCbor
        "Conway witness set"
        (decCBOR :: forall s. Decoder s (Annotator (TxWits ConwayEra)))

decodeCbor
    :: Text
    -> (forall s. Decoder s (Annotator a))
    -> ByteString
    -> Either Text a
decodeCbor label decoder bytes =
    case decodeFullAnnotator
        (natVersion @11)
        label
        decoder
        (BSL.fromStrict bytes) of
        Left err -> Left (T.pack (show err))
        Right decoded -> Right decoded

decodeHex :: Text -> Text -> Either Text ByteString
decodeHex field hexText =
    case B16.decode (T.encodeUtf8 hexText) of
        Left err -> Left (field <> " hex: " <> T.pack err)
        Right bytes -> Right bytes

renderBuildError :: BuildError -> Text
renderBuildError err =
    "build_error: " <> T.pack (show err)

renderHex :: ByteString -> Text
renderHex =
    T.decodeUtf8 . B16.encode

parseCageConfig :: Value -> Parser CageConfig
parseCageConfig = withObject "CageConfig" $ \o -> do
    cageBytes <- o .: "cage_script_bytes" >>= hexShort
    requestBytes <- o .: "request_script_bytes" >>= hexShort
    processTime <- o .: "default_process_time"
    retractTime <- o .: "default_retract_time"
    tip <- o .: "default_tip"
    net <- o .: "network" >>= parseNetwork
    scriptHash <- computeScriptHashParser cageBytes
    pure
        CageConfig
            { cageScriptBytes = cageBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = scriptHash
            , defaultProcessTime = processTime
            , defaultRetractTime = retractTime
            , defaultTip = Coin tip
            , network = net
            }

hexShort :: Text -> Parser ShortByteString
hexShort value =
    case B16.decode (T.encodeUtf8 value) of
        Left err -> fail ("invalid hex: " <> err)
        Right bytes -> pure (toShort bytes)

computeScriptHashParser
    :: ShortByteString
    -> Parser ScriptHash
computeScriptHashParser scriptBytes =
    let plutus =
            Plutus @PlutusV3
                $ PlutusBinary scriptBytes
    in  case mkPlutusScript @ConwayEra plutus of
            Nothing -> fail "invalid PlutusV3 script"
            Just script ->
                pure
                    $ hashScript @ConwayEra
                    $ fromPlutusScript script

parseNetwork :: Text -> Parser Network
parseNetwork "mainnet" = pure Mainnet
parseNetwork "testnet" = pure Testnet
parseNetwork "preprod" = pure Testnet
parseNetwork "preview" = pure Testnet
parseNetwork other = fail ("unknown network: " <> T.unpack other)

parseWalletPolicyMaybe :: Maybe Value -> Parser WalletPolicy
parseWalletPolicyMaybe Nothing = pure defaultWalletPolicy
parseWalletPolicyMaybe (Just value) = parseWalletPolicy value

parseWalletPolicy :: Value -> Parser WalletPolicy
parseWalletPolicy = withObject "WalletPolicy" $ \o -> do
    maxFee <-
        Coin
            <$> ( o
                    .:? "max_fee"
                    .!= coinValue (wpMaxFee defaultWalletPolicy)
                )
    maxMinUtxo <-
        Coin
            <$> ( o
                    .:? "max_min_utxo_coin_per_byte"
                    .!= coinValue
                        (wpMaxMinUtxoCoinPerByte defaultWalletPolicy)
                )
    maxValidityWindow <-
        SlotNo
            <$> ( o
                    .:? "max_validity_window"
                    .!= slotValue (wpMaxValidityWindow defaultWalletPolicy)
                )
    _maxExUnitPrices <- o .:? "max_ex_unit_prices" :: Parser (Maybe Value)
    pure
        WalletPolicy
            { wpMaxFee = maxFee
            , wpMaxExUnitPrices = wpMaxExUnitPrices defaultWalletPolicy
            , wpMaxMinUtxoCoinPerByte = maxMinUtxo
            , wpMaxValidityWindow = maxValidityWindow
            }

defaultWalletPolicy :: WalletPolicy
defaultWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

coinValue :: Coin -> Integer
coinValue (Coin value) = value

slotValue :: SlotNo -> Word64
slotValue (SlotNo value) = value
