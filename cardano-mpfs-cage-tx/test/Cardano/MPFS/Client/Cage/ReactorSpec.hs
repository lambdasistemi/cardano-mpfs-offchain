{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.ReactorSpec
-- Description : Native/WASM byte-identity tests for the cage reactor.
module Cardano.MPFS.Client.Cage.ReactorSpec
    ( spec
    ) where

import Cardano.Ledger.Api.Tx (mkBasicTx)
import Cardano.Ledger.Api.Tx.Body (mkBasicTxBody)
import Cardano.Ledger.Api.Tx.Wits (mkBasicTxWits)
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.MPFS.Cage.Ledger (ConwayEra)
import Cardano.MPFS.Client.Cage.Reactor (runCageEnvelope)
import Cardano.Tx.Ledger (ConwayTx)
import Control.Monad (forM)
import Data.Aeson (Value, encode, object, (.=))
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.Functor (($>))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text.Encoding qualified as T
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)
import Test.Hspec (Spec, describe, expectationFailure, it)
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
    , endEnvelope
    , requestInsertEnvelope
    , requestUpdateEnvelope
    , requestDeleteEnvelope
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
