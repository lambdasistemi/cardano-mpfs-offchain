-- |
-- Module      : Cardano.MPFS.Client.Verify.ReactorSpec
-- Description : Contract tests for the cross-target reactor dispatch.
--
-- Locks the deterministic verdict contract that 'runEnvelope' must keep
-- byte-stable across native, WASM, and GHC-JS (constitution IX). The
-- cross-target QuickCheck suite (#258 S6) extends these into a
-- byte-identity property over generated inputs; here we pin the honest
-- path and the error taxonomy on the native backend.
module Cardano.MPFS.Client.Verify.ReactorSpec
    ( spec
    ) where

import Data.Aeson
    ( Value (..)
    , eitherDecodeStrict'
    , encode
    , object
    , toJSON
    , withObject
    , (.:)
    , (.=)
    )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Parser, parseEither)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short (ShortByteString, fromShort)
import Data.Text (Text)
import System.Environment (getEnv)
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (UnsignedTxResponse (..))
import Cardano.MPFS.Cage.Blueprint
    ( Blueprint
    , extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , UnverifiedPParams (..)
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootTrustedRoot
    , honestUnsignedBootResponse
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Reactor (runEnvelope)

spec :: Spec
spec = describe "runEnvelope" $ do
    it "verifies an honest boot envelope"
        $ runEnvelope
            (envelope "boot" honestBootTrustedRoot (toJSON honestBootFacts))
        `shouldBe` "verify_ok"

    it "rejects an honest boot envelope under a forged root"
        $ runEnvelope
            (envelope "boot" forgedRoot (toJSON honestBootFacts))
        `shouldSatisfy` hasPrefix "verify_error: "

    it "reports an unknown op verbatim"
        $ runEnvelope
            (envelope "frobnicate" honestBootTrustedRoot (object []))
        `shouldBe` "unknown_op: frobnicate"

    it "reports a malformed envelope"
        $ runEnvelope "not json"
        `shouldSatisfy` hasPrefix "bad_envelope: "

    it "reports a facts payload that fails to decode"
        $ runEnvelope
            (envelope "boot" honestBootTrustedRoot (object []))
        `shouldSatisfy` hasPrefix "bad_facts: "

    describe "read-side ops captured from umpfs.plutimus.com" $ do
        it "verifies a raw /tokens response" $ do
            cfg <- cageConfigValue
            payload <- fixtureValue "tokens.json"
            runEnvelope
                ( envelopeWithCage
                    "verify_tokens"
                    (trustedRootOf payload)
                    payload
                    cfg
                )
                `shouldBe` "verify_ok"

        it "verifies a raw /tokens/:id/requests response" $ do
            cfg <- cageConfigValue
            payload <- fixtureValue "token-requests.json"
            runEnvelope
                ( snapshotEnvelope
                    "verify_snapshot"
                    requestsTokenId
                    (trustedRootOf payload)
                    payload
                    cfg
                )
                `shouldBe` "verify_ok"

        it "verifies a raw /tokens/:id/facts/:key response" $ do
            payload <- fixtureValue "fact.json"
            runEnvelope
                ( factEnvelope
                    "verify_fact_inclusion"
                    factKey
                    (trustedRootOf payload)
                    payload
                )
                `shouldBe` "verify_ok"

        it "verifies a raw /tokens/:id/facts response" $ do
            payload <- fixtureValue "facts.json"
            runEnvelope
                ( envelope
                    "verify_facts"
                    (trustedRootOf payload)
                    payload
                )
                `shouldBe` "verify_ok"

        it "rejects a tampered /tokens response" $ do
            cfg <- cageConfigValue
            payload <- fixtureValue "tokens.json"
            runEnvelope
                ( envelopeWithCage
                    "verify_tokens"
                    (trustedRootOf payload)
                    (tamperSnapshotRoot payload)
                    cfg
                )
                `shouldSatisfy` hasPrefix "verify_error: "

        it "rejects a tampered /tokens/:id/requests response" $ do
            cfg <- cageConfigValue
            payload <- fixtureValue "token-requests.json"
            runEnvelope
                ( snapshotEnvelope
                    "verify_snapshot"
                    requestsTokenId
                    (trustedRootOf payload)
                    (tamperSnapshotRoot payload)
                    cfg
                )
                `shouldSatisfy` hasPrefix "verify_error: "

        it "rejects a tampered /tokens/:id/facts/:key response" $ do
            payload <- fixtureValue "fact.json"
            runEnvelope
                ( factEnvelope
                    "verify_fact_inclusion"
                    factKey
                    (trustedRootOf payload)
                    (tamperSnapshotRoot payload)
                )
                `shouldSatisfy` hasPrefix "verify_error: "

        it "rejects a tampered /tokens/:id/facts response" $ do
            payload <- fixtureValue "facts.json"
            runEnvelope
                ( envelope
                    "verify_facts"
                    (trustedRootOf payload)
                    (tamperSnapshotRoot payload)
                )
                `shouldSatisfy` hasPrefix "verify_error: "

-- | Build a request envelope as bytes, matching the reactor contract.
-- The trusted root is re-encoded through its 'Hex' 'ToJSON' so the
-- envelope carries the same hex string the reactor decodes.
envelope :: Text -> TrustedRoot -> Value -> ByteString
envelope op tr facts =
    BSL.toStrict
        $ encode
        $ object
            [ "op" .= op
            , "trusted_root" .= unTrustedRoot tr
            , "facts" .= facts
            ]

envelopeWithCage
    :: Text -> TrustedRoot -> Value -> Value -> ByteString
envelopeWithCage op tr facts cfg =
    BSL.toStrict
        $ encode
        $ object
            [ "op" .= op
            , "trusted_root" .= unTrustedRoot tr
            , "facts" .= facts
            , "cage_config" .= cfg
            ]

snapshotEnvelope
    :: Text -> Text -> TrustedRoot -> Value -> Value -> ByteString
snapshotEnvelope op token tr facts cfg =
    BSL.toStrict
        $ encode
        $ object
            [ "op" .= op
            , "token_id" .= token
            , "trusted_root" .= unTrustedRoot tr
            , "facts" .= facts
            , "cage_config" .= cfg
            ]

factEnvelope :: Text -> Text -> TrustedRoot -> Value -> ByteString
factEnvelope op key tr facts =
    BSL.toStrict
        $ encode
        $ object
            [ "op" .= op
            , "key" .= key
            , "trusted_root" .= unTrustedRoot tr
            , "facts" .= facts
            ]

hasPrefix :: ByteString -> ByteString -> Bool
hasPrefix = BS.isPrefixOf

-- | A length-valid (32-byte) but mismatching trusted root.
forgedRoot :: TrustedRoot
forgedRoot = TrustedRoot (Hex (BS.replicate 32 0))

honestBootFacts :: BootFacts
honestBootFacts =
    BootFacts
        { bfSnapshot = utrSnapshot honestUnsignedBootResponse
        , bfWalletUtxos = utrInputs honestUnsignedBootResponse
        , bfProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor = Hex "\x82\x01\x02"
                }
        }

fixtureValue :: FilePath -> IO Value
fixtureValue name = do
    bytes <-
        BS.readFile
            ("cardano-mpfs-client/test/fixtures/verify-reactor/" <> name)
    case eitherDecodeStrict' bytes of
        Left err -> fail ("fixture decode failed: " <> name <> ": " <> err)
        Right value -> pure value

trustedRootOf :: Value -> TrustedRoot
trustedRootOf value =
    case parseEither parseSnapshotRoot value of
        Left err -> error ("trustedRootOf: " <> err)
        Right root -> TrustedRoot root

parseSnapshotRoot :: Value -> Parser Hex
parseSnapshotRoot = withObject "captured read response" $ \o -> do
    snapshot <- o .: "snapshot"
    withObject "snapshot" (.: "utxo_root") snapshot

tamperSnapshotRoot :: Value -> Value
tamperSnapshotRoot (Object o) =
    case KM.lookup "snapshot" o of
        Just (Object snapshot) ->
            Object
                $ KM.insert
                    "snapshot"
                    ( Object
                        $ KM.insert
                            "utxo_root"
                            (toJSON (unTrustedRoot forgedRoot))
                            snapshot
                    )
                    o
        _ -> Object o
tamperSnapshotRoot value = value

cageConfigValue :: IO Value
cageConfigValue = do
    blueprintPath <- getEnv "MPFS_BLUEPRINT"
    eBlueprint <- loadBlueprint blueprintPath
    blueprint <- case eBlueprint of
        Left err -> fail ("loadBlueprint failed: " <> err)
        Right bp -> pure bp
    stateBytes <- requireCompiledCode "state." blueprint
    requestBytes <- requireCompiledCode "request." blueprint
    pure
        $ object
            [ "cage_script_bytes" .= scriptHex stateBytes
            , "request_script_bytes" .= scriptHex requestBytes
            , "default_process_time" .= (60_000 :: Integer)
            , "default_retract_time" .= (30_000 :: Integer)
            , "default_tip" .= (1_000_000 :: Integer)
            , "network" .= ("testnet" :: Text)
            ]

requireCompiledCode :: Text -> Blueprint -> IO ShortByteString
requireCompiledCode prefix blueprint =
    case extractCompiledCode prefix blueprint of
        Just bytes -> pure bytes
        Nothing ->
            fail ("compiled code not found in MPFS_BLUEPRINT: " <> show prefix)

scriptHex :: ShortByteString -> Hex
scriptHex = Hex . fromShort

requestsTokenId :: Text
requestsTokenId =
    "976821dbd0922f93cda689da92a6faf1894c8151bc86d6c8f725ec089aaacbc6"

factKey :: Text
factKey = "70616f6c696e6f"
