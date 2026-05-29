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

import Data.Aeson (Value, encode, object, toJSON, (.=))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Text (Text)
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (UnsignedTxResponse (..))
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
