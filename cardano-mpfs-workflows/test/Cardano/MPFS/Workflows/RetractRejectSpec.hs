{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Workflows.RetractRejectSpec
-- Description : Unit tests for the retractRequest and rejectExpired workflows.
--
-- Drives 'retractRequest' and 'rejectExpired' through a stub
-- 'HttpClient'. Both verifiers replay UTxO inclusion proofs that
-- cannot be satisfied without real CSMT proofs, so the build / happy
-- path is covered by the integration tests gated on #288; here we
-- assert routing and that the HTTP, decode, and verify failure stages
-- map to the matching 'WorkflowError' constructor (a trusted-root
-- mismatch is rejected before any proof replay).
module Cardano.MPFS.Workflows.RetractRejectSpec
    ( spec
    ) where

import Data.Aeson (eitherDecodeStrict', encode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Maybe (fromJust)
import Data.Text (Text)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.Crypto.Hash (hashFromBytes)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Slotting.Slot (SlotNo (..))

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( RejectRequest (..)
    , RetractRequest (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RejectFacts (..)
    , RetractFacts (..)
    )
import Cardano.MPFS.Client.Cage.Config (CageConfig (..))
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Workflows
    ( HttpClient (..)
    , HttpError (..)
    , UnsignedTx
    , WorkflowError (..)
    , WorkflowsConfig (..)
    , rejectExpired
    , retractRequest
    )
import Cardano.MPFS.Workflows.TestEvalContext (testEvalContext)

-- | A workflow under test: endpoint path, invocation, the address
-- expected in the posted body, how to recover it from the body, and a
-- mismatched-root facts response.
data Scenario = Scenario
    { scPath :: Text
    , scInvoke :: HttpClient -> IO (Either WorkflowError UnsignedTx)
    , scExpectedAddr :: Hex
    , scDecodeAddr :: ByteString -> Either String Hex
    , scMismatchFacts :: ByteString
    }

spec :: Spec
spec = do
    describe "retractRequest" $ opSpec retractScenario
    describe "rejectExpired" $ opSpec rejectScenario

opSpec :: Scenario -> Spec
opSpec Scenario{..} = do
    it "posts the request to its facts endpoint" $ do
        ref <- newIORef Nothing
        _ <-
            scInvoke
                ( HttpClient $ \path body -> do
                    writeIORef ref (Just (path, body))
                    pure (Right "{}")
                )
        recorded <- readIORef ref
        case recorded of
            Nothing ->
                expectationFailure "client was never invoked"
            Just (path, body) -> do
                path `shouldBe` scPath
                scDecodeAddr body `shouldBe` Right scExpectedAddr

    it "maps a transport failure to WorkflowHttpError" $ do
        result <- scInvoke (constClient (Left (HttpStatus 502 "down")))
        result `shouldSatisfy` isHttpError

    it "maps a malformed response to WorkflowDecodeError" $ do
        result <- scInvoke (constClient (Right "{not json"))
        result `shouldSatisfy` isDecodeError

    it "maps a trusted-root mismatch to WorkflowVerifyError" $ do
        result <- scInvoke (constClient (Right scMismatchFacts))
        result `shouldSatisfy` isVerifyError

constClient :: Either HttpError ByteString -> HttpClient
constClient response = HttpClient $ \_ _ -> pure response

retractScenario :: Scenario
retractScenario =
    Scenario
        { scPath = "/facts/retract"
        , scInvoke = \http -> retractRequest http (config trustedRoot) req
        , scExpectedAddr = addr
        , scDecodeAddr = fmap rrAddr . eitherDecodeStrict'
        , scMismatchFacts = BSL.toStrict (encode facts)
        }
  where
    req = RetractRequest{rrUtxo = "ab#0", rrAddr = addr}
    facts =
        RetractFacts
            { rfSnapshot = mismatchedSnapshot
            , rfToken = token
            , rfRequestUtxo = dummyUtxo
            , rfStateUtxo = dummyUtxo
            , rfWalletUtxos = []
            , rfValidityStartSlot = 0
            , rfValidityEndSlot = 0
            , rfProtocolParameters = malformedPParams
            }

rejectScenario :: Scenario
rejectScenario =
    Scenario
        { scPath = "/facts/reject"
        , scInvoke = \http -> rejectExpired http (config trustedRoot) req
        , scExpectedAddr = addr
        , scDecodeAddr = fmap rejAddr . eitherDecodeStrict'
        , scMismatchFacts = BSL.toStrict (encode facts)
        }
  where
    req =
        RejectRequest
            { rejToken = token
            , rejAddr = addr
            , rejRequests = []
            }
    facts =
        RejectFacts
            { rfSnapshot = mismatchedSnapshot
            , rfToken = token
            , rfStateUtxo = dummyUtxo
            , rfRequestUtxos = []
            , rfWalletUtxos = []
            , rfValidityLowerSlot = 0
            , rfValidityUpperSlot = 0
            , rfProtocolParameters = malformedPParams
            }

-- shared fixtures ---------------------------------------------------

token :: TokenIdJSON
token = TokenIdJSON (BS.replicate 28 0x11)

addr :: Hex
addr = Hex (BS.replicate 28 0x42)

dummyUtxo :: UtxoEntry
dummyUtxo =
    UtxoEntry
        { ueRef = UtxoRef{urTxId = Hex (BS.replicate 32 0), urTxIx = 0}
        , ueTxOutCbor = Hex BS.empty
        , ueInclusionProof = Hex BS.empty
        }

trustedRoot :: TrustedRoot
trustedRoot = TrustedRoot (Hex (BS.replicate 32 0x07))

mismatchedSnapshot :: VerificationSnapshot
mismatchedSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex (BS.replicate 32 0x08)
        , vsChainPoint = ChainPointJSON{cpSlot = 0, cpBlockId = Hex BS.empty}
        }

malformedPParams :: UnverifiedPParams
malformedPParams =
    UnverifiedPParams{uppVerified = False, uppCbor = Hex "\x82\x01\x02"}

config :: TrustedRoot -> WorkflowsConfig
config root =
    WorkflowsConfig
        { wcCage = dummyCage
        , wcPolicy = permissivePolicy
        , wcTrustedRoot = root
        , wcEvalContext = testEvalContext
        }

dummyCage :: CageConfig
dummyCage =
    CageConfig
        { cageScriptBytes = SBS.empty
        , requestScriptBytes = SBS.empty
        , cfgScriptHash =
            ScriptHash (fromJust (hashFromBytes (BS.replicate 28 0)))
        , defaultProcessTime = 300_000
        , defaultRetractTime = 600_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }

permissivePolicy :: WalletPolicy
permissivePolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

isHttpError :: Either WorkflowError UnsignedTx -> Bool
isHttpError (Left (WorkflowHttpError _)) = True
isHttpError _ = False

isDecodeError :: Either WorkflowError UnsignedTx -> Bool
isDecodeError (Left (WorkflowDecodeError _)) = True
isDecodeError _ = False

isVerifyError :: Either WorkflowError UnsignedTx -> Bool
isVerifyError (Left (WorkflowVerifyError _)) = True
isVerifyError _ = False
