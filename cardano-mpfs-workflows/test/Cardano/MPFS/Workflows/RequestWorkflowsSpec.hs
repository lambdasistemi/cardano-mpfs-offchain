{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Workflows.RequestWorkflowsSpec
-- Description : Unit tests for the requester request workflows.
--
-- Drives 'insertFact', 'updateFact', and 'deleteFact' through a stub
-- 'HttpClient'. Each workflow shares the same pipeline shape, so a
-- single 'Scenario' record parameterizes the five checks: request
-- routing, and that each failure stage (HTTP, decode, verify, build)
-- maps to the matching 'WorkflowError' constructor. The build-stage
-- check is reachable here because the request verifiers only replay
-- wallet UTxOs, so an empty wallet set lets verification pass and the
-- cage builder then fails on the (deliberately malformed) protocol
-- parameters.
module Cardano.MPFS.Workflows.RequestWorkflowsSpec
    ( spec
    ) where

import Data.Aeson (ToJSON, eitherDecodeStrict', encode)
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
    ( DeleteRequest (..)
    , InsertRequest (..)
    , UpdateValueRequest (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , RequestUpdateFacts (..)
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
    , deleteFact
    , insertFact
    , updateFact
    )
import Cardano.MPFS.Workflows.TestEvalContext (testEvalContext)

-- | A single request workflow under test: its endpoint path, the
-- invocation (transport-injected), the address expected in the posted
-- body, how to recover that address from the recorded body, and the
-- matching- / mismatched-root facts responses.
data Scenario = Scenario
    { scPath :: Text
    , scInvoke :: HttpClient -> IO (Either WorkflowError UnsignedTx)
    , scExpectedAddr :: Hex
    , scDecodeAddr :: ByteString -> Either String Hex
    , scMatchingFacts :: ByteString
    , scMismatchFacts :: ByteString
    }

spec :: Spec
spec = do
    describe "insertFact" $ opSpec insertScenario
    describe "updateFact" $ opSpec updateScenario
    describe "deleteFact" $ opSpec deleteScenario

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

    it "maps a build failure to WorkflowBuildError" $ do
        result <- scInvoke (constClient (Right scMatchingFacts))
        result `shouldSatisfy` isBuildError

-- | A stub transport that ignores its inputs and returns a fixed
-- response.
constClient :: Either HttpError ByteString -> HttpClient
constClient response = HttpClient $ \_ _ -> pure response

insertScenario :: Scenario
insertScenario =
    Scenario
        { scPath = "/facts/request/insert"
        , scInvoke = \http -> insertFact http (config trustedRoot) req
        , scExpectedAddr = addr
        , scDecodeAddr = fmap irAddr . eitherDecodeStrict'
        , scMatchingFacts = encodeFacts (insertFacts matchingSnapshot)
        , scMismatchFacts = encodeFacts (insertFacts mismatchedSnapshot)
        }
  where
    req =
        InsertRequest
            { irToken = token
            , irKey = key
            , irValue = val
            , irAddr = addr
            }
    insertFacts snapshot =
        RequestInsertFacts
            { rifSnapshot = snapshot
            , rifToken = token
            , rifKey = key
            , rifValue = val
            , rifAddress = addr
            , rifSubmittedAt = 0
            , rifWalletUtxos = []
            , rifProtocolParameters = malformedPParams
            }

updateScenario :: Scenario
updateScenario =
    Scenario
        { scPath = "/facts/request/update"
        , scInvoke = \http -> updateFact http (config trustedRoot) req
        , scExpectedAddr = addr
        , scDecodeAddr = fmap uvrAddr . eitherDecodeStrict'
        , scMatchingFacts = encodeFacts (updateFacts matchingSnapshot)
        , scMismatchFacts = encodeFacts (updateFacts mismatchedSnapshot)
        }
  where
    req =
        UpdateValueRequest
            { uvrToken = token
            , uvrKey = key
            , uvrOldValue = val
            , uvrNewValue = newVal
            , uvrAddr = addr
            }
    updateFacts snapshot =
        RequestUpdateFacts
            { rufSnapshot = snapshot
            , rufToken = token
            , rufKey = key
            , rufOldValue = val
            , rufNewValue = newVal
            , rufAddress = addr
            , rufSubmittedAt = 0
            , rufWalletUtxos = []
            , rufProtocolParameters = malformedPParams
            }

deleteScenario :: Scenario
deleteScenario =
    Scenario
        { scPath = "/facts/request/delete"
        , scInvoke = \http -> deleteFact http (config trustedRoot) req
        , scExpectedAddr = addr
        , scDecodeAddr = fmap drAddr . eitherDecodeStrict'
        , scMatchingFacts = encodeFacts (deleteFacts matchingSnapshot)
        , scMismatchFacts = encodeFacts (deleteFacts mismatchedSnapshot)
        }
  where
    req =
        DeleteRequest
            { drToken = token
            , drKey = key
            , drValue = val
            , drAddr = addr
            }
    deleteFacts snapshot =
        RequestDeleteFacts
            { rdfSnapshot = snapshot
            , rdfToken = token
            , rdfKey = key
            , rdfValue = val
            , rdfAddress = addr
            , rdfSubmittedAt = 0
            , rdfWalletUtxos = []
            , rdfProtocolParameters = malformedPParams
            }

-- shared fixtures ---------------------------------------------------

token :: TokenIdJSON
token = TokenIdJSON (BS.replicate 28 0x11)

key :: Hex
key = Hex (BS.replicate 4 0x01)

val :: Hex
val = Hex (BS.replicate 4 0x02)

newVal :: Hex
newVal = Hex (BS.replicate 4 0x03)

addr :: Hex
addr = Hex (BS.replicate 28 0x42)

trustedRootBytes :: ByteString
trustedRootBytes = BS.replicate 32 0x07

trustedRoot :: TrustedRoot
trustedRoot = TrustedRoot (Hex trustedRootBytes)

matchingSnapshot :: VerificationSnapshot
matchingSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex trustedRootBytes
        , vsChainPoint = chainPoint
        }

mismatchedSnapshot :: VerificationSnapshot
mismatchedSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex (BS.replicate 32 0x08)
        , vsChainPoint = chainPoint
        }

chainPoint :: ChainPointJSON
chainPoint = ChainPointJSON{cpSlot = 0, cpBlockId = Hex BS.empty}

-- | Undecodable protocol parameters: lets the cage builder fail at
-- the parameter-decoding stage once verification has passed.
malformedPParams :: UnverifiedPParams
malformedPParams =
    UnverifiedPParams{uppVerified = False, uppCbor = Hex "\x82\x01\x02"}

encodeFacts :: (ToJSON a) => a -> ByteString
encodeFacts = BSL.toStrict . encode

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

isBuildError :: Either WorkflowError UnsignedTx -> Bool
isBuildError (Left (WorkflowBuildError _)) = True
isBuildError _ = False
