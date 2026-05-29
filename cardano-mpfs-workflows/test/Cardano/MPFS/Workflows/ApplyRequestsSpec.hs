{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Workflows.ApplyRequestsSpec
-- Description : Unit tests for the applyRequests (oracle update) workflow.
--
-- Drives 'applyRequests' through a stub 'HttpClient'. Unlike the
-- requester request workflows, the update verifier replays the cage
-- state UTxO inclusion proof, which cannot be satisfied without a
-- real CSMT proof. So the build / happy path is covered by the
-- integration tests gated on #288; here we assert request routing
-- and that the HTTP, decode, and verify failure stages map to the
-- matching 'WorkflowError' constructor (the trusted-root mismatch is
-- rejected before any proof replay).
module Cardano.MPFS.Workflows.ApplyRequestsSpec
    ( spec
    ) where

import Data.Aeson (eitherDecodeStrict', encode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Maybe (fromJust)
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
import Cardano.MPFS.API.Types (UpdateRequest (..))
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts (UpdateFacts (..))
import Cardano.MPFS.Client.Cage.Config (CageConfig (..))
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Workflows
    ( HttpClient (..)
    , HttpError (..)
    , UnsignedTx
    , WorkflowError (..)
    , WorkflowsConfig (..)
    , applyRequests
    )

spec :: Spec
spec = describe "applyRequests" $ do
    it "posts the update request to /facts/update" $ do
        ref <- newIORef Nothing
        _ <-
            applyRequests
                ( HttpClient $ \path body -> do
                    writeIORef ref (Just (path, body))
                    pure (Right "{}")
                )
                (config trustedRoot)
                req
        recorded <- readIORef ref
        case recorded of
            Nothing ->
                expectationFailure "client was never invoked"
            Just (path, body) -> do
                path `shouldBe` "/facts/update"
                (urAddr <$> decodeReq body) `shouldBe` Right addr

    it "maps a transport failure to WorkflowHttpError" $ do
        result <-
            applyRequests
                (constClient (Left (HttpStatus 502 "down")))
                (config trustedRoot)
                req
        result `shouldSatisfy` isHttpError

    it "maps a malformed response to WorkflowDecodeError" $ do
        result <-
            applyRequests
                (constClient (Right "{not json"))
                (config trustedRoot)
                req
        result `shouldSatisfy` isDecodeError

    it "maps a trusted-root mismatch to WorkflowVerifyError" $ do
        result <-
            applyRequests
                (constClient (Right (encodeFacts mismatchedFacts)))
                (config trustedRoot)
                req
        result `shouldSatisfy` isVerifyError
  where
    decodeReq :: ByteString -> Either String UpdateRequest
    decodeReq = eitherDecodeStrict'

req :: UpdateRequest
req = UpdateRequest{urToken = token, urAddr = addr}

-- | Update facts whose snapshot root does not match the trusted
-- root, so verification fails before any UTxO proof is replayed. The
-- UTxO fields are placeholders that are never inspected.
mismatchedFacts :: UpdateFacts
mismatchedFacts =
    UpdateFacts
        { ufSnapshot = mismatchedSnapshot
        , ufToken = token
        , ufStateUtxo = dummyUtxo
        , ufRequestUtxos = []
        , ufWalletUtxos = []
        , ufTrieRoot = Hex (BS.replicate 32 0)
        , ufTrieFacts = []
        , ufValidityUpperSlot = 0
        , ufProtocolParameters = malformedPParams
        }

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

trustedRootBytes :: ByteString
trustedRootBytes = BS.replicate 32 0x07

trustedRoot :: TrustedRoot
trustedRoot = TrustedRoot (Hex trustedRootBytes)

mismatchedSnapshot :: VerificationSnapshot
mismatchedSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex (BS.replicate 32 0x08)
        , vsChainPoint = ChainPointJSON{cpSlot = 0, cpBlockId = Hex BS.empty}
        }

malformedPParams :: UnverifiedPParams
malformedPParams =
    UnverifiedPParams{uppVerified = False, uppCbor = Hex "\x82\x01\x02"}

encodeFacts :: UpdateFacts -> ByteString
encodeFacts = BSL.toStrict . encode

constClient :: Either HttpError ByteString -> HttpClient
constClient response = HttpClient $ \_ _ -> pure response

config :: TrustedRoot -> WorkflowsConfig
config root =
    WorkflowsConfig
        { wcCage = dummyCage
        , wcPolicy = permissivePolicy
        , wcTrustedRoot = root
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
