{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Workflows.RegisterTokenSpec
-- Description : Unit tests for the registerToken workflow.
--
-- Drives 'registerToken' through a stub 'HttpClient' that records
-- the @(path, body)@ it is handed and returns a canned response.
-- The tests assert request routing and that each failure stage
-- (HTTP, decode, verify, build) is wired into the right
-- 'WorkflowError' constructor.
module Cardano.MPFS.Workflows.RegisterTokenSpec
    ( spec
    ) where

import Data.Aeson (eitherDecodeStrict', encode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.IORef
    ( IORef
    , newIORef
    , readIORef
    , writeIORef
    )
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
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , UnverifiedPParams (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts (BootFacts (..))
import Cardano.MPFS.Client.Cage.Config (CageConfig (..))
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Workflows
    ( BootRequest (..)
    , HttpClient (..)
    , HttpError (..)
    , UnsignedTx
    , WorkflowError (..)
    , WorkflowsConfig (..)
    , registerToken
    )

spec :: Spec
spec = describe "registerToken" $ do
    it "posts the boot request to /facts/boot" $ do
        ref <- newIORef (Nothing :: Maybe (Text, ByteString))
        _ <-
            registerToken
                (recordingClient ref (Right "{}"))
                (config trustedRoot)
                bootRequest
        recorded <- readIORef ref
        case recorded of
            Nothing ->
                expectationFailure "client was never invoked"
            Just (path, body) -> do
                path `shouldBe` "/facts/boot"
                case eitherDecodeStrict' body of
                    Left err ->
                        expectationFailure
                            ("request body did not decode: " <> err)
                    Right (BootRequest addr) ->
                        addr `shouldBe` brAddr bootRequest

    it "maps a transport failure to WorkflowHttpError" $ do
        ref <- newIORef (Nothing :: Maybe (Text, ByteString))
        result <-
            registerToken
                (recordingClient ref (Left (HttpStatus 502 "down")))
                (config trustedRoot)
                bootRequest
        result `shouldSatisfy` isHttpError

    it "maps a malformed response to WorkflowDecodeError" $ do
        ref <- newIORef (Nothing :: Maybe (Text, ByteString))
        result <-
            registerToken
                (recordingClient ref (Right "{not json"))
                (config trustedRoot)
                bootRequest
        result `shouldSatisfy` isDecodeError

    it "maps a trusted-root mismatch to WorkflowVerifyError" $ do
        ref <- newIORef (Nothing :: Maybe (Text, ByteString))
        let response = encodeFacts (bootFacts mismatchedSnapshot)
        result <-
            registerToken
                (recordingClient ref (Right response))
                (config trustedRoot)
                bootRequest
        result `shouldSatisfy` isVerifyError

    it "maps a build failure to WorkflowBuildError" $ do
        ref <- newIORef (Nothing :: Maybe (Text, ByteString))
        let response = encodeFacts (bootFacts matchingSnapshot)
        result <-
            registerToken
                (recordingClient ref (Right response))
                (config trustedRoot)
                bootRequest
        result `shouldSatisfy` isBuildError

-- | A stub transport that records the @(path, body)@ it receives
-- and returns a fixed response.
recordingClient
    :: IORef (Maybe (Text, ByteString))
    -> Either HttpError ByteString
    -> HttpClient
recordingClient ref response =
    HttpClient $ \path body -> do
        writeIORef ref (Just (path, body))
        pure response

bootRequest :: BootRequest
bootRequest = BootRequest{brAddr = Hex (BS.replicate 28 0x42)}

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
chainPoint =
    ChainPointJSON
        { cpSlot = 0
        , cpBlockId = Hex BS.empty
        }

-- | Boot facts with no wallet UTxOs and undecodable protocol
-- parameters: verification passes when the snapshot root matches,
-- and the cage builder then fails on the malformed parameters.
bootFacts :: VerificationSnapshot -> BootFacts
bootFacts snapshot =
    BootFacts
        { bfSnapshot = snapshot
        , bfWalletUtxos = []
        , bfProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor = Hex "\x82\x01\x02"
                }
        }

encodeFacts :: BootFacts -> ByteString
encodeFacts = BSL.toStrict . encode

-- | A throwaway cage configuration. Its script bytes are empty and
-- its hash is zero: the build-stage tests reach the builder only far
-- enough to fail on the malformed protocol parameters, never far
-- enough to use the script.
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

isBuildError :: Either WorkflowError UnsignedTx -> Bool
isBuildError (Left (WorkflowBuildError _)) = True
isBuildError _ = False
