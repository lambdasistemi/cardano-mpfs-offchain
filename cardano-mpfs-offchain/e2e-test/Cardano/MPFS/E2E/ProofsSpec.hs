{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.ProofsSpec
-- Description : Verify proof-bearing read endpoints end-to-end
-- License     : Apache-2.0
--
-- Boot a token, insert a fact, process it, then call the
-- four proof-bearing read endpoints against a real devnet.
-- For each response we:
--
--   * parse the embedded verification snapshot;
--   * run 'verifyVerificationSnapshot' from
--     @cardano-mpfs-client@ (structural check on the
--     @utxo_root@ length and @chainpoint.block_id@);
--   * assert the fields that the bundled UTxO-CSMT and
--     MPF proofs live in are non-empty hex.
--
-- This gives us a minimum-viable end-to-end contract
-- check: the server emits proof-bearing envelopes that
-- the released verification client can parse and
-- structurally validate.
module Cardano.MPFS.E2E.ProofsSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Data.Aeson
    ( FromJSON (..)
    , Value
    , decode
    , withObject
    , (.:)
    )
import Data.Aeson.Key (Key)
import Data.Aeson.Key qualified as Key
import Data.Aeson.Types (parseEither)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as T
import Lens.Micro ((^.))
import Network.HTTP.Types (status200)
import Network.Wai (Application)
import Network.Wai.Test
    ( SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    )
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    , shouldSatisfy
    )

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Api.Tx (Tx, bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.TxIn (TxId (..))

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( applyVersion
    , extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , TokenId (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.TxBuilder (TxBuilder (..))
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , computeScriptHash
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )

import Cardano.MPFS.Client
    ( Hex (..)
    , VerificationSnapshot
    , verifyVerificationSnapshot
    )

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Proof-bearing reads E2E" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (no MPFS_BLUEPRINT)"
                $ pure @IO ()
        Just path -> do
            ebp <- runIO $ loadBlueprint path
            case ebp of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right bp ->
                    case extractCompiledCode
                        "cage."
                        bp of
                        Nothing ->
                            it "no compiled code"
                                $ expectationFailure
                                    "cage script \
                                    \not found"
                        Just sb ->
                            proofsSpec
                                $ applyVersion 1 sb

-- -------------------------------------------------
-- Scenario
-- -------------------------------------------------

-- | Await timeout in seconds. Devnet slots are 100ms.
awaitTimeout :: Int
awaitTimeout = 60

-- | Fact key and value used throughout the scenario.
factKey :: ByteString
factKey = "hello"

factValue :: ByteString
factValue = "world"

proofsSpec :: SBS.ShortByteString -> Spec
proofsSpec scriptBytes =
    it "all four reads carry a verifiable snapshot"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx
                submit =
                    signSubmitAwait
                        awaitTimeout
                        app
                        ctx

            -- Boot → insert → update to land the fact
            bootTx <-
                submit $ bootToken tb genesisAddr
            let tid = extractTokenId cfg bootTx
                tidHex = tokenIdHex tid
                keyHex = B16.encode factKey

            _ <-
                submit
                    $ requestInsert
                        tb
                        tid
                        factKey
                        factValue
                        genesisAddr
            _ <-
                submit
                    $ updateToken
                        tb
                        tid
                        genesisAddr

            -- Add a second pending request so the
            -- /requests endpoint returns a non-empty
            -- witnessed list.
            _ <-
                submit
                    $ requestInsert
                        tb
                        tid
                        "bye"
                        "moon"
                        genesisAddr

            -- Pull every proof-bearing read.
            tokenObj <- getJSON app ("/tokens/" <> tidHex)
            factObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/facts/"
                        <> keyHex
            proofObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/proofs/"
                        <> keyHex
            requestsObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/requests"

            -- Every response carries the same snapshot
            -- shape and that snapshot passes the client
            -- structural verifier.
            tokenSnap <- extractSnapshot tokenObj
            factSnap <- extractSnapshot factObj
            proofSnap <- extractSnapshot proofObj
            reqsSnap <- extractSnapshot requestsObj

            mapM_
                assertSnapshotValid
                [tokenSnap, factSnap, proofSnap, reqsSnap]

            -- Each response ships the proofs the client
            -- will later consume — assert they are
            -- present as non-empty hex.
            assertWitnessedUtxo
                =<< lookupObj tokenObj "state"
            assertFactEnvelope factObj
            assertProofEnvelope proofObj

            assertRequestsEnvelope requestsObj

-- -------------------------------------------------
-- Assertions on response shape
-- -------------------------------------------------

-- | Verify a 'VerificationSnapshot' with the offline
-- verifier shipped by @cardano-mpfs-client@.
assertSnapshotValid :: VerificationSnapshot -> IO ()
assertSnapshotValid snap =
    case verifyVerificationSnapshot snap of
        Right () -> pure ()
        Left err ->
            expectationFailure
                $ "snapshot verification failed: "
                    <> show err

-- | Pull the @snapshot@ field out of a response object.
extractSnapshot :: Value -> IO VerificationSnapshot
extractSnapshot v =
    case parseEither snapshotField v of
        Right s -> pure s
        Left err ->
            do
                expectationFailure
                    $ "snapshot parse: " <> err
                error "unreachable"
  where
    snapshotField =
        withObject "response" $ \o ->
            o .: "snapshot"

-- | Every witnessed UTxO envelope must ship
-- @tx_in@, @tx_out@ and @utxo_proof@ as non-empty hex.
assertWitnessedUtxo :: Value -> IO ()
assertWitnessedUtxo v = do
    utxo <- lookupObj v "utxo"
    _ <- lookupObj utxo "tx_in"
    txOut <- lookupHex utxo "tx_out"
    txOut `shouldSatisfy` (not . T.null)
    proof <- lookupHex utxo "utxo_proof"
    proof `shouldSatisfy` (not . T.null)

-- | The @/facts/:key@ envelope carries the stored
-- MPF value, the state witness, and the MPF proof.
-- MPFS stores values as 32-byte content hashes, so we
-- only assert the @value@ field is non-empty hex.
assertFactEnvelope :: Value -> IO ()
assertFactEnvelope v = do
    val <- lookupHex v "value"
    val `shouldSatisfy` (not . T.null)
    fact <- lookupObj v "fact"
    state <- lookupObj fact "state"
    assertWitnessedUtxo state
    mpfProof <- lookupHex fact "mpf_proof"
    mpfProof `shouldSatisfy` (not . T.null)

-- | The @/proofs/:key@ envelope carries the state
-- witness and the MPF proof (no value).
assertProofEnvelope :: Value -> IO ()
assertProofEnvelope v = do
    fact <- lookupObj v "fact"
    state <- lookupObj fact "state"
    assertWitnessedUtxo state
    mpfProof <- lookupHex fact "mpf_proof"
    mpfProof `shouldSatisfy` (not . T.null)

-- | The @/requests@ envelope must contain at least one
-- witnessed request with both its UTxO witness and a
-- decoded request payload.
assertRequestsEnvelope :: Value -> IO ()
assertRequestsEnvelope v = do
    requests <- lookupArr v "requests"
    case requests of
        [] ->
            expectationFailure
                "expected at least one witnessed \
                \request"
        (wreq : _) -> do
            assertWitnessedUtxo wreq
            _ <- lookupObj wreq "request"
            pure ()

-- -------------------------------------------------
-- JSON plumbing
-- -------------------------------------------------

lookupObj :: Value -> Key -> IO Value
lookupObj = parseField

lookupHex :: Value -> Key -> IO Text
lookupHex v k = do
    Hex t <- parseField v k
    pure t

lookupArr :: Value -> Key -> IO [Value]
lookupArr = parseField

parseField :: (FromJSON a) => Value -> Key -> IO a
parseField v k =
    case parseEither
        (withObject "obj" (.: k))
        v of
        Right x -> pure x
        Left err ->
            do
                expectationFailure
                    $ "field "
                        <> Key.toString k
                        <> ": "
                        <> err
                error "unreachable"

-- -------------------------------------------------
-- DSL (mirrors HTTPLifecycleSpec)
-- -------------------------------------------------

signSubmitAwait
    :: Int
    -> Application
    -> Context IO
    -> IO (Tx ConwayEra)
    -> IO (Tx ConwayEra)
signSubmitAwait timeout app ctx buildTx = do
    unsigned <- buildTx
    let signed =
            addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx timeout app (txIdTx signed)
    pure signed

awaitTx :: Int -> Application -> TxId -> IO ()
awaitTx timeout app tid = do
    resp <- getRaw app path
    simpleStatus resp `shouldBe` status200
  where
    path =
        "/tx/"
            <> txIdHex tid
            <> "?timeout="
            <> bshow timeout
    bshow =
        BS.pack
            . map (fromIntegral . fromEnum)
            . show

getJSON :: Application -> ByteString -> IO Value
getJSON app path = do
    resp <- getRaw app path
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just v -> pure v
        Nothing ->
            do
                expectationFailure
                    $ "non-JSON response: "
                        <> show (simpleBody resp)
                error "unreachable"

getRaw :: Application -> ByteString -> IO SResponse
getRaw app path =
    runSession
        (request (setPath defaultRequest path))
        app

assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

-- | Hex-encode a 'TokenId' for URL paths.
tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

-- | Raw 32-byte hash as hex for URL paths.
txIdHex :: TxId -> ByteString
txIdHex (TxId sh) =
    B16.encode
        $ Crypto.hashToBytes
        $ extractHash sh

-- | Extract the sole minted 'TokenId'.
extractTokenId
    :: CageConfig -> Tx ConwayEra -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "extractTokenId: \
                    \unexpected mint"

-- -------------------------------------------------
-- Bracket
-- -------------------------------------------------

withE2E
    :: SBS.ShortByteString
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scriptBytes action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-proofs-e2e"
            $ \tmpDir -> do
                let cfg = cageCfg scriptBytes
                    appCfg =
                        AppConfig
                            { epochSlots =
                                EpochSlots 4320
                            , shelleyGenesisPath =
                                gDir
                                    </> "shelley-genesis.json"
                            , socketPath = sock
                            , dbPath = tmpDir </> "db"
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , byronGenesisPath =
                                Nothing
                            , followerEnabled = True
                            , appTracer = nullTracer
                            }
                withApplication appCfg $ \ctx -> do
                    _ <-
                        queryProtocolParams
                            (provider ctx)
                    threadDelay 10_000_000
                    action cfg ctx

cageCfg :: SBS.ShortByteString -> CageConfig
cageCfg scriptBytes =
    CageConfig
        { cageScriptBytes = scriptBytes
        , cfgScriptHash =
            computeScriptHash scriptBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
