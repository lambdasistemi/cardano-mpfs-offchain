{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RequestsSpec
-- Description : Tests for GET /tokens/:id/requests
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RequestsSpec (spec) where

import Control.Monad (forM_)
import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Data.Vector qualified as V
import Network.HTTP.Types
    ( status200
    , status404
    , status503
    )
import Network.Wai.Test
    ( SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    )
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck (generate)

import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize
    , serialize'
    )
import Cardano.Ledger.Mary.Value (AssetName (..))
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    , CSMTOps (..)
    , mkCSMTOps
    )
import Cardano.UTxOCSMT.Application.Run.Config (context)
import ChainFollower.Rollbacks.Store qualified as Store
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( RunTransaction (..)
    , mapColumns
    , newRunTransaction
    )
import Database.RocksDB
    ( DB (..)
    , withDBCF
    )
import System.Directory (doesFileExist)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.Unsafe (unsafePerformIO)

import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    , unifiedCodecs
    )
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Read (verifyTokenRequests)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , LocatedRequest (LocatedRequest)
    , LocatedTokenState (..)
    , SlotNo (..)
    , TokenId (..)
    , TxIn
    )
import Cardano.MPFS.Generators
    ( genRequest
    , genTxIn
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.HTTP.Types
    ( RequestsResponse (..)
    , TokenIdJSON (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx (..)
    , readMerkleRoot
    )
import Cardano.MPFS.Indexer.TxFixtures (testScriptHash)
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , requestAddrFromCfg
    )

-- | "cafe" as hex = "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | "aabb" as hex = "61616262".
aabbTid :: TokenId
aabbTid = TokenId (AssetName (SBS.toShort "aabb"))

-- | Stub the verification snapshot and the UTxO
-- witness machinery and seed a checkpoint so the
-- proof-bearing handler can assemble its envelope.
withSnapshot
    :: BS.ByteString
    -> BS.ByteString
    -> BS.ByteString
    -> Context IO
    -> IO (Context IO)
withSnapshot rootBs outBs proofBs ctx = do
    St.putCheckpoint
        (St.checkpoints (state ctx))
        (SlotNo 42)
        (BlockId "block-id-bytes")
    pure
        ctx
            { utxoRoot = pure (Just rootBs)
            , resolveUtxo = \_ -> pure (Just outBs)
            , utxoProof = \_ -> pure (Just proofBs)
            , cfgCage = testCageConfig
            }

-- | Run the handler against a real UTxO CSMT indexer
-- transaction, seeded with request-address UTxO bytes.
withRequestSet
    :: [(TxIn, ByteString)]
    -> Context IO
    -> (Context IO -> IO a)
    -> IO a
withRequestSet entries ctx action =
    withRequestSetRoot Nothing entries ctx
        $ \_ ctxWithSet -> action ctxWithSet

withRequestSetRoot
    :: Maybe ByteString
    -> [(TxIn, ByteString)]
    -> Context IO
    -> (ByteString -> Context IO -> IO a)
    -> IO a
withRequestSetRoot mOutOfTxRoot entries ctx action =
    withSystemTempDirectory "requests-indexer-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db -> do
                    let database =
                            mkRocksDBDatabase
                                db
                                ( mkColumns
                                    (columnFamilies db)
                                    unifiedCodecs
                                )
                        CSMTContext{fromKV, hashing} = context
                        csmtOps = mkCSMTOps fromKV hashing
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    sentinelTxIn <- generate genTxIn
                    let seededEntries =
                            ( serialize
                                (natVersion @11)
                                sentinelTxIn
                            , otherAddressTxOutBytes
                            )
                                : [ ( serialize
                                        (natVersion @11)
                                        txIn
                                    , BSL.fromStrict txOutBytes
                                    )
                                  | (txIn, txOutBytes) <- entries
                                  ]
                    runTransaction
                        $ mapColumns InUtxo
                        $ forM_ seededEntries
                        $ uncurry
                            (csmtInsert csmtOps)
                    runTransaction
                        $ Store.armageddonSetup
                            InRollbacks
                            (SlotNo 42)
                            (Just (BlockId "block-id-bytes"))
                    let IndexerTx readRoot = readMerkleRoot
                    mRoot <- runTransaction readRoot
                    case mRoot of
                        Nothing ->
                            failExpectation
                                "request-set helper did not seed a UTxO root"
                        Just txRoot ->
                            action
                                txRoot
                                ctx
                                    { runIndexerTx =
                                        \(IndexerTx body) ->
                                            runTransaction body
                                    , utxoRoot =
                                        pure
                                            $ Just
                                            $ fromMaybe
                                                txRoot
                                                mOutOfTxRoot
                                    }

testCageConfig :: CageConfig
testCageConfig =
    CageConfig
        { cageScriptBytes = SBS.toShort "dummy"
        , requestScriptBytes = testRequestScriptBytes
        , cfgScriptHash = testScriptHash
        , defaultProcessTime = 60_000
        , defaultRetractTime = 30_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }

testRequestScriptBytes :: SBS.ShortByteString
testRequestScriptBytes =
    unsafePerformIO loadTestRequestScriptBytes
{-# NOINLINE testRequestScriptBytes #-}

loadTestRequestScriptBytes :: IO SBS.ShortByteString
loadTestRequestScriptBytes = do
    hex <- tryRead candidatePaths
    let trimmed =
            BS.takeWhile
                (\b -> b /= 10 && b /= 13)
                hex
    case B16.decode trimmed of
        Right bs -> pure (SBS.toShort bs)
        Left err ->
            error
                $ "loadTestRequestScriptBytes: "
                    <> err
  where
    candidatePaths =
        [ "test-data/request.uplc.hex"
        , "cardano-mpfs-offchain/test-data/request.uplc.hex"
        ]
    tryRead [] =
        error
            "loadTestRequestScriptBytes: \
            \test-data/request.uplc.hex not found \
            \in any of the candidate paths"
    tryRead (p : ps) = do
        exists <- doesFileExist p
        if exists
            then BS.readFile p
            else tryRead ps

-- | Serialized request UTxO bytes at the token's
-- per-cage request address.
sampleRequestOutBytes
    :: TokenId
    -> Integer
    -> ByteString
sampleRequestOutBytes tid _submittedAt =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    txOut =
        mkBasicTxOut
            (requestAddrFromCfg testCageConfig tid Testnet)
            (inject (Coin 2_000_000))

otherAddressTxOutBytes :: BSL.ByteString
otherAddressTxOutBytes =
    BSL.fromStrict
        $ serialize'
            (natVersion @11)
            ( mkBasicTxOut
                (cageAddrFromCfg testCageConfig Testnet)
                (inject (Coin 2_000_000))
                :: TxOut ConwayEra
            )

-- | Seed an indexed token state for a token id so
-- the proof-bearing handler can find a state UTxO
-- to anchor against.
seedTokenState :: Context IO -> TokenId -> IO ()
seedTokenState ctx tid = do
    ts <- mkDummyTokenState
    txIn <- generate genTxIn
    St.putToken
        (St.tokens (state ctx))
        tid
        (LocatedTokenState txIn ts)

spec :: Spec
spec = describe "GET /tokens/:id/requests" $ do
    it
        "uses the in-transaction root for the response snapshot"
        $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot
                    "stale-root-r1"
                    "tx-out"
                    "proof"
                    ctx0
            seedTokenState ctx cafeTid
            txIn <- generate genTxIn
            req <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn req)
            withRequestSetRoot
                (Just "stale-root-r1")
                [(txIn, sampleRequestOutBytes cafeTid 0)]
                ctx
                $ \txRoot ctxWithSet -> do
                    resp <- getRequests ctxWithSet "63616665"
                    simpleStatus resp `shouldBe` status200
                    root <- responseSnapshotRoot resp
                    root `shouldBe` txRoot

    it "returns empty list when no requests exist" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        seedTokenState ctx cafeTid
        withRequestSet [] ctx $ \ctxWithSet -> do
            resp <- getRequests ctxWithSet "63616665"
            simpleStatus resp `shouldBe` status200
            assertEnvelope resp 0

    it
        "returns a single witnessed request after \
        \insertion"
        $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot
                    "root"
                    "tx-out"
                    "proof"
                    ctx0
            seedTokenState ctx cafeTid
            txIn <- generate genTxIn
            req <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn req)
            withRequestSet
                [(txIn, sampleRequestOutBytes cafeTid 0)]
                ctx
                $ \ctxWithSet -> do
                    resp <- getRequests ctxWithSet "63616665"
                    simpleStatus resp `shouldBe` status200
                    assertEnvelope resp 1
                    assertWitnessedRequestFields resp
                    assertVerifiesForCafe resp

    it
        "returns multiple witnessed requests for same \
        \token"
        $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot
                    "root"
                    "tx-out"
                    "proof"
                    ctx0
            seedTokenState ctx cafeTid
            txIn1 <- generate genTxIn
            txIn2 <- generate genTxIn
            req1 <- generate (genRequest cafeTid)
            req2 <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn1 req1)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn2 req2)
            withRequestSet
                [ (txIn1, sampleRequestOutBytes cafeTid 0)
                , (txIn2, sampleRequestOutBytes cafeTid 1)
                ]
                ctx
                $ \ctxWithSet -> do
                    resp <- getRequests ctxWithSet "63616665"
                    simpleStatus resp `shouldBe` status200
                    assertEnvelope resp 2

    it "filters by token — other tokens excluded" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        seedTokenState ctx cafeTid
        seedTokenState ctx aabbTid
        txIn1 <- generate genTxIn
        txIn2 <- generate genTxIn
        req1 <- generate (genRequest cafeTid)
        req2 <- generate (genRequest aabbTid)
        St.putRequest
            (St.requests (state ctx))
            (LocatedRequest txIn1 req1)
        St.putRequest
            (St.requests (state ctx))
            (LocatedRequest txIn2 req2)
        withRequestSet
            [(txIn1, sampleRequestOutBytes cafeTid 0)]
            ctx
            $ \ctxWithCafeSet -> do
                resp1 <- getRequests ctxWithCafeSet "63616665"
                simpleStatus resp1 `shouldBe` status200
                assertEnvelope resp1 1
        withRequestSet
            [(txIn2, sampleRequestOutBytes aabbTid 0)]
            ctx
            $ \ctxWithAabbSet -> do
                resp2 <- getRequests ctxWithAabbSet "61616262"
                simpleStatus resp2 `shouldBe` status200
                assertEnvelope resp2 1

    it "returns 404 for unknown token" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        resp <- getRequests ctx "63616665"
        simpleStatus resp `shouldBe` status404

    it "returns 503 when snapshot not yet available"
        $ do
            ctx0 <- mkTestContext
            let ctx = ctx0{indexerProofsReady = pure False}
            seedTokenState ctx cafeTid
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status503

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | @GET \/tokens\/:id\/requests@
getRequests
    :: Context IO -> ByteString -> IO SResponse
getRequests ctx tokenHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ( "/tokens/"
                    <> tokenHex
                    <> "/requests"
                )
            )
        )
        (mkApp ctx)

-- | Assert the response body is a 'RequestsResponse'
-- envelope with exactly @n@ witnessed requests.
assertEnvelope :: SResponse -> Int -> IO ()
assertEnvelope resp n =
    case decode (simpleBody resp) of
        Just (Object obj) -> do
            KM.member "snapshot" obj `shouldBe` True
            case KM.lookup "requests" obj of
                Just (Array arr) ->
                    length arr `shouldBe` n
                _ ->
                    expectationFailure
                        "requests is not an array"
            case KM.lookup "request_set" obj of
                Just (Object requestSetObj) -> do
                    case KM.lookup "entries" requestSetObj of
                        Just (Array entries) ->
                            V.length entries `shouldBe` n
                        _ ->
                            expectationFailure
                                "request_set.entries is not an array"
                    case KM.lookup
                        "completeness_proof"
                        requestSetObj of
                        Just (String proof) ->
                            proof
                                `shouldSatisfy` (not . T.null)
                        _ ->
                            expectationFailure
                                "request_set.completeness_proof \
                                \is not a string"
                _ ->
                    expectationFailure
                        "request_set is not an object"
        _ ->
            expectationFailure
                "Expected JSON object"

responseSnapshotRoot :: SResponse -> IO ByteString
responseSnapshotRoot resp =
    case decode (simpleBody resp) of
        Just
            RequestsResponse
                { rrSnapshot =
                    VerificationSnapshot
                        { vsUtxoRoot = Hex root
                        }
                } -> pure root
        Nothing ->
            failExpectation "Expected RequestsResponse JSON"

-- | Assert the first witnessed request has both the
-- @utxo@ witness and the decoded @request@ payload.
assertWitnessedRequestFields :: SResponse -> IO ()
assertWitnessedRequestFields resp =
    case decode (simpleBody resp) of
        Just (Object obj) ->
            case KM.lookup "requests" obj of
                Just (Array arr)
                    | not (V.null arr) ->
                        case V.head arr of
                            Object wreq -> do
                                KM.member "utxo" wreq
                                    `shouldBe` True
                                case KM.lookup
                                    "request"
                                    wreq of
                                    Just (Object r) -> do
                                        KM.member "token" r
                                            `shouldBe` True
                                        KM.member "owner" r
                                            `shouldBe` True
                                        KM.member "key" r
                                            `shouldBe` True
                                        KM.member
                                            "operation"
                                            r
                                            `shouldBe` True
                                        KM.member "fee" r
                                            `shouldBe` True
                                        KM.member
                                            "submitted_at"
                                            r
                                            `shouldBe` True
                                    _ ->
                                        expectationFailure
                                            "request \
                                            \is not an \
                                            \object"
                            _ ->
                                expectationFailure
                                    "witnessed \
                                    \request is not \
                                    \an object"
                _ ->
                    expectationFailure
                        "requests is empty or not \
                        \an array"
        _ ->
            expectationFailure
                "Expected JSON object"

-- | Assert the response's request-set witness is accepted by the
-- client verifier using the locally-derived token request prefix.
assertVerifiesForCafe :: SResponse -> IO ()
assertVerifiesForCafe resp =
    case decode (simpleBody resp) of
        Just body@RequestsResponse{rrSnapshot} ->
            case verifyTokenRequests
                (toClientCageConfig testCageConfig)
                cafeTokenJSON
                (TrustedRoot (vsUtxoRoot rrSnapshot))
                body of
                Right _ -> pure ()
                Left err ->
                    expectationFailure
                        ("verifyTokenRequests failed: " <> show err)
        Nothing ->
            expectationFailure
                "Expected RequestsResponse JSON"

cafeTokenJSON :: TokenIdJSON
cafeTokenJSON = TokenIdJSON "cafe"

toClientCageConfig :: CageConfig -> Client.CageConfig
toClientCageConfig cfg =
    Client.CageConfig
        { Client.cageScriptBytes = cageScriptBytes cfg
        , Client.requestScriptBytes = requestScriptBytes cfg
        , Client.cfgScriptHash = cfgScriptHash cfg
        , Client.defaultProcessTime = defaultProcessTime cfg
        , Client.defaultRetractTime = defaultRetractTime cfg
        , Client.defaultTip = defaultTip cfg
        , Client.network = network cfg
        }

failExpectation :: String -> IO a
failExpectation msg = do
    expectationFailure msg
    pure (error msg)
