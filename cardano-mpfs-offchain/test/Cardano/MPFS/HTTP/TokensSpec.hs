{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokensSpec
-- Description : Tests for GET /tokens endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokensSpec
    ( spec
    , mkDummyTokenState
    ) where

import Control.Monad (forM_, when)
import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Either (isRight)
import Data.List (sort)
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Data.Vector qualified as V
import Network.HTTP.Types
    ( status200
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

import Cardano.Ledger.Binary
    ( natVersion
    , serialize
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
import System.IO.Temp (withSystemTempDirectory)

import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    , unifiedCodecs
    )
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , LocatedTokenState (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Generators (genKeyHash, genTxIn)
import Cardano.MPFS.HTTP.AtomicReadFixture
    ( cafeTid
    , sampleRequestOutBytes
    , sampleStateOutBytes
    , staleRoot
    , stateSetPrefix
    , testCageConfig
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Types
    ( TokenIdJSON (..)
    , TokenSetWitness (..)
    , TokenUtxoEntry (..)
    , TokensResponse (..)
    , UtxoEntryRefOnly (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx (..)
    , readMerkleRoot
    )
import Cardano.MPFS.State qualified as St
import Test.QuickCheck (generate)

getTokens :: Context IO -> IO SResponse
getTokens ctx =
    runSession
        ( request
            (setPath defaultRequest "/tokens")
        )
        (mkApp ctx)

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

withTokenSet
    :: [(TxIn, ByteString)]
    -> ByteString
    -> Context IO
    -> (Context IO -> IO a)
    -> IO a
withTokenSet entries _proofBs ctx action =
    withTokenSetRoot Nothing entries ctx
        $ \_ ctxWithSet -> action ctxWithSet

withTokenSetRoot
    :: Maybe ByteString
    -> [(TxIn, ByteString)]
    -> Context IO
    -> (ByteString -> Context IO -> IO a)
    -> IO a
withTokenSetRoot mOutOfTxRoot entries ctx action =
    withSystemTempDirectory "tokens-indexer-test"
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
                    let seededEntries =
                            [ ( serialize
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
                    when (null seededEntries) $ do
                        sentinel <- generate genTxIn
                        let sentinelKey =
                                serialize
                                    (natVersion @11)
                                    sentinel
                        runTransaction
                            $ mapColumns InUtxo
                            $ csmtInsert
                                csmtOps
                                sentinelKey
                                ( BSL.fromStrict
                                    $ sampleRequestOutBytes
                                        cafeTid
                                        0
                                )
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
                                "token-set helper did not seed a UTxO root"
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

-- | A dummy token state for testing.
mkDummyTokenState :: IO TokenState
mkDummyTokenState = do
    kh <- generate genKeyHash
    pure
        TokenState
            { owner = kh
            , root = Root (BS.replicate 32 0)
            , tip = Coin 1_000_000
            , processTime = 60_000
            , retractTime = 30_000
            }

spec :: Spec
spec = describe "GET /tokens" $ do
    it
        "uses the in-transaction root for the response snapshot"
        $ do
            ctx0 <- mkTestContext
            ctx <- withSnapshot staleRoot "tx-out" "proof" ctx0
            ts <- mkDummyTokenState
            txIn <- generate genTxIn
            let tid =
                    TokenId (AssetName (SBS.toShort "deadbeef"))
                txOut = sampleStateOutBytes 0
            St.putToken
                (St.tokens (state ctx))
                tid
                (LocatedTokenState txIn ts)
            withTokenSetRoot
                (Just staleRoot)
                [(txIn, txOut)]
                ctx
                $ \txRoot ctxWithSet -> do
                    resp <- getTokens ctxWithSet
                    simpleStatus resp `shouldBe` status200
                    root <- responseSnapshotRoot resp
                    root `shouldBe` txRoot
                    assertTokenSetVerifies resp

    it "returns empty proof-bearing token set on fresh state" $ do
        ctx0 <- mkTestContext
        ctx <- withSnapshot "root" "tx-out" "proof" ctx0
        withTokenSet [] "proof" ctx $ \ctxWithSet -> do
            resp <- getTokens ctxWithSet
            simpleStatus resp `shouldBe` status200
            assertEnvelope resp 0

    it "returns proof-bearing witness for all token UTxOs" $ do
        ctx0 <- mkTestContext
        ctx <- withSnapshot "root" "tx-out" "proof" ctx0
        ts <- mkDummyTokenState
        txIn1 <- generate genTxIn
        txIn2 <- generate genTxIn
        let tid1 =
                TokenId (AssetName (SBS.toShort "deadbeef"))
            tid2 =
                TokenId (AssetName (SBS.toShort "cafebabe"))
            txOut1 = sampleStateOutBytes 0
            txOut2 = sampleStateOutBytes 1
        St.putToken
            (St.tokens (state ctx))
            tid1
            (LocatedTokenState txIn1 ts)
        St.putToken
            (St.tokens (state ctx))
            tid2
            (LocatedTokenState txIn2 ts)
        withTokenSet
            [(txIn1, txOut1), (txIn2, txOut2)]
            "proof"
            ctx
            $ \ctxWithSet -> do
                resp <- getTokens ctxWithSet
                simpleStatus resp `shouldBe` status200
                assertEnvelope resp 2
                assertTokenEntries
                    resp
                    [ ("deadbeef", txOut1)
                    , ("cafebabe", txOut2)
                    ]

    it "returns 503 when snapshot not yet available" $ do
        ctx0 <- mkTestContext
        let ctx = ctx0{indexerProofsReady = pure False}
        resp <- getTokens ctx
        simpleStatus resp `shouldBe` status503

assertEnvelope :: SResponse -> Int -> IO ()
assertEnvelope resp n =
    case decode (simpleBody resp) of
        Just (Object obj) -> do
            KM.member "snapshot" obj `shouldBe` True
            case KM.lookup "tokens" obj of
                Just (Object tokensObj) -> do
                    case KM.lookup "entries" tokensObj of
                        Just (Array entries) ->
                            V.length entries `shouldBe` n
                        _ ->
                            expectationFailure
                                "tokens.entries is not an array"
                    case KM.lookup
                        "completeness_proof"
                        tokensObj of
                        Just (String proof) ->
                            proof `shouldSatisfy` (not . T.null)
                        _ ->
                            expectationFailure
                                "tokens.completeness_proof \
                                \is not a string"
                _ ->
                    expectationFailure
                        "tokens is not an object"
        _ ->
            expectationFailure
                "Expected JSON object"

responseSnapshotRoot :: SResponse -> IO ByteString
responseSnapshotRoot resp =
    case decode (simpleBody resp) of
        Just
            TokensResponse
                { trsSnapshot =
                    VerificationSnapshot
                        { vsUtxoRoot = Hex root
                        }
                } -> pure root
        Nothing ->
            failExpectation "Expected TokensResponse JSON"

assertTokenEntries :: SResponse -> [(ByteString, ByteString)] -> IO ()
assertTokenEntries resp expected =
    case decode (simpleBody resp) of
        Just
            TokensResponse
                { trsTokens = TokenSetWitness{tswEntries}
                } ->
                sort
                    ( map
                        ( \TokenUtxoEntry
                            { tueTokenId = TokenIdJSON tokenId
                            , tueTxOutCbor = Hex txOut
                            } -> (tokenId, txOut)
                        )
                        tswEntries
                    )
                    `shouldBe` sort expected
        _ ->
            expectationFailure
                "Expected TokensResponse"

assertTokenSetVerifies :: SResponse -> IO ()
assertTokenSetVerifies resp =
    case decode (simpleBody resp) of
        Just TokensResponse{trsSnapshot, trsTokens} ->
            let Hex root = vsUtxoRoot trsSnapshot
            in  verifyUtxoSetCompleteness
                    "tokens.tokens"
                    root
                    (stateSetPrefix testCageConfig)
                    (tokenSetAsUtxoSet trsTokens)
                    `shouldSatisfy` isRight
        _ ->
            expectationFailure
                "Expected TokensResponse"

tokenSetAsUtxoSet :: TokenSetWitness -> UtxoSetWitness
tokenSetAsUtxoSet TokenSetWitness{..} =
    UtxoSetWitness
        { uswEntries =
            [ UtxoEntryRefOnly
                { uerRef = tueRef
                , uerTxOutCbor = tueTxOutCbor
                }
            | TokenUtxoEntry{..} <- tswEntries
            ]
        , uswCompletenessProof = tswCompletenessProof
        }

failExpectation :: String -> IO a
failExpectation msg = do
    expectationFailure msg
    pure (error msg)
