{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- |
-- Module      : Cardano.MPFS.Indexer.UnifiedSpec
-- Description : 14-CF unified column tests
-- License     : Apache-2.0
--
-- Incremental tests that open RocksDB with ALL 14
-- column families (same as production) and exercise
-- transactions across 'UnifiedColumns'. No
-- cardano-node needed — uses synthetic data.
module Cardano.MPFS.Indexer.UnifiedSpec (spec) where

import Control.Concurrent.Async
    ( concurrently_
    , replicateConcurrently_
    )
import Control.Monad (forM_, replicateM_)
import Control.Tracer (nullTracer)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromJust)
import Data.Word (Word16)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )

import CSMT (FromKV (..), Hashing)
import CSMT.Hashes
    ( Hash
    , fromKVHashes
    , hashHashing
    , isoHash
    )
import CSMT.MTS (kvCommon)
import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromStringAsHex
    )
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Hashes (unsafeMakeSafeHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.TxIn qualified as Ledger
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    , Prisms (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTOps (..)
    , DbState (..)
    , ReadyState (..)
    , kvCommonToCSMTOps
    , mkCSMTOps
    , openCSMTOps
    )
import Cardano.UTxOCSMT.Application.Database.Interface
    ( TipOf
    )
import Cardano.UTxOCSMT.Application.Database.Interface qualified as UTxO
    ( Operation (..)
    )
import ChainFollower.Backend qualified as Backend
import ChainFollower.Rollbacks.Store qualified as Store
import ChainFollower.Runner
    ( Phase (..)
    , processBlock
    , rollbackTo
    )
import Codec.CBOR.Decoding qualified as CBOR
import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Control.Lens (iso, lazy, prism', strict, view)
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( RunTransaction (..)
    , Transaction
    , mapColumns
    , newRunTransaction
    , query
    , runTransactionUnguarded
    )
import Database.RocksDB
    ( BatchOp
    , ColumnFamily
    , DB (..)
    , withDBCF
    )
import Ouroboros.Network.Block (SlotNo (..))
import System.IO.Temp (withSystemTempDirectory)

import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , BlockId (..)
    , Coin (..)
    , LocatedTokenState (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Indexer.Codecs
    ( allUnifiedCodecs
    )
import Cardano.MPFS.Indexer.Columns
    ( UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.ComposedInv (ComposedInv (..))
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageBlockEvents
    , applyCageInverses
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkTransactionalState
    )
import Cardano.MPFS.State (State (..), Tokens (..))
import Cardano.MPFS.Trie (Trie (..), TrieManager (..))
import Cardano.MPFS.Trie.Persistent
    ( mkPersistentTrieManager
    , mkUnifiedTrieManager
    )

-- ---------------------------------------------------------
-- Prisms for UTxO columns (test-only, simple types)
-- ---------------------------------------------------------

type instance TipOf SlotNo = SlotNo

testPrisms
    :: Prisms SlotNo Hash BL.ByteString BL.ByteString
testPrisms =
    Prisms
        { slotP =
            prism'
                ( BL.toStrict
                    . CBOR.toLazyByteString
                    . CBOR.encodeWord64
                    . fromIntegral
                    . unSlotNo
                )
                ( \bs ->
                    case CBOR.deserialiseFromBytes
                        CBOR.decodeWord64
                        (BL.fromStrict bs) of
                        Right (_, w) ->
                            Just (fromIntegral w)
                        Left _ -> Nothing
                )
        , hashP = isoHash
        , keyP = lazy
        , valueP = lazy
        }

-- ---------------------------------------------------------
-- Unified columns type
-- ---------------------------------------------------------

type Unified =
    UnifiedColumns
        SlotNo
        Hash
        BL.ByteString
        BL.ByteString

-- ---------------------------------------------------------
-- DB bracket: all 14 CFs
-- ---------------------------------------------------------

withUnifiedDB
    :: ( ( forall a
            . Transaction
                IO
                ColumnFamily
                Unified
                BatchOp
                a
           -> IO a
         )
         -> IO r
       )
    -> IO r
withUnifiedDB action =
    withSystemTempDirectory "unified-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db -> do
                    let cols =
                            mkColumns
                                (columnFamilies db)
                                (allUnifiedCodecs testPrisms)
                        database =
                            mkRocksDBDatabase db cols
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    action runTransaction

-- | Same as withUnifiedDB but also creates the
-- persistent trie manager from raw CF handles
-- (indices 10, 11, 12) — matching withApplication.
withUnifiedDBAndTrie
    :: ( ( forall a
            . Transaction
                IO
                ColumnFamily
                Unified
                BatchOp
                a
           -> IO a
         )
         -> TrieManager IO
         -> IO r
       )
    -> IO r
withUnifiedDBAndTrie action =
    withSystemTempDirectory "unified-trie-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db -> do
                    let cols =
                            mkColumns
                                (columnFamilies db)
                                (allUnifiedCodecs testPrisms)
                        database =
                            mkRocksDBDatabase db cols
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    -- Extract trie CFs at indices 10-12
                    -- (same as drop 9 in withApplication)
                    case drop 9 (columnFamilies db) of
                        (nodesCF : kvCF : metaCF : _) -> do
                            tm <-
                                mkPersistentTrieManager
                                    db
                                    nodesCF
                                    kvCF
                                    metaCF
                            action runTransaction tm
                        _ ->
                            error
                                "withUnifiedDBAndTrie: \
                                \not enough CFs"

-- | Full production wiring: 14 CFs, openCSMTOps with
-- journal/crash recovery, persistent trie manager.
-- Returns the kvOnly CSMTOps (from openCSMTOps) and
-- the transaction runner.
withUnifiedDBFull
    :: ( CSMTOps
            ( Transaction
                IO
                ColumnFamily
                ( Columns
                    SlotNo
                    Hash
                    BL.ByteString
                    BL.ByteString
                )
                BatchOp
            )
            BL.ByteString
            BL.ByteString
            Hash
         -> ( forall a
               . Transaction
                    IO
                    ColumnFamily
                    Unified
                    BatchOp
                    a
              -> IO a
            )
         -> TrieManager IO
         -> IO r
       )
    -> IO r
withUnifiedDBFull action =
    withSystemTempDirectory "unified-full-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db -> do
                    let cols =
                            mkColumns
                                (columnFamilies db)
                                (allUnifiedCodecs testPrisms)
                        database =
                            mkRocksDBDatabase db cols
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    -- openCSMTOps: same as withApplication
                    dbState <-
                        openCSMTOps
                            4
                            1000
                            (iso BL.toStrict BL.fromStrict)
                            testFromKV
                            testHashing
                            (runTransaction . mapColumns InUtxo)
                            ( runTransactionUnguarded database
                                . mapColumns InUtxo
                            )
                            (const $ pure ())
                    let resolveDb (NeedsRecovery recover) =
                            recover >>= resolveDb
                        resolveDb (Ready (ChooseKVOnly ops)) =
                            pure ops
                        resolveDb (Ready (ChooseFull _)) =
                            error "unexpected ChooseFull"
                    kvOnlyOps <- resolveDb dbState
                    let csmtOps =
                            kvCommonToCSMTOps (kvCommon kvOnlyOps)
                    -- Persistent trie manager
                    case drop 9 (columnFamilies db) of
                        (nodesCF : kvCF : metaCF : _) -> do
                            tm <-
                                mkPersistentTrieManager
                                    db
                                    nodesCF
                                    kvCF
                                    metaCF
                            action csmtOps runTransaction tm
                        _ ->
                            error "not enough CFs"

-- ---------------------------------------------------------
-- Spec
-- ---------------------------------------------------------

spec :: Spec
spec = describe "Unified 14-CF" $ do
    it "opens DB with 14 CFs" $ do
        withUnifiedDB $ \_transact -> do
            pure ()

    it "write/read cage token via InCage" $ do
        withUnifiedDB $ \transact -> do
            -- Boot a token through cage columns
            _ <-
                transact
                    $ mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        [CageBoot tokA stRefA tokStateA]
            -- Read it back
            mts <-
                transact
                    $ mapColumns InCage
                    $ getToken
                        (tokens mkTransactionalState)
                        tokA
            mts
                `shouldBe` Just
                    (LocatedTokenState stRefA tokStateA)

    it "write/read UTxO KV via InUtxo" $ do
        withUnifiedDB $ \transact -> do
            let csmtOps =
                    mkCSMTOps testFromKV testHashing
                k = "test-key-padded-to-32-bytesXXXX"
                v = "test-value"
            -- Write to UTxO KV column
            transact
                $ mapColumns InUtxo
                $ csmtInsert csmtOps k v
            -- Read back
            mv <-
                transact
                    $ mapColumns InUtxo
                    $ query
                        KVCol
                        k
            mv `shouldBe` Just v

    it "mixed cage + UTxO in same transaction" $ do
        withUnifiedDB $ \transact -> do
            let csmtOps =
                    mkCSMTOps testFromKV testHashing
            -- Both subsystems in one transaction
            transact $ do
                _ <-
                    mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [CageBoot tokA stRefA tokStateA]
                mapColumns InUtxo
                    $ csmtInsert csmtOps "key-pad-32-bytes-xxxxxxxxxx" "val"
            -- Verify both
            mts <-
                transact
                    $ mapColumns InCage
                    $ getToken
                        (tokens mkTransactionalState)
                        tokA
            mts
                `shouldBe` Just
                    (LocatedTokenState stRefA tokStateA)
            mv <-
                transact
                    $ mapColumns InUtxo
                    $ query
                        KVCol
                        "key-pad-32-bytes-xxxxxxxxxx"
            mv `shouldBe` Just "val"

    it "cage boot + request + update across transactions" $ do
        withUnifiedDB $ \transact -> do
            -- Boot
            _ <-
                transact
                    $ mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        [CageBoot tokA stRefA tokStateA]
            -- Request
            _ <-
                transact
                    $ mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        [CageRequest txInA reqA]
            -- Update (consumes request, mutates trie)
            _ <-
                transact
                    $ mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        [ CageUpdate
                            tokA
                            newStRefA
                            (Root "new-root")
                            [txInA]
                        ]
            -- Verify
            mts <-
                transact
                    $ mapColumns InCage
                    $ getToken
                        (tokens mkTransactionalState)
                        tokA
            mts
                `shouldBe` Just
                    ( LocatedTokenState
                        newStRefA
                        tokStateA{root = Root "new-root"}
                    )

    describe "with persistent trie manager" $ do
        it "tx boot then IO trie read" $ do
            withUnifiedDBAndTrie $ \transact tm -> do
                -- Boot via transaction
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [ CageBoot tokA stRefA tokStateA
                            , CageRequest txInA reqA
                            ]
                -- Update via transaction (trie mutation)
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [ CageUpdate
                                tokA
                                newStRefA
                                (Root "new-root")
                                [txInA]
                            ]
                -- Read trie via IO layer (raw CF handles)
                withTrie tm tokA $ \trie -> do
                    Root r <- getRoot trie
                    -- MPF hash, not empty = trie exists
                    (BS.length r > 0) `shouldBe` True

        it "interleaved tx writes and IO reads" $ do
            withUnifiedDBAndTrie $ \transact tm -> do
                -- Boot
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [CageBoot tokA stRefA tokStateA]
                -- IO read after boot
                withTrie tm tokA $ \trie -> do
                    _ <- getRoot trie
                    pure () -- trie accessible, no crash
                    -- Request + update
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [CageRequest txInA reqA]
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [ CageUpdate
                                tokA
                                newStRefA
                                (Root "updated")
                                [txInA]
                            ]
                -- IO read after update
                withTrie tm tokA $ \trie -> do
                    Root r <- getRoot trie
                    (BS.length r > 0) `shouldBe` True

        it "mixed UTxO + cage tx then IO trie" $ do
            withUnifiedDBAndTrie $ \transact tm -> do
                let csmtOps =
                        mkCSMTOps testFromKV testHashing
                -- Both in one transaction
                _ <-
                    transact $ do
                        inv <-
                            mapColumns InCage
                                $ applyCageBlockEvents
                                    mkTransactionalState
                                    mkUnifiedTrieManager
                                    [ CageBoot
                                        tokA
                                        stRefA
                                        tokStateA
                                    , CageRequest txInA reqA
                                    ]
                        mapColumns InUtxo
                            $ csmtInsert
                                csmtOps
                                "key-pad-32-bytes-xxxxxxxxxx"
                                "val"
                        pure inv
                -- Update trie
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [ CageUpdate
                                tokA
                                newStRefA
                                (Root "mixed")
                                [txInA]
                            ]
                -- IO layer reads
                withTrie tm tokA $ \trie -> do
                    Root r <- getRoot trie
                    (BS.length r > 0) `shouldBe` True
                mv <-
                    transact
                        $ mapColumns InUtxo
                        $ query
                            KVCol
                            "key-pad-32-bytes-xxxxxxxxxx"
                mv `shouldBe` Just "val"

    describe "composed Runner on UnifiedColumns" $ do
        it "restore then follow via composedInit pattern" $ do
            withUnifiedDB $ \transact -> do
                let csmtOps =
                        mkCSMTOps testFromKV testHashing
                    -- Mimic composedInit: cage + UTxO
                    -- in same transaction, using
                    -- mapColumns
                    restoring =
                        mkComposedRestoring csmtOps
                    phase0 =
                        InRestoration restoring
                -- Restore a block
                phase1 <-
                    processBlock
                        nullTracer
                        False
                        transact
                        InRollbacks
                        maxBound
                        (1 :: SlotNo)
                        ( [CageBoot tokA stRefA tokStateA]
                        , [UTxO.Insert "k1-pad-32bytes-xxxxxxxxxxxx" "v1"]
                        )
                        phase0
                case phase1 of
                    InRestoration _ -> pure ()
                    InFollowing _ _ ->
                        fail "expected Restoration"
                -- Verify cage state persisted
                mts <-
                    transact
                        $ mapColumns InCage
                        $ getToken
                            (tokens mkTransactionalState)
                            tokA
                mts
                    `shouldBe` Just
                        (LocatedTokenState stRefA tokStateA)

        it "full composed lifecycle: restore, follow, rollback" $ do
            withUnifiedDB $ \transact -> do
                let csmtOps =
                        mkCSMTOps testFromKV testHashing
                    phase0 =
                        InRestoration
                            (mkComposedRestoring csmtOps)
                -- Restore: boot + request + UTxO insert
                phase1 <-
                    processBlock
                        nullTracer
                        False
                        transact
                        InRollbacks
                        maxBound
                        (1 :: SlotNo)
                        (
                            [ CageBoot tokA stRefA tokStateA
                            , CageRequest txInA reqA
                            ]
                        , [UTxO.Insert "utxo1-pad-32bytes-xxxxxxxxxx" "out1"]
                        )
                        phase0
                -- Transition to Following
                case phase1 of
                    InRestoration r -> do
                        following <-
                            Backend.toFollowing r
                        transact
                            $ Store.armageddonSetup
                                InRollbacks
                                (1 :: SlotNo)
                                (Just (BlockId mempty))
                        let phase2 = InFollowing 1 following
                        -- Follow: update + UTxO insert
                        phase3 <-
                            processBlock
                                nullTracer
                                False
                                transact
                                InRollbacks
                                maxBound
                                (2 :: SlotNo)
                                (
                                    [ CageUpdate
                                        tokA
                                        newStRefA
                                        (Root "root2")
                                        [txInA]
                                    ]
                                ,
                                    [ UTxO.Insert
                                        "utxo2-pad-32bytes-xxxxxxxxxx"
                                        "out2"
                                    ]
                                )
                                phase2
                        -- Follow: another block
                        phase4 <-
                            processBlock
                                nullTracer
                                False
                                transact
                                InRollbacks
                                maxBound
                                (3 :: SlotNo)
                                ( [CageRequest txInB reqB]
                                , []
                                )
                                phase3
                        -- Rollback to slot 2
                        case phase4 of
                            InFollowing n f -> do
                                (result, _) <-
                                    transact
                                        $ rollbackTo
                                            InRollbacks
                                            f
                                            n
                                            (2 :: SlotNo)
                                case result of
                                    Store.RollbackSucceeded d ->
                                        d `shouldBe` 1
                                    Store.RollbackImpossible ->
                                        fail "unexpected"
                            InRestoration _ ->
                                fail "expected Following"
                    InFollowing _ _ ->
                        fail "expected Restoration"

        it "5 composed restore-follow-rollback cycles" $ do
            withUnifiedDB $ \transact -> do
                let csmtOps =
                        mkCSMTOps testFromKV testHashing
                forM_ [0 .. 4 :: SlotNo] $ \n -> do
                    let s = n * 10
                        phase0 =
                            InRestoration
                                ( mkComposedRestoring
                                    csmtOps
                                )
                    -- Restore
                    phase1 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            InRollbacks
                            maxBound
                            (s + 1)
                            ( [CageBoot tokA stRefA tokStateA]
                            , []
                            )
                            phase0
                    case phase1 of
                        InRestoration r -> do
                            following <-
                                Backend.toFollowing r
                            transact
                                $ Store.armageddonSetup
                                    InRollbacks
                                    (s + 1)
                                    Nothing
                            let phase2 =
                                    InFollowing
                                        1
                                        following
                            -- Follow 2 blocks
                            phase3 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    InRollbacks
                                    maxBound
                                    (s + 2)
                                    (
                                        [ CageRequest
                                            txInA
                                            reqA
                                        ]
                                    , []
                                    )
                                    phase2
                            phase4 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    InRollbacks
                                    maxBound
                                    (s + 3)
                                    (
                                        [ CageUpdate
                                            tokA
                                            newStRefA
                                            (Root "r")
                                            [txInA]
                                        ]
                                    , []
                                    )
                                    phase3
                            -- Rollback
                            case phase4 of
                                InFollowing nn f -> do
                                    _ <-
                                        transact
                                            $ rollbackTo
                                                InRollbacks
                                                f
                                                nn
                                                (s + 2)
                                    pure ()
                                InRestoration _ ->
                                    fail "expected Following"
                        InFollowing _ _ ->
                            fail "expected Restoration"

    describe "full wiring (openCSMTOps + trie manager + Runner)" $ do
        it "restore + follow + rollback with openCSMTOps" $ do
            withUnifiedDBFull $ \csmtOps transact _tm -> do
                let phase0 =
                        InRestoration
                            (mkComposedRestoring csmtOps)
                -- Restore: boot + UTxO insert
                phase1 <-
                    processBlock
                        nullTracer
                        False
                        transact
                        InRollbacks
                        maxBound
                        (1 :: SlotNo)
                        ( [CageBoot tokA stRefA tokStateA, CageRequest txInA reqA]
                        , [UTxO.Insert "utxo-key-padded-to-32-bytesXX" "val1"]
                        )
                        phase0
                -- Transition
                case phase1 of
                    InRestoration r -> do
                        following <- Backend.toFollowing r
                        transact
                            $ Store.armageddonSetup
                                InRollbacks
                                (1 :: SlotNo)
                                (Just (BlockId mempty))
                        let phase2 = InFollowing 1 following
                        -- Follow: update + UTxO
                        phase3 <-
                            processBlock
                                nullTracer
                                False
                                transact
                                InRollbacks
                                maxBound
                                (2 :: SlotNo)
                                ( [CageUpdate tokA newStRefA (Root "r2") [txInA]]
                                , [UTxO.Insert "utxo-key2-padded-to-32-byteX" "val2"]
                                )
                                phase2
                        phase4 <-
                            processBlock
                                nullTracer
                                False
                                transact
                                InRollbacks
                                maxBound
                                (3 :: SlotNo)
                                ([CageRequest txInB reqB], [])
                                phase3
                        -- Rollback
                        case phase4 of
                            InFollowing n f -> do
                                (result, _) <-
                                    transact
                                        $ rollbackTo
                                            InRollbacks
                                            f
                                            n
                                            (2 :: SlotNo)
                                case result of
                                    Store.RollbackSucceeded d ->
                                        d `shouldBe` 1
                                    Store.RollbackImpossible ->
                                        fail "unexpected"
                            InRestoration _ ->
                                fail "expected Following"
                    InFollowing _ _ ->
                        fail "expected Restoration"

        it "5 cycles with openCSMTOps" $ do
            withUnifiedDBFull $ \csmtOps transact _tm -> do
                forM_ [0 .. 4 :: SlotNo] $ \n -> do
                    let s = n * 10
                        phase0 =
                            InRestoration
                                (mkComposedRestoring csmtOps)
                    phase1 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            InRollbacks
                            maxBound
                            (s + 1)
                            ([CageBoot tokA stRefA tokStateA], [])
                            phase0
                    case phase1 of
                        InRestoration r -> do
                            following <- Backend.toFollowing r
                            transact
                                $ Store.armageddonSetup
                                    InRollbacks
                                    (s + 1)
                                    Nothing
                            let phase2 = InFollowing 1 following
                            phase3 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    InRollbacks
                                    maxBound
                                    (s + 2)
                                    ([CageRequest txInA reqA], [])
                                    phase2
                            phase4 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    InRollbacks
                                    maxBound
                                    (s + 3)
                                    ( [CageUpdate tokA newStRefA (Root "r") [txInA]]
                                    , []
                                    )
                                    phase3
                            case phase4 of
                                InFollowing nn f -> do
                                    _ <-
                                        transact
                                            $ rollbackTo
                                                InRollbacks
                                                f
                                                nn
                                                (s + 2)
                                    pure ()
                                InRestoration _ ->
                                    fail "expected Following"
                        InFollowing _ _ ->
                            fail "expected Restoration"

    describe "concurrency stress" $ do
        it "concurrent tx writes + IO trie reads" $ do
            withUnifiedDBFull $ \csmtOps transact tm -> do
                -- Boot a token first
                _ <-
                    transact
                        $ mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            [CageBoot tokA stRefA tokStateA]
                -- Concurrent: tx writes and IO reads
                concurrently_
                    ( do
                        -- Thread 1: sequential tx writes
                        forM_ [1 .. 20 :: SlotNo] $ \s -> do
                            _ <-
                                transact $ do
                                    mapColumns InUtxo
                                        $ csmtInsert
                                            csmtOps
                                            ( BL.pack
                                                [ fromIntegral (unSlotNo s)
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                , 0
                                                ]
                                            )
                                            "v"
                            pure ()
                    )
                    ( do
                        -- Thread 2: IO trie reads
                        replicateM_ 20 $ do
                            withTrie tm tokA $ \trie -> do
                                _ <- getRoot trie
                                pure ()
                    )

        it "concurrent Runner processBlock" $ do
            withUnifiedDBFull $ \csmtOps transact _tm -> do
                let mkPhase =
                        InRestoration
                            (mkComposedRestoring csmtOps)
                -- Run 10 concurrent restore cycles
                -- (each in its own phase, serialized
                -- by the RunTransaction mutex)
                replicateConcurrently_ 10 $ do
                    let phase0 = mkPhase
                    _ <-
                        processBlock
                            nullTracer
                            False
                            transact
                            InRollbacks
                            maxBound
                            (1 :: SlotNo)
                            ( [CageBoot tokA stRefA tokStateA]
                            , []
                            )
                            phase0
                    pure ()

-- ---------------------------------------------------------
-- Composed Backend (mimics composedInit)
-- ---------------------------------------------------------

-- | Block data for the composed backend.
type ComposedBlock =
    ( [CageEvent]
    , [UTxO.Operation BL.ByteString BL.ByteString]
    )

-- | Build a composed Restoring that operates on
-- UnifiedColumns via mapColumns InCage / InUtxo.
mkComposedRestoring
    :: CSMTOps
        ( Transaction
            IO
            ColumnFamily
            ( Columns
                SlotNo
                Hash
                BL.ByteString
                BL.ByteString
            )
            BatchOp
        )
        BL.ByteString
        BL.ByteString
        Hash
    -> Backend.Restoring
        IO
        ( Transaction
            IO
            ColumnFamily
            Unified
            BatchOp
        )
        ComposedBlock
        ComposedInv
        BlockId
mkComposedRestoring csmtOps =
    Backend.Restoring
        { Backend.restore = \(events, utxoOps) -> do
            _ <-
                mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        events
            mapColumns InUtxo
                $ forM_ utxoOps
                $ \case
                    UTxO.Insert k v ->
                        csmtInsert csmtOps k v
                    UTxO.Delete k ->
                        csmtDelete csmtOps k
            pure $ mkComposedRestoring csmtOps
        , Backend.toFollowing =
            pure $ mkComposedFollowing csmtOps
        }

-- | Build a composed Following.
mkComposedFollowing
    :: CSMTOps
        ( Transaction
            IO
            ColumnFamily
            ( Columns
                SlotNo
                Hash
                BL.ByteString
                BL.ByteString
            )
            BatchOp
        )
        BL.ByteString
        BL.ByteString
        Hash
    -> Backend.Following
        IO
        ( Transaction
            IO
            ColumnFamily
            Unified
            BatchOp
        )
        ComposedBlock
        ComposedInv
        BlockId
mkComposedFollowing csmtOps =
    Backend.Following
        { Backend.follow = \(events, utxoOps) -> do
            cageInvs <-
                mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        events
            utxoInvs <-
                mapColumns InUtxo
                    $ concat
                        <$> mapM
                            ( \case
                                UTxO.Insert k v -> do
                                    csmtInsert csmtOps k v
                                    pure [UTxO.Delete k]
                                UTxO.Delete k -> do
                                    mx <- query KVCol k
                                    csmtDelete csmtOps k
                                    pure $ case mx of
                                        Nothing -> []
                                        Just old ->
                                            [UTxO.Insert k old]
                            )
                            utxoOps
            pure
                ( ComposedInv
                    { utxoInverses = utxoInvs
                    , cageInverses = cageInvs
                    }
                , Just (BlockId mempty)
                , mkComposedFollowing csmtOps
                )
        , Backend.toRestoring =
            pure $ mkComposedRestoring csmtOps
        , Backend.applyInverse =
            \ComposedInv{utxoInverses, cageInverses} ->
                do
                    mapColumns InCage
                        $ applyCageInverses
                            mkTransactionalState
                            mkUnifiedTrieManager
                            (reverse cageInverses)
                    mapColumns InUtxo
                        $ forM_
                            (reverse utxoInverses)
                        $ \case
                            UTxO.Insert k v ->
                                csmtInsert csmtOps k v
                            UTxO.Delete k ->
                                csmtDelete csmtOps k
        }

-- ---------------------------------------------------------
-- Fixtures
-- ---------------------------------------------------------

testFromKV :: FromKV BL.ByteString BL.ByteString Hash
testFromKV =
    FromKV
        { isoK = strict . isoK fromKVHashes
        , fromV = fromV fromKVHashes . view strict
        , treePrefix = const []
        }

testHashing :: Hashing Hash
testHashing = hashHashing

tokA :: TokenId
tokA =
    TokenId
        (AssetName (SBS.toShort ("token-a" :: BS.ByteString)))

tokStateA :: TokenState
tokStateA =
    TokenState
        { owner = testKeyHash
        , root = Root (BS.replicate 32 0)
        , tip = Coin 1_000_000
        , processTime = 100
        , retractTime = 200
        }

txInA :: TxIn
txInA = genTestTxIn 0

txInB :: TxIn
txInB = genTestTxIn 1

stRefA :: TxIn
stRefA = genTestTxIn 10

newStRefA :: TxIn
newStRefA = genTestTxIn 20

reqA :: Request
reqA =
    Request
        { requestToken = tokA
        , requestOwner = testKeyHash
        , requestKey = "key-1"
        , requestValue = Insert "value-1"
        , requestFee = Coin 500_000
        , requestSubmittedAt = 1000
        }

reqB :: Request
reqB =
    Request
        { requestToken = tokA
        , requestOwner = testKeyHash
        , requestKey = "key-2"
        , requestValue = Insert "value-2"
        , requestFee = Coin 600_000
        , requestSubmittedAt = 2000
        }

testKeyHash :: KeyHash Payment
testKeyHash =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "0000000000000000000000000000\
            \000000000000000000000000000a"

genTestTxIn :: Word16 -> TxIn
genTestTxIn ix =
    Ledger.TxIn
        ( Ledger.TxId
            $ unsafeMakeSafeHash
            $ fromJust
            $ hashFromStringAsHex @Blake2b_256
                "00000000000000000000000000000\
                \00000000000000000000000000000000001"
        )
        (TxIx ix)
