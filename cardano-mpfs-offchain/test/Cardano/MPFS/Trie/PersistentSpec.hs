{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Trie.PersistentSpec
-- Description : Integration tests for RocksDB-backed trie
-- License     : Apache-2.0
--
-- Runs the parameterized 'TrieSpec' and
-- 'TrieManagerSpec' suites against the persistent
-- RocksDB backend. Also includes property-based
-- tests comparing persistent and pure backends,
-- and persistence-specific tests verifying data
-- survives DB close/reopen cycles.
module Cardano.MPFS.Trie.PersistentSpec
    ( -- * Test suite
      spec

      -- * Test utilities
    , withTestDB
    ) where

import Control.Exception (displayException, try)
import Control.Monad (forM_, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Short qualified as SBS
import Data.IORef
    ( IORef
    , atomicModifyIORef'
    )
import Data.List (isInfixOf, nubBy)
import Data.Maybe (isJust)
import Data.Word (Word8)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck
    ( Gen
    , Property
    , choose
    , forAll
    , ioProperty
    , listOf1
    , property
    , shuffle
    , vectorOf
    , (===)
    , (==>)
    )

import Cardano.Ledger.Mary.Value (AssetName (..))
import Database.RocksDB
    ( BatchOp
    , ColumnFamily
    , Config (..)
    , DB (..)
    , withDBCF
    )
import System.IO.Temp
    ( withSystemTempDirectory
    )

import MPF.Hashes
    ( mkMPFHash
    , renderMPFHash
    )
import MPF.Test.Lib
    ( encodeHex
    , expectedFullTrieRoot
    , fruitsTestData
    , getRootHashM
    , insertByteStringM
    , runMPFPure'
    )

import Cardano.MPFS.Core.Types
    ( Root (..)
    , TokenId (..)
    )
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( RunTransaction (..)
    , Transaction
    , newRunTransaction
    )
import Database.KV.Transaction qualified as KV
    ( insert
    , query
    )

import MPF.Interface (byteStringToHexKey)

import Cardano.MPFS.Application
    ( SchemaMigrationRequired (..)
    , cageColumnFamilies
    , checkSchemaMigration
    , dbConfig
    )
import Cardano.MPFS.Indexer.Codecs (allCodecs)
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageInverseOp (..)
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageInverses
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkTransactionalState
    )
import Cardano.MPFS.Trie.Persistent
    ( mkPersistentTrieManager
    , mkUnifiedTrieManager
    )
import Cardano.MPFS.TrieManagerSpec qualified as TrieManagerSpec
import Cardano.MPFS.TrieSpec qualified as TrieSpec

-- ---------------------------------------------------------
-- RocksDB config & helpers
-- ---------------------------------------------------------

-- | Default config for test RocksDB.
testConfig :: Config
testConfig =
    Config
        { createIfMissing = True
        , errorIfExists = False
        , paranoidChecks = False
        , maxFiles = Nothing
        , prefixLength = Nothing
        , bloomFilter = False
        }

-- | Column family definitions for tests.
testCFs :: [(String, Config)]
testCFs =
    [ ("nodes", testConfig)
    , ("kv", testConfig)
    , ("meta", testConfig)
    , ("raw", testConfig)
    ]

-- | Run an action with a temporary RocksDB that has
-- "nodes", "kv", and "meta" column families.
withTestDB
    :: ( DB
         -> ColumnFamily
         -> ColumnFamily
         -> ColumnFamily
         -> IO a
       )
    -> IO a
withTestDB action =
    withSystemTempDirectory "mpfs-test" $ \dir ->
        withTestDBAt dir action

-- | Open a RocksDB at a specific path with the
-- standard column families. Used for reopen tests
-- where the same directory is opened multiple times.
withTestDBAt
    :: FilePath
    -> ( DB
         -> ColumnFamily
         -> ColumnFamily
         -> ColumnFamily
         -> IO a
       )
    -> IO a
withTestDBAt dir action =
    withDBCF dir testConfig testCFs
        $ \db@DB{columnFamilies = cfs} ->
            case cfs of
                [nodesCF, kvCF, metaCF, _rawCF] ->
                    action db nodesCF kvCF metaCF
                _ ->
                    error
                        "Expected 4 column \
                        \families"

-- ---------------------------------------------------------
-- Generators
-- ---------------------------------------------------------

-- | Generate a random ByteString key.
genKeyBytes :: Gen ByteString
genKeyBytes =
    B.pack <$> listOf1 (choose (0, 255))

-- | Generate a random ByteString value.
genValue :: Gen ByteString
genValue =
    B.pack <$> listOf1 (choose (0, 255))

-- | Hash key bytes (Aiken convention).
hashKey :: ByteString -> ByteString
hashKey = renderMPFHash . mkMPFHash

-- | Generate unique key-value pairs (unique by
-- hashed key).
genUniqueKVs :: Gen [(ByteString, ByteString)]
genUniqueKVs = do
    kvs <-
        listOf1
            ((,) <$> genKeyBytes <*> genValue)
    pure
        $ nubBy
            ( \(k1, _) (k2, _) ->
                hashKey k1 == hashKey k2
            )
            kvs

-- ---------------------------------------------------------
-- Token ID helpers
-- ---------------------------------------------------------

-- | Generate a unique 'TokenId' from a counter.
nextTokenId :: IORef Int -> IO TokenId
nextTokenId ref =
    atomicModifyIORef' ref $ \n ->
        ( n + 1
        , TokenId
            $ AssetName
            $ SBS.pack
            $ encodeInt n
        )

-- | Encode an 'Int' as bytes.
encodeInt :: Int -> [Word8]
encodeInt n =
    [ fromIntegral (n `div` 256)
    , fromIntegral (n `mod` 256)
    ]

-- | Fixed token IDs for reopen tests (safe because
-- each reopen test uses its own temp directory).
reopenTidA :: TokenId
reopenTidA =
    TokenId (AssetName (SBS.pack [42, 1]))

reopenTidB :: TokenId
reopenTidB =
    TokenId (AssetName (SBS.pack [42, 2]))

-- ---------------------------------------------------------
-- Fresh persistent trie construction
-- ---------------------------------------------------------

-- | Create a persistent TrieManager with clean meta
-- state for TrieManagerSpec test tokens. Deletes
-- metaCF entries for the fixed token IDs used by
-- TrieManagerSpec so each test starts fresh.
freshTrieManager
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IO (TrieManager IO)
freshTrieManager db nodesCF kvCF metaCF = do
    mgr <-
        mkPersistentTrieManager
            db
            nodesCF
            kvCF
            metaCF
    -- Clean up TrieManagerSpec's fixed tokens
    let tmTokenA =
            TokenId
                (AssetName (SBS.pack [1, 2, 3]))
        tmTokenB =
            TokenId
                (AssetName (SBS.pack [4, 5, 6]))
    deleteTrie mgr tmTokenA
    deleteTrie mgr tmTokenB
    mkPersistentTrieManager
        db
        nodesCF
        kvCF
        metaCF

-- | Create a fresh persistent 'Trie IO' for a test.
-- Uses the counter to generate a unique 'TokenId',
-- ensuring isolation across test iterations.
newPersistentTrie
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> IO (Trie IO)
newPersistentTrie db nodesCF kvCF metaCF counterRef =
    do
        tm <-
            mkPersistentTrieManager
                db
                nodesCF
                kvCF
                metaCF
        tid <- nextTokenId counterRef
        createTrie tm tid
        withTrie tm tid pure

-- ---------------------------------------------------------
-- Top-level spec
-- ---------------------------------------------------------

-- | Run all persistent trie tests.
spec
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Spec
spec db nodesCF kvCF metaCF counterRef = do
    describe "Persistent Trie"
        $ TrieSpec.spec
            ( newPersistentTrie
                db
                nodesCF
                kvCF
                metaCF
                counterRef
            )
    describe "Persistent TrieManager"
        $ TrieManagerSpec.spec
            ( freshTrieManager
                db
                nodesCF
                kvCF
                metaCF
            )
    describe "Persistent properties"
        $ propertySpec
            db
            nodesCF
            kvCF
            metaCF
            counterRef
    describe "Persistence-specific"
        $ persistenceSpec
            db
            nodesCF
            kvCF
            metaCF
            counterRef
    describe
        "Cross-layer consistency"
        crossLayerSpec
    describe
        "TrieRawValues column + schema check"
        schemaSpec
    describe
        "Value-bearing lookup (#247 Slice 2)"
        valueBearingLookupSpec

-- ---------------------------------------------------------
-- Property-based tests
-- ---------------------------------------------------------

propertySpec
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Spec
propertySpec db nodesCF kvCF metaCF counterRef = do
    it "same root as pure backend"
        $ property
        $ propPureEquivalentRoot
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "insertion order independence"
        $ property
        $ propInsertOrderPersistent
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "deleted key not verifiable"
        $ property
        $ propDeleteRemovesPersistent
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "deletion preserves siblings"
        $ property
        $ propDeletePreservesSiblingsPersistent
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "per-token isolation"
        $ property
        $ propTokenIsolation
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "createTrie overwrites existing"
        $ property
        $ propCreateOverwrites
            db
            nodesCF
            kvCF
            metaCF
            counterRef

    it "delete then re-insert restores root"
        $ property
        $ propDeleteInsertRoundtrip
            db
            nodesCF
            kvCF
            metaCF
            counterRef

-- | Same operations produce same root on pure and
-- persistent backends.
propPureEquivalentRoot
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propPureEquivalentRoot
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \kvs ->
            not (null kvs) ==>
                ioProperty $ do
                    let (mRoot, _) = runMPFPure' $ do
                            forM_ kvs
                                $ uncurry
                                    insertByteStringM
                            getRootHashM
                        pureRoot =
                            maybe
                                (B.replicate 32 0)
                                renderMPFHash
                                mRoot
                    trie <-
                        newPersistentTrie
                            db
                            nodesCF
                            kvCF
                            metaCF
                            counterRef
                    forM_ kvs
                        $ uncurry (insert trie)
                    Root persistRoot <- getRoot trie
                    pure (pureRoot === persistRoot)

-- | Insertion order doesn't affect root hash on
-- the persistent backend.
propInsertOrderPersistent
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propInsertOrderPersistent
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \kvs ->
            length kvs >= 2 ==>
                forAll (shuffle kvs) $ \shuffled ->
                    ioProperty $ do
                        trie1 <-
                            newPersistentTrie
                                db
                                nodesCF
                                kvCF
                                metaCF
                                counterRef
                        forM_ kvs
                            $ uncurry (insert trie1)
                        root1 <- getRoot trie1

                        trie2 <-
                            newPersistentTrie
                                db
                                nodesCF
                                kvCF
                                metaCF
                                counterRef
                        forM_ shuffled
                            $ uncurry (insert trie2)
                        root2 <- getRoot trie2
                        pure (root1 === root2)

-- | Deleted key cannot be looked up on a
-- non-empty persistent trie.
propDeleteRemovesPersistent
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propDeleteRemovesPersistent
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \bg ->
            not (null bg) ==>
                forAll genKeyBytes $ \keyBs ->
                    forAll genValue $ \valBs ->
                        let hk = hashKey keyBs
                            noCollision =
                                all
                                    ( (/= hk)
                                        . hashKey
                                        . fst
                                    )
                                    bg
                        in  noCollision ==>
                                ioProperty $ do
                                    trie <-
                                        newPersistentTrie
                                            db
                                            nodesCF
                                            kvCF
                                            metaCF
                                            counterRef
                                    forM_ bg
                                        $ uncurry
                                            (insert trie)
                                    _ <-
                                        insert
                                            trie
                                            keyBs
                                            valBs
                                    _ <-
                                        Cardano.MPFS.Trie.delete
                                            trie
                                            keyBs
                                    mVal <-
                                        Cardano.MPFS.Trie.lookup
                                            trie
                                            keyBs
                                    pure
                                        (mVal === Nothing)

-- | Deleting one key preserves siblings on a
-- non-empty persistent trie.
propDeletePreservesSiblingsPersistent
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propDeletePreservesSiblingsPersistent
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \bg ->
            not (null bg) ==>
                forAll
                    ( vectorOf
                        3
                        ( (,)
                            <$> genKeyBytes
                            <*> genValue
                        )
                    )
                    $ \rawKvs ->
                        let kvs =
                                nubBy
                                    ( \(k1, _) (k2, _) ->
                                        hashKey k1
                                            == hashKey k2
                                    )
                                    rawKvs
                            allHashes =
                                map
                                    (hashKey . fst)
                                    bg
                            noCollision =
                                all
                                    ( (`notElem` allHashes)
                                        . hashKey
                                        . fst
                                    )
                                    kvs
                        in  length kvs == 3
                                && noCollision
                                ==> let ( (keepK, _)
                                            , (delK, _)
                                            ) =
                                                case kvs of
                                                    (a : b : _) ->
                                                        (a, b)
                                                    _ ->
                                                        error
                                                            "impossible"
                                    in  ioProperty $ do
                                            trie <-
                                                newPersistentTrie
                                                    db
                                                    nodesCF
                                                    kvCF
                                                    metaCF
                                                    counterRef
                                            forM_ bg
                                                $ uncurry
                                                    (insert trie)
                                            forM_ kvs
                                                $ uncurry
                                                    (insert trie)
                                            _ <-
                                                Cardano.MPFS.Trie.delete
                                                    trie
                                                    delK
                                            mVal <-
                                                Cardano.MPFS.Trie.lookup
                                                    trie
                                                    keepK
                                            pure
                                                (isJust mVal)

-- | Random operations on token A don't affect
-- token B's root.
propTokenIsolation
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propTokenIsolation
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \kvs ->
            not (null kvs) ==>
                ioProperty $ do
                    tm <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    tidA <- nextTokenId counterRef
                    tidB <- nextTokenId counterRef
                    createTrie tm tidA
                    createTrie tm tidB
                    withTrie tm tidA $ \trie ->
                        forM_ kvs
                            $ uncurry (insert trie)
                    withTrie tm tidB $ \trie -> do
                        Root root <- getRoot trie
                        pure
                            ( root
                                === B.replicate 32 0
                            )

-- | Creating a token that already has data resets
-- its root to empty.
propCreateOverwrites
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propCreateOverwrites
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \kvs ->
            not (null kvs) ==>
                ioProperty $ do
                    tm <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    tid <- nextTokenId counterRef
                    createTrie tm tid
                    withTrie tm tid $ \trie ->
                        forM_ kvs
                            $ uncurry (insert trie)
                    createTrie tm tid
                    withTrie tm tid $ \trie -> do
                        Root root <- getRoot trie
                        pure
                            ( root
                                === B.replicate 32 0
                            )

-- | Delete then re-insert restores root on a
-- non-empty persistent trie.
propDeleteInsertRoundtrip
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Property
propDeleteInsertRoundtrip
    db
    nodesCF
    kvCF
    metaCF
    counterRef =
        forAll genUniqueKVs $ \bg ->
            not (null bg) ==>
                forAll genKeyBytes $ \keyBs ->
                    forAll genValue $ \valBs ->
                        let hk = hashKey keyBs
                            noCollision =
                                all
                                    ( (/= hk)
                                        . hashKey
                                        . fst
                                    )
                                    bg
                        in  noCollision ==>
                                ioProperty $ do
                                    trie <-
                                        newPersistentTrie
                                            db
                                            nodesCF
                                            kvCF
                                            metaCF
                                            counterRef
                                    forM_ bg
                                        $ uncurry
                                            (insert trie)
                                    root1 <-
                                        insert
                                            trie
                                            keyBs
                                            valBs
                                    _ <-
                                        Cardano.MPFS.Trie.delete
                                            trie
                                            keyBs
                                    root2 <-
                                        insert
                                            trie
                                            keyBs
                                            valBs
                                    pure
                                        (root1 === root2)

-- ---------------------------------------------------------
-- Persistence-specific unit tests
-- ---------------------------------------------------------

persistenceSpec
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef Int
    -> Spec
persistenceSpec db nodesCF kvCF metaCF counterRef = do
    it
        "data persists across DB reopen"
        persistsAcrossReopen

    it
        "deleted trie stays deleted after reopen"
        deletedTrieStaysDeletedAfterReopen

    it
        "multiple tokens survive reopen"
        multipleToksSurviveReopen

    it "fruit test vectors on persistent" $ do
        trie <-
            newPersistentTrie
                db
                nodesCF
                kvCF
                metaCF
                counterRef
        forM_ fruitsTestData
            $ uncurry (insert trie)
        root <- getRoot trie
        encodeHex (unRoot root)
            `shouldBe` encodeHex
                expectedFullTrieRoot

-- | Insert data, close DB, reopen, verify data is
-- still present (no registerTrie needed).
persistsAcrossReopen :: IO ()
persistsAcrossReopen =
    withSystemTempDirectory "reopen" $ \dir -> do
        -- Phase 1: insert data
        root1 <-
            withTestDBAt dir
                $ \db nodesCF kvCF metaCF -> do
                    mgr <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    createTrie mgr reopenTidA
                    withTrie mgr reopenTidA
                        $ \trie -> do
                            _ <-
                                insert
                                    trie
                                    "hello"
                                    "world"
                            getRoot trie
        -- Phase 2: reopen and verify
        withTestDBAt dir
            $ \db nodesCF kvCF metaCF -> do
                mgr <-
                    mkPersistentTrieManager
                        db
                        nodesCF
                        kvCF
                        metaCF
                withTrie mgr reopenTidA $ \trie ->
                    do
                        root2 <- getRoot trie
                        root2 `shouldBe` root1

-- | Delete trie, close DB, reopen, verify data is
-- gone.
deletedTrieStaysDeletedAfterReopen :: IO ()
deletedTrieStaysDeletedAfterReopen =
    withSystemTempDirectory "reopen-del" $ \dir ->
        do
            -- Phase 1: insert then delete
            withTestDBAt dir
                $ \db nodesCF kvCF metaCF -> do
                    mgr <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    createTrie mgr reopenTidA
                    withTrie mgr reopenTidA
                        $ \trie ->
                            void
                                $ insert
                                    trie
                                    "hello"
                                    "world"
                    deleteTrie mgr reopenTidA
            -- Phase 2: reopen and verify gone
            withTestDBAt dir
                $ \db nodesCF kvCF metaCF -> do
                    mgr <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    createTrie mgr reopenTidA
                    withTrie mgr reopenTidA
                        $ \trie -> do
                            Root root <-
                                getRoot trie
                            root
                                `shouldBe` B.replicate
                                    32
                                    0

-- | Two tokens with data, close + reopen, both
-- intact (no registerTrie needed).
multipleToksSurviveReopen :: IO ()
multipleToksSurviveReopen =
    withSystemTempDirectory "reopen-multi"
        $ \dir -> do
            -- Phase 1: insert into both tokens
            (rootA1, rootB1) <-
                withTestDBAt dir
                    $ \db nodesCF kvCF metaCF -> do
                        mgr <-
                            mkPersistentTrieManager
                                db
                                nodesCF
                                kvCF
                                metaCF
                        createTrie mgr reopenTidA
                        createTrie mgr reopenTidB
                        rA <-
                            withTrie
                                mgr
                                reopenTidA
                                $ \trie -> do
                                    _ <-
                                        insert
                                            trie
                                            "key-a"
                                            "val-a"
                                    getRoot trie
                        rB <-
                            withTrie
                                mgr
                                reopenTidB
                                $ \trie -> do
                                    _ <-
                                        insert
                                            trie
                                            "key-b"
                                            "val-b"
                                    getRoot trie
                        pure (rA, rB)
            -- Phase 2: reopen and verify both
            withTestDBAt dir
                $ \db nodesCF kvCF metaCF -> do
                    mgr <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    withTrie mgr reopenTidA
                        $ \trie -> do
                            rootA2 <- getRoot trie
                            rootA2 `shouldBe` rootA1
                    withTrie mgr reopenTidB
                        $ \trie -> do
                            rootB2 <- getRoot trie
                            rootB2 `shouldBe` rootB1
                    -- Verify isolation still holds
                    rootA <-
                        withTrie mgr reopenTidA
                            $ \trie -> getRoot trie
                    rootB <-
                        withTrie mgr reopenTidB
                            $ \trie -> getRoot trie
                    rootA
                        `shouldSatisfy` (/= rootB)

-- ---------------------------------------------------------
-- Cross-layer consistency tests
-- ---------------------------------------------------------

-- | Verify that data written by the transactional
-- layer (mkUnifiedTrie / mkUnifiedTrieManager) is
-- readable by the IO layer (mkPersistentTrieManager).
--
-- This is the code path exercised in production:
-- CageFollower writes via unified transactions,
-- HTTP server reads via the persistent IO manager.
crossLayerSpec :: Spec
crossLayerSpec = do
    it
        "IO layer reads data written by \
        \transactional layer"
        crossLayerReadAfterWrite
    it
        "speculative batch insert produces \
        \valid proofs"
        speculativeBatchInsert

-- | Write via transactional layer, read via IO
-- layer. Checks root, lookup, and proof.
crossLayerReadAfterWrite :: IO ()
crossLayerReadAfterWrite =
    withSystemTempDirectory "cross-layer"
        $ \dir ->
            withDBCF dir dbConfig cageColumnFamilies
                $ \db -> do
                    let cfs = columnFamilies db
                    -- Set up transactional layer
                    let columns =
                            mkColumns cfs allCodecs
                        database =
                            mkRocksDBDatabase
                                db
                                columns
                    RunTransaction{runTransaction} <-
                        newRunTransaction database

                    let tid = reopenTidA

                    -- Phase 1: write via transactional
                    -- layer (like CageFollower)
                    txRoot <- runTransaction $ do
                        let tm = mkUnifiedTrieManager
                        createTrie tm tid
                        withTrie tm tid $ \trie ->
                            insert trie "hello" "world"

                    -- Phase 2: read via IO layer
                    -- (like HTTP server)
                    case drop 3 cfs of
                        ( nodesCF
                                : kvCF
                                : metaCF
                                : _
                            ) -> do
                                mgr <-
                                    mkPersistentTrieManager
                                        db
                                        nodesCF
                                        kvCF
                                        metaCF
                                withTrie mgr tid
                                    $ \trie -> do
                                        -- Root must match
                                        ioRoot <-
                                            getRoot trie
                                        ioRoot
                                            `shouldBe` txRoot
                                        -- Root must be
                                        -- non-empty
                                        ioRoot
                                            `shouldSatisfy` ( \(Root r) ->
                                                                r
                                                                    /= B.empty
                                                            )
                                        -- Lookup must find
                                        -- the key
                                        mVal <-
                                            Cardano.MPFS.Trie.lookup
                                                trie
                                                "hello"
                                        mVal
                                            `shouldSatisfy` isJust
                                        -- Proof must exist
                                        mProof <-
                                            getProof
                                                trie
                                                "hello"
                                        isJust mProof
                                            `shouldBe` True
                        _ ->
                            error
                                "Expected at least \
                                \7 column families"

-- | Mimics the e2e batch update: create a trie
-- via IO layer, then speculatively insert 2 keys,
-- get proof steps for each, and verify root is
-- non-empty. This is what updateToken does.
speculativeBatchInsert :: IO ()
speculativeBatchInsert =
    withTestDB
        $ \db nodesCF kvCF metaCF -> do
            mgr <-
                mkPersistentTrieManager
                    db
                    nodesCF
                    kvCF
                    metaCF
            let tid = reopenTidA
            createTrie mgr tid
            -- Speculative batch insert (like
            -- updateToken with 2 requests)
            (steps1, steps2, newRoot) <-
                withSpeculativeTrie
                    mgr
                    tid
                    $ \trie -> do
                        -- Insert key1, get proof
                        _ <-
                            insert trie "key1" "val1"
                        s1 <-
                            getProofSteps trie "key1"
                        -- Insert key2, get proof
                        _ <-
                            insert trie "key2" "val2"
                        s2 <-
                            getProofSteps trie "key2"
                        r <- getRoot trie
                        pure (s1, s2, r)
            -- Root must be non-empty
            newRoot
                `shouldSatisfy` ( \(Root r) ->
                                    r /= B.empty
                                )
            -- Both proofs must exist
            isJust steps1 `shouldBe` True
            isJust steps2 `shouldBe` True

-- ---------------------------------------------------------
-- TrieRawValues column + schema-migration check (#247)
-- ---------------------------------------------------------

-- | Issue #247 Slice 1: the indexer gains a new
-- @TrieRawValues@ column family that stores raw
-- value bytes alongside the existing @TrieKV@
-- (key-hash → value-hash) map. A startup
-- pre-flight refuses to open a pre-#247 RocksDB
-- whose @TrieKV@ has rows but whose
-- @TrieRawValues@ is empty.
schemaSpec :: Spec
schemaSpec = do
    it
        "opens with TrieRawValues column \
        \(fresh DB exposes empty queryable CF)"
        opensWithTrieRawValuesColumn
    it
        "inserts into TrieRawValues column \
        \(round-trip raw bytes)"
        insertsIntoTrieRawValuesColumn
    it
        "refuses to start on stale schema \
        \(TrieKV non-empty + TrieRawValues empty)"
        refusesToStartOnStaleSchema

-- | A fresh database exposes @TrieRawValues@ as
-- an empty, queryable column family. Querying a
-- key returns 'Nothing' without throwing.
opensWithTrieRawValuesColumn :: IO ()
opensWithTrieRawValuesColumn =
    withSystemTempDirectory "trie-raw-values" $ \dir ->
        withDBCF dir dbConfig cageColumnFamilies
            $ \db -> do
                let cfs = columnFamilies db
                    columns = mkColumns cfs allCodecs
                    database =
                        mkRocksDBDatabase db columns
                RunTransaction{runTransaction} <-
                    newRunTransaction database
                let absentKey =
                        byteStringToHexKey "absent"
                result <-
                    runTransaction
                        $ KV.query TrieRawValues absentKey
                result `shouldBe` Nothing

-- | Inserting a raw value into @TrieRawValues@
-- and reading it back returns the exact bytes
-- (identity codec, no hashing).
insertsIntoTrieRawValuesColumn :: IO ()
insertsIntoTrieRawValuesColumn =
    withSystemTempDirectory "trie-raw-values-rt" $ \dir ->
        withDBCF dir dbConfig cageColumnFamilies
            $ \db -> do
                let cfs = columnFamilies db
                    columns = mkColumns cfs allCodecs
                    database =
                        mkRocksDBDatabase db columns
                RunTransaction{runTransaction} <-
                    newRunTransaction database
                let k =
                        byteStringToHexKey "the-key"
                    v = "the raw value bytes" :: ByteString
                runTransaction
                    $ KV.insert TrieRawValues k v
                result <-
                    runTransaction
                        $ KV.query TrieRawValues k
                result `shouldBe` Just v

-- | Opening a pre-#247 database — one whose
-- @TrieKV@ carries rows but whose
-- @TrieRawValues@ is empty — must throw
-- 'SchemaMigrationRequired' with a message
-- naming the resync step.
refusesToStartOnStaleSchema :: IO ()
refusesToStartOnStaleSchema =
    withSystemTempDirectory "stale-schema" $ \dir ->
        withDBCF dir dbConfig cageColumnFamilies
            $ \db -> do
                let cfs = columnFamilies db
                    columns = mkColumns cfs allCodecs
                    database =
                        mkRocksDBDatabase db columns
                RunTransaction{runTransaction} <-
                    newRunTransaction database
                -- Fabricate pre-migration state:
                -- TrieKV has a row, TrieRawValues
                -- is empty.
                let k =
                        byteStringToHexKey "stale-key"
                    vh = mkMPFHash "stale-value"
                runTransaction $ KV.insert TrieKV k vh
                result <-
                    try @SchemaMigrationRequired
                        (checkSchemaMigration runTransaction)
                case result of
                    Left exc ->
                        displayException exc
                            `shouldSatisfy` ( "drop the RocksDB directory and resync from genesis"
                                                `isInfixOf`
                                            )
                    Right () ->
                        expectationFailure
                            "expected SchemaMigrationRequired \
                            \but schema check succeeded"

-- ---------------------------------------------------------
-- Value-bearing lookup (#247 Slice 2)
-- ---------------------------------------------------------

-- | Issue #247 Slice 2: 'Trie.lookup' must return
-- the original raw value bytes (read from
-- 'TrieRawValues'), not @hashBS k@. Insert and
-- delete must update both the existing
-- 'TrieKV'\/'TrieNodes' columns and the new
-- 'TrieRawValues' column atomically (INV-1).
-- Rollback machinery re-uses the same 'insert' /
-- 'delete', so a replayed 'InvTrieInsert' must
-- restore both columns to the previous value
-- (INV-2).
valueBearingLookupSpec :: Spec
valueBearingLookupSpec = do
    it
        "insert then lookup returns the raw value \
        \(not hashBS k)"
        insertThenLookupReturnsRawValue
    it
        "delete then lookup returns Nothing and \
        \clears the TrieRawValues row"
        deleteThenLookupReturnsNothing
    it
        "rollback restores both TrieKV and \
        \TrieRawValues (INV-2)"
        rollbackUndoesBothColumns

-- | Fixed token id for slice-2 tests.
slice2TokenId :: TokenId
slice2TokenId =
    TokenId (AssetName (SBS.pack [247, 2]))

-- | Bracket: open a unified-cage DB with all 7
-- cage column families, build the transactional
-- 'mkUnifiedTrieManager', create a fresh trie for
-- 'slice2TokenId', and run the action with the
-- transaction runner.
withValueBearingDB
    :: ( ( forall a
            . Transaction
                IO
                ColumnFamily
                AllColumns
                BatchOp
                a
           -> IO a
         )
         -> IO ()
       )
    -> IO ()
withValueBearingDB action =
    withSystemTempDirectory "slice2" $ \dir ->
        withDBCF dir dbConfig cageColumnFamilies
            $ \db -> do
                let cfs = columnFamilies db
                    columns = mkColumns cfs allCodecs
                    database =
                        mkRocksDBDatabase db columns
                RunTransaction{runTransaction} <-
                    newRunTransaction database
                runTransaction
                    $ createTrie
                        mkUnifiedTrieManager
                        slice2TokenId
                action runTransaction

-- | Insert a key with a non-trivial value, then
-- look it up: the value-bearing contract requires
-- 'Trie.lookup' to return the raw value, not
-- @hashBS k@.
insertThenLookupReturnsRawValue :: IO ()
insertThenLookupReturnsRawValue =
    withValueBearingDB $ \runTransaction -> do
        let tid = slice2TokenId
            tm = mkUnifiedTrieManager
            k = "the-key" :: ByteString
            v = "hello" :: ByteString
        runTransaction
            $ withTrie tm tid
            $ \trie -> void $ insert trie k v
        mVal <-
            runTransaction
                $ withTrie tm tid
                $ \trie ->
                    Cardano.MPFS.Trie.lookup trie k
        mVal `shouldBe` Just v

-- | Insert wires the raw value into
-- 'TrieRawValues' (precondition, fails in RED).
-- Delete clears both columns (postcondition).
deleteThenLookupReturnsNothing :: IO ()
deleteThenLookupReturnsNothing =
    withValueBearingDB $ \runTransaction -> do
        let tid = slice2TokenId
            tm = mkUnifiedTrieManager
            k = "the-key" :: ByteString
            v = "world" :: ByteString
            hexKey =
                byteStringToHexKey
                    (renderMPFHash (mkMPFHash k))
        -- Phase 1: insert
        runTransaction
            $ withTrie tm tid
            $ \trie -> void $ insert trie k v
        -- Precondition: the raw value is in
        -- TrieRawValues. Fails in RED because
        -- unifiedInsert never wrote it.
        rawAfterInsert <-
            runTransaction
                $ KV.query TrieRawValues hexKey
        rawAfterInsert `shouldBe` Just v
        -- Phase 2: delete
        runTransaction
            $ withTrie tm tid
            $ \trie ->
                void
                    $ Cardano.MPFS.Trie.delete
                        trie
                        k
        -- Postcondition 1: trie lookup is Nothing.
        mVal <-
            runTransaction
                $ withTrie tm tid
                $ \trie ->
                    Cardano.MPFS.Trie.lookup trie k
        mVal `shouldBe` Nothing
        -- Postcondition 2: the raw column is
        -- empty for this key.
        rawAfterDelete <-
            runTransaction
                $ KV.query TrieRawValues hexKey
        rawAfterDelete `shouldBe` Nothing

-- | INV-2 verification. Insert (k, v), then
-- overwrite with (k, v'), then replay the
-- inverse op 'InvTrieInsert tid k v' through
-- 'applyCageInverses'. Both 'TrieKV' /
-- 'TrieNodes' and 'TrieRawValues' must reflect
-- @v@ (not @v'@, not absent) after rollback.
rollbackUndoesBothColumns :: IO ()
rollbackUndoesBothColumns =
    withValueBearingDB $ \runTransaction -> do
        let tid = slice2TokenId
            tm = mkUnifiedTrieManager
            k = "the-key" :: ByteString
            valueA = "value-A" :: ByteString
            valueB = "value-B" :: ByteString
            hexKey =
                byteStringToHexKey
                    (renderMPFHash (mkMPFHash k))
        -- Forward block 1: insert (k, valueA)
        runTransaction
            $ withTrie tm tid
            $ \trie ->
                void $ insert trie k valueA
        -- Forward block 2: overwrite with valueB
        runTransaction
            $ withTrie tm tid
            $ \trie ->
                void $ insert trie k valueB
        -- Rollback: replay the inverse of block
        -- 2, which is "re-insert the old value
        -- valueA". The brief notes that the
        -- existing rollback path uses
        -- 'applyCageInverses' on top of the
        -- 'Trie m' interface — so making
        -- 'unifiedInsert' write both columns is
        -- enough to make rollback atomic.
        runTransaction
            $ applyCageInverses
                mkTransactionalState
                tm
                [InvTrieInsert tid k valueA]
        -- Assertion 1: 'Trie.lookup' returns
        -- valueA. Fails in RED because lookup
        -- still returns 'Just (hashBS k)'.
        mVal <-
            runTransaction
                $ withTrie tm tid
                $ \trie ->
                    Cardano.MPFS.Trie.lookup trie k
        mVal `shouldBe` Just valueA
        -- Assertion 2: 'TrieRawValues' has valueA.
        -- Fails in RED because TrieRawValues was
        -- never written.
        mRaw <-
            runTransaction
                $ KV.query TrieRawValues hexKey
        mRaw `shouldBe` Just valueA
