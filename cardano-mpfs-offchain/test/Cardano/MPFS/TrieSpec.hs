{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.TrieSpec
-- Description : Property tests for the Trie interface
-- License     : Apache-2.0
module Cardano.MPFS.TrieSpec (spec) where

-- NOTE: This module is parameterized — 'spec' takes an
-- @IO (Trie IO)@ constructor so the same tests can run
-- against any backend (pure, RocksDB, …).

import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.List (nubBy)
import Data.Maybe (isJust, isNothing)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldReturn
    , shouldSatisfy
    )

import Test.QuickCheck
    ( Gen
    , Property
    , choose
    , forAll
    , listOf1
    , property
    , shuffle
    , vectorOf
    , (===)
    , (==>)
    )

import MPF.Hashes
    ( mkMPFHash
    , renderMPFHash
    )
import MPF.Interface (byteStringToHexKey)
import MPF.Test.Lib
    ( deleteMPFM
    , encodeHex
    , expectedFullTrieRoot
    , fruitsTestData
    , getRootHashM
    , insertByteStringM
    , runMPFPure'
    , verifyMPFM
    )

import Cardano.MPFS.Core.Types (Root (..))
import Cardano.MPFS.Trie (Trie (..))
import Cardano.MPFS.Trie.Pure (mkPureTrie)

-- -----------------------------------------------------------------
-- Generators
-- -----------------------------------------------------------------

-- | Generate a random ByteString key.
genKeyBytes :: Gen ByteString
genKeyBytes =
    B.pack <$> listOf1 (choose (0, 255))

-- | Generate a random ByteString value.
genValue :: Gen ByteString
genValue = B.pack <$> listOf1 (choose (0, 255))

-- | Hash key bytes (Aiken convention).
hashKey :: ByteString -> ByteString
hashKey = renderMPFHash . mkMPFHash

-- | Generate unique key-value pairs (unique by
-- hashed key).
genUniqueKVs :: Gen [(ByteString, ByteString)]
genUniqueKVs = do
    kvs <-
        listOf1 ((,) <$> genKeyBytes <*> genValue)
    pure
        $ nubBy
            ( \(k1, _) (k2, _) ->
                hashKey k1 == hashKey k2
            )
            kvs

-- -----------------------------------------------------------------
-- Specs
-- -----------------------------------------------------------------

spec :: IO (Trie IO) -> Spec
spec newTrie = do
    describe "Trie" $ trieSpec newTrie
    describe "Pure trie" pureTrieSpec
    describe "Trie test vectors"
        $ testVectorSpec newTrie
    describe "Trie properties" propertySpec

trieSpec :: IO (Trie IO) -> Spec
trieSpec newTrie = do
    it "insert/getRoot produces non-empty root" $ do
        trie <- newTrie
        root <- insert trie "hello" "world"
        unRoot root `shouldSatisfy` (not . B.null)

    it "insert/getRoot is deterministic" $ do
        trie1 <- newTrie
        trie2 <- newTrie
        root1 <- insert trie1 "hello" "world"
        root2 <- insert trie2 "hello" "world"
        root1 `shouldBe` root2

    it "insert/lookup finds the key" $ do
        trie <- newTrie
        _ <- insert trie "hello" "world"
        mVal <-
            Cardano.MPFS.Trie.lookup trie "hello"
        mVal `shouldSatisfy` isJust

    it "lookup on empty returns Nothing" $ do
        trie <- newTrie
        Cardano.MPFS.Trie.lookup trie "missing"
            `shouldReturn` Nothing

    it "delete removes key" $ do
        trie <- newTrie
        _ <- insert trie "hello" "world"
        _ <- Cardano.MPFS.Trie.delete trie "hello"
        mVal <-
            Cardano.MPFS.Trie.lookup trie "hello"
        mVal `shouldBe` Nothing

    it "delete preserves siblings" $ do
        trie <- newTrie
        _ <- insert trie "keep" "value1"
        _ <- insert trie "remove" "value2"
        _ <-
            Cardano.MPFS.Trie.delete trie "remove"
        mVal <-
            Cardano.MPFS.Trie.lookup trie "keep"
        mVal `shouldSatisfy` isJust

    it "getRoot on empty returns null root" $ do
        trie <- newTrie
        root <- getRoot trie
        unRoot root `shouldBe` B.replicate 32 0

    it "getProof for existing key" $ do
        trie <- newTrie
        _ <- insert trie "hello" "world"
        mProof <- getProof trie "hello"
        isJust mProof `shouldBe` True

    it "getProof for missing key" $ do
        trie <- newTrie
        _ <- insert trie "hello" "world"
        mProof <- getProof trie "nonexistent"
        isNothing mProof `shouldBe` True

    it "getProof on empty trie" $ do
        trie <- newTrie
        mProof <- getProof trie "any"
        isNothing mProof `shouldBe` True

pureTrieSpec :: Spec
pureTrieSpec = do
    it "pure_insert_then_lookup_returns_raw_value" $ do
        trie <- mkPureTrie
        _ <- insert trie "hello" "raw hello value"
        Cardano.MPFS.Trie.lookup trie "hello"
            `shouldReturn` Just "raw hello value"

    it "pure_delete_then_lookup_returns_nothing" $ do
        trie <- mkPureTrie
        _ <- insert trie "hello" "raw hello value"
        _ <- Cardano.MPFS.Trie.delete trie "hello"
        Cardano.MPFS.Trie.lookup trie "hello"
            `shouldReturn` Nothing

-- -----------------------------------------------------------------
-- Properties (via merkle-patricia-forestry pure backend)
-- -----------------------------------------------------------------

propertySpec :: Spec
propertySpec = do
    it "insertion order independence"
        $ property propInsertionOrder

    it "deleted key not verifiable"
        $ property propDeleteRemoves

    it "deletion preserves siblings"
        $ property propDeletePreserves

    it "single insert produces root"
        $ property propSingleInsertRoot

-- | Insertion order doesn't affect root hash.
propInsertionOrder :: Property
propInsertionOrder =
    forAll genUniqueKVs $ \kvs ->
        length kvs >= 2 ==>
            forAll (shuffle kvs) $ \shuffled ->
                let (root1, _) = runMPFPure' $ do
                        forM_ kvs
                            $ uncurry insertByteStringM
                        getRootHashM
                    (root2, _) = runMPFPure' $ do
                        forM_ shuffled
                            $ uncurry insertByteStringM
                        getRootHashM
                in  fmap renderMPFHash root1
                        === fmap renderMPFHash root2

-- | Deleted key cannot be verified.
propDeleteRemoves :: Property
propDeleteRemoves =
    forAll genKeyBytes $ \keyBs ->
        forAll genValue $ \valBs ->
            let key =
                    byteStringToHexKey
                        $ hashKey keyBs
                value = mkMPFHash valBs
                (verified, _) = runMPFPure' $ do
                    insertByteStringM keyBs valBs
                    deleteMPFM key
                    verifyMPFM key value
            in  not verified

-- | Deletion preserves sibling proofs.
propDeletePreserves :: Property
propDeletePreserves =
    forAll
        ( vectorOf
            3
            ((,) <$> genKeyBytes <*> genValue)
        )
        $ \rawKvs ->
            let kvs =
                    nubBy
                        ( \(k1, _) (k2, _) ->
                            hashKey k1 == hashKey k2
                        )
                        rawKvs
            in  length kvs == 3 ==>
                    let ((keepK, keepV), (delK, _)) =
                            case kvs of
                                (a : b : _) -> (a, b)
                                _ -> error "impossible"
                        keepHex =
                            byteStringToHexKey
                                $ hashKey keepK
                        keepHash = mkMPFHash keepV
                        delHex =
                            byteStringToHexKey
                                $ hashKey delK
                        (verified, _) = runMPFPure' $ do
                            forM_
                                kvs
                                ( uncurry
                                    insertByteStringM
                                )
                            deleteMPFM delHex
                            verifyMPFM
                                keepHex
                                keepHash
                    in  verified

-- | Single insert produces a root hash.
propSingleInsertRoot :: Property
propSingleInsertRoot =
    forAll genKeyBytes $ \keyBs ->
        forAll genValue $ \valBs ->
            let (mRoot, _) = runMPFPure' $ do
                    insertByteStringM keyBs valBs
                    getRootHashM
            in  case mRoot of
                    Just _ -> True
                    Nothing -> False

-- -----------------------------------------------------------------
-- Test vectors (Aiken compatibility)
-- -----------------------------------------------------------------

testVectorSpec :: IO (Trie IO) -> Spec
testVectorSpec newTrie = do
    it "single apple root hash" $ do
        trie <- newTrie
        root <-
            insert
                trie
                "apple[uid: 58]"
                "\xf0\x9f\x8d\x8e"
        encodeHex (unRoot root)
            `shouldBe` "93c4ed2d36f2409c38b8112d70c23eaf92eeb325b5098c0195be7e5cfaf7d824"

    it "apple + apricot root hash" $ do
        trie <- newTrie
        _ <-
            insert
                trie
                "apple[uid: 58]"
                "\xf0\x9f\x8d\x8e"
        root <-
            insert
                trie
                "apricot[uid: 0]"
                "\xf0\x9f\xa4\xb7"
        encodeHex (unRoot root)
            `shouldBe` "d9e614a87dff7b38d59706f00085d1b23f8c3e32ab9f5c39dbfa090412012003"

    it "apple + banana root hash" $ do
        trie <- newTrie
        _ <-
            insert
                trie
                "apple[uid: 58]"
                "\xf0\x9f\x8d\x8e"
        root <-
            insert
                trie
                "banana[uid: 218]"
                "\xf0\x9f\x8d\x8c"
        encodeHex (unRoot root)
            `shouldBe` "6a00036a5182ad02098cc99e00ab679263571dbec847b12aa7abde525affbe39"

    it "3 fruits root hash" $ do
        trie <- newTrie
        _ <-
            insert
                trie
                "apple[uid: 58]"
                "\xf0\x9f\x8d\x8e"
        _ <-
            insert
                trie
                "apricot[uid: 0]"
                "\xf0\x9f\xa4\xb7"
        root <-
            insert
                trie
                "banana[uid: 218]"
                "\xf0\x9f\x8d\x8c"
        encodeHex (unRoot root)
            `shouldBe` "3b9c8a23238aeef2bee260daec21acfdad07cb7d8f23bb5b97147323ef65ff5f"

    it "full fruits dataset root hash" $ do
        trie <- newTrie
        forM_ fruitsTestData
            $ uncurry (insert trie)
        root <- getRoot trie
        encodeHex (unRoot root)
            `shouldBe` encodeHex expectedFullTrieRoot

    it "proof verifies apple in full dataset" $ do
        trie <- newTrie
        forM_ fruitsTestData
            $ uncurry (insert trie)
        mProof <- getProof trie "apple[uid: 58]"
        isJust mProof `shouldBe` True
