{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.TrieManagerSpec
-- Description : Tests for the TrieManager interface
-- License     : Apache-2.0
module Cardano.MPFS.TrieManagerSpec (spec) where

import Data.ByteString qualified as B
import Data.ByteString.Short qualified as SBS

import Data.Maybe (isJust)
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )

import Control.Exception (SomeException, try)
import Control.Monad (void)

import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Root (..)
    , TokenId (..)
    )
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )

-- -----------------------------------------------------------------
-- Deterministic token IDs for tests
-- -----------------------------------------------------------------

-- | A deterministic 'TokenId' for tests.
tokenA :: TokenId
tokenA = TokenId (AssetName (SBS.pack [1, 2, 3]))

-- | A second deterministic 'TokenId'.
tokenB :: TokenId
tokenB = TokenId (AssetName (SBS.pack [4, 5, 6]))

-- -----------------------------------------------------------------
-- Specs
-- -----------------------------------------------------------------

spec :: IO (TrieManager IO) -> Spec
spec newTM =
    describe "TrieManager" $ trieManagerSpec newTM

trieManagerSpec :: IO (TrieManager IO) -> Spec
trieManagerSpec newTM = do
    it "createTrie/withTrie round-trip" $ do
        tm <- newTM
        createTrie tm tokenA
        withTrie tm tokenA $ \trie -> do
            root <- getRoot trie
            unRoot root `shouldBe` B.replicate 32 0

    it "withTrie on missing trie throws" $ do
        tm <- newTM
        result <-
            try
                $ withTrie tm tokenA
                $ \_ -> pure ()
        case ( result
                :: Either SomeException ()
             ) of
            Left _ -> pure ()
            Right _ ->
                error
                    "Expected exception for \
                    \missing trie"

    it "deleteTrie removes trie" $ do
        tm <- newTM
        createTrie tm tokenA
        deleteTrie tm tokenA
        result <-
            try
                $ withTrie tm tokenA
                $ \_ -> pure ()
        case ( result
                :: Either SomeException ()
             ) of
            Left _ -> pure ()
            Right _ ->
                error
                    "Expected exception after \
                    \delete"

    it "per-token isolation" $ do
        tm <- newTM
        createTrie tm tokenA
        createTrie tm tokenB
        -- Insert into tokenA only
        withTrie tm tokenA $ \trie ->
            void $ insert trie "key" "value"
        -- tokenB should still be empty
        withTrie tm tokenB $ \trie -> do
            root <- getRoot trie
            unRoot root `shouldBe` B.replicate 32 0

    it "createTrie overwrites existing" $ do
        tm <- newTM
        createTrie tm tokenA
        withTrie tm tokenA $ \trie ->
            void $ insert trie "key" "value"
        -- Recreate should give empty trie
        createTrie tm tokenA
        withTrie tm tokenA $ \trie -> do
            root <- getRoot trie
            unRoot root `shouldBe` B.replicate 32 0

    it "withTrie insert reflects in root" $ do
        tm <- newTM
        createTrie tm tokenA
        withTrie tm tokenA $ \trie -> do
            root <- insert trie "hello" "world"
            unRoot root
                `shouldSatisfy` (not . B.null)

    it "deleteTrie on empty doesn't crash" $ do
        tm <- newTM
        deleteTrie tm tokenA
        pure ()

    it "speculative ops don't persist" $ do
        tm <- newTM
        createTrie tm tokenA
        -- Insert speculatively
        void
            $ withSpeculativeTrie tm tokenA
            $ \trie -> insert trie "k" "v"
        -- Real trie should still be empty
        withTrie tm tokenA $ \trie -> do
            root <- getRoot trie
            unRoot root `shouldBe` B.replicate 32 0

    it "speculative root matches real root" $ do
        tm <- newTM
        createTrie tm tokenA
        -- Insert via real trie
        realRoot <- withTrie tm tokenA $ \trie ->
            insert trie "a" "1"
        -- Insert same key via speculation
        specRoot <-
            withSpeculativeTrie tm tokenA
                $ \trie -> insert trie "a" "1"
        unRoot specRoot
            `shouldBe` unRoot realRoot

    it "hideTrie blocks withTrie" $ do
        tm <- newTM
        createTrie tm tokenA
        void
            $ withTrie tm tokenA
            $ \trie -> insert trie "k" "v"
        hideTrie tm tokenA
        result <-
            try
                $ withTrie tm tokenA
                $ \_ -> pure ()
        case ( result
                :: Either SomeException ()
             ) of
            Left _ -> pure ()
            Right _ ->
                error
                    "Expected exception for \
                    \hidden trie"

    it "unhideTrie restores access" $ do
        tm <- newTM
        createTrie tm tokenA
        rootBefore <- withTrie tm tokenA $ \trie ->
            insert trie "k" "v"
        hideTrie tm tokenA
        unhideTrie tm tokenA
        withTrie tm tokenA $ \trie -> do
            root <- getRoot trie
            root `shouldBe` rootBefore

    it "hide preserves data" $ do
        tm <- newTM
        createTrie tm tokenA
        _ <- withTrie tm tokenA $ \trie ->
            insert trie "hello" "world"
        hideTrie tm tokenA
        unhideTrie tm tokenA
        withTrie tm tokenA $ \trie -> do
            mVal <-
                Cardano.MPFS.Trie.lookup
                    trie
                    "hello"
            mVal `shouldSatisfy` isJust

    it "hide preserves other tokens" $ do
        tm <- newTM
        createTrie tm tokenA
        createTrie tm tokenB
        _ <- withTrie tm tokenA $ \trie ->
            insert trie "a" "1"
        rootB <- withTrie tm tokenB $ \trie ->
            insert trie "b" "2"
        hideTrie tm tokenA
        -- tokenB should be unaffected
        withTrie tm tokenB $ \trie -> do
            root <- getRoot trie
            root `shouldBe` rootB

    it "speculative read-your-writes" $ do
        tm <- newTM
        createTrie tm tokenA
        createTrie tm tokenB
        -- Do insert A, insert B, delete A
        -- speculatively on tokenA
        specRoot <-
            withSpeculativeTrie tm tokenA
                $ \trie -> do
                    void $ insert trie "a" "1"
                    void $ insert trie "b" "2"
                    void
                        $ Cardano.MPFS.Trie.delete
                            trie
                            "a"
                    getRoot trie
        -- Do the same via real trie on tokenB
        realRoot <- withTrie tm tokenB $ \trie -> do
            void $ insert trie "a" "1"
            void $ insert trie "b" "2"
            void $ Cardano.MPFS.Trie.delete trie "a"
            getRoot trie
        unRoot specRoot
            `shouldBe` unRoot realRoot
