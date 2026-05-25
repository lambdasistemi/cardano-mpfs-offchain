{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Trie.PureManager
-- Description : Pure in-memory TrieManager
-- License     : Apache-2.0
--
-- In-memory implementation of the 'TrieManager'
-- interface backed by a 'Map TokenId (IORef
-- PureTrieState)'. Each token gets its own isolated
-- in-memory MPF database plus raw-value mirror;
-- speculative sessions copy the 'IORef' contents so
-- the original is never mutated.
--
-- Useful for unit tests and development where
-- RocksDB is not available or not desired. For
-- production use "Cardano.MPFS.Trie.Persistent".
module Cardano.MPFS.Trie.PureManager
    ( -- * Construction
      mkPureTrieManager
    ) where

import Data.IORef
    ( IORef
    , modifyIORef'
    , newIORef
    , readIORef
    )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set

import MPF.Backend.Pure
    ( emptyMPFInMemoryDB
    )

import Cardano.MPFS.Core.Types (TokenId)
import Cardano.MPFS.Trie (Trie, TrieManager (..))
import Cardano.MPFS.Trie.Pure
    ( PureTrieState (..)
    , mkPureTrieFromRef
    )

-- | Create a new 'TrieManager IO' backed by a 'Map'
-- of per-token in-memory MPF databases. Each token
-- gets its own isolated trie.
mkPureTrieManager :: IO (TrieManager IO)
mkPureTrieManager = do
    ref <-
        newIORef
            ( Map.empty
                :: Map TokenId (IORef PureTrieState)
            )
    hiddenRef <- newIORef (Set.empty :: Set TokenId)
    pure
        TrieManager
            { withTrie = pureWithTrie ref hiddenRef
            , withSpeculativeTrie =
                pureWithSpeculativeTrie ref hiddenRef
            , createTrie = pureCreateTrie ref
            , deleteTrie = pureDeleteTrie ref
            , hideTrie = pureHideTrie hiddenRef
            , unhideTrie = pureUnhideTrie hiddenRef
            }

-- | Run an action with access to a token's trie.
-- Throws if the trie doesn't exist or is hidden.
pureWithTrie
    :: IORef (Map TokenId (IORef PureTrieState))
    -> IORef (Set TokenId)
    -> TokenId
    -> (Trie IO -> IO a)
    -> IO a
pureWithTrie ref hiddenRef tid action = do
    hidden <- readIORef hiddenRef
    if Set.member tid hidden
        then
            error
                $ "Trie is hidden: " ++ show tid
        else do
            tries <- readIORef ref
            case Map.lookup tid tries of
                Nothing ->
                    error
                        $ "Trie not found: " ++ show tid
                Just dbRef ->
                    action (mkPureTrieFromRef dbRef)

-- | Run a speculative session on a copy of the
-- trie. The copy is discarded after the action.
pureWithSpeculativeTrie
    :: IORef (Map TokenId (IORef PureTrieState))
    -> IORef (Set TokenId)
    -> TokenId
    -> (forall n. Monad n => Trie n -> n a)
    -> IO a
pureWithSpeculativeTrie ref hiddenRef tid action = do
    hidden <- readIORef hiddenRef
    if Set.member tid hidden
        then
            error
                $ "Trie is hidden: " ++ show tid
        else do
            tries <- readIORef ref
            case Map.lookup tid tries of
                Nothing ->
                    error
                        $ "Trie not found: "
                            ++ show tid
                Just dbRef -> do
                    db <- readIORef dbRef
                    copyRef <- newIORef db
                    action (mkPureTrieFromRef copyRef)

-- | Create a new empty trie for a token.
pureCreateTrie
    :: IORef (Map TokenId (IORef PureTrieState))
    -> TokenId
    -> IO ()
pureCreateTrie ref tid = do
    dbRef <-
        newIORef
            $ PureTrieState
                { ptsMpfDb = emptyMPFInMemoryDB
                , ptsRawValues = Map.empty
                }
    modifyIORef' ref (Map.insert tid dbRef)

-- | Delete a token's trie (permanent removal).
pureDeleteTrie
    :: IORef (Map TokenId (IORef PureTrieState))
    -> TokenId
    -> IO ()
pureDeleteTrie ref =
    modifyIORef' ref . Map.delete

-- | Mark a token's trie as hidden. Data is preserved.
pureHideTrie :: IORef (Set TokenId) -> TokenId -> IO ()
pureHideTrie hiddenRef tid =
    modifyIORef' hiddenRef (Set.insert tid)

-- | Restore a hidden token's trie to visible.
pureUnhideTrie
    :: IORef (Set TokenId) -> TokenId -> IO ()
pureUnhideTrie hiddenRef tid =
    modifyIORef' hiddenRef (Set.delete tid)
