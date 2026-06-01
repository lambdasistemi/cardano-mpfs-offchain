{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Trie
-- Description : Per-token MPF trie management interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for managing
-- per-token Merkle Patricia Forestry tries. The
-- 'TrieManager' mediates access to individual 'Trie'
-- handles and provides speculative (read-your-writes,
-- then-discard) sessions for transaction building.
--
-- Implementations: "Cardano.MPFS.Trie.PureManager"
-- (in-memory 'IORef'-based) and
-- "Cardano.MPFS.Trie.Persistent" (RocksDB-backed).
module Cardano.MPFS.Trie
    ( -- * Trie manager
      TrieManager (..)

      -- * Single trie operations
    , Trie (..)

      -- * Proof
    , Proof (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.MPFS.Core.OnChain (ProofStep)
import Cardano.MPFS.Core.Types (Root, TokenId)

-- | Serialised Merkle inclusion proof.
-- Opaque at this layer; produced by the trie
-- implementation and consumed by the transaction
-- builder for embedding in redeemers.
newtype Proof = Proof
    { unProof :: ByteString
    }

-- | Manager for per-token tries.
data TrieManager m = TrieManager
    { withTrie
        :: forall a
         . TokenId
        -> (Trie m -> m a)
        -> m a
    -- ^ Run an action with access to a token's trie
    , withSpeculativeTrie
        :: forall a
         . TokenId
        -> ( forall n
              . Monad n
             => Trie n
             -> n a
           )
        -> m a
    -- ^ Run a read-your-writes session whose
    -- mutations are discarded at the end. The
    -- rank-2 callback ensures the action cannot
    -- perform arbitrary effects — only trie
    -- operations and pure computation.
    , createTrie :: TokenId -> m ()
    -- ^ Create a new empty trie for a token.
    -- Deletes any existing data first.
    , deleteTrie :: TokenId -> m ()
    -- ^ Delete a token's trie (permanent removal)
    , hideTrie :: TokenId -> m ()
    -- ^ Mark a token's trie as hidden (burn forward).
    -- Data is preserved; 'withTrie' will fail until
    -- 'unhideTrie' is called.
    , unhideTrie :: TokenId -> m ()
    -- ^ Restore a hidden token's trie to visible
    -- (burn rollback).
    }

-- | Operations on a single trie.
data Trie m = Trie
    { insert
        :: ByteString -> ByteString -> m Root
    -- ^ Insert a key-value pair, returning new root
    , delete :: ByteString -> m Root
    -- ^ Delete a key, returning new root
    , lookup :: ByteString -> m (Maybe ByteString)
    -- ^ Look up a value by key
    , enumerate :: m [(ByteString, ByteString)]
    -- ^ Enumerate original keys and raw values
    , getRoot :: m Root
    -- ^ Get current root hash
    , getProof :: ByteString -> m (Maybe Proof)
    -- ^ Generate a Merkle proof for a key
    , getProofSteps
        :: ByteString -> m (Maybe [ProofStep])
    -- ^ Generate on-chain proof steps for a key
    }
