{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Columns
-- Description : Column family GADTs for indexer state
-- License     : Apache-2.0
--
-- Type-safe column family definitions for the
-- indexer's RocksDB-backed persistent state using
-- @rocksdb-kv-transactions@. Two GADT selectors:
--
--   * 'AllColumns' — the seven cage\/trie column
--     families (used inside @'mapColumns' 'InCage'@):
--
--       - Cage state: 'CageTokens', 'CageRequests',
--         'CageCfg'
--       - Rollback storage: 'CageRollbacks'
--       - Trie storage: 'TrieNodes', 'TrieKV'
--       - Trie registry: 'TrieMeta'
--
--   * 'UnifiedColumns' — combines the five UTxO
--     columns ('Columns' from @cardano-utxo-csmt@,
--     including the journal column) with the seven
--     cage\/trie columns via 'InUtxo' and 'InCage'.
--     A single 'Transaction' over 'UnifiedColumns'
--     addresses all 12 column families, enforcing
--     the one-block-one-commit invariant.
--
-- Serialization codecs for these columns live in
-- "Cardano.MPFS.Indexer.Codecs".
module Cardano.MPFS.Indexer.Columns
    ( -- * Column selector
      AllColumns (..)

      -- * Unified column selector
    , UnifiedColumns (..)

      -- * Checkpoint type
    , CageCheckpoint (..)

      -- * Trie registry status
    , TrieStatus (..)
    ) where

import Control.Lens (type (:~:) (..))
import Database.KV.Transaction
    ( GCompare (..)
    , GEq (..)
    , GOrdering (..)
    , KV
    )

import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns
    )

import MPF.Hashes (MPFHash)
import MPF.Interface (HexIndirect, HexKey)

import MTS.Rollbacks.Types (RollbackPoint)

import Cardano.MPFS.Core.Types
    ( BlockId
    , Request
    , SlotNo
    , TokenId
    , TokenState
    , TxIn
    )
import Cardano.MPFS.Indexer.Event (CageInverseOp)

-- | Visibility status for a token's trie in the
-- persistent registry. Stored in the @trie-meta@
-- column family so trie state survives restarts.
data TrieStatus
    = -- | Trie is visible and accessible
      Visible
    | -- | Trie is hidden (burned token); data
      -- preserved but 'withTrie' fails
      Hidden
    deriving stock (Eq, Show)

-- | Chain sync checkpoint stored in the cage-cfg
-- column family.
data CageCheckpoint = CageCheckpoint
    { checkpointSlot :: !SlotNo
    -- ^ Slot of the last processed block
    , checkpointBlockId :: !BlockId
    -- ^ Header hash of the last processed block
    }
    deriving stock (Eq, Show)

-- | Column family selector for indexer persistent
-- state. Covers cage state, rollback storage, and
-- per-token trie storage.
data AllColumns x where
    -- | Token state: maps token identifiers to
    -- their on-chain state.
    CageTokens
        :: AllColumns (KV TokenId TokenState)
    -- | Pending requests: maps UTxO references to
    -- request details.
    CageRequests
        :: AllColumns (KV TxIn Request)
    -- | Singleton checkpoint: stores the last
    -- processed block position.
    CageCfg
        :: AllColumns (KV () CageCheckpoint)
    -- | Rollback storage: maps slot numbers to
    -- inverse 'RollbackPoint's for rollback.
    CageRollbacks
        :: AllColumns
            ( KV
                SlotNo
                ( RollbackPoint
                    CageInverseOp
                    BlockId
                )
            )
    -- | Trie nodes: MPF trie structure. Keys are
    -- 'HexKey' paths, values are 'HexIndirect'
    -- nodes containing hash pointers.
    TrieNodes
        :: AllColumns
            (KV HexKey (HexIndirect MPFHash))
    -- | Trie key-value pairs: user data stored in
    -- per-token tries. Keys are 'HexKey' paths,
    -- values are 'MPFHash' content hashes.
    TrieKV
        :: AllColumns (KV HexKey MPFHash)
    -- | Trie registry: maps token identifiers to
    -- their visibility status ('Visible' or
    -- 'Hidden'). Scanned at startup to rebuild
    -- the in-memory known\/hidden sets.
    TrieMeta
        :: AllColumns (KV TokenId TrieStatus)

instance GEq AllColumns where
    geq CageTokens CageTokens = Just Refl
    geq CageRequests CageRequests = Just Refl
    geq CageCfg CageCfg = Just Refl
    geq CageRollbacks CageRollbacks = Just Refl
    geq TrieNodes TrieNodes = Just Refl
    geq TrieKV TrieKV = Just Refl
    geq TrieMeta TrieMeta = Just Refl
    geq _ _ = Nothing

instance GCompare AllColumns where
    gcompare CageTokens CageTokens = GEQ
    gcompare CageTokens _ = GLT
    gcompare _ CageTokens = GGT
    gcompare CageRequests CageRequests = GEQ
    gcompare CageRequests _ = GLT
    gcompare _ CageRequests = GGT
    gcompare CageCfg CageCfg = GEQ
    gcompare CageCfg _ = GLT
    gcompare _ CageCfg = GGT
    gcompare CageRollbacks CageRollbacks = GEQ
    gcompare CageRollbacks _ = GLT
    gcompare _ CageRollbacks = GGT
    gcompare TrieNodes TrieNodes = GEQ
    gcompare TrieNodes _ = GLT
    gcompare _ TrieNodes = GGT
    gcompare TrieKV TrieKV = GEQ
    gcompare TrieKV _ = GLT
    gcompare _ TrieKV = GGT
    gcompare TrieMeta TrieMeta = GEQ

-- | Unified column selector covering both UTxO
-- (cardano-utxo-csmt) and cage\/trie columns.
-- Enables a single RocksDB transaction runner for
-- all 12 column families via 'mapColumns'.
data UnifiedColumns slot hash key value x where
    -- | UTxO columns (first 5, including journal)
    InUtxo
        :: Columns slot hash key value x
        -> UnifiedColumns slot hash key value x
    -- | Cage\/trie columns (last 7)
    InCage
        :: AllColumns x
        -> UnifiedColumns slot hash key value x

instance GEq (UnifiedColumns slot hash key value) where
    geq (InUtxo a) (InUtxo b) = geq a b
    geq (InCage a) (InCage b) = geq a b
    geq _ _ = Nothing

instance GCompare (UnifiedColumns slot hash key value) where
    gcompare (InUtxo a) (InUtxo b) = gcompare a b
    gcompare (InUtxo _) (InCage _) = GLT
    gcompare (InCage _) (InUtxo _) = GGT
    gcompare (InCage a) (InCage b) = gcompare a b
