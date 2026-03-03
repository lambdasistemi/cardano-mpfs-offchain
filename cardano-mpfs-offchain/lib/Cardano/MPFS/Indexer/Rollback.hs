-- |
-- Module      : Cardano.MPFS.Indexer.Rollback
-- Description : Rollback to a previous slot
-- License     : Apache-2.0
--
-- Slot-based rollback for the cage indexer. All
-- operations are transactional: they compose into a
-- single DB write batch, ensuring atomicity from
-- protocol to protocol (one rollback = one commit).
module Cardano.MPFS.Indexer.Rollback
    ( -- * Transactional operations
      storeRollbackT
    , loadRollbackT
    , deleteRollbackT
    , putCheckpointT
    , rollbackToSlotT
    ) where

import Control.Monad (forM_)

import Cardano.MPFS.Core.Types (BlockId (..), SlotNo)
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , CageCheckpoint (..)
    , CageRollbackEntry (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageInverseOp
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageInverses
    )
import Cardano.MPFS.State
    ( Checkpoints (..)
    , State (..)
    )
import Cardano.MPFS.Trie (TrieManager)
import Database.KV.Transaction
    ( Transaction
    , delete
    , insert
    , query
    )

-- | Store inverse ops for a block within a
-- transaction. No auto-commit.
storeRollbackT
    :: SlotNo
    -> [CageInverseOp]
    -> Transaction m cf AllColumns ops ()
storeRollbackT slot invs =
    insert
        CageRollbacks
        slot
        (CageRollbackEntry invs)

-- | Load inverse ops for a slot within a
-- transaction.
loadRollbackT
    :: (Monad m)
    => SlotNo
    -> Transaction
        m
        cf
        AllColumns
        ops
        (Maybe [CageInverseOp])
loadRollbackT slot = do
    mEntry <- query CageRollbacks slot
    pure $ fmap unRollbackEntry mEntry

-- | Delete stored inverse ops within a transaction.
deleteRollbackT
    :: SlotNo
    -> Transaction m cf AllColumns ops ()
deleteRollbackT = delete CageRollbacks

-- | Store a checkpoint within a transaction.
putCheckpointT
    :: SlotNo
    -> BlockId
    -> [SlotNo]
    -> Transaction m cf AllColumns ops ()
putCheckpointT s b slots =
    insert CageCfg () (CageCheckpoint s b slots)

-- | Roll back cage state to a target slot within a
-- transaction. Replays inverse ops for all slots
-- after the target, in reverse order. The entire
-- rollback (load inverses, apply them, delete
-- entries, update checkpoint) executes in one
-- atomic write batch.
rollbackToSlotT
    :: (Monad m)
    => State
        (Transaction m cf AllColumns ops)
    -> TrieManager
        (Transaction m cf AllColumns ops)
    -> SlotNo
    -> Transaction m cf AllColumns ops ()
rollbackToSlotT st tm targetSlot = do
    mCp <- getCheckpoint (checkpoints st)
    case mCp of
        Nothing -> pure ()
        Just (_currentSlot, _blockId, activeSlots) ->
            do
                let slotsToRevert =
                        reverse
                            $ filter
                                (> targetSlot)
                                activeSlots
                forM_ slotsToRevert $ \slot -> do
                    mInvs <- loadRollbackT slot
                    forM_ mInvs $ \invs ->
                        applyCageInverses
                            st
                            tm
                            (reverse invs)
                    deleteRollbackT slot
                -- Update checkpoint
                let keptSlots =
                        filter
                            (<= targetSlot)
                            activeSlots
                putCheckpointT
                    targetSlot
                    (BlockId mempty)
                    keptSlots
