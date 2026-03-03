-- |
-- Module      : Cardano.MPFS.Indexer.Rollback
-- Description : Rollback to a previous slot
-- License     : Apache-2.0
--
-- Slot-based rollback for the cage indexer. Provides
-- both transactional ('storeRollbackT',
-- 'rollbackToSlotT') and IO ('storeRollback',
-- 'rollbackToSlot') variants. The transactional
-- versions compose into a single DB commit; the IO
-- versions auto-commit each operation.
module Cardano.MPFS.Indexer.Rollback
    ( -- * Transactional (composable)
      storeRollbackT
    , loadRollbackT
    , deleteRollbackT
    , putCheckpointT
    , rollbackToSlotT

      -- * IO (auto-committing)
    , storeRollback
    , loadRollback
    , deleteRollback
    , rollbackToSlot
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
    ( RunTransaction (..)
    , Transaction
    , delete
    , insert
    , query
    )

-- --------------------------------------------------------
-- Transactional (composable) variants
-- --------------------------------------------------------

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
-- after the target, in reverse order.
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

-- --------------------------------------------------------
-- IO (auto-committing) variants
-- --------------------------------------------------------

-- | Store inverse ops for a block. Auto-commits.
storeRollback
    :: RunTransaction IO cf AllColumns op
    -> SlotNo
    -> [CageInverseOp]
    -> IO ()
storeRollback RunTransaction{runTransaction = run} slot invs =
    run $ storeRollbackT slot invs

-- | Load inverse ops for a slot. Auto-commits.
loadRollback
    :: RunTransaction IO cf AllColumns op
    -> SlotNo
    -> IO (Maybe [CageInverseOp])
loadRollback RunTransaction{runTransaction = run} slot =
    run $ loadRollbackT slot

-- | Delete stored inverse ops. Auto-commits.
deleteRollback
    :: RunTransaction IO cf AllColumns op
    -> SlotNo
    -> IO ()
deleteRollback RunTransaction{runTransaction = run} slot =
    run $ deleteRollbackT slot

-- | Roll back cage state to a target slot.
-- Each slot's inverses are loaded and replayed
-- in its own transaction.
rollbackToSlot
    :: State IO
    -> TrieManager IO
    -> RunTransaction IO cf AllColumns op
    -> SlotNo
    -> IO ()
rollbackToSlot st tm rt targetSlot = do
    mCp <- getCheckpoint (checkpoints st)
    case mCp of
        Nothing -> pure ()
        Just (_currentSlot, _blockId, activeSlots) ->
            do
                let slotsToRevert =
                        reverse
                            $ filter (> targetSlot) activeSlots
                forM_ slotsToRevert $ \slot -> do
                    mInvs <- loadRollback rt slot
                    forM_ mInvs $ \invs ->
                        applyCageInverses
                            st
                            tm
                            (reverse invs)
                    deleteRollback rt slot
                -- Update checkpoint
                let keptSlots =
                        filter (<= targetSlot) activeSlots
                    emptyBlockId =
                        BlockId mempty
                putCheckpoint
                    (checkpoints st)
                    targetSlot
                    emptyBlockId
                    keptSlots
