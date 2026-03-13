-- |
-- Module      : Cardano.MPFS.Indexer.Rollback
-- Description : Rollback to a previous slot
-- License     : Apache-2.0
--
-- Slot-based rollback for the cage indexer using the
-- @mts:rollbacks@ library. All operations are
-- transactional: they compose into a single DB write
-- batch, ensuring atomicity (one rollback = one
-- commit).
module Cardano.MPFS.Indexer.Rollback
    ( -- * Transactional operations
      storeRollbackPointT
    , putCheckpointT
    , rollbackToSlotT
    , queryTipT
    , pruneRollbacksT

      -- * Armageddon
    , cageArmageddonT
    ) where

import MTS.Rollbacks.Store qualified as Store
import MTS.Rollbacks.Types (RollbackPoint (..))

import Cardano.MPFS.Core.Types (BlockId (..), SlotNo)
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , CageCheckpoint (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageInverseOp
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageInverses
    )
import Cardano.MPFS.State
    ( State (..)
    )
import Cardano.MPFS.Trie (TrieManager)
import Database.KV.Transaction
    ( Transaction
    , insert
    )

-- | Store a rollback point at the given slot.
storeRollbackPointT
    :: SlotNo
    -> [CageInverseOp]
    -> Maybe BlockId
    -> Transaction m cf AllColumns ops ()
storeRollbackPointT slot invs meta =
    Store.storeRollbackPoint
        CageRollbacks
        slot
        RollbackPoint
            { rpInverses = invs
            , rpMeta = meta
            }

-- | Store a checkpoint within a transaction.
putCheckpointT
    :: SlotNo
    -> BlockId
    -> Transaction m cf AllColumns ops ()
putCheckpointT s b =
    insert CageCfg () (CageCheckpoint s b)

-- | Query the current rollback tip slot.
queryTipT
    :: (Monad m)
    => Transaction
        m
        cf
        AllColumns
        ops
        (Maybe SlotNo)
queryTipT = Store.queryTip CageRollbacks

-- | Prune rollback points below a slot.
pruneRollbacksT
    :: (Monad m)
    => SlotNo
    -> Transaction m cf AllColumns ops Int
pruneRollbacksT =
    Store.pruneBelow CageRollbacks

-- | Roll back cage state to a target slot within
-- a transaction. Uses the library's cursor-based
-- backward walk to iterate from tip, applying
-- inverses for each point strictly after target.
rollbackToSlotT
    :: (Monad m)
    => State
        (Transaction m cf AllColumns ops)
    -> TrieManager
        (Transaction m cf AllColumns ops)
    -> SlotNo
    -> Transaction m cf AllColumns ops ()
rollbackToSlotT st tm targetSlot = do
    result <-
        Store.rollbackTo
            CageRollbacks
            ( \RollbackPoint{rpInverses} ->
                applyCageInverses
                    st
                    tm
                    (reverse rpInverses)
            )
            targetSlot
    case result of
        Store.RollbackSucceeded _ ->
            putCheckpointT
                targetSlot
                (BlockId mempty)
        Store.RollbackImpossible ->
            pure ()

-- | Wipe cage rollback points and reset checkpoint.
-- Called during armageddon when both subsystems need
-- to restart from Origin.
cageArmageddonT
    :: (Monad m)
    => Transaction m cf AllColumns ops ()
cageArmageddonT = do
    _ <-
        Store.rollbackTo
            CageRollbacks
            (\_ -> pure ())
            0
    putCheckpointT 0 (BlockId mempty)
