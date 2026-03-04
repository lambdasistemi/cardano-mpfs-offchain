{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Indexer.CageFollower
-- Description : Block processor with unified transactions
-- License     : Apache-2.0
--
-- __Invariant: one block = one DB transaction.__
--
-- Implements a 'Follower' that processes each block
-- from ChainSync in a single atomic RocksDB
-- transaction covering both UTxO (cardano-utxo-csmt)
-- and cage state mutations via 'UnifiedColumns':
--
--   1. Detect cage events — resolves spent UTxOs
--      from the CSMT KV column
--      ('mapColumns' 'InUtxo', read-only)
--   2. Apply cage state\/trie mutations
--      ('mapColumns' 'InCage')
--   3. Apply UTxO CSMT changes
--      ('mapColumns' 'InUtxo', 'forwardTip')
--   4. Store rollback inverse ops and update
--      checkpoint ('mapColumns' 'InCage')
--
-- All four steps run inside a single 'Transaction'
-- over 'UnifiedColumns'. The runner commits them as
-- one atomic RocksDB write batch — if any step
-- fails, none are persisted.
--
-- Rollback reverses both in a single transaction:
-- 'rollbackTip' for the UTxO index and
-- 'rollbackToSlotT' for cage state.
--
-- Bypasses the 'Update' continuation from
-- cardano-utxo-csmt; calls 'forwardTip' and
-- 'rollbackTip' directly and tracks the rollback
-- point count via an 'IORef'.
module Cardano.MPFS.Indexer.CageFollower
    ( -- * Construction
      mkCageFollower
    , mkCageIntersector
    ) where

import Control.Monad (forM_)
import Control.Tracer (Tracer)
import Data.ByteString.Lazy (LazyByteString)
import Data.IORef
    ( IORef
    , modifyIORef'
    , readIORef
    )
import Ouroboros.Network.Block qualified as Network
import Ouroboros.Network.Point
    ( Block (..)
    , WithOrigin (..)
    )

import Cardano.Ledger.Binary
    ( DecCBOR
    , DecoderError
    , EncCBOR
    , decodeFull
    , natVersion
    , serialize
    )

import Cardano.UTxOCSMT.Application.BlockFetch
    ( Fetched (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.RollbackPoint
    ( pattern UTxORollbackPoint
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTOps (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Update
    ( UpdateTrace
    , forwardTip
    , sampleRollbackPoints
    )
import Cardano.UTxOCSMT.Application.Database.Interface
    ( Operation (..)
    )
import Cardano.UTxOCSMT.Application.UTxOs
    ( Change (..)
    , uTxOs
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Follower (..)
    , Intersector (..)
    , Point
    , ProgressOrRewind (..)
    )
import MTS.Rollbacks.Store qualified as Store

import Database.KV.Transaction
    ( Transaction
    , iterating
    , mapColumns
    , query
    )

import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , ConwayEra
    , ScriptHash
    , SlotNo (..)
    , TxIn
    , TxOut
    )
import Cardano.MPFS.Indexer.Columns
    ( UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageBlockEvents
    , detectCageBlockEvents
    , extractConwayTxs
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkTransactionalState
    )
import Cardano.MPFS.Indexer.Rollback
    ( putCheckpointT
    , rollbackToSlotT
    , storeRollbackPointT
    )
import Cardano.MPFS.Trie.Persistent
    ( mkUnifiedTrieManager
    )

-- | Shorthand for the unified column type.
type Unified hash =
    UnifiedColumns
        Point
        hash
        LazyByteString
        LazyByteString

-- | Shorthand for UTxO columns.
type UTxO hash =
    Columns
        Point
        hash
        LazyByteString
        LazyByteString

-- | Convert a 'Change' to a UTxO 'Operation'.
changeToOp
    :: Change
    -> Operation LazyByteString LazyByteString
changeToOp (Spend k) = Delete k
changeToOp (Create k v) = Insert k v

-- | Extract the slot number from a Cardano 'Point'.
-- Returns 0 for 'Origin'.
pointToSlot :: Point -> SlotNo
pointToSlot (Network.Point Origin) = SlotNo 0
pointToSlot
    (Network.Point (At (Block s _))) = s

-- | Extract the block hash from a Cardano 'Point'.
-- Returns empty bytes for 'Origin'.
pointToBlockId :: Point -> BlockId
pointToBlockId (Network.Point Origin) =
    BlockId mempty
pointToBlockId
    (Network.Point (At (Block _ _))) =
        BlockId mempty

-- | Resolve a 'TxIn' to its 'TxOut' by querying
-- the UTxO KV column inside a unified transaction.
resolveUtxoT
    :: TxIn
    -> Transaction
        IO
        cf
        (Unified hash)
        op
        (Maybe (TxOut ConwayEra))
resolveUtxoT txIn = do
    let key = cborEncode txIn
    mVal <- mapColumns InUtxo $ query KVCol key
    pure $ case mVal of
        Nothing -> Nothing
        Just val -> case cborDecode val of
            Left _ -> Nothing
            Right txOut -> Just txOut

-- | CBOR-encode a ledger type using protocol
-- version 11.
cborEncode :: EncCBOR a => a -> LazyByteString
cborEncode = serialize (natVersion @11)

-- | CBOR-decode a ledger type using protocol
-- version 11.
cborDecode
    :: DecCBOR a
    => LazyByteString
    -> Either DecoderError a
cborDecode = decodeFull (natVersion @11)

-- | Build an 'Intersector' for the cage follower.
-- On intersection found, produces a 'Follower'
-- that processes both UTxO and cage events in a
-- single atomic transaction per block.
mkCageIntersector
    :: ( Ord hash
       , Show hash
       )
    => ScriptHash
    -- ^ Cage script hash for event detection
    -> Tracer IO (UpdateTrace Point hash)
    -- ^ Tracer for UTxO update events
    -> CSMTOps
        ( Transaction
            IO
            cf
            (UTxO hash)
            op
        )
        LazyByteString
        LazyByteString
        hash
    -- ^ CSMT operations (insert, delete, root)
    -> (Point -> hash)
    -- ^ Slot-to-hash function
    -> IORef Int
    -- ^ Mutable rollback point counter
    -> ( forall a
          . Transaction
                IO
                cf
                (Unified hash)
                op
                a
         -> IO a
       )
    -- ^ Unified transaction runner
    -> Intersector Fetched
mkCageIntersector
    scriptHash
    tracer
    ops
    slotHash
    countRef
    run =
        Intersector
            { intersectFound = \point -> do
                let f =
                        mkCageFollower
                            scriptHash
                            tracer
                            ops
                            slotHash
                            countRef
                            run
                pure $ f point
            , intersectNotFound = do
                pure
                    ( mkCageIntersector
                        scriptHash
                        tracer
                        ops
                        slotHash
                        countRef
                        run
                    , [Network.Point Origin]
                    )
            }

-- | Build a 'Follower' that processes each block
-- in a single unified transaction. Calls
-- 'forwardTip' and cage state mutations atomically.
mkCageFollower
    :: ( Ord hash
       , Show hash
       )
    => ScriptHash
    -- ^ Cage script hash for event detection
    -> Tracer IO (UpdateTrace Point hash)
    -- ^ Tracer for UTxO update events
    -> CSMTOps
        ( Transaction
            IO
            cf
            (UTxO hash)
            op
        )
        LazyByteString
        LazyByteString
        hash
    -- ^ CSMT operations (insert, delete, root)
    -> (Point -> hash)
    -- ^ Slot-to-hash function
    -> IORef Int
    -- ^ Mutable rollback point counter
    -> ( forall a
          . Transaction
                IO
                cf
                (Unified hash)
                op
                a
         -> IO a
       )
    -- ^ Unified transaction runner
    -> Point
    -- ^ Intersection point
    -> Follower Fetched
mkCageFollower
    scriptHash
    tracer
    ops
    slotHash
    countRef
    run =
        go
      where
        go _intersectPt = follower

        follower =
            Follower
                { rollForward = rollFwd
                , rollBackward = rollBwd
                }

        -- \| Forward: single unified transaction
        -- covering UTxO resolution, cage event
        -- detection, cage mutations, UTxO forward,
        -- rollback storage, and checkpoint.
        rollFwd fetched _tipSlot = do
            let Fetched
                    { fetchedPoint
                    , fetchedBlock
                    } = fetched
                slot =
                    pointToSlot fetchedPoint
                blockId =
                    pointToBlockId fetchedPoint
                conwayTxs =
                    extractConwayTxs fetchedBlock
                utxoOps =
                    changeToOp
                        <$> uTxOs fetchedBlock

            count <- readIORef countRef
            let hash = slotHash fetchedPoint

            stored <- run $ do
                -- 1. Detect cage events (resolves
                -- spent UTxOs from CSMT KV column)
                events <-
                    detectCageBlockEvents
                        scriptHash
                        resolveUtxoT
                        conwayTxs

                -- 2. Apply cage state + trie
                -- mutations
                invs <-
                    mapColumns InCage
                        $ applyCageBlockEvents
                            txState
                            txTm
                            events

                -- 3. Forward UTxO CSMT tip
                tipStored <-
                    mapColumns InUtxo
                        $ forwardTip
                            tracer
                            ops
                            hash
                            count
                            fetchedPoint
                            utxoOps

                -- 4. Store rollback inverses and
                -- update checkpoint
                mapColumns InCage $ do
                    storeRollbackPointT
                        slot
                        invs
                        (Just blockId)
                    putCheckpointT slot blockId

                pure tipStored

            -- Post-commit: bump rollback counter
            -- if tip was actually stored
            modifyIORef'
                countRef
                (\c -> if stored then c + 1 else c)

            pure follower

        -- \| Backward: single unified transaction
        -- covering UTxO rollback and cage state
        -- rollback.
        rollBwd point = do
            let targetSlot = pointToSlot point

            result <- run $ do
                -- Guard: if the rollback target is
                -- ahead of the UTxO tip, the rollback
                -- is a no-op (e.g. initial RollBackward
                -- to genesis when tip is Origin).
                utxoTip <-
                    mapColumns InUtxo
                        $ Store.queryTip
                            RollbackPoints
                case utxoTip of
                    Just tip
                        | At point > tip ->
                            pure
                                $ Store.RollbackSucceeded
                                    0
                    _ -> do
                        -- 1. Rollback UTxO CSMT
                        utxoResult <-
                            mapColumns InUtxo
                                $ Store.rollbackTo
                                    RollbackPoints
                                    ( \(UTxORollbackPoint _ ios _) ->
                                        forM_ ios
                                            $ \case
                                                Insert k v ->
                                                    csmtInsert
                                                        ops
                                                        k
                                                        v
                                                Delete k ->
                                                    csmtDelete
                                                        ops
                                                        k
                                    )
                                    (At point)
                        -- 2. Rollback cage state only
                        -- if UTxO rollback succeeded —
                        -- both subsystems must stay in
                        -- sync.
                        case utxoResult of
                            Store.RollbackSucceeded _ ->
                                mapColumns InCage
                                    $ rollbackToSlotT
                                        txState
                                        txTm
                                        targetSlot
                            Store.RollbackImpossible ->
                                pure ()
                        pure utxoResult

            -- Post-commit: adjust rollback counter
            case result of
                Store.RollbackSucceeded deleted -> do
                    modifyIORef'
                        countRef
                        (subtract deleted)
                    pure $ Progress follower
                Store.RollbackImpossible -> do
                    -- Sample remaining rollback
                    -- points to find new
                    -- intersection
                    pts <-
                        run
                            $ mapColumns InUtxo
                            $ iterating
                                RollbackPoints
                                sampleRollbackPoints
                    if null pts
                        then
                            pure
                                $ Reset
                                $ mkCageIntersector
                                    scriptHash
                                    tracer
                                    ops
                                    slotHash
                                    countRef
                                    run
                        else
                            pure
                                $ Rewind pts
                                $ mkCageIntersector
                                    scriptHash
                                    tracer
                                    ops
                                    slotHash
                                    countRef
                                    run

        -- Transactional state and trie manager,
        -- built fresh in each transaction scope.
        -- They operate on AllColumns (lifted via
        -- mapColumns InCage at each call site).
        txState = mkTransactionalState
        txTm = mkUnifiedTrieManager
