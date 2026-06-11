{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Indexer.CageFollower
-- Description : Block processor using chain-follower Runner
-- License     : Apache-2.0
--
-- __Invariant: one block = one DB transaction.__
--
-- Uses 'Runner.processBlock' and 'Runner.rollbackTo'
-- from chain-follower to handle rollback storage,
-- pruning, and phase management automatically.
-- The business logic (cage event detection, UTxO
-- mutations) lives in 'Backend.composedInit'.
module Cardano.MPFS.Indexer.CageFollower
    ( -- * Construction
      mkCageFollower
    , mkCageIntersector
    ) where

import Data.ByteString.Lazy (LazyByteString)
import Ouroboros.Network.Block qualified as Network
import Ouroboros.Network.Point
    ( Block (..)
    , WithOrigin (..)
    )

import Cardano.UTxOCSMT.Application.BlockFetch
    ( Fetched (..)
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Follower (..)
    , Intersector (..)
    , Point
    , ProgressOrRewind (..)
    )

import ChainFollower.Backend (Init (..))
import ChainFollower.Rollbacks.Store qualified as Store
import ChainFollower.Runner
    ( Phase (..)
    , processBlock
    , rollbackTo
    )
import Control.Tracer (nullTracer)

import Database.KV.Transaction
    ( Transaction
    )

import Cardano.MPFS.Core.Types
    ( BlockId
    , SlotNo (..)
    )
import Cardano.MPFS.Indexer.Columns
    ( UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.ComposedInv
    ( ComposedInv
    )

-- | Shorthand for the unified column type.
type Unified hash =
    UnifiedColumns
        Point
        hash
        LazyByteString
        LazyByteString

-- | Shorthand for the phase type.
type AppPhase hash cf op =
    Phase
        IO
        cf
        (Unified hash)
        op
        Fetched
        ComposedInv
        BlockId

-- | Extract the slot number from a 'Point'.
pointToSlot :: Point -> SlotNo
pointToSlot (Network.Point Origin) = SlotNo 0
pointToSlot
    (Network.Point (At (Block s _))) = s

phaseProofReadsReady :: AppPhase hash cf op -> Bool
phaseProofReadsReady (InFollowing _ _) = True
phaseProofReadsReady (InRestoration _) = False

-- | Build an 'Intersector' for the cage follower.
-- Phase is threaded through continuations (no IORef).
--
-- The two depth parameters are deliberately decoupled
-- (see #355): the stability window governs /when/ the
-- phase flips from restoration to following (a slot
-- distance from the tip), while the security parameter
-- @k@ governs /how many/ rollback points are retained (a
-- block count). They are derived from the same genesis
-- @k@ but have different units, so conflating them keeps
-- a window ~60× too deep.
mkCageIntersector
    :: Int
    -- ^ Stability window in slots: phase-transition
    -- threshold (restoration → following). A block is
    -- \"within the window\" when @slot + window >= tip@.
    -> Int
    -- ^ Security parameter @k@ in blocks: rollback-history
    -- pruning depth. The store keeps @k + 1@ points
    -- (@k@ for the max Cardano rollback plus one fence
    -- post), never fewer than @k@.
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
    -> Init
        IO
        ( Transaction
            IO
            cf
            (Unified hash)
            op
        )
        Fetched
        ComposedInv
        BlockId
    -- ^ Backend initializer
    -> IO ()
    -- ^ Armageddon action (wipe + reset)
    -> IO ()
    -- ^ Post-commit callback (e.g. TVar notification)
    -> (Bool -> IO ())
    -- ^ Proof-read readiness callback. False while the
    -- runner is restoring in KVOnly mode or applying a
    -- live write; True only in idle following/full CSMT
    -- mode.
    -> AppPhase hash cf op
    -- ^ Current phase
    -> Intersector Point SlotNo Fetched
mkCageIntersector
    stabilityWindowSlots
    securityParamK
    run
    backendInit
    armageddon
    onCommit
    setProofReadsReady
    phase =
        Intersector
            { intersectFound = \_point ->
                pure
                    $ mkCageFollower
                        stabilityWindowSlots
                        securityParamK
                        run
                        backendInit
                        armageddon
                        onCommit
                        setProofReadsReady
                        phase
            , intersectNotFound =
                pure
                    ( mkCageIntersector
                        stabilityWindowSlots
                        securityParamK
                        run
                        backendInit
                        armageddon
                        onCommit
                        setProofReadsReady
                        phase
                    , [Network.Point Origin]
                    )
            }

-- | Build a 'Follower' using chain-follower Runner.
-- Phase is threaded through continuations: each
-- 'rollForward' returns a new 'Follower' capturing
-- the updated phase. Phase transitions (Restoring →
-- Following) happen inside the same transaction as
-- block processing.
mkCageFollower
    :: Int
    -- ^ Stability window in slots: phase-transition
    -- threshold (restoration → following).
    -> Int
    -- ^ Security parameter @k@ in blocks: rollback-history
    -- pruning depth (keeps @k + 1@ points).
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
    -> Init
        IO
        ( Transaction
            IO
            cf
            (Unified hash)
            op
        )
        Fetched
        ComposedInv
        BlockId
    -- ^ Backend initializer
    -> IO ()
    -- ^ Armageddon action (wipe + reset)
    -> IO ()
    -- ^ Post-commit callback (e.g. TVar notification)
    -> (Bool -> IO ())
    -- ^ Proof-read readiness callback.
    -> AppPhase hash cf op
    -- ^ Current phase
    -> Follower Point SlotNo Fetched
mkCageFollower
    stabilityWindowSlots
    securityParamK
    run
    backendInit
    armageddon
    onCommit
    setProofReadsReady =
        go
      where
        go phase =
            Follower
                { rollForward = rollFwd phase
                , rollBackward = rollBwd phase
                }

        rollFwd phase fetched tipSlot = do
            let slot =
                    pointToSlot (fetchedPoint fetched)
                -- Phase transition uses the stability
                -- window in SLOTS (distance from tip).
                withinWindow =
                    unSlotNo slot
                        + fromIntegral stabilityWindowSlots
                        >= unSlotNo tipSlot
            setProofReadsReady False
            phase' <-
                -- Pruning uses the security parameter k in
                -- BLOCKS (rollback-history depth).
                processBlock
                    nullTracer
                    withinWindow
                    run
                    InRollbacks
                    securityParamK
                    slot
                    fetched
                    phase
            setProofReadsReady
                (phaseProofReadsReady phase')
            onCommit
            pure $ go phase'

        rollBwd phase point = do
            let targetSlot = pointToSlot point
            case phase of
                InFollowing n f -> do
                    setProofReadsReady False
                    (result, n') <-
                        run
                            $ rollbackTo
                                InRollbacks
                                f
                                n
                                targetSlot
                    case result of
                        Store.RollbackSucceeded _ -> do
                            setProofReadsReady True
                            pure
                                $ Progress
                                $ go (InFollowing n' f)
                        Store.RollbackImpossible
                            -- No rollback points yet: the
                            -- rollback predates any block
                            -- we processed — safe to ignore
                            | n' == 0 ->
                                do
                                    setProofReadsReady True
                                    pure
                                        $ Progress
                                        $ go
                                        $ InFollowing 0 f
                            | otherwise -> do
                                armageddon
                                restoring <-
                                    start backendInit
                                setProofReadsReady False
                                pure
                                    $ Reset
                                    $ mkCageIntersector
                                        stabilityWindowSlots
                                        securityParamK
                                        run
                                        backendInit
                                        armageddon
                                        onCommit
                                        setProofReadsReady
                                        ( InRestoration
                                            restoring
                                        )
                InRestoration _ ->
                    -- During restoration rollbacks are
                    -- harmless — we're replaying from
                    -- origin anyway. Just continue.
                    pure $ Progress $ go phase
