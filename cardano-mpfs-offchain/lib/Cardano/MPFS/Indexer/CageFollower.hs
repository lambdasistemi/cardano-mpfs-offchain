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

-- | Build an 'Intersector' for the cage follower.
-- Phase is threaded through continuations (no IORef).
mkCageIntersector
    :: Int
    -- ^ Security parameter (stability window)
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
    -> AppPhase hash cf op
    -- ^ Current phase
    -> Intersector Point SlotNo Fetched
mkCageIntersector
    securityParam
    run
    backendInit
    armageddon
    phase =
        Intersector
            { intersectFound = \_point ->
                pure
                    $ mkCageFollower
                        securityParam
                        run
                        backendInit
                        armageddon
                        phase
            , intersectNotFound =
                pure
                    ( mkCageIntersector
                        securityParam
                        run
                        backendInit
                        armageddon
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
    -- ^ Security parameter (stability window)
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
    -> AppPhase hash cf op
    -- ^ Current phase
    -> Follower Point SlotNo Fetched
mkCageFollower
    securityParam
    run
    backendInit
    armageddon =
        go
      where
        go phase =
            Follower
                { rollForward = rollFwd phase
                , rollBackward = rollBwd phase
                }

        rollFwd phase fetched _tipSlot = do
            let slot =
                    pointToSlot (fetchedPoint fetched)
            phase' <-
                run
                    $ processBlock
                        InRollbacks
                        securityParam
                        slot
                        fetched
                        phase
            pure $ go phase'

        rollBwd phase point = do
            let targetSlot = pointToSlot point
            case phase of
                InFollowing n f -> do
                    (result, n') <-
                        run
                            $ rollbackTo
                                InRollbacks
                                f
                                n
                                targetSlot
                    case result of
                        Store.RollbackSucceeded _ ->
                            pure
                                $ Progress
                                $ go (InFollowing n' f)
                        Store.RollbackImpossible
                            -- No rollback points yet: the
                            -- rollback predates any block
                            -- we processed — safe to ignore
                            | n' == 0 ->
                                pure
                                    $ Progress
                                    $ go (InFollowing 0 f)
                            | otherwise -> do
                                armageddon
                                restoring <-
                                    startRestoring
                                        backendInit
                                pure
                                    $ Reset
                                    $ mkCageIntersector
                                        securityParam
                                        run
                                        backendInit
                                        armageddon
                                        ( InRestoration
                                            0
                                            restoring
                                        )
                InRestoration _ _ -> do
                    armageddon
                    restoring <-
                        startRestoring backendInit
                    pure
                        $ Reset
                        $ mkCageIntersector
                            securityParam
                            run
                            backendInit
                            armageddon
                            (InRestoration 0 restoring)
