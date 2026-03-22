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
import Data.IORef
    ( IORef
    , readIORef
    , writeIORef
    )
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
mkCageIntersector
    :: Int
    -- ^ Security parameter (stability window)
    -> IORef (AppPhase hash cf op)
    -- ^ Mutable phase reference
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
    -> IO ()
    -- ^ Armageddon action (wipe + reset)
    -> Intersector Point SlotNo Fetched
mkCageIntersector
    securityParam
    phaseRef
    run
    armageddon =
        Intersector
            { intersectFound = \_point -> do
                pure
                    $ mkCageFollower
                        securityParam
                        phaseRef
                        run
                        armageddon
            , intersectNotFound = do
                pure
                    ( mkCageIntersector
                        securityParam
                        phaseRef
                        run
                        armageddon
                    , [Network.Point Origin]
                    )
            }

-- | Build a 'Follower' using chain-follower Runner.
mkCageFollower
    :: Int
    -- ^ Security parameter (stability window)
    -> IORef (AppPhase hash cf op)
    -- ^ Mutable phase reference
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
    -> IO ()
    -- ^ Armageddon action (wipe + reset)
    -> Follower Point SlotNo Fetched
mkCageFollower
    securityParam
    phaseRef
    run
    armageddon =
        follower
      where
        follower =
            Follower
                { rollForward = rollFwd
                , rollBackward = rollBwd
                }

        rollFwd fetched _tipSlot = do
            let slot = pointToSlot (fetchedPoint fetched)
            phase <- readIORef phaseRef
            phase' <-
                run
                    $ processBlock
                        InRollbacks
                        securityParam
                        slot
                        fetched
                        phase
            writeIORef phaseRef phase'
            pure follower

        rollBwd point = do
            let targetSlot =
                    pointToSlot point
            phase <- readIORef phaseRef
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
                        Store.RollbackSucceeded _ -> do
                            writeIORef
                                phaseRef
                                (InFollowing n' f)
                            pure $ Progress follower
                        Store.RollbackImpossible -> do
                            armageddon
                            pure
                                $ Reset
                                $ mkCageIntersector
                                    securityParam
                                    phaseRef
                                    run
                                    armageddon
                InRestoration _ _ -> do
                    armageddon
                    pure
                        $ Reset
                        $ mkCageIntersector
                            securityParam
                            phaseRef
                            run
                            armageddon
