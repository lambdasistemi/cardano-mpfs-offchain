{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Indexer.ResumeSpec
-- Description : Chain-sync resume-point ordering (#355)
-- License     : Apache-2.0
--
-- Tests that 'resumePoints' orders the ChainSync
-- intersection candidates newest-first. The rollback
-- history is persisted oldest-first, but ChainSync
-- @findIntersect@ intersects at the /first/ offered
-- point on the node's chain, so candidates must be
-- offered newest-first. Offering them oldest-first made
-- the node intersect ~1.1M slots behind the tip and
-- replay the whole history on restart (#355).
module Cardano.MPFS.Indexer.ResumeSpec (spec) where

import Data.ByteString (ByteString)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )

import Ouroboros.Network.Block qualified as Network
import Ouroboros.Network.Point (WithOrigin (..))

import Cardano.MPFS.Application (resumePoints)
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import ChainFollower.Rollbacks.Types (RollbackPoint (..))

-- | A rollback point carrying a block id as its
-- per-point metadata (the only field 'resumePoints'
-- reads). The inverse-operation type is irrelevant
-- here, so it is fixed to @()@.
mkPoint :: ByteString -> RollbackPoint () BlockId
mkPoint h =
    RollbackPoint
        { rpInverses = []
        , rpMeta = Just (BlockId h)
        }

-- | Extract the slots of a candidate list, in order.
candidateSlots :: [Network.Point a] -> [WithOrigin SlotNo]
candidateSlots = map Network.pointSlot

spec :: Spec
spec =
    describe
        "resumePoints (chain-sync intersection ordering, #355)"
        $ do
            it
                "offers candidates newest-first (descending slot)"
                $ do
                    -- queryHistory yields points oldest-first.
                    let history =
                            [ (SlotNo 100, mkPoint "a")
                            , (SlotNo 200, mkPoint "b")
                            , (SlotNo 300, mkPoint "c")
                            ]
                    candidateSlots (resumePoints history)
                        `shouldBe` [ At (SlotNo 300)
                                   , At (SlotNo 200)
                                   , At (SlotNo 100)
                                   ]

            it
                "puts the newest (tip) point first so the node intersects at the tip"
                $ do
                    let history =
                            [ (SlotNo s, mkPoint "x")
                            | s <- [10, 20 .. 100]
                            ]
                    case resumePoints history of
                        [] -> fail "expected non-empty candidates"
                        (first : _) ->
                            Network.pointSlot first
                                `shouldBe` At (SlotNo 100)

            it
                "resumes from Origin on empty history"
                $ resumePoints []
                `shouldBe` [Network.Point Origin]
