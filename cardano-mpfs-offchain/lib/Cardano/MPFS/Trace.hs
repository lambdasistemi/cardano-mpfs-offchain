{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Trace
-- Description : Structured application tracing
-- License     : Apache-2.0
--
-- Unified trace type for the MPFS offchain service.
-- All runtime events — block processing, chain tip
-- updates, Mithril skip progress, and armageddon
-- setup — are funnelled through 'AppTrace' so a
-- single 'Tracer IO AppTrace' can drive structured
-- logging.
module Cardano.MPFS.Trace
    ( -- * Trace types
      AppTrace (..)

      -- * Formatters
    , jsonLinesTracer
    ) where

import Control.Tracer (Tracer (..))
import Data.Aeson
    ( ToJSON (..)
    , object
    , (.=)
    )
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy.Char8 qualified as BSL
import Data.Time (getCurrentTime)
import Ouroboros.Network.Block (SlotNo, unSlotNo)
import System.IO (hFlush, stderr)

import ChainFollower.Runner (RunnerEvent (..))

import Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon
    ( ArmageddonTrace (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( ReplayEvent (..)
    )

-- | Unified application trace type.
data AppTrace
    = -- | Armageddon setup events (once at startup)
      TraceArmageddon ArmageddonTrace
    | -- | Per-block UTxO update (slot, inserts, deletes)
      TraceBlock SlotNo Int Int
    | -- | Chain tip slot update
      TraceChainTip SlotNo
    | -- | Mithril skip progress (current, target)
      TraceSkipProgress SlotNo SlotNo
    | -- | Raw block received from ChainSync
      TraceBlockReceived SlotNo
    | -- | Journal replay event
      TraceReplay ReplayEvent
    | -- | Phase-boundary event surfaced by the
      -- upstream chain-follower 'Runner'. Carries the
      -- slot the event fired at. The
      -- 'PhaseTransition' variant is the
      -- restoration→following boundary the readiness
      -- contract gates on (#275).
      TraceRunner (RunnerEvent SlotNo)
    | -- | One-shot startup classification emitted in
      -- @withApplication@ after the on-disk rollback
      -- count is read and before the cage follower is
      -- started. @fresh_db@ is true iff no rollback
      -- points exist; @initial_rollback_count@ is the
      -- raw count.
      TraceStartupClassification
        Bool
        -- ^ @fresh_db@
        Int
        -- ^ @initial_rollback_count@
    | -- | Emitted exactly once when the readiness
      -- TVar flips from @NotReady@ to @Ready@. Marks
      -- the recovery-decision boundary required by
      -- spec FR-005.
      TraceReady
    deriving (Show)

instance ToJSON AppTrace where
    toJSON (TraceArmageddon t) =
        object
            [ "event" .= ("armageddon" :: String)
            , "phase" .= show t
            ]
    toJSON (TraceBlock slot ins del) =
        object
            [ "event" .= ("block" :: String)
            , "slot" .= show slot
            , "inserts" .= ins
            , "deletes" .= del
            ]
    toJSON (TraceChainTip slot) =
        object
            [ "event" .= ("chain_tip" :: String)
            , "slot" .= show slot
            ]
    toJSON (TraceSkipProgress cur tgt) =
        object
            [ "event"
                .= ("skip_progress" :: String)
            , "current" .= show cur
            , "target" .= show tgt
            ]
    toJSON (TraceBlockReceived slot) =
        object
            [ "event"
                .= ( "block_received"
                        :: String
                   )
            , "slot" .= show slot
            ]
    toJSON (TraceReplay (ReplayStart cs bs tb opb remaining)) =
        object
            [ "event"
                .= ("replay_start" :: String)
            , "chunk_size" .= cs
            , "buckets" .= bs
            , "total_buckets" .= tb
            , "ops_per_bucket" .= opb
            , "remaining" .= remaining
            ]
    toJSON (TraceReplay ReplayStop) =
        object
            [ "event"
                .= ("replay_stop" :: String)
            ]
    toJSON (TraceRunner (BlockRestored slot)) =
        object
            [ "event"
                .= ("runner_block_restored" :: String)
            , "slot" .= unSlotNo slot
            ]
    toJSON (TraceRunner (BlockFollowed slot)) =
        object
            [ "event"
                .= ("runner_block_followed" :: String)
            , "slot" .= unSlotNo slot
            ]
    toJSON (TraceRunner (PhaseTransition slot)) =
        object
            [ "event"
                .= ("runner_phase_transition" :: String)
            , "slot" .= unSlotNo slot
            ]
    toJSON (TraceStartupClassification fresh n) =
        object
            [ "event"
                .= ("startup_classification" :: String)
            , "fresh_db" .= fresh
            , "initial_rollback_count" .= n
            ]
    toJSON TraceReady =
        object
            [ "event" .= ("ready" :: String)
            ]

-- | JSON-lines tracer writing to stderr with
-- timestamps.
jsonLinesTracer :: Tracer IO AppTrace
jsonLinesTracer = Tracer $ \ev -> do
    now <- getCurrentTime
    let entry =
            object
                [ "ts" .= show now
                , "trace" .= ev
                ]
    BSL.hPut stderr
        $ Aeson.encode entry <> "\n"
    hFlush stderr
