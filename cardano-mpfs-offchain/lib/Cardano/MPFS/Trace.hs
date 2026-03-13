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

      -- * Sub-tracer adapters
    , adaptUpdate
    ) where

import Control.Tracer (Tracer (..))
import Data.Aeson
    ( ToJSON (..)
    , object
    , (.=)
    )
import Data.ByteString.Lazy.Char8 qualified as BSL
import Data.Time (getCurrentTime)
import Ouroboros.Network.Block
    ( SlotNo
    , pointSlot
    )
import Ouroboros.Network.Point (WithOrigin (..))
import System.IO (hFlush, stderr)

import Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon
    ( ArmageddonTrace (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Update
    ( UpdateTrace (..)
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Point
    )

import Data.Aeson qualified as Aeson

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

-- | Adapt 'UpdateTrace' to 'AppTrace'.
-- Extracts the interesting fields from the
-- polymorphic 'UpdateTrace' sum.
adaptUpdate
    :: UpdateTrace Point hash -> AppTrace
adaptUpdate (UpdateArmageddon t) =
    TraceArmageddon t
adaptUpdate (UpdateForwardTip pt ins del _) =
    TraceBlock (ptSlot pt) ins del
adaptUpdate (UpdateNewState pts) =
    case pts of
        (p : _) -> TraceChainTip (ptSlot p)
        [] -> TraceChainTip 0
adaptUpdate _ = TraceChainTip 0

-- | Extract 'SlotNo' from a 'Point', defaulting
-- to 0 for 'Origin'.
ptSlot :: Point -> SlotNo
ptSlot p = case pointSlot p of
    Origin -> 0
    At s -> s
