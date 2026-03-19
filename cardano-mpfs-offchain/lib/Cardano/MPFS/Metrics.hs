-- |
-- Module      : Cardano.MPFS.Metrics
-- Description : Extract metrics events from update traces
-- License     : Apache-2.0
--
-- Maps 'UpdateTrace' constructors to upstream
-- 'MetricsEvent' values so the CSMT metrics pipeline
-- can accumulate them via 'metricsFold'.
module Cardano.MPFS.Metrics
    ( -- * Extraction
      stealMetrics
    ) where

import Cardano.UTxOCSMT.Application.Database.Implementation.Update
    ( UpdateTrace (..)
    )
import Cardano.UTxOCSMT.Application.Metrics.Types
    ( MetricsEvent (..)
    )

-- | Extract 'MetricsEvent' values from an
-- 'UpdateTrace'. A single update trace may produce
-- multiple metrics events (e.g. one 'UTxOChangeEvent'
-- per insert\/delete).
stealMetrics
    :: UpdateTrace slot hash -> [MetricsEvent]
stealMetrics (UpdateForwardTip _ ins del _) =
    replicate (ins + del) UTxOChangeEvent
stealMetrics (UpdateCSMTMeasured _ _ _ dur) =
    [CSMTDurationEvent dur]
stealMetrics
    (UpdateRollbackMeasured _ _ _ _ dur) =
        [RollbackDurationEvent dur]
stealMetrics (UpdateTransactMeasured _ dur) =
    [TransactionDurationEvent dur]
stealMetrics _ = []
