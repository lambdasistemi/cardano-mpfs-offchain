{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Workflows.TestEvalContext
-- Description : Shared inert eval-context fixture for workflow tests.
module Cardano.MPFS.Workflows.TestEvalContext
    ( testEvalContext
    ) where

import Data.Time.Clock.POSIX (posixSecondsToUTCTime)

import Cardano.Ledger.Api.PParams (emptyPParams)
import Cardano.MPFS.Client.Cage.Eval (DecodedEvalContext (..))
import Cardano.Slotting.EpochInfo (fixedEpochInfo)
import Cardano.Slotting.Slot (EpochSize (..))
import Cardano.Slotting.Time
    ( SystemStart (..)
    , slotLengthFromMillisec
    )

testEvalContext :: DecodedEvalContext
testEvalContext =
    DecodedEvalContext
        { evalProtocolParameters = emptyPParams
        , evalSystemStart = SystemStart (posixSecondsToUTCTime 0)
        , evalEpochInfo =
            fixedEpochInfo
                (EpochSize 432_000)
                (slotLengthFromMillisec 1_000)
        }
