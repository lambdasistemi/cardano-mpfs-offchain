{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.TestEvalContext
-- Description : Shared eval-context fixture for client cage-builder tests.
module Cardano.MPFS.Client.Cage.TestEvalContext
    ( testEvalContext
    , testEvalPParams
    ) where

import Data.Map.Strict qualified as Map
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)

import Cardano.Ledger.Api.PParams
    ( ppCostModelsL
    , ppMaxTxExUnitsL
    )
import Cardano.Ledger.Core (PParams)
import Cardano.Ledger.Plutus.CostModels
    ( CostModel
    , costModelInitParamCount
    , mkCostModel
    , mkCostModels
    )
import Cardano.Ledger.Plutus.ExUnits (ExUnits (..))
import Cardano.Ledger.Plutus.Language (Language (..))
import Cardano.MPFS.Cage.Ledger (ConwayEra)
import Cardano.MPFS.Client (DecodedEvalContext (..))
import Cardano.Slotting.EpochInfo (fixedEpochInfo)
import Cardano.Slotting.Slot (EpochSize (..))
import Cardano.Slotting.Time
    ( SystemStart (..)
    , slotLengthFromMillisec
    )
import Lens.Micro ((&), (.~))

testEvalContext :: PParams ConwayEra -> DecodedEvalContext
testEvalContext pp =
    DecodedEvalContext
        { evalProtocolParameters = testEvalPParams pp
        , evalSystemStart = SystemStart (posixSecondsToUTCTime 0)
        , evalEpochInfo =
            fixedEpochInfo
                (EpochSize 432_000)
                (slotLengthFromMillisec 1_000)
        }

testEvalPParams :: PParams ConwayEra -> PParams ConwayEra
testEvalPParams pp =
    pp
        & ppCostModelsL
            .~ mkCostModels (Map.singleton PlutusV3 plutusV3CostModel)
        & ppMaxTxExUnitsL .~ ExUnits 140_000_000 10_000_000_000

plutusV3CostModel :: CostModel
plutusV3CostModel =
    case mkCostModel
        PlutusV3
        (replicate (costModelInitParamCount PlutusV3) 1_000) of
        Left err -> error ("invalid synthetic PlutusV3 cost model: " <> show err)
        Right model -> model
