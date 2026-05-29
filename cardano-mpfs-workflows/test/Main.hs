-- |
-- Module      : Main
-- Description : Entry point for cardano-mpfs-workflows unit tests.
module Main
    ( main
    ) where

import Test.Hspec (hspec)

import Cardano.MPFS.Workflows.ApplyRequestsSpec qualified as ApplyRequestsSpec
import Cardano.MPFS.Workflows.EndCageSpec qualified as EndCageSpec
import Cardano.MPFS.Workflows.RegisterTokenSpec qualified as RegisterTokenSpec
import Cardano.MPFS.Workflows.RequestWorkflowsSpec qualified as RequestWorkflowsSpec
import Cardano.MPFS.Workflows.RetractRejectSpec qualified as RetractRejectSpec

main :: IO ()
main = hspec $ do
    RegisterTokenSpec.spec
    RequestWorkflowsSpec.spec
    ApplyRequestsSpec.spec
    RetractRejectSpec.spec
    EndCageSpec.spec
