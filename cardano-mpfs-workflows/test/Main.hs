-- |
-- Module      : Main
-- Description : Entry point for cardano-mpfs-workflows unit tests.
module Main
    ( main
    ) where

import Test.Hspec (hspec)

import Cardano.MPFS.Workflows.RegisterTokenSpec qualified as RegisterTokenSpec
import Cardano.MPFS.Workflows.RequestWorkflowsSpec qualified as RequestWorkflowsSpec

main :: IO ()
main = hspec $ do
    RegisterTokenSpec.spec
    RequestWorkflowsSpec.spec
