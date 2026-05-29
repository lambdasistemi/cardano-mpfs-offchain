-- |
-- Module      : Main
-- Description : Entry point for cardano-mpfs-workflows unit tests.
module Main
    ( main
    ) where

import Test.Hspec (hspec)

import Cardano.MPFS.Workflows.RegisterTokenSpec qualified as RegisterTokenSpec

main :: IO ()
main = hspec RegisterTokenSpec.spec
