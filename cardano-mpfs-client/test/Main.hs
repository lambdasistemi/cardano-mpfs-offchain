module Main (main) where

import Cardano.MPFS.Client.SnapshotSpec qualified as SnapshotSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec SnapshotSpec.spec
