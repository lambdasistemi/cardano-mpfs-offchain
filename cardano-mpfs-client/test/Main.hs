module Main (main) where

import Cardano.MPFS.Client.BundleSpec qualified as BundleSpec
import Cardano.MPFS.Client.SnapshotSpec qualified as SnapshotSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
    SnapshotSpec.spec
    BundleSpec.spec
