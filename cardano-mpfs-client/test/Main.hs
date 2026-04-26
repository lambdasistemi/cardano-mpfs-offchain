module Main (main) where

import Cardano.MPFS.Client.BundleSpec qualified as BundleSpec
import Cardano.MPFS.Client.HttpSpec qualified as HttpSpec
import Cardano.MPFS.Client.ReadSpec qualified as ReadSpec
import Cardano.MPFS.Client.SnapshotSpec qualified as SnapshotSpec
import Cardano.MPFS.Client.VerifySpec qualified as VerifySpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
    SnapshotSpec.spec
    BundleSpec.spec
    VerifySpec.spec
    ReadSpec.spec
    HttpSpec.spec
