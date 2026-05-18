module Main (main) where

import Cardano.MPFS.Client.APITypeSplitSpec qualified as APITypeSplitSpec
import Cardano.MPFS.Client.BootFactsSpec qualified as BootFactsSpec
import Cardano.MPFS.Client.BundleSpec qualified as BundleSpec
import Cardano.MPFS.Client.Cage.BootSpec qualified as BootSpec
import Cardano.MPFS.Client.Cage.EndSpec qualified as EndSpec
import Cardano.MPFS.Client.EndFactsSpec qualified as EndFactsSpec
import Cardano.MPFS.Client.HttpSpec qualified as HttpSpec
import Cardano.MPFS.Client.SnapshotSpec qualified as SnapshotSpec
import Cardano.MPFS.Client.Verify.WriteSpec qualified as WriteSpec
import Cardano.MPFS.Client.VerifySpec qualified as VerifySpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
    APITypeSplitSpec.spec
    SnapshotSpec.spec
    BundleSpec.spec
    BootFactsSpec.spec
    EndFactsSpec.spec
    BootSpec.spec
    EndSpec.spec
    VerifySpec.spec
    WriteSpec.spec
    HttpSpec.spec
