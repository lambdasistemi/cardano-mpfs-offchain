module Main (main) where

import Cardano.MPFS.Client.APITypeSplitSpec qualified as APITypeSplitSpec
import Cardano.MPFS.Client.BootFactsSpec qualified as BootFactsSpec
import Cardano.MPFS.Client.BundleSpec qualified as BundleSpec
import Cardano.MPFS.Client.Cage.BootSpec qualified as BootSpec
import Cardano.MPFS.Client.Cage.EndSpec qualified as EndSpec
import Cardano.MPFS.Client.Cage.RejectSpec qualified as CageRejectSpec
import Cardano.MPFS.Client.Cage.RequestSpec qualified as RequestSpec
import Cardano.MPFS.Client.Cage.RetractSpec qualified as CageRetractSpec
import Cardano.MPFS.Client.Cage.UpdateSpec qualified as CageUpdateSpec
import Cardano.MPFS.Client.EndFactsSpec qualified as EndFactsSpec
import Cardano.MPFS.Client.HttpSpec qualified as HttpSpec
import Cardano.MPFS.Client.RejectFactsSpec qualified as RejectFactsSpec
import Cardano.MPFS.Client.RequestDeleteFactsSpec qualified as RequestDeleteFactsSpec
import Cardano.MPFS.Client.RequestInsertFactsSpec qualified as RequestInsertFactsSpec
import Cardano.MPFS.Client.RequestUpdateFactsSpec qualified as RequestUpdateFactsSpec
import Cardano.MPFS.Client.RetractFactsSpec qualified as RetractFactsSpec
import Cardano.MPFS.Client.SnapshotSpec qualified as SnapshotSpec
import Cardano.MPFS.Client.UpdateFactsSpec qualified as UpdateFactsSpec
import Cardano.MPFS.Client.Verify.ReactorSpec qualified as ReactorSpec
import Cardano.MPFS.Client.Verify.WriteSpec qualified as WriteSpec
import Cardano.MPFS.Client.VerifySpec qualified as VerifySpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
    APITypeSplitSpec.spec
    SnapshotSpec.spec
    BundleSpec.spec
    BootFactsSpec.spec
    RequestInsertFactsSpec.spec
    RequestDeleteFactsSpec.spec
    RequestUpdateFactsSpec.spec
    RetractFactsSpec.spec
    RejectFactsSpec.spec
    EndFactsSpec.spec
    UpdateFactsSpec.spec
    BootSpec.spec
    RequestSpec.spec
    CageRetractSpec.spec
    CageRejectSpec.spec
    EndSpec.spec
    CageUpdateSpec.spec
    VerifySpec.spec
    WriteSpec.spec
    ReactorSpec.spec
    HttpSpec.spec
