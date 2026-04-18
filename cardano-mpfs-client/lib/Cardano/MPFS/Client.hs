-- |
-- Module      : Cardano.MPFS.Client
-- Description : Public surface of the MPFS verification client.
--
-- Re-exports the snapshot JSON contract and offline verifiers for
-- consumers that want one import.
module Cardano.MPFS.Client
    ( -- * Snapshot
      VerificationSnapshot (..)
    , ChainPoint (..)
    , Hex (..)
    , statusSnapshot

      -- * Verification
    , VerifyError (..)
    , verifyVerificationSnapshot
    ) where

import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    , statusSnapshot
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyVerificationSnapshot
    )
