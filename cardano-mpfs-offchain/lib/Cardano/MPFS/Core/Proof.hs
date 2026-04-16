-- |
-- Module      : Cardano.MPFS.Core.Proof
-- Description : Proof serialization (re-exports from cage)
-- License     : Apache-2.0
--
-- Re-exports proof serialization from @cardano-mpfs-cage@.
module Cardano.MPFS.Core.Proof
    ( -- * Serialization
      serializeProof

      -- * Conversion to on-chain types
    , toProofSteps
    ) where

import Cardano.MPFS.Cage.Proof
    ( serializeProof
    , toProofSteps
    )
