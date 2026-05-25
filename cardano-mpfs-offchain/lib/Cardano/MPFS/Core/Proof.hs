-- |
-- Module      : Cardano.MPFS.Core.Proof
-- Description : Proof serialization (re-exports from cage)
-- License     : Apache-2.0
--
-- Re-exports proof serialization from @cardano-mpfs-cage@.
module Cardano.MPFS.Core.Proof
    ( -- * Serialization
      serializeProof
    , serializeExclusionProof

      -- * Conversion to on-chain types
    , toProofSteps
    ) where

import Data.ByteString qualified as BS

import Cardano.MPFS.Cage.Proof
    ( serializeProof
    , toProofSteps
    )
import MPF.Hashes (MPFHash, mkMPFHash)
import MPF.Proof.Exclusion
    ( MPFExclusionProof
    , mpfExclusionProofSteps
    )
import MPF.Proof.Insertion (MPFProof (..))

-- | Serialize an exclusion proof using the same CBOR step
-- encoder as inclusion proofs. Empty-tree exclusion emits
-- an empty proof-step list, matching the verifier's nullHash
-- handling.
serializeExclusionProof
    :: MPFExclusionProof MPFHash -> BS.ByteString
serializeExclusionProof excProof =
    serializeProof
        MPFProof
            { mpfProofSteps = mpfExclusionProofSteps excProof
            , mpfProofRootPrefix = []
            , mpfProofLeafSuffix = []
            , mpfProofValueHash = mkMPFHash BS.empty
            }
