-- |
-- Module      : Cardano.MPFS.Client.Verify.Completeness
-- Description : CSMT prefix-completeness verifier.
--
-- Verifies a 'UtxoSetWitness' against a 'TrustedRoot' and a
-- locally-derived script address. The empty-leaf-set case is
-- supported and is the load-bearing primitive for
-- @POST \/tx\/oracle\/end@.
--
-- Used by end-facts verification to prove the locally-derived
-- request-address subtree is complete.
module Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
    ) where

import Data.ByteString (ByteString)
import Data.List (sort)
import Data.Text (Text)

import CSMT.Core.CBOR
    ( renderCompletenessProof
    )
import CSMT.Core.Hash
    ( Hash (..)
    , byteStringToKey
    )
import CSMT.Core.Types
    ( Indirect (..)
    , Key
    )
import CSMT.Verify
    ( CompletenessProof (..)
    , parseCompletenessProof
    , verifyCompletenessProof
    )
import CSMT.Verify.Blake2b
    ( blake2b256
    )

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types.Common
    ( UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , encodeTxIn
    )

-- | Verify a CSMT request-set completeness witness against a
-- trusted root and a locally-derived address prefix.
verifyUtxoSetCompleteness
    :: Text
    -- ^ Dotted path to the request set.
    -> ByteString
    -- ^ Trusted UTxO-CSMT root bytes.
    -> Key
    -- ^ Locally-derived address prefix.
    -> UtxoSetWitness
    -> Either VerifyError ()
verifyUtxoSetCompleteness path trustedRoot prefix UtxoSetWitness{..}
    | verifyCompletenessProofCompat
        trustedRoot
        prefix
        (sort (map (entryLeaf prefix) uswEntries))
        (Wire.unHex uswCompletenessProof) =
        Right ()
    | otherwise =
        Left
            ( CompletenessProofInvalid
                (path <> ".completeness_proof")
            )

verifyCompletenessProofCompat
    :: ByteString
    -> Key
    -> [Indirect Hash]
    -> ByteString
    -> Bool
verifyCompletenessProofCompat trustedRoot prefix leaves proofBytes =
    verifyCompletenessProof trustedRoot prefix leaves proofBytes
        || verifyReversedLiveWitness
  where
    verifyReversedLiveWitness =
        case parseCompletenessProof proofBytes of
            Just (CompletenessWitness ops steps) ->
                verifyCompletenessProof trustedRoot prefix leaves
                    $ renderCompletenessProof
                    $ CompletenessWitness ops (reverse steps)
            _ -> False

entryLeaf :: Key -> UtxoEntryRefOnly -> Indirect Hash
entryLeaf prefix UtxoEntryRefOnly{..} =
    Indirect
        { jump =
            prefix
                <> byteStringToKey
                    ( encodeTxIn
                        (Wire.unHex (urTxId uerRef))
                        (urTxIx uerRef)
                    )
        , value = Hash (blake2b256 (Wire.unHex uerTxOutCbor))
        }
