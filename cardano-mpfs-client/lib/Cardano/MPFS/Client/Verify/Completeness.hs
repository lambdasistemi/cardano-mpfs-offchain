-- |
-- Module      : Cardano.MPFS.Client.Verify.Completeness
-- Description : CSMT prefix-completeness verifier.
--
-- Verifies a 'UtxoSetWitness' against a 'TrustedRoot' and a
-- locally-derived script address. The empty-leaf-set case is
-- supported and is the load-bearing primitive for
-- @POST \/tx\/oracle\/end@.
--
-- Mirrors the Lean 'Phase4.Completeness.replayLeaf' /
-- 'empty_witness_records_no_leaves' theorems: the verifier
-- replays each advertised leaf into a 'CompletenessEnvelope'
-- anchored at @trustedRoot@ with 'scriptPrefix' derived
-- locally from the trusted blueprint, then runs the
-- cryptographic 'verifyCompletenessProof' from
-- @csmt-verify@. Cryptographic soundness is delegated to
-- the upstream primitive; this module only enforces the
-- structural recordkeeping and emits structured errors.
module Cardano.MPFS.Client.Verify.Completeness
    ( verifyCompleteness
    , verifyCompletenessEmpty
    ) where

import Data.ByteString (ByteString)
import Data.Text (Text)

import CSMT.Core.Hash
    ( Hash (..)
    , byteStringToKey
    )
import CSMT.Core.Types (Indirect (..), Key)
import CSMT.Verify (verifyCompletenessProof)
import CSMT.Verify.Blake2b (blake2b256)

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types
    ( UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    )
import Cardano.MPFS.Client.TrustedRoot
    ( Address (..)
    , TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , encodeTxIn
    )

-- | Verify a 'UtxoSetWitness' is a complete enumeration of
-- the leaves under the locally-derived script-address prefix
-- in the trusted CSMT root.
--
-- Three checks:
--
--  1. The trusted root and the supplied address bytes are
--     32-byte hashes (length check, 'WrongHexLength' on
--     mismatch).
--  2. Each entry's @inclusion_proof@ is /not/ replayed here
--     — the wire shape carries no per-entry inclusion
--     proof; soundness comes from the single completeness
--     proof.
--  3. The completeness proof bytes plus the leaves
--     reconstructed from the entry list verify against
--     @trustedRoot@ under @byteStringToKey
--     (blake2b256 addressBytes)@. On failure emits
--     'CompletenessProofInvalid' at the supplied dotted
--     field path.
verifyCompleteness
    :: Text
    -- ^ dotted field path, e.g. @"tokens.requests"@.
    -> TrustedRoot
    -> Address
    -> UtxoSetWitness
    -> Either VerifyError ()
verifyCompleteness path trustedRoot address witness = do
    let TrustedRoot (Wire.Hex rootBs) = trustedRoot
        Address (Wire.Hex addressBs) = address
        UtxoSetWitness
            { uswEntries = entries
            , uswCompletenessProof =
                Wire.Hex proofBs
            } = witness
        prefix = byteStringToKey (blake2b256 addressBs)
        leaves = map (entryToIndirect prefix) entries
    if verifyCompletenessProof rootBs prefix leaves proofBs
        then Right ()
        else Left (CompletenessProofInvalid path)

-- | Verify that a 'UtxoSetWitness' attests an /empty/
-- leaf-set under the given script-address prefix. The
-- entries list MUST be empty — any leaf is a contract
-- violation and emits 'CompletenessExtraLeaf' at the
-- supplied path with the offending ref.
--
-- Otherwise delegates to 'verifyCompleteness': the
-- completeness proof must validate against an empty leaf
-- list under the locally-derived prefix.
--
-- Load-bearing primitive for @POST \/tx\/oracle\/end@:
-- the oracle's signature must not be obtainable while
-- pending requests still sit at the per-cage request
-- address.
verifyCompletenessEmpty
    :: Text
    -> TrustedRoot
    -> Address
    -> UtxoSetWitness
    -> Either VerifyError ()
verifyCompletenessEmpty path trustedRoot address witness =
    case uswEntries witness of
        [] -> verifyCompleteness path trustedRoot address witness
        (entry : _) ->
            Left
                ( CompletenessExtraLeaf
                    path
                    (uerRef entry)
                )

-- | Convert a wire 'UtxoEntryRefOnly' to the 'Indirect Hash'
-- shape the cryptographic verifier expects.
--
-- The CSMT key for a UTxO under address @A@ is
-- @addressPrefix(A) ++ encodeTxIn(ref)@ where
-- @addressPrefix(A) = byteStringToKey (blake2b256 A)@. The
-- 'Indirect' presented to 'verifyCompletenessProof' carries
-- the *absolute* path bits as its @jump@ — i.e. the
-- @addressPrefix(A) ++ byteStringToKey(encodeTxIn ref)@
-- concatenation — per the @csmt-verify@ contract since
-- @haskell-mts#158@. The 'Hash' value is the leaf's stored
-- hash, which is @blake2b256@ of the @TxOut@ CBOR.
entryToIndirect
    :: Key -> UtxoEntryRefOnly -> Indirect Hash
entryToIndirect prefix UtxoEntryRefOnly{uerRef, uerTxOutCbor} =
    let txInBytes :: ByteString
        txInBytes =
            encodeTxIn
                (Wire.unHex (urTxId uerRef))
                (urTxIx uerRef)
    in  Indirect
            { jump =
                prefix <> byteStringToKey txInBytes
            , value = Hash (blake2b256 (Wire.unHex uerTxOutCbor))
            }
