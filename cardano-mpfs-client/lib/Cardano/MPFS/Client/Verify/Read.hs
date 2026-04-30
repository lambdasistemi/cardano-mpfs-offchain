-- |
-- Module      : Cardano.MPFS.Client.Verify.Read
-- Description : Verifiers for the read-side responses.
--
-- Mirror of the Lean 'Phase4.ProofRedesign.TokensListEnvelope'
-- state machine: each verifier threads an externally-supplied
-- 'TrustedRoot' through every cryptographic check, and locally
-- derives the validator addresses it cross-checks against from
-- the trusted 'Blueprint'. Soundness of the cryptographic
-- primitives is delegated to upstream
-- @csmt-verify@ / @mpf-verify@; this module only enforces the
-- structural binding (snapshot ↔ trusted root, address ↔
-- blueprint).
module Cardano.MPFS.Client.Verify.Read
    ( verifyTokensListResponse
    ) where

import Data.ByteString qualified as BS
import Data.Text (Text)

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types
    ( TokensListResponse (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.TrustedRoot
    ( Blueprint (..)
    , TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyCompleteness
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    )

-- | Verify a @GET \/tokens@ response.
--
-- Two checks:
--
--  1. @snapshot.utxo_root@ matches the externally-supplied
--     'TrustedRoot' byte-for-byte
--     ('TrustedRootMismatch' otherwise).
--  2. The 'UtxoSetWitness' inside @tokens@ is a complete
--     enumeration of the leaves under the locally-derived
--     global state script address (from the trusted
--     'Blueprint') against the trusted root, via
--     'verifyCompleteness'.
--
-- Per-entry classification (legitimate state UTxO vs
-- sweepable garbage, NFT policy / asset-name checks, datum
-- well-formedness) is intentionally /not/ part of this
-- verifier — those are the wrapping application's
-- concern. The trust-minimised guarantee here is that
-- the entries list is exactly what sits at the global
-- state validator address in the trusted snapshot.
verifyTokensListResponse
    :: TrustedRoot
    -> Blueprint
    -> TokensListResponse
    -> Either VerifyError ()
verifyTokensListResponse trustedRoot blueprint response = do
    let TrustedRoot (Wire.Hex trustedBs) = trustedRoot
        snapshotPath = "tokens.snapshot.utxo_root"
        Wire.Hex snapshotBs =
            vsUtxoRoot (tlrSnapshot response)
    checkLength
        "tokens.trusted_root"
        trustedBs
    checkLength snapshotPath snapshotBs
    if snapshotBs == trustedBs
        then Right ()
        else Left (TrustedRootMismatch snapshotPath)
    verifyCompleteness
        "tokens"
        trustedRoot
        (bpStateScriptAddress blueprint)
        (tlrTokens response)
  where
    checkLength :: Text -> BS.ByteString -> Either VerifyError ()
    checkLength field bs
        | BS.length bs == 32 = Right ()
        | otherwise =
            Left (WrongHexLength field 32 (BS.length bs))
