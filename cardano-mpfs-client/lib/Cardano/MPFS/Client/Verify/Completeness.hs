-- |
-- Module      : Cardano.MPFS.Client.Verify.Completeness
-- Description : CSMT prefix-completeness verifier.
--
-- Verifies a 'UtxoSetWitness' against a 'TrustedRoot' and a
-- locally-derived script address. The empty-leaf-set case is
-- supported and is the load-bearing primitive for
-- @POST \/tx\/oracle\/end@.
--
-- Populated by tasks T013 + T014 — see
-- @specs\/243-proof-redesign\/tasks.md@.
module Cardano.MPFS.Client.Verify.Completeness
    (
    ) where
