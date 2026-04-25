/-
  Phase 4 Verify — Replay state machine for
  `Cardano.MPFS.Client.Verify`.

  Abstract model of the cryptographic CSMT / MPF proof replay
  introduced in issue #226. The predicates `verifyCsmt`,
  `verifyCsmtAbsence`, `verifyMpf`, `verifyMpfAbsence` are
  treated opaquely: the Lean model proves *structural*
  preservation theorems about the replay transition (key
  binding, value binding, trusted-root invariance), not
  cryptographic soundness. Soundness lives in the upstream
  `mts:csmt-verify` and `mts:mpf-write` proofs.

  Three preservation theorems are exported and each is
  consumed by a QuickCheck `prop_matchesLeanReference` on the
  Haskell side:

  * `replay_binds_key` — the transition records the
    advertised key verbatim; the Haskell replay code must
    mirror this shape.
  * `replay_binds_value` — same for the advertised value.
  * `replay_preserves_root_trust` — the trusted root never
    mutates under replay; replay is observation, not
    mutation.
-/

namespace Phase4.Verify

-- `verifyCsmt`, `verifyCsmtAbsence`, `verifyMpf`,
-- `verifyMpfAbsence` are the cryptographic predicates
-- the Haskell side delegates to `mts:csmt-verify` and
-- `mts:mpf-write`. Here they travel as explicit parameters
-- so the Lean statements are neutral about the specific
-- verifier — structural theorems must hold for any
-- verifier that decides them.

/-- A verified envelope consumed by the proof-replay loop.

    * `trustedRoot` is the advertised `snapshot.utxo_root`
      (or `UpdateProof.trie_root` for MPF). It is only ever
      compared, never rewritten.
    * `acceptedWitnesses` records every `WitnessedUtxo`
      triple the CSMT verifier accepted against
      `trustedRoot`.
    * `acceptedTrieFacts` records every `TrieFact` the MPF
      verifier accepted; the `Option Value` dispatches
      inclusion (`some _`) vs exclusion (`none`). -/
structure VerifiedEnvelope
    (Root Key Value Proof : Type) where
  trustedRoot : Root
  acceptedWitnesses : List (Key × Value × Proof)
  acceptedTrieFacts : List (Key × Option Value × Proof)

namespace VerifiedEnvelope

variable {Root Key Value Proof : Type}

/-- Empty envelope anchored at a trusted root. -/
def init (r : Root)
    : VerifiedEnvelope Root Key Value Proof :=
  { trustedRoot := r
  , acceptedWitnesses := []
  , acceptedTrieFacts := [] }

/-- Extend an envelope with a CSMT witness. The
    cryptographic step (the upstream
    `verifyInclusionProof`) is assumed to have succeeded
    at the Haskell call site; this transition only records
    the advertised triple. -/
def replayWitness
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (v : Value) (p : Proof)
    : VerifiedEnvelope Root Key Value Proof :=
  { env with
      acceptedWitnesses :=
        (k, v, p) :: env.acceptedWitnesses }

/-- Extend an envelope with an MPF fact. The `Option Value`
    dispatches inclusion (`some v`) vs exclusion
    (`none`). -/
def replayTrieFact
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (mv : Option Value) (p : Proof)
    : VerifiedEnvelope Root Key Value Proof :=
  { env with
      acceptedTrieFacts :=
        (k, mv, p) :: env.acceptedTrieFacts }

end VerifiedEnvelope

-- =========================================================
-- Preservation theorems
-- =========================================================

open VerifiedEnvelope

variable {Root Key Value Proof : Type}

/-- After replaying a witness, the head of
    `acceptedWitnesses` carries the advertised key `k`.
    The Haskell replay code binds `proofKey` from the
    decoded `InclusionProof` CBOR to the advertised
    `TxIn` exactly as this transition records `k`. -/
theorem replay_binds_key
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayWitness env k v p).acceptedWitnesses.head?
      = some (k, v, p) := by
  simp [replayWitness]

/-- After replaying a witness, the head of
    `acceptedWitnesses` carries the advertised value `v`
    (its `.2.1` projection). Parallels
    `replay_binds_key` for the value field; the Haskell
    replay binds `proofValue` from the decoded
    `InclusionProof` CBOR to the advertised `TxOut`. -/
theorem replay_binds_value
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayWitness env k v p).acceptedWitnesses.head?.map
        (fun t => t.2.1)
      = some v := by
  simp [replayWitness]

/-- Replay never rewrites the envelope's trusted root:
    every per-endpoint verifier threads a single advertised
    root through every witness, and the Haskell
    implementation must preserve this invariant. -/
theorem replay_preserves_root_trust
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayWitness env k v p).trustedRoot
      = env.trustedRoot := by
  simp [replayWitness]

/-- MPF counterpart of `replay_preserves_root_trust`: the
    trusted root is invariant under the trie-fact
    transition as well. -/
theorem replayTrieFact_preserves_root_trust
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (mv : Option Value) (p : Proof) :
    (replayTrieFact env k mv p).trustedRoot
      = env.trustedRoot := by
  simp [replayTrieFact]

-- =========================================================
-- Transaction binding model (issue #227)
-- =========================================================

/-- The fragment of an unsigned transaction body the client
    verifier decodes for proof/transaction binding. The
    Haskell decoder reads these lists from Conway tx-body
    fields `0` (inputs) and `18` (reference inputs). -/
structure TxView (Key : Type) where
  inputs : List Key
  referenceInputs : List Key

/-- Proof roles advertised by a proof-bearing response after
    endpoint-specific role collection. `consumed` contains
    roles that must be regular tx inputs; `referenced`
    contains roles that must be tx reference inputs. -/
structure ProofRoles (Key : Type) where
  consumed : List Key
  referenced : List Key

/-- The asset fragment of an unsigned transaction body used
    by the client verifier. `minted` contains signed mint
    quantities from tx-body field `9`; `stateOutputs` contains
    token assets carried by continuing state outputs. -/
structure TxAssetView (Asset : Type) where
  minted : List Asset
  stateOutputs : List Asset

/-- Asset roles implied by the endpoint proof payload. Burned
    assets are represented as negative mint quantities in the
    Haskell verifier; this abstract model keeps minted and
    burned roles separate so the proof obligations are explicit. -/
structure ProofAssetRoles (Asset : Type) where
  minted : List Asset
  burned : List Asset
  continuingState : List Asset

namespace TxBinding

variable {Key : Type}
variable {Asset : Type}

/-- A response covers a decoded transaction view exactly when
    its consumed proof roles are exactly the tx inputs and its
    referenced proof roles are exactly the tx reference inputs.
    The Haskell implementation compares sets to ignore CBOR
    ordering, but this abstract model uses lists; the theorem
    shape is the same after canonicalisation. -/
def coversTxView (roles : ProofRoles Key) (tx : TxView Key) : Prop :=
  tx.inputs = roles.consumed ∧
  tx.referenceInputs = roles.referenced

/-- Coverage exposes exact regular-input equality. -/
theorem covers_inputs_exact
    {roles : ProofRoles Key} {tx : TxView Key}
    (h : coversTxView roles tx) :
    tx.inputs = roles.consumed := h.1

/-- Coverage exposes exact reference-input equality. -/
theorem covers_references_exact
    {roles : ProofRoles Key} {tx : TxView Key}
    (h : coversTxView roles tx) :
    tx.referenceInputs = roles.referenced := h.2

/-- If a tx input is not advertised by any consumed proof
    role, coverage is impossible. -/
theorem missing_input_rejected
    {roles : ProofRoles Key} {tx : TxView Key}
    (k : Key)
    (hInTx : k ∈ tx.inputs)
    (hNotInRoles : k ∉ roles.consumed) :
    ¬ coversTxView roles tx := by
  intro h
  rw [h.1] at hInTx
  exact hNotInRoles hInTx

/-- If a consumed proof role does not appear in the tx inputs,
    coverage is impossible. -/
theorem extra_input_rejected
    {roles : ProofRoles Key} {tx : TxView Key}
    (k : Key)
    (hInRoles : k ∈ roles.consumed)
    (hNotInTx : k ∉ tx.inputs) :
    ¬ coversTxView roles tx := by
  intro h
  rw [← h.1] at hInRoles
  exact hNotInTx hInRoles

/-- If a tx reference input is not advertised by any referenced
    proof role, coverage is impossible. -/
theorem missing_reference_rejected
    {roles : ProofRoles Key} {tx : TxView Key}
    (k : Key)
    (hInTx : k ∈ tx.referenceInputs)
    (hNotInRoles : k ∉ roles.referenced) :
    ¬ coversTxView roles tx := by
  intro h
  rw [h.2] at hInTx
  exact hNotInRoles hInTx

/-- If a referenced proof role does not appear in tx reference
    inputs, coverage is impossible. -/
theorem extra_reference_rejected
    {roles : ProofRoles Key} {tx : TxView Key}
    (k : Key)
    (hInRoles : k ∈ roles.referenced)
    (hNotInTx : k ∉ tx.referenceInputs) :
    ¬ coversTxView roles tx := by
  intro h
  rw [← h.2] at hInRoles
  exact hNotInTx hInRoles

/-- Asset coverage is exact for mints, burns, and continuing
    state outputs after the Haskell verifier has canonicalised
    signed mint quantities into role-specific lists. -/
def coversAssetView
    (roles : ProofAssetRoles Asset)
    (tx : TxAssetView Asset) : Prop :=
  tx.minted = roles.minted ++ roles.burned ∧
  tx.stateOutputs = roles.continuingState

/-- Coverage exposes exact mint/burn equality. -/
theorem covers_mint_exact
    {roles : ProofAssetRoles Asset} {tx : TxAssetView Asset}
    (h : coversAssetView roles tx) :
    tx.minted = roles.minted ++ roles.burned := h.1

/-- Coverage exposes exact continuing-state-output equality. -/
theorem covers_state_outputs_exact
    {roles : ProofAssetRoles Asset} {tx : TxAssetView Asset}
    (h : coversAssetView roles tx) :
    tx.stateOutputs = roles.continuingState := h.2

/-- If a tx mints or burns an asset not present in the proof
    roles, asset coverage is impossible. -/
theorem missing_mint_role_rejected
    {roles : ProofAssetRoles Asset} {tx : TxAssetView Asset}
    (a : Asset)
    (hInTx : a ∈ tx.minted)
    (hNotInRoles : a ∉ roles.minted ++ roles.burned) :
    ¬ coversAssetView roles tx := by
  intro h
  rw [h.1] at hInTx
  exact hNotInRoles hInTx

/-- If the proof roles expect a continuing state asset that
    is absent from tx outputs, asset coverage is impossible. -/
theorem missing_state_output_rejected
    {roles : ProofAssetRoles Asset} {tx : TxAssetView Asset}
    (a : Asset)
    (hInRoles : a ∈ roles.continuingState)
    (hNotInTx : a ∉ tx.stateOutputs) :
    ¬ coversAssetView roles tx := by
  intro h
  rw [← h.2] at hInRoles
  exact hNotInTx hInRoles

end TxBinding

end Phase4.Verify
