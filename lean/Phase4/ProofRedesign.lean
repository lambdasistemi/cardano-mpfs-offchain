/-
  Phase 4 ProofRedesign — per-endpoint state machine for
  the post-split API redesign (#243).

  Extends the read-side replay state machine of `Phase4.Verify`
  and the prefix-completeness machine of `Phase4.Completeness`
  with predicates and preservation theorems for the new
  per-endpoint response shapes.

  This file scopes the US1 (oracle reads cage state) shapes
  only:

  * `tokenResponseValid`  — `GET /tokens/:id`
  * `factPresentResponseValid` — `GET /tokens/:id/facts/:key`
                                 (HTTP 200, present)
  * `factAbsentResponseValid`  — `GET /tokens/:id/facts/:key`
                                 (HTTP 404, absent with body)

  Successor user stories (US2–US7) extend this file as their
  slices land. Each predicate is a structural assertion over
  the verifier's recordkeeping; cryptographic soundness is
  delegated to the opaque `verifyCsmt`, `verifyMpf`,
  `verifyMpfAbsence`, and prefix-completeness primitives.
-/

import Phase4.Verify
import Phase4.Completeness

namespace Phase4.ProofRedesign

open Phase4.Verify
open Phase4.Completeness

/-- The verifier's full state during a single read-side
    response check. The state UTxO check feeds a
    `Phase4.Verify.VerifiedEnvelope`; the per-cage requests
    completeness check feeds a `CompletenessEnvelope`. Both
    envelopes share the externally-supplied `trustedRoot`. -/
structure ReadEnvelope
    (Root Prefix Key Value Proof : Type) where
  stateEnvelope : VerifiedEnvelope Root Key Value Proof
  requestsEnvelope :
    CompletenessEnvelope Root Prefix Key Value

namespace ReadEnvelope

variable {Root Prefix Key Value Proof : Type}

/-- Initial envelope for a read response anchored at one
    trusted root and one per-cage request prefix. -/
def init
    (r : Root) (pfx : Prefix)
    : ReadEnvelope Root Prefix Key Value Proof :=
  { stateEnvelope := VerifiedEnvelope.init r
  , requestsEnvelope := CompletenessEnvelope.init r pfx }

/-- Replay the state UTxO inclusion proof. -/
def replayState
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) (p : Proof)
    : ReadEnvelope Root Prefix Key Value Proof :=
  { env with
      stateEnvelope :=
        VerifiedEnvelope.replayWitness
          env.stateEnvelope k v p }

/-- Replay a single per-cage request leaf into the
    completeness envelope. -/
def replayRequest
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value)
    : ReadEnvelope Root Prefix Key Value Proof :=
  { env with
      requestsEnvelope :=
        CompletenessEnvelope.replayLeaf
          env.requestsEnvelope k v }

end ReadEnvelope

-- =========================================================
-- Preservation theorems — token response (US1)
-- =========================================================

open ReadEnvelope

variable {Root Prefix Key Value Proof : Type}

/-- After replaying the state UTxO the head of the state
    envelope's `acceptedWitnesses` carries the advertised
    triple verbatim. The Haskell `verifyTokenResponse` binds
    `state_utxo.{ref, txout_cbor, inclusion_proof}` to the
    same triple. -/
theorem replayState_records_state_utxo
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayState env k v p).stateEnvelope.acceptedWitnesses.head?
      = some (k, v, p) := by
  simp [replayState, VerifiedEnvelope.replayWitness]

/-- Replaying the state UTxO does not touch the requests
    envelope. The two replay paths are independent. -/
theorem replayState_preserves_requests_envelope
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayState env k v p).requestsEnvelope
      = env.requestsEnvelope := by
  simp [replayState]

/-- Replaying a request leaf does not touch the state
    envelope. -/
theorem replayRequest_preserves_state_envelope
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) :
    (replayRequest env k v).stateEnvelope
      = env.stateEnvelope := by
  simp [replayRequest]

/-- Replay never rewrites the trusted root carried by either
    envelope. The Haskell `verifyTokenResponse` threads the
    externally-supplied root through every check unchanged. -/
theorem replayState_preserves_state_root
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (replayState env k v p).stateEnvelope.trustedRoot
      = env.stateEnvelope.trustedRoot := by
  simp [replayState, VerifiedEnvelope.replayWitness]

/-- Replay never rewrites the trusted root inside the
    requests envelope either. -/
theorem replayRequest_preserves_requests_root
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) :
    (replayRequest env k v).requestsEnvelope.trustedRoot
      = env.requestsEnvelope.trustedRoot := by
  simp [replayRequest, CompletenessEnvelope.replayLeaf]

/-- The per-cage request prefix is fixed across every replay
    in the read envelope. Specifically `verifyTokenResponse`
    must derive the prefix locally from the trusted blueprint
    and hold it constant for the entire response. -/
theorem replayRequest_preserves_requests_prefix
    (env : ReadEnvelope Root Prefix Key Value Proof)
    (k : Key) (v : Value) :
    (replayRequest env k v).requestsEnvelope.scriptPrefix
      = env.requestsEnvelope.scriptPrefix := by
  simp [replayRequest, CompletenessEnvelope.replayLeaf]

-- =========================================================
-- Fact present / absent (US1, second sub-flow)
-- =========================================================

/-- A fact-present response replays the state UTxO and then
    records a single `(key, value)` pair the MPF inclusion
    verifier accepted against the trie root recovered from
    the state UTxO datum.

    This file does not model the trie-root recovery step
    explicitly — it lives in the Haskell layer between the
    state UTxO replay and the trie-fact replay. The structural
    invariant captured here is that the trie-fact replay does
    not perturb the state-UTxO record. -/
theorem replayTrieFact_preserves_acceptedWitnesses
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (mv : Option Value) (p : Proof) :
    (VerifiedEnvelope.replayTrieFact env k mv p).acceptedWitnesses
      = env.acceptedWitnesses := by
  simp [VerifiedEnvelope.replayTrieFact]

/-- A fact-absent response replays the state UTxO and then
    records an exclusion (`mv = none`). The Haskell
    `verifyFactAbsentResponse` mirrors the same shape; the
    only difference vs the present case is the `Option Value`
    discriminator. -/
theorem replayTrieFact_records_absence
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (p : Proof) :
    (VerifiedEnvelope.replayTrieFact env k none p).acceptedTrieFacts.head?
      = some (k, none, p) := by
  simp [VerifiedEnvelope.replayTrieFact]

/-- Symmetric to `replayTrieFact_records_absence` for the
    inclusion case. The Haskell `verifyFactPresentResponse`
    binds `(key, value, mpf_inclusion_proof)` to this exact
    triple before invoking the cryptographic MPF verifier. -/
theorem replayTrieFact_records_inclusion
    (env : VerifiedEnvelope Root Key Value Proof)
    (k : Key) (v : Value) (p : Proof) :
    (VerifiedEnvelope.replayTrieFact env k (some v) p).acceptedTrieFacts.head?
      = some (k, some v, p) := by
  simp [VerifiedEnvelope.replayTrieFact]

-- =========================================================
-- Uniform write response (boot, requester, reject, sweeps,
-- submit) — the inputs-only replay fold (#243 US2 boot
-- subset). Endpoints that bundle a per-cage requests
-- completeness witness extend this with a
-- `CompletenessEnvelope` step in their own slices.
-- =========================================================

/-- Replay every input from an `UnsignedTxResponse` against
    the same trusted root in one fold. The Haskell call
    site enforces `snapshot.utxo_root = trustedRoot` before
    initialising the envelope; this transition only models
    the per-input replay step. -/
def replayInputs
    (env : VerifiedEnvelope Root Key Value Proof)
    (inputs : List (Key × Value × Proof))
    : VerifiedEnvelope Root Key Value Proof :=
  inputs.foldl
    (fun e kvp =>
      VerifiedEnvelope.replayWitness
        e kvp.1 kvp.2.1 kvp.2.2)
    env

/-- Replay over the input list never rewrites the trusted
    root. -/
theorem replayInputs_preserves_root
    (env : VerifiedEnvelope Root Key Value Proof)
    (inputs : List (Key × Value × Proof)) :
    (replayInputs env inputs).trustedRoot
      = env.trustedRoot := by
  induction inputs generalizing env with
  | nil => rfl
  | cons _ rest ih =>
      simp [replayInputs, VerifiedEnvelope.replayWitness]
      exact ih _

/-- Membership characterisation for the post-replay
    accepted-witnesses set: a triple is accepted iff it
    was already accepted or it was advertised in the
    input list. -/
theorem mem_replayInputs_acceptedWitnesses
    (env : VerifiedEnvelope Root Key Value Proof)
    (inputs : List (Key × Value × Proof))
    (k : Key) (v : Value) (p : Proof) :
    (k, v, p)
      ∈ (replayInputs env inputs).acceptedWitnesses
    ↔ (k, v, p) ∈ env.acceptedWitnesses
      ∨ (k, v, p) ∈ inputs := by
  induction inputs generalizing env with
  | nil => simp [replayInputs]
  | cons head rest ih =>
      have step :
          replayInputs env (head :: rest)
            = replayInputs
                (VerifiedEnvelope.replayWitness env
                  head.1 head.2.1 head.2.2)
                rest := rfl
      rw [step, ih]
      have eta : (head.1, head.2.1, head.2.2) = head := by
        rcases head with ⟨_, _, _⟩; rfl
      simp only [VerifiedEnvelope.replayWitness, eta,
                 List.mem_cons, or_assoc, or_left_comm]

/-- Forgery preservation theorem for `POST /tx/boot`
    (and every other write endpoint that ships only an
    inputs list with no completeness witness): a triple
    not in the advertised inputs cannot appear in the
    replayed envelope's accepted set when starting from
    an empty initial envelope.

    The Haskell `verifyUnsignedTxResponse` mirrors the
    converse direction by replaying each advertised input
    individually; this theorem rules out attacker-injected
    triples from sneaking in. -/
theorem forge_boot_input_breaks_validity
    (r : Root) (inputs : List (Key × Value × Proof))
    (k : Key) (v : Value) (p : Proof)
    (h : (k, v, p) ∉ inputs) :
    let env := replayInputs (VerifiedEnvelope.init r) inputs
    (k, v, p) ∉ env.acceptedWitnesses := by
  intro env
  show ¬ ((k, v, p) ∈ env.acceptedWitnesses)
  rw [show env =
        replayInputs (VerifiedEnvelope.init r) inputs from rfl,
      mem_replayInputs_acceptedWitnesses]
  simp [VerifiedEnvelope.init, h]

-- =========================================================
-- GET /tokens (US5) — trust-minimised cage discovery
-- =========================================================

/-- Replay state for the `GET /tokens` response: a single
    `CompletenessEnvelope` anchored at the externally-supplied
    `trustedRoot` and the locally-derived global state script
    address as `scriptPrefix`. The cryptographic verifier is
    delegated to the upstream `verifyCompletenessProof`; the
    structural invariants captured here are exactly those of
    `Phase4.Completeness.CompletenessEnvelope`. -/
abbrev TokensListEnvelope (Root Prefix Key Value : Type) :=
  CompletenessEnvelope Root Prefix Key Value

namespace TokensListEnvelope

variable {Root Prefix Key Value : Type}

/-- Initial envelope for a `GET /tokens` response. -/
def init
    (r : Root) (pfx : Prefix)
    : TokensListEnvelope Root Prefix Key Value :=
  CompletenessEnvelope.init r pfx

/-- Replay one advertised entry into the listing. -/
def replayEntry
    (env : TokensListEnvelope Root Prefix Key Value)
    (k : Key) (v : Value)
    : TokensListEnvelope Root Prefix Key Value :=
  CompletenessEnvelope.replayLeaf env k v

end TokensListEnvelope

variable {Root Prefix Key Value : Type}

/-- The replay never rewrites the trusted root supplied at
    initialisation. Specialisation of
    `Phase4.Completeness.replayLeaf_preserves_root_trust` to
    the `GET /tokens` envelope. -/
theorem tokensList_replayEntry_preserves_root_trust
    (env : TokensListEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (TokensListEnvelope.replayEntry env k v).trustedRoot
      = env.trustedRoot := by
  simp [TokensListEnvelope.replayEntry,
        CompletenessEnvelope.replayLeaf]

/-- The replay never rewrites the script-address prefix.
    Specialisation of
    `Phase4.Completeness.replayLeaf_preserves_script_prefix`. -/
theorem tokensList_replayEntry_preserves_script_prefix
    (env : TokensListEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (TokensListEnvelope.replayEntry env k v).scriptPrefix
      = env.scriptPrefix := by
  simp [TokensListEnvelope.replayEntry,
        CompletenessEnvelope.replayLeaf]

/-- Folding `replayEntry` over an entries list preserves the
    trusted root anchor — generalisation across the whole
    listing of
    `tokensList_replayEntry_preserves_root_trust`. -/
theorem tokensList_fold_preserves_root_trust
    (env : TokensListEnvelope Root Prefix Key Value)
    (entries : List (Key × Value)) :
    (entries.foldl
        (fun e kv =>
          TokensListEnvelope.replayEntry e kv.1 kv.2)
        env).trustedRoot
      = env.trustedRoot := by
  induction entries generalizing env with
  | nil => rfl
  | cons _ rest ih =>
      simp [TokensListEnvelope.replayEntry,
            CompletenessEnvelope.replayLeaf]
      exact ih _

/-- Folding `replayEntry` over an entries list preserves the
    script-address prefix — generalisation across the whole
    listing of
    `tokensList_replayEntry_preserves_script_prefix`. -/
theorem tokensList_fold_preserves_script_prefix
    (env : TokensListEnvelope Root Prefix Key Value)
    (entries : List (Key × Value)) :
    (entries.foldl
        (fun e kv =>
          TokensListEnvelope.replayEntry e kv.1 kv.2)
        env).scriptPrefix
      = env.scriptPrefix := by
  induction entries generalizing env with
  | nil => rfl
  | cons _ rest ih =>
      simp [TokensListEnvelope.replayEntry,
            CompletenessEnvelope.replayLeaf]
      exact ih _

end Phase4.ProofRedesign
