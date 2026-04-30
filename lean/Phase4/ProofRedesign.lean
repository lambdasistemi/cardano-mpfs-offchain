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

end Phase4.ProofRedesign
