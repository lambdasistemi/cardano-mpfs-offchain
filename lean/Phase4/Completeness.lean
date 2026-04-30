/-
  Phase 4 Completeness — CSMT prefix-completeness state machine
  for `Cardano.MPFS.Client.Verify.Completeness`.

  Models the structural invariants of a `UtxoSetWitness`: an
  enumerated set of UTxOs at a known script-hash prefix, attested
  by a single CSMT prefix-completeness proof against a trusted
  `utxo_root`.

  As in `Phase4.Verify`, the cryptographic predicate
  (`verifyPrefixCompleteness`) is treated as an opaque parameter.
  This file proves *structural* preservation theorems about the
  envelope's recordkeeping during replay, not cryptographic
  soundness — soundness lives in upstream `mts:csmt-write`'s
  `CSMT.Proof.Completeness` proofs.

  Five preservation theorems are exported, each mirrored on the
  Haskell side by `prop_matchesLeanReference` properties in
  `cardano-mpfs-client/test/Cardano/MPFS/Client/CompletenessSpec.hs`:

  * `replayLeaf_records_leaf` — every accepted leaf is recorded
    verbatim at the head of `attestedLeaves`.
  * `replayLeaf_preserves_root_trust` — replay does not rewrite
    the trusted root; replay is observation, not mutation.
  * `replayLeaf_preserves_script_prefix` — replay does not rewrite
    the prefix the witness is anchored to.
  * `replayLeaf_preserves_count` — N replays produce exactly N
    recorded leaves.
  * `empty_witness_records_no_leaves` — the empty-leaf-set
    witness records no leaves; this is the load-bearing primitive
    for `POST /tx/oracle/end` (US4).

  Note: the field name `scriptPrefix` is used instead of
  `prefix` because `prefix` is a reserved keyword in Lean 4
  (used by `prefix` notation declarations).

  See `specs/243-proof-redesign/` for the corresponding spec,
  plan, and contracts.
-/

namespace Phase4.Completeness

/-- A verified prefix-completeness envelope.

    * `trustedRoot` is the externally-supplied `utxo_root` the
      cryptographic verifier checks against. Never rewritten.
    * `scriptPrefix` is the script-hash prefix the witness is
      anchored to. Derived client-side from the trusted Aiken
      blueprint; never rewritten during replay.
    * `attestedLeaves` records every leaf the cryptographic
      verifier accepted under `scriptPrefix` against
      `trustedRoot`. -/
structure CompletenessEnvelope
    (Root Prefix Key Value : Type) where
  trustedRoot : Root
  scriptPrefix : Prefix
  attestedLeaves : List (Key × Value)
  deriving Repr

namespace CompletenessEnvelope

variable {Root Prefix Key Value : Type}

/-- Empty envelope anchored at a trusted root and a prefix.

    Used as the initial state of a replay over a
    `UtxoSetWitness`. -/
def init (r : Root) (pfx : Prefix)
    : CompletenessEnvelope Root Prefix Key Value :=
  { trustedRoot := r
  , scriptPrefix := pfx
  , attestedLeaves := [] }

/-- Extend an envelope with a single leaf the cryptographic
    verifier accepted under the envelope's prefix.

    The cryptographic step (the upstream
    `CSMT.Proof.Completeness` verifier) is assumed to have
    succeeded at the Haskell call site; this transition only
    records the advertised pair. -/
def replayLeaf
    (env : CompletenessEnvelope Root Prefix Key Value)
    (k : Key) (v : Value)
    : CompletenessEnvelope Root Prefix Key Value :=
  { env with
      attestedLeaves :=
        (k, v) :: env.attestedLeaves }

end CompletenessEnvelope

-- =========================================================
-- Preservation theorems
-- =========================================================

open CompletenessEnvelope

variable {Root Prefix Key Value : Type}

/-- After replaying a leaf, the head of `attestedLeaves`
    carries the advertised pair `(k, v)` verbatim. The Haskell
    replay binds the decoded `(ref, txout_cbor)` from the
    witness JSON to the corresponding pair exactly as this
    transition records `(k, v)`. -/
theorem replayLeaf_records_leaf
    (env : CompletenessEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (replayLeaf env k v).attestedLeaves.head?
      = some (k, v) := by
  simp [replayLeaf]

/-- Replay never rewrites the envelope's trusted root: every
    completeness witness threads a single externally-supplied
    root through every leaf, and the Haskell implementation
    must preserve this invariant. -/
theorem replayLeaf_preserves_root_trust
    (env : CompletenessEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (replayLeaf env k v).trustedRoot = env.trustedRoot := by
  simp [replayLeaf]

/-- Replay never rewrites the envelope's script-hash prefix:
    the address the witness is anchored to is fixed at envelope
    creation time and persists across every recorded leaf. -/
theorem replayLeaf_preserves_script_prefix
    (env : CompletenessEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (replayLeaf env k v).scriptPrefix = env.scriptPrefix := by
  simp [replayLeaf]

/-- Replaying a leaf grows the recorded list by exactly one.
    Together with `replayLeaf_records_leaf`, this gives a
    one-to-one correspondence between leaves accepted by the
    cryptographic verifier and entries in the envelope's
    record. -/
theorem replayLeaf_preserves_count
    (env : CompletenessEnvelope Root Prefix Key Value)
    (k : Key) (v : Value) :
    (replayLeaf env k v).attestedLeaves.length
      = env.attestedLeaves.length + 1 := by
  simp [replayLeaf]

/-- The empty-leaf-set witness records no leaves. This is the
    load-bearing primitive for `POST /tx/oracle/end` (US4):
    when the oracle wants to destroy a cage, the verifier
    must accept a witness with zero leaves under the per-cage
    request prefix as a cryptographic attestation that the
    address is empty.

    The cryptographic verifier still has to be sound — the
    upstream `mts:csmt-write` `CSMT.Proof.Completeness` is
    responsible for that. This theorem is the structural
    counterpart: an envelope initialised at `(r, pfx)` and
    never replayed records exactly the empty list. -/
theorem empty_witness_records_no_leaves
    (r : Root) (pfx : Prefix) :
    ((init r pfx
      : CompletenessEnvelope Root Prefix Key Value)
        ).attestedLeaves
      = [] := by
  simp [init]

end Phase4.Completeness
