<!-- Sync Impact Report
Version: 1.1.0 → 2.0.0
Amended:
  - Principle IV. External Signing → Client-Side Transaction Construction
Rationale: Returning unsigned transactions makes the server a tx-shape
authority and forces the client verifier to validate that authority's
output, which is anti-pattern (two answers to one question). The pivot
(issue #257, spec 259-fact-provider-pivot) makes the server a
fact-provider only — it serves snapshot + indexer-resolved UTxOs with
CSMT inclusion proofs + MPF facts where applicable + protocol parameters
— and the client builds the unsigned transaction locally using the
shared cage-protocol DSL, then signs with its own keys. Principle IV
is renamed accordingly. The no-keys-on-server invariant is preserved
(it follows trivially from a server that never produces transactions).
This is a MAJOR bump because the principle's normative direction is
reversed for the API shape: the server now MUST NOT return unsigned
transactions, where v1.x said it MUST return them.
Waiver — Principle IX (One Verifier, Many Targets): the cross-target
build infrastructure (GHC-WASM + GHC-JS CI matrix, npm publish, byte-
identity QuickCheck across backends) is not yet wired up in this
repository. It is tracked separately at
https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258.
Spec 259-fact-provider-pivot ships under this explicit waiver; the
verifier surface defined by the pivot is shaped to be cross-target
compatible (no Cardano.Ledger.Api.Tx imports, no IO, pure folds), so
when #258 lands the existing verifier code is admissible without
rework.
Templates requiring updates: spec/plan/tasks under
specs/259-fact-provider-pivot/ already reflect this amendment.
Follow-up: spec 259-fact-provider-pivot Phase 3 verifies this v2.0.0
amendment is on main before opening its server-cutover PR (T037).
-->

# Cardano MPFS Offchain Constitution

## Core Principles

### I. Ledger-Native Types

All domain types MUST come from `cardano-ledger-*`. No shadow types that
duplicate ledger representations. This ensures wire-compatibility with
on-chain validators and eliminates an entire class of encoding bugs.

### II. Records of Functions

No typeclasses for service interfaces. Every boundary (Provider, State,
TxBuilder, Submitter, Indexer) MUST be a record of functions. This keeps
the dependency graph visible, makes mocking trivial, and eliminates
orphan-instance hazards.

### III. Atomic Block Processing

One block MUST equal one RocksDB write batch across all column families.
No partial application of a block is permitted. Crash-safety follows
from this invariant: either the full block is persisted or nothing is.

### IV. Client-Side Transaction Construction

The MPFS server MUST NOT return unsigned transactions. The server
serves only proof-bearing material — snapshot, indexer-resolved
UTxOs with CSMT inclusion proofs, MPF facts where applicable, and
protocol parameters — anchored to a single indexer snapshot.
Clients verify the proofs against an independently-obtained
trusted root, build the unsigned transaction locally using the
shared cage-protocol DSL, and sign with their own keys.

The MPFS server MUST NOT hold or accept private keys. The
no-keys-on-server invariant follows trivially from the above:
since the server never produces transactions, it has no signing
code paths.

### V. Aiken Compatibility

Proof encoding, trie hashing, and datum construction MUST match the
on-chain Aiken validators in `cardano-mpfs-onchain`. Any encoding
divergence is a critical bug.

### VI. Test Locally First

All tests MUST run locally without CI. Unit tests use mocks via
record-of-functions. E2E tests spin up a subprocess `cardano-node`
devnet. Docker and external services are not required for testing.

### VII. Nix Reproducibility

All builds, tests, and CI MUST run inside `nix develop`. No system-level
dependencies outside the flake. CI mirrors local `just ci` exactly.

### VIII. Pure Offline Verification

Every verifier shipped to clients MUST be a pure function
`Hex -> Bundle -> Either VerifyError a`, with no `IO`, no network, no
disk, no timeouts, and no non-determinism. Given one trusted
`utxo_root`, a client MUST be able to answer "does this proof-bearing
response check out?" without making any further call.

This is what makes the proof-bearing API load-bearing. The server is
untrusted infrastructure; trust collapses to the single `utxo_root` the
client obtains independently. Every further check — UTxO-CSMT inclusion,
MPF inclusion/non-inclusion, nested sub-trie traversal, unsigned-tx
input cover — MUST be expressible as a pure fold over the proof data.

Verifier implementations MUST compose as `Kleisli (Either VerifyError)`
arrows. Any verifier that needs `IO` is the wrong shape and MUST be
redesigned; any dependency that forces `IO` into the verifier MUST be
swapped or vendored.

### IX. One Verifier, Many Targets

There MUST be exactly one implementation of every client-side verifier,
written in Haskell in the `cardano-mpfs-client` package, compiled to
every runtime a client might live in:

- GHC native — server, CLI, Haskell tests
- GHC-WASM — browsers, Node, embedded wallets, hardware signers
- GHC-JS backend — environments that cannot load WASM

Re-implementing a verifier in TypeScript, JavaScript, Rust, or any other
language is forbidden. Parallel implementations diverge silently; a
security fix in one lags the others for months; every wallet vendor ends
up shipping a different buggy verifier, which defeats the whole trust
model.

Consequences for `cardano-mpfs-client`:

- No `IO`, no `unix`, no `process`, no native C FFI beyond pure hashing.
- Every new dep MUST be checked against the GHC-WASM and GHC-JS
  compatibility matrix before it is added; if it does not cross-compile,
  it does not go in.
- CI MUST build the WASM and JS artifacts on every commit and run a
  cross-target QuickCheck suite asserting that GHC-native, GHC-WASM, and
  GHC-JS produce byte-identical `Either VerifyError a` outputs for the
  same input. A disagreement between targets is a merge block.
- Every release MUST publish the npm package alongside the Hackage
  package; a release that ships Haskell but not WASM/JS is incomplete.

### X. Lean as Source of Truth

The verifier's state machine MUST be formalized in Lean before it is
implemented in Haskell. The Lean artifacts (predicates on the proof
graph, preservation theorems, the fold that discharges proofs) are the
authoritative specification; the Haskell implementation exists to match
them.

Process:

1. Invariants are discussed and written in prose in the plan / spec.
2. The invariants are translated to Lean 4 predicates and theorems.
3. Theorems compile with no `sorry` and no custom axioms.
4. The Haskell implementation is written with the same signature shape
   as the Lean functions.
5. A QuickCheck property `prop_matchesLeanReference` generates random
   inputs and asserts the Haskell implementation agrees with the
   Lean-extracted reference.
6. Cross-target QuickCheck (Principle IX) extends the same properties to
   the compiled WASM and JS artifacts.

When the implementation reveals a gap, the fix goes into Lean first and
propagates back down the stack. The code does not race ahead of the
proof.

## Cardano Constraints

- Conway era only — no backward compatibility with older eras
- PlutusV3 scripts — datum and redeemer encoding must match Aiken output
- N2C protocols (LocalStateQuery, LocalTxSubmission) — no cardano-db-sync
- RocksDB for persistence — no SQL databases
- Hackage-ready packages — `cabal check` must pass on all libraries

## Development Workflow

- Fourmolu with 70-char line limit, leading commas and arrows
- Haddock on all exports, module headers required
- `just ci` must pass before pushing (build, test, format-check, hlint)
- Conventional commits, linear git history (rebase merge only)
- One branch per worktree, PRs for all changes

## Governance

This constitution is the authority on architectural decisions. Amendments
require a version bump, rationale, and propagation check across dependent
templates. Complexity beyond what a principle allows MUST be justified in
the plan's Complexity Tracking table.

**Version**: 2.0.0 | **Ratified**: 2026-03-27 | **Last Amended**: 2026-05-04
