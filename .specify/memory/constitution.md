<!-- Sync Impact Report
Version: 1.0.0 → 1.1.0
Added:
  - Principle VIII. Pure Offline Verification
  - Principle IX. One Verifier, Many Targets
  - Principle X. Lean as Source of Truth
Rationale: The proof-bearing API (#208) is only useful if the client can
actually run the verifier. That imposes three architectural constraints
on the project that were implicit before and are now normative: the
verifier must be pure, it must be written once and compiled to every
client runtime, and its shape must be formally specified in Lean before
it is implemented.
Templates requiring updates: none — these principles scope the
cardano-mpfs-client boundary, not the service boundary.
Follow-up: before slice 4 lands, prove GHC-WASM / GHC-JS backends can
compile the current cardano-mpfs-client deps; pin or swap any dep that
fails.
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

### IV. External Signing

The API MUST return unsigned CBOR transactions. Signing happens
client-side. The server MUST NOT hold or accept private keys.

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

**Version**: 1.1.0 | **Ratified**: 2026-03-27 | **Last Amended**: 2026-04-20
