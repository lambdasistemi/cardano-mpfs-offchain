<!-- Sync Impact Report
Version: 0.0.0 → 1.0.0
Added: All principles (new constitution)
Templates requiring updates:
  - plan-template.md: no changes needed (Constitution Check section is generic)
  - spec-template.md: no changes needed
  - tasks-template.md: no changes needed
Follow-up: none
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

**Version**: 1.0.0 | **Ratified**: 2026-03-27 | **Last Amended**: 2026-03-27
