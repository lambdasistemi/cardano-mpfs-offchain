# Feature Specification: Shared Ledger WASM Kernel for MPFS Verify

**Feature Branch**: `359-shared-ledger-wasm`
**Created**: 2026-06-13
**Status**: Draft
**Input**: Issue #359 — migrate `cardano-mpfs-verify` to the released
`cardano-ledger-wasm` v0.1.1 kernel and remove the local duplicate wasm
ledger build.

## User Scenarios & Testing

### User Story 1 - Shared Ledger Kernel (Priority: P1)

As an MPFS maintainer, I build the verify WASM reactor from the released
`cardano-ledger-wasm` kernel instead of a local copy of the ledger wasm
builder and fork pins, so future ledger/fork pin fixes are single-sourced.

**Why this priority**: This removes the independent `1.65.0.0-wasm32.1`
Plutus repin that issue #359 was opened to eliminate.

**Independent Test**: `nix build .#wasm-mpfs-verify` produces
`mpfs-cage-reactor.wasm`, and repository search shows no MPFS-owned
`nix/wasm/forks.json` or `mkCardanoLedgerWasm.nix` source of truth remains.

**Acceptance Scenarios**:

1. **Given** the repo currently has local wasm ledger machinery, **When** the
   WASM verify package is built, **Then** the build uses
   `cardano-ledger-wasm.lib.wasm.mkCardanoLedgerWasm`.
2. **Given** `cardano-ledger-wasm` v0.1.1 owns the wasm Plutus fork pin,
   **When** MPFS builds the reactor, **Then** MPFS does not carry a second
   ledger-kernel Plutus pin.

---

### User Story 2 - Verify Verdict Stability (Priority: P1)

As a wallet or SPA integrator, I receive the same verifier verdict from the
native verifier and the browser-loaded reactor, so the migration does not
change the trust decision clients see.

**Why this priority**: The shared kernel migration is only acceptable if it
preserves the existing MPFS verification semantics.

**Independent Test**: Existing client verifier tests pass, and the reactor
smoke exercises the same fixtures through the bundled WASM path.

**Acceptance Scenarios**:

1. **Given** existing verification fixtures, **When** they are evaluated by the
   native verifier, **Then** all expected verdicts remain unchanged.
2. **Given** the SPA bundles `mpfs-cage-reactor.wasm`, **When** the Playwright
   reactor smoke runs, **Then** it loads the built reactor and completes.

---

### User Story 3 - Release Pin Alignment (Priority: P2)

As a release maintainer, I can audit the dependency pins and see that MPFS is
aligned with the released kernel it consumes.

**Why this priority**: A mismatched CHaP pin can silently re-solve Cardano
dependencies differently from the released kernel.

**Independent Test**: `cabal.project`, `flake.lock`, and wasm project metadata
show the CHaP pin used by `cardano-ledger-wasm` v0.1.1:
`00c90c10812a98ef9680f4bfa269d42366d46d89` /
`2026-04-15T11:20:53Z`.

**Acceptance Scenarios**:

1. **Given** the initial repo used CHaP `2026-05-25T13:25:40Z`, **When** this
   branch is reviewed, **Then** the divergence is resolved or explicitly
   documented in the plan.
2. **Given** `cardano-ledger-wasm` is pinned by source-repository-package,
   **When** Cabal evaluates dependencies, **Then** the source hash is nix32,
   not SRI.

### Edge Cases

- The shared kernel does not include MPFS-specific source-repository packages
  such as `cardano-mpfs-onchain`, `haskell-mts`, `aiken-codegen`,
  `cardano-tx-tools`, or `rocksdb-kv-transactions`; MPFS may still need to
  pass those as extra project content.
- The released builder's dependency hash may not match the combined MPFS
  package set; recomputation is acceptable, but the local ledger-kernel fork
  source of truth must not be reintroduced.
- If aligning CHaP to `2026-04-15T11:20:53Z` breaks native MPFS dependency
  resolution, that is a ticket-level blocker requiring a Q-file rather than a
  silent divergent pin.

## Requirements

### Functional Requirements

- **FR-001**: The repo MUST pin
  `lambdasistemi/cardano-ledger-wasm` at v0.1.1 in `cabal.project` using
  nix32 hash
  `1gamv01par1zgj6wr1lldk51fpad1jw4pwf5jyfvi18x01jnvplx`.
- **FR-002**: The repo MUST consume `cardano-ledger-wasm`'s exported
  `lib.wasm.mkCardanoLedgerWasm`, `forks`, and project fragment as the single
  ledger-kernel wasm source of truth.
- **FR-003**: The MPFS-owned `nix/wasm/forks.json`,
  `nix/wasm/mkCardanoLedgerWasm.nix`, and equivalent local ledger builder
  machinery MUST be removed unless a remaining file is purely MPFS-specific.
- **FR-004**: `wasm-mpfs-verify` MUST still build both `mpfs-verify-reactor`
  and `mpfs-cage-reactor`; the SPA MUST bundle the resulting
  `mpfs-cage-reactor.wasm`.
- **FR-005**: Native verifier tests and reactor-focused smoke tests MUST pass
  after the migration.
- **FR-006**: The independent MPFS Plutus wasm fork repin MUST be gone; any
  required Plutus fork pin MUST be inherited from `cardano-ledger-wasm`.
- **FR-007**: The PR MUST leave CI green, drop `gate.sh` at finalization, and
  be marked ready for human review without self-merging.

### Key Entities

- **Shared Kernel Pin**: The `cardano-ledger-wasm` v0.1.1 git revision,
  Cabal source hash, flake input, and CHaP revision that define the reusable
  wasm ledger closure.
- **MPFS WASM Target**: The `wasm-mpfs-verify` flake package producing
  `mpfs-verify-reactor.wasm` and `mpfs-cage-reactor.wasm`.
- **MPFS-Specific Project Fragment**: Any remaining cabal-wasm content needed
  for MPFS packages that is not part of the shared ledger kernel.

## Success Criteria

### Measurable Outcomes

- **SC-001**: `nix build --quiet .#wasm-mpfs-verify` succeeds and the result
  contains `mpfs-cage-reactor.wasm`.
- **SC-002**: `nix build --quiet .#mpfs-spa` succeeds using the real reactor
  artifact.
- **SC-003**: `nix develop --quiet -c just unit-client "Verify"` succeeds.
- **SC-004**: `nix develop --quiet -c just e2e-spa` succeeds or a live-boundary
  limitation is documented with evidence.
- **SC-005**: Repository search finds no MPFS-owned duplicate
  `1.65.0.0-wasm32.1` Plutus pin outside generated lock/source history from
  `cardano-ledger-wasm`.
- **SC-006**: GitHub CI for PR #360 is green before the PR is marked ready.

## Assumptions

- The CHaP divergence is resolved by aligning this branch to
  `cardano-ledger-wasm` v0.1.1's CHaP pin unless native dependency resolution
  proves that impossible.
- GHC-JS verification remains within the existing project surface for this
  ticket; #359's concrete local acceptance is native plus wasm plus SPA
  reactor smoke.
- `cardano-ledger-wasm` v0.1.1 is the released compatibility point even if
  newer commits exist upstream.
