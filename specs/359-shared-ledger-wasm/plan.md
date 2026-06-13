# Implementation Plan: Shared Ledger WASM Kernel for MPFS Verify

**Branch**: `359-shared-ledger-wasm` | **Date**: 2026-06-13 |
**Spec**: [spec.md](./spec.md)

## Summary

Migrate the MPFS verify wasm build from a local ledger-kernel copy to the
released `lambdasistemi/cardano-ledger-wasm` v0.1.1 flake and Cabal source
package. Align CHaP to the released kernel, delete the duplicated
ledger-wasm builder/fork source of truth, keep the MPFS-specific verifier and
cage reactor package set, and prove native verifier tests plus wasm/SPA builds
still pass.

## Technical Context

**Language/Version**: Haskell via haskell.nix, native GHC plus
ghc-wasm-meta `all_9_12` for wasm32-wasi
**Primary Dependencies**: `cardano-ledger-wasm` v0.1.1,
`cardano-mpfs-verify`, `cardano-mpfs-cage-tx`, `mpfs-spa`
**Storage**: N/A
**Testing**: Nix flake builds, `just unit-client`, `just ci`, Playwright SPA
smoke via `just e2e-spa`
**Target Platform**: Native Linux CI, wasm32-wasi reactor, browser SPA bundle
**Project Type**: Haskell/Nix multi-package repository with PureScript SPA
**Performance Goals**: No performance regression target; preserve byte-identical
verification verdicts
**Constraints**: nix32 hashes in `cabal.project`; no duplicate MPFS-owned
ledger-kernel Plutus fork pin; no verifier rewrite
**Scale/Scope**: One issue-backed PR, one branch, final PR #360

## Constitution Check

- **I. Ledger-Native Types**: PASS. The migration does not introduce shadow
  ledger types; it changes the wasm build source of truth.
- **VII. Nix Reproducibility**: PASS. The shared kernel is pinned by flake
  input and source-repository-package hash; verification is Nix-driven.
- **VIII. Pure Offline Verification**: PASS. The verifier remains the existing
  pure MPFS verifier surface.
- **IX. One Verifier, Many Targets**: PASS target. The Haskell verifier remains
  canonical; WASM/SPA use the compiled reactor.

Complexity tracking: no constitutional violation. The only extra dependency is
the released shared kernel, which replaces duplicate local machinery.

## Project Structure

```text
specs/359-shared-ledger-wasm/
├── spec.md
├── research.md
├── plan.md
├── tasks.md
└── checklists/requirements.md

cabal.project              # source-repository-package + CHaP index-state
flake.nix                  # cardano-ledger-wasm flake input and lib wiring
flake.lock                 # pinned CHaP and cardano-ledger-wasm inputs
cabal-wasm.project         # MPFS-specific wasm package/project content
nix/wasm-targets.nix       # wasm-mpfs-verify target using shared builder
nix/wasm/                  # delete duplicated ledger-kernel builder/fork files
mpfs-spa/                  # unchanged consumer of mpfs-cage-reactor.wasm
```

**Structure Decision**: Keep the existing package layout. Only the dependency
source of truth and wasm target wiring change.

## Phase 0 Research Decisions

- CHaP must align to `cardano-ledger-wasm` v0.1.1:
  `00c90c10812a98ef9680f4bfa269d42366d46d89` /
  `2026-04-15T11:20:53Z`.
- `cardano-ledger-wasm` source pin:
  `845877fde0907b58b150a2c8302033b4e73e9061`,
  nix32 `1gamv01par1zgj6wr1lldk51fpad1jw4pwf5jyfvi18x01jnvplx`.
- Use the released flake's `lib.wasm.mkCardanoLedgerWasm`; do not vendor the
  builder or fork JSON back into MPFS.
- MPFS may retain local project content for its application-specific SRPs.

## Slice Plan

### Slice S1 — Pin and Align Released Kernel

Owned files:

- `cabal.project`
- `flake.nix`
- `flake.lock`

Work:

- Add source-repository-package for `cardano-ledger-wasm` v0.1.1 using nix32
  hash.
- Add flake input for `cardano-ledger-wasm`.
- Align CHaP flake input and Cabal CHaP index-state to the released kernel.

Proof:

- `nix flake metadata --json`
- `nix develop --quiet -c just unit-client "Verify"`

### Slice S2 — Replace Local WASM Builder with Shared Builder

Owned files:

- `flake.nix`
- `nix/wasm-targets.nix`
- `cabal-wasm.project`
- `nix/wasm/default.nix`
- `nix/wasm/cabal-project-fragment.nix`
- `nix/wasm/mkCardanoLedgerWasm.nix`
- `nix/wasm/forks.json`
- `nix/wasm/c-libs/default.nix`
- `nix/wasm/c-libs/libsodium.nix`
- `nix/wasm/c-libs/secp256k1.nix`
- `nix/wasm/c-libs/blst.nix`

Work:

- Import `cardano-ledger-wasm.lib.wasm` in `flake.nix`.
- Rewrite `nix/wasm-targets.nix` so `wasm-mpfs-verify` uses the shared
  builder and only MPFS-specific extra project content.
- Delete MPFS-owned duplicated ledger-kernel builder/fork/C-lib files if no
  remaining local import needs them.
- Remove the local `lambdasistemi/plutus 1.65.0.0-wasm32.1` source of truth.

Proof:

- `rg "1.65.0.0-wasm32.1|dec7b4980|nix/wasm/forks.json|mkCardanoLedgerWasm"`
- `nix build --quiet .#wasm-mpfs-verify`

### Slice S3 — Verification and SPA Smoke

Owned files:

- `gate.sh`
- `.github/workflows/ci.yml` if CI lacks the wasm/SPA build proof
- `justfile` only if a missing focused recipe is needed
- `specs/359-shared-ledger-wasm/tasks.md`

Work:

- Extend the gate or CI if the migrated build is not covered by existing CI.
- Run native verifier tests, wasm build, SPA bundle, SPA reactor smoke, and
  repo CI mirror.
- Capture any live-boundary limitation in `WIP.md` if Playwright cannot run
  locally for environmental reasons.

Proof:

- `./gate.sh`
- `nix develop --quiet -c just e2e-spa`
- GitHub checks green on PR #360

## Data Model

No runtime data model changes. The changed entities are build-time dependency
pins and derivations.

## Contracts

- `wasm-mpfs-verify` still produces `mpfs-cage-reactor.wasm` for
  `nix/mpfs-spa.nix` to copy into `src/assets/mpfs-cage-reactor.wasm`.
- `cardano-mpfs-verify` public Haskell API and reactor envelope behavior stay
  unchanged.

## Quickstart

```bash
nix develop --quiet -c just unit-client "Verify"
nix build --quiet .#wasm-mpfs-verify
test -f result/mpfs-cage-reactor.wasm
nix build --quiet .#mpfs-spa
nix develop --quiet -c just e2e-spa
./gate.sh
```
