# Implementation Plan: Cross-Target Client Verifier Builds

**Branch**: `spike/spike-prove-cardano-mpfs-client-cross-compiles-to-` | **Date**: 2026-04-25 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/221-client-cross-targets/spec.md`
**Issue**: [#221](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/221)

## Status

**Completed**: Issue #227 closed; the verifier is now fully tx-bound
for inputs, assets, redeemers, state roots, and update MPF payloads.

**Current**: Establish the #221 cross-target proof path. First commit is
speckit documentation and a dependency/build-surface audit. Next step is
to add the smallest repeatable Nix outputs for the client library
targets, then run them and record any blockers precisely.

**Blockers**: Unknown until the WASM/JS build attempts run. Likely risk
areas are haskell.nix cross support for public sublibraries from `mts`
and any transitive package that assumes native GHC.

## Summary

Prove whether `cardano-mpfs-client` can be built once and shipped across
native GHC, GHC-WASM, and GHC-JS. Add stable Nix targets for the client
library cross builds, document any blockers with exact commands and
errors, and only wire CI as a merge gate for targets that build
repeatably.

## Technical Context

**Language/Version**: Haskell, repo-pinned GHC 9.8.4 for native builds;
cross compiler versions to be determined by haskell.nix/ghc-wasm inputs.
**Primary Dependencies**: `aeson`, `base16-bytestring`, `bytestring`,
`cborg`, `text`, `operational`, and pure `mts` CSMT/MPF sublibraries.
**Storage**: N/A for the client verifier.
**Testing**: `cardano-mpfs-client:unit-tests`; optional cross-target
parity runner over the existing verifier fixture corpus.
**Target Platform**: Native GHC, GHC-WASM, GHC-JS.
**Project Type**: Haskell client library in a multi-package flake.
**Performance Goals**: Build proof only; runtime performance is out of
scope except that parity checks must finish in CI.
**Constraints**: No verifier dependency on `cardano-ledger-*`,
`crypton`, `unix`, `process`, native C FFI, network, disk, or IO.
**Scale/Scope**: One client package, flake/Nix outputs, CI wiring, and
minimal package/release notes if artifacts build.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | N/A | Client verifier intentionally uses API DTOs and generic CBOR terms; no server ledger types touched. |
| II. Records of Functions | PASS | No service interfaces planned. |
| III. Atomic Block Processing | N/A | No persistence/indexer path touched. |
| IV. External Signing | PASS | Verifier remains client-side and offline. |
| V. Aiken Compatibility | GUARDED | Cross-target builds must preserve the same proof bytes and CBOR decoding surface. |
| VI. Test Locally First | PASS | All new build checks must have documented local commands. |
| VII. Nix Reproducibility | GUARDED | The feature is primarily a Nix reproducibility proof. |
| VIII. Pure Offline Verification | PASS | Library target stays pure; HTTP/anchor tickets remain separate. |
| IX. One Verifier, Many Targets | GUARDED | This issue exists to satisfy the principle. |
| X. Lean as Source of Truth | PASS | No new verifier invariant is introduced; this is a build portability proof over the existing Lean-backed verifier. |

No justified violations. Guarded items must be resolved or documented as
blockers before implementation is complete.

## Project Structure

### Documentation

```text
specs/221-client-cross-targets/
├── spec.md
├── plan.md
├── research.md
├── quickstart.md
└── tasks.md
```

### Source Code

```text
flake.nix
nix/project.nix
.github/workflows/ci.yml
cardano-mpfs-client/
├── cardano-mpfs-client.cabal
├── README.md
└── npm/                 # only if packaging can be meaningfully stubbed
```

**Structure Decision**: keep cross-target build logic in the existing
flake/Nix layer. Do not split the client package or create a parallel
verifier package unless a blocker proves that a package boundary is
required.

## Phase 0: Research

Record:

- current `cardano-mpfs-client` dependency surface
- available haskell.nix cross compilation mechanisms in this flake
- command/output for the first GHC-WASM attempt
- command/output for the first GHC-JS attempt
- blocker table with owner action for every failure

## Phase 1: Design

Add the smallest Nix outputs that name the desired artifacts:

- `packages.<system>.cardano-mpfs-client-wasm`
- `packages.<system>.cardano-mpfs-client-js`
- optional cross-target test/parity outputs only after library builds

CI wiring is allowed only for outputs that build locally.

## Phase 2: Implementation

Implement the Nix outputs, package notes, and CI checks. Keep each commit
focused:

1. speckit docs and audit
2. WASM output and blocker/fix
3. JS output and blocker/fix
4. CI/package notes

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none yet)* | - | - |
