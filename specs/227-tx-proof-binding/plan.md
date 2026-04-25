# Implementation Plan: Bind proof bundles to unsigned transactions

**Branch**: `feat/client-bind-proof-bundle-content-to-the-unsigned-t` (spec dir `227-tx-proof-binding`) | **Date**: 2026-04-25 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/227-tx-proof-binding/spec.md`
**Issue**: [#227](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/227)

## Status

**Completed**: Issue #224 merged; issue #227 worktree created; design
decision recorded in `research.md`; Lean binding model and theorems
compile; Haskell client decodes tx inputs/reference inputs and compares
them with endpoint proof roles; focused unit suite passes with the new
binding forgery corpus.

**Current**: Commit, push, and open the issue #227 PR for the
input/reference-input binding slice.

**Blockers**: Full mint/redeemer/output binding is outside this first
slice and must be tracked explicitly after input/reference-input binding
lands.

## Summary

Add a pure `cardano-mpfs-client` binding pass that decodes the unsigned
transaction CBOR far enough to read tx inputs and reference inputs, then
checks those sets against the endpoint proof roles. This rejects the
class of forged response where the proof bundle is valid but belongs to
a different transaction.

## Technical Context

**Language/Version**: Haskell with GHC 9.10.1 target in project docs;
local shell currently uses the repo-pinned GHC.
**Primary Dependencies**: Existing `cborg`, `bytestring`, `text`,
`base16-bytestring`, and existing CSMT/MPF verifier deps. Add no
server-side dependency.
**Storage**: N/A.
**Testing**: `cardano-mpfs-client:unit-tests`.
**Target Platform**: `cardano-mpfs-client` native, WASM, and JS.
**Project Type**: Pure Haskell client library inside a multi-package
repository.
**Performance Goals**: Decode one small transaction CBOR term per
response. Linear in tx body size; not a hot path.
**Constraints**: No `cardano-ledger-*`, no `crypton`, no RocksDB, no IO,
no server-authored summary as an authority.
**Scale/Scope**: One Lean file update, one new client module, small
changes in `Verify.hs`, fixtures, tests, and cabal exposed modules.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | GUARDED | Server keeps ledger-native types. Client intentionally avoids ledger deps per Principle IX and decodes only stable Conway CBOR fields. |
| II. Records of Functions | PASS | No interfaces added. |
| III. Atomic Block Processing | N/A | No indexer or RocksDB path touched. |
| IV. External Signing | PASS | Binding happens before signing; server still returns unsigned tx CBOR. |
| V. Aiken Compatibility | PASS | No proof encoding or datum encoding change. |
| VI. Test Locally First | PASS | Unit fixtures are pure and local. |
| VII. Nix Reproducibility | PASS | No flake input or system package added. |
| VIII. Pure Offline Verification | PASS | Binding pass is pure over response data. |
| IX. One Verifier, Many Targets | PASS | Uses existing pure `cborg`; no native FFI or ledger dependency. |
| X. Lean as Source of Truth | GUARDED | Add binding predicates/theorems before Haskell verifier wiring. |

No justified violations. The guarded items are resolved by the task
ordering.

## Project Structure

### Documentation

```text
specs/227-tx-proof-binding/
├── spec.md
├── research.md
├── plan.md
├── quickstart.md
└── tasks.md
```

### Source Code

```text
lean/Phase4/Verify.lean
cardano-mpfs-client/
├── cardano-mpfs-client.cabal
├── lib/Cardano/MPFS/Client/
│   ├── Bundle.hs
│   ├── Verify.hs
│   ├── Verify/Replay.hs
│   └── Verify/TxView.hs
└── test/Cardano/MPFS/Client/
    ├── Fixtures.hs
    └── VerifySpec.hs
```

**Structure Decision**: isolate generic tx-body parsing and set
comparison in `Verify.TxView`; keep endpoint-specific expected-role
logic in `Verify.hs` beside the existing per-endpoint verifier walks.

## Phase 1.5: Lean before Haskell

Extend `lean/Phase4/Verify.lean` with an abstract transaction-binding
model:

- `TxView` with `inputs` and `referenceInputs`
- `ProofRoles` with `consumed` and `referenced`
- predicate `coversTxView roles tx`
- theorems:
  - `covers_inputs_exact`
  - `covers_references_exact`
  - `missing_input_rejected`
  - `extra_input_rejected`

These theorems are structural and do not model CBOR parsing. They define
what the Haskell decoder result must satisfy after parsing.

## Post-design Constitution Re-check

Re-checked after research: the design remains inside the pure client
verifier boundary. Principle I is respected on the server side; the
client-side exception is already required by Principle IX and is limited
to generic Conway CBOR field extraction.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)* | - | - |
