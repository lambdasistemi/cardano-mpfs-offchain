# Implementation Plan: E2E /tx/reject proof verification

**Branch**: `feat/e2e-cover-txreject-in-proofsspec-verifiable-snapsh` (spec dir `224-e2e-tx-reject`) | **Date**: 2026-04-25 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/224-e2e-tx-reject/spec.md`
**Issue**: [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224)

## Status

**Completed**: Constitution baseline confirmed; repo guidance refreshed in
PR #234; feature spec, research, plan, quickstart, and tasks drafted;
`ProofsSpec` now covers `POST /tx/reject`; focused formatter and E2E
validation passed.

**Current**: Commit, push, and update PR #234.

**Blockers**: `just e2e` wrapper is blocked by a stale
`mpfs-bootstrap-genesis` recipe target; direct Cabal E2E validation
passes.

## Summary

Extend `Cardano.MPFS.E2E.ProofsSpec` so the proof-bearing write endpoint
coverage includes `POST /tx/reject`. The scenario already creates a
pending request and the local devnet uses a 10 second reject deadline, so
the implementation waits just past that deadline, posts `/tx/reject`,
decodes `RejectTxResponse`, asserts `verifyRejectTxResponse` accepts it,
then tampers one proof with the existing verifier DSL and asserts a
structured `CsmtReplayFailed`.

## Technical Context

**Language/Version**: Haskell with GHC 9.10.1 in the repo dev shell.
**Primary Dependencies**: Existing `cardano-mpfs-offchain` E2E harness,
`cardano-mpfs-client` verifier DSL, Servant/WAI test session helpers.
No new dependency.
**Storage**: Existing temporary RocksDB database created by `withE2E`.
No schema change.
**Testing**: Focused `ProofsSpec` E2E via `just e2e` or Cabal with a
match pattern; optional client unit tests for existing reject fixtures.
**Target Platform**: Native devnet E2E test. The client verifier path
remains pure and cross-target compatible.
**Project Type**: Multi-package Haskell repository.
**Performance Goals**: Add only the minimum deadline wait required by the
devnet cage config, expected about 11 to 12 seconds.
**Constraints**: Do not submit the built reject transaction; do not
change HTTP JSON contracts; do not introduce new verifier dependencies;
keep the E2E scenario readable as client documentation.
**Scale/Scope**: One E2E test module, one small speckit feature
directory, no production code changes expected.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | PASS | Test continues to use ledger `Tx`, `TxIn`, `TxId`, and `Addr` types. |
| II. Records of Functions | PASS | Reuses existing `Context` record and `TxBuilder` record fields; no typeclasses introduced. |
| III. Atomic Block Processing | PASS | The scenario relies on existing follower/indexer behavior and does not change block processing. |
| IV. External Signing | PASS | `/tx/reject` response is verified but not signed or submitted in this scenario. |
| V. Aiken Compatibility | PASS | No proof encoding changes; the existing server and client verifier produce/consume the same bytes. |
| VI. Test Locally First | PASS | The coverage runs in the local devnet E2E harness. |
| VII. Nix Reproducibility | PASS | No new system dependency or flake input. |
| VIII. Pure Offline Verification | PASS | The verifier call remains `verifyRejectTxResponse :: RejectTxResponse -> Either VerifyError ()`. |
| IX. One Verifier, Many Targets | PASS | No client dependency or verifier implementation change. |
| X. Lean as Source of Truth | N/A | This ticket adds E2E coverage for an already specified verifier path; it does not change verifier invariants. |

No violations. Complexity tracking is empty.

## Project Structure

### Documentation

```text
specs/224-e2e-tx-reject/
├── spec.md
├── research.md
├── plan.md
├── quickstart.md
└── tasks.md
```

### Source Code

```text
cardano-mpfs-offchain/
└── e2e-test/
    └── Cardano/MPFS/E2E/
        └── ProofsSpec.hs
```

**Structure Decision**: keep the reject coverage in the existing
`ProofsSpec` scenario so the write endpoint verifier examples stay in
one place.

## Post-design Constitution Re-check

Re-checked after research: all applicable principles remain PASS. The
chosen design is test-only, uses existing ledger-native response types,
and does not alter the verifier model or server contract.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)* | - | - |
