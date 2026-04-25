---
description: "Task list for feature 227-tx-proof-binding"
---

# Tasks: Bind proof bundles to unsigned transactions

**Input**: Design documents from `specs/227-tx-proof-binding/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [quickstart.md](quickstart.md)
**Issue**: [#227](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/227)

## Phase 1: Setup

- [X] T001 Start issue #227 worktree and mark it WIP.
- [X] T002 Record design decision: targeted client-side CBOR reader, not authoritative server summary.
- [X] T003 Create speckit artifacts for issue #227.

## Phase 2: Lean model

- [X] T004 Extend `lean/Phase4/Verify.lean` with `TxView`, `ProofRoles`, and `coversTxView`.
- [X] T005 Prove exact-input/reference coverage theorems with no `sorry`.
- [X] T006 Run Lean diagnostics/build for the updated theorem file.

## Phase 3: Client binding implementation

- [X] T007 Add `Cardano.MPFS.Client.Verify.TxView` to decode tx inputs and reference inputs from generic CBOR terms.
- [X] T008 Extend `VerifyError` with `TxBindingFailed Text Text`.
- [X] T009 Wire endpoint-specific expected input/reference roles into every `verify*TxResponse`.
- [X] T010 Update fixture tx CBOR so honest responses have matching inputs/reference inputs.
- [X] T011 Add forged-binding unit tests for funding-only, reference-input, and state/request-consuming endpoint families.
- [X] T012 Expose any new module/error surface in `cardano-mpfs-client.cabal` and top-level client exports.

## Phase 4: Validation and PR

- [X] T013 Run `git diff --check`.
- [X] T014 Run `nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct`.
- [X] T015 Commit and push.
- [X] T016 Open/update PR with scope, residual binding work, and validation status.

## Deferred follow-up

- Redeemer payload binding.
- Exact `UpdateProof.trie_read` to redeemer MPF proof binding.

## Phase 5: Mint and continuing-state-output binding

- [X] T017 Extend Lean with `TxAssetView`, `ProofAssetRoles`, and asset coverage theorems.
- [X] T018 Extend `TxView` to decode mint field `9`, tx outputs, value multiassets, and inline datum markers.
- [X] T019 Bind boot mint to exactly one continuing inline state output.
- [X] T020 Bind reject/update continuing state output to the witnessed state token and reject mint/burns.
- [X] T021 Bind end burn to the witnessed state token and reject continuing state outputs.
- [X] T022 Refresh fixtures and forged-binding tests for mint/output mismatches.
- [X] T023 Run Lean build, `git diff --check`, `just format-check`, `just hlint`, and focused client unit tests.
- [X] T024 Commit, push, open PR, and wait for merge.

## Phase 6: Redeemer and MPF proof binding

- [ ] T025 Extend Lean with redeemer-role and update MPF-read coverage predicates.
- [ ] T026 Prove exact redeemer coverage and update MPF-read mismatch rejection theorems with no `sorry`.
- [ ] T027 Extend `TxView` to decode witness-set field `5` redeemers from supported Conway transaction CBOR.
- [ ] T028 Bind boot/end minting redeemer tags and retract/reject/update spending redeemer tags to endpoint proof roles.
- [ ] T029 Bind update redeemer trie root and MPF proof payload to `UpdateProof.trie_root` and `UpdateProof.trie_read`.
- [ ] T030 Fix server-side update proof emission if real tx redeemers carry MPF reads not present in the response proof.
- [ ] T031 Add forged-binding fixtures/tests for wrong redeemer tag, missing redeemer, and mismatched update MPF proof.
- [ ] T032 Run Lean build, `git diff --check`, `just format-check`, `just hlint`, focused unit tests, and any needed E2E coverage.
- [ ] T033 Commit, push, update PR, wait for green CI, and merge through merge-guard.
