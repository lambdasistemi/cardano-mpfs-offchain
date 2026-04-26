---
description: "Task list for feature 230-typed-http-wrappers"
---

# Tasks: Typed HTTP wrappers for MOOG

**Input**: Design documents from `specs/230-typed-http-wrappers/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [quickstart.md](quickstart.md)
**Issue**: [#230](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/230)

## Phase 1: Setup and Speckit

- [X] T001 Start issue #230 worktree and move the issue to WIP.
- [X] T002 Move browser/WASM hard-gate language out of the #230 issue body.
- [X] T003 Create speckit spec, research, plan, quickstart, and tasks artifacts.
- [ ] T004 Commit and push initial speckit artifacts, then open a PR.

## Phase 2: HTTP Surface

- [ ] T005 Add native HTTP transport dependencies to `cardano-mpfs-client/cardano-mpfs-client.cabal`.
- [ ] T006 Add exposed module `Cardano.MPFS.Client.Http`.
- [ ] T007 Define `BaseUrl`, `VerifierMode`, `MpfsHttp`, and `ClientError`.
- [ ] T008 Define typed request parameter records for boot, request insert/delete/update, retract, reject, update, and end.
- [ ] T009 Add `ToJSON` instances matching the server write-endpoint request contract.

## Phase 3: Endpoint Implementation

- [ ] T010 Implement base URL/path joining for configured MPFS services.
- [ ] T011 Implement shared JSON POST helper with transport, status, and decode error handling.
- [ ] T012 Implement `bootTx`, `requestInsertTx`, `requestDeleteTx`, and `requestUpdateTx`.
- [ ] T013 Implement `retractTx`, `rejectTx`, `updateTx`, and `endTx`.
- [ ] T014 Wire `RunVerifier` / `SkipVerifier` handling for every endpoint.
- [ ] T015 Re-export the HTTP surface from `Cardano.MPFS.Client`.

## Phase 4: Tests

- [ ] T016 Add `Cardano.MPFS.Client.HttpSpec` and register it in the client test suite.
- [ ] T017 Cover JSON request encoding and path selection for every write endpoint.
- [ ] T018 Cover successful response decode for every write endpoint.
- [ ] T019 Cover `VerifyFailed` when `RunVerifier` rejects a response.
- [ ] T020 Cover `SkipVerifier` returning a decoded response without verifier rejection.
- [ ] T021 Cover transport, non-2xx status, and decode error cases.

## Phase 5: Validation and Merge

- [ ] T022 Run `git diff --check`.
- [ ] T023 Run `nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct`.
- [ ] T024 Run `nix develop --quiet -c just format-check`.
- [ ] T025 Run `nix develop --quiet -c just hlint`.
- [ ] T026 Update PR body with scope, design decisions, and validation.
- [ ] T027 Wait for green CI and merge through merge-guard.
