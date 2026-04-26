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
- [X] T004 Commit and push initial speckit artifacts, then open a PR.

## Phase 2: Shared Wire Contract

- [X] T005 Add the `cardano-mpfs-api` package to `cabal.project`.
- [X] T006 Extract shared `Cardano.MPFS.API`, `Cardano.MPFS.API.Encoding`, and `Cardano.MPFS.API.Types`.
- [X] T007 Add `TxWriteAPI` for the transaction-building endpoint subset.
- [X] T008 Keep server metrics and ledger conversion helpers in `cardano-mpfs-offchain`.
- [X] T009 Add compatibility re-exports under `Cardano.MPFS.HTTP.*`.

## Phase 3: Endpoint Implementation

- [X] T010 Add `servant-client` dependencies to `cardano-mpfs-client/cardano-mpfs-client.cabal`.
- [X] T011 Add exposed module `Cardano.MPFS.Client.Http`.
- [X] T012 Define `VerifierMode`, `MpfsHttp`, `ClientError`, and MOOG-facing request parameter records.
- [X] T013 Derive write endpoint clients from `TxWriteAPI`.
- [X] T014 Wire `RunVerifier` / `SkipVerifier` handling for every endpoint.
- [X] T015 Re-export the HTTP surface from `Cardano.MPFS.Client`.

## Phase 4: Tests

- [X] T016 Add `Cardano.MPFS.Client.HttpSpec` and register it in the client test suite.
- [X] T017 Cover JSON request encoding and path selection for every write endpoint.
- [X] T018 Cover successful response decode for every write endpoint.
- [X] T019 Cover `VerifyFailed` when `RunVerifier` rejects a response.
- [X] T020 Cover `SkipVerifier` returning a decoded response without verifier rejection.
- [X] T021 Cover transport, non-2xx status, and decode error cases.

## Phase 5: Validation and Merge

- [X] T022 Run `git diff --check`.
- [X] T023 Run `nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct`.
- [X] T024 Run `nix develop --quiet -c cabal build cardano-mpfs-offchain -O0`.
- [X] T025 Run `nix develop --quiet -c just format-check`.
- [X] T026 Run `nix develop --quiet -c just hlint`.
- [X] T027 Update PR body with scope, design decisions, and validation.
- [ ] T028 Wait for green CI and merge through merge-guard.
