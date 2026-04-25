---
description: "Task list for feature 221-client-cross-targets"
---

# Tasks: Cross-Target Client Verifier Builds

**Input**: Design documents from `specs/221-client-cross-targets/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [quickstart.md](quickstart.md)
**Issue**: [#221](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/221)

## Phase 1: Setup and Speckit

- [X] T001 Start issue #221 worktree and move the issue to WIP.
- [X] T002 Create speckit spec, plan, research, quickstart, and tasks artifacts.
- [ ] T003 Commit and push initial speckit artifacts, then open a PR.

## Phase 2: Native Baseline

- [ ] T004 Run `cardano-mpfs-client:unit-tests` natively and record the result.
- [ ] T005 Add or expose a native `cardano-mpfs-client` library Nix package output if needed for cross-output symmetry.
- [ ] T006 Confirm the client library dependency surface remains verifier-only and pure.

## Phase 3: GHC-WASM Build Proof

- [ ] T007 Prototype the smallest Nix output for `cardano-mpfs-client-wasm`.
- [ ] T008 Run `nix build .#cardano-mpfs-client-wasm --quiet`.
- [ ] T009 If the WASM build fails, record the exact blocker in `research.md`.
- [ ] T010 Fix dependency or Nix exposure blockers that are local to this repository.
- [ ] T011 Mark the WASM target as working only after a clean local build.

## Phase 4: GHC-JS Build Proof

- [ ] T012 Prototype the smallest Nix output for `cardano-mpfs-client-js`.
- [ ] T013 Run `nix build .#cardano-mpfs-client-js --quiet`.
- [ ] T014 If the JS build fails, record the exact blocker in `research.md`.
- [ ] T015 Fix dependency or Nix exposure blockers that are local to this repository.
- [ ] T016 Mark the JS target as working only after a clean local build.

## Phase 5: Cross-Target Guarding

- [ ] T017 Add CI checks for every cross-target output that builds locally.
- [ ] T018 Add a minimal parity/check output if the artifacts are runnable in CI without introducing a second verifier.
- [ ] T019 Add `cardano-mpfs-client` README/package notes documenting supported targets and any remaining blockers.
- [ ] T020 Update `quickstart.md` with final commands and known limitations.

## Phase 6: Validation and Merge

- [ ] T021 Run `git diff --check`.
- [ ] T022 Run `nix develop --quiet -c just format-check`.
- [ ] T023 Run `nix develop --quiet -c just hlint`.
- [ ] T024 Run native client unit tests.
- [ ] T025 Run all claimed cross-target `nix build` outputs.
- [ ] T026 Update the PR body with scope, blockers, and validation.
- [ ] T027 Wait for green CI and merge through merge-guard.
