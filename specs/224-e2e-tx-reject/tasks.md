---
description: "Task list for feature 224-e2e-tx-reject"
---

# Tasks: E2E /tx/reject proof verification

**Input**: Design documents from `specs/224-e2e-tx-reject/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [quickstart.md](quickstart.md)
**Issue**: [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224)

**Tests**: This feature is itself E2E test coverage. The implementation
must include the reject positive path and one reject negative verifier
assertion.

## Phase 1: Setup

**Purpose**: Speckit prerequisites and import audit.

- [X] T001 Confirm `.specify/memory/constitution.md` exists and is not a placeholder.
- [X] T002 Refresh `CLAUDE.md` so future feature work points at the real constitution and current repo commands.
- [X] T003 Create `specs/224-e2e-tx-reject/` with spec, research, plan, quickstart, and tasks artifacts.
- [X] T004 Audit existing `ProofsSpec` imports for the reject response type and forge runner.

## Phase 2: User Story 1 - Verify reject tx response (P1)

**Goal**: `ProofsSpec` exercises the `/tx/reject` HTTP path and verifies
the proof-bearing response with `cardano-mpfs-client`.

**Independent Test**: focused `ProofsSpec` E2E run with `MPFS_BLUEPRINT`
set.

- [X] T005 [US1] Add `RejectTxResponse`, `runForgeReject`, and any needed matcher imports to `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`.
- [X] T006 [US1] Compute a reject wait from the local `CageConfig` process and retract windows, with a small safety margin.
- [X] T007 [US1] POST `/tx/reject` after the request is old enough and decode the response as `RejectTxResponse`.
- [X] T008 [US1] Assert the honest reject response with `shouldAccept verifyRejectTxResponse`.
- [X] T009 [US1] Tamper one reject proof via the DSL and assert `shouldRejectWith` reports the expected `CsmtReplayFailed` path.
- [X] T010 [US1] Remove the stale comment that says reject E2E coverage is skipped.

## Phase 3: Validation

**Purpose**: prove the speckit artifacts and E2E change are coherent.

- [X] T011 Run `git diff --check`.
- [X] T012 Run a focused formatter or formatting check for the touched Haskell file.
- [X] T013 Run focused `ProofsSpec` E2E if `MPFS_BLUEPRINT` is available.
- [X] T014 If focused E2E cannot run locally, document the blocker in the PR and rely on CI for full validation. **Note:** the scenario did run successfully through direct Cabal; the `just e2e` wrapper is separately blocked by a stale `mpfs-bootstrap-genesis` recipe target.

## Phase 4: PR Update

**Purpose**: keep the PR body current.

- [ ] T015 Commit the speckit artifacts and E2E change.
- [ ] T016 Push the branch.
- [ ] T017 Update PR #234 with the reject coverage narrative, runtime impact, and validation status.

## Dependencies & Execution Order

- Phase 1 must finish before editing `ProofsSpec`.
- Phase 2 tasks are sequential because they touch the same scenario.
- Phase 3 follows the implementation.
- Phase 4 follows validation.

## Parallel Opportunities

None. This is a narrow single-file E2E change plus documentation.
