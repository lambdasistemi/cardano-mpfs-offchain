# Tasks: Boot fact-provider pivot

**Input**: Design documents from `/specs/261-boot-fact-provider-pivot/`
**Prerequisites**: spec.md, plan.md, research.md, data-model.md,
contracts/

**Tests**: Required. Each behavior-changing slice uses RED-GREEN
ordering and lands tests plus implementation in the same bisect-safe
commit.

**Organization**: Tasks are grouped by user story and by the vertical
slice that will become one reviewed implementation commit.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other tasks in the same phase only
  when owned files do not overlap.
- **[Story]**: User story tag from `spec.md`.
- Every task names exact files or modules.

## Phase 1: Foundational Gate And Baseline

**Purpose**: Prepare the local gate for implementation without adding
commands for tests that do not exist yet.

- [ ] T001 Record the current baseline gate result in PR metadata after each implementation slice: `./gate.sh` in repository root and PR #272 body
- [ ] T002 Extend `gate.sh` only after the focused boot tests exist, adding the accepted focused commands from `quickstart.md`

**Checkpoint**: Baseline is documented; focused gate extension is timed
to avoid a false-red gate before tests are added.

---

## Phase 2: User Story 1 - Boot through verified facts and local construction (Priority: P1)

**Goal**: MOOG or another client can request boot facts, verify them,
build the boot transaction locally, sign, submit, and observe indexing.

**Independent Test**: The boot facts e2e flow calls the live HTTP
`POST /facts/boot` endpoint, verifies the response client-side, builds
the transaction locally, signs/submits it, and observes the indexed boot
event.

### Slice A: Boot facts type and verifier

**Commit shape**: One commit closes T003, T004, and T005. Tests are
written first and observed failing before implementation.

- [X] T003 (commit: 01c5371) [P] [US1] RED: add failing BootFacts JSON and `verifyBootFacts` tests for happy path, snapshot tamper, trusted-root mismatch, and proof tamper in `cardano-mpfs-client/test/Cardano/MPFS/Client/BootFactsSpec.hs`
- [X] T004 (commit: 01c5371) [US1] GREEN: add `BootFacts`, `UnverifiedPParams`, `VerifiedBootFacts`, and `verifyBootFacts` in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`, `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`, and `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`
- [X] T005 (commit: 01c5371) [US1] Wire new client modules/tests into `cardano-mpfs-client/cardano-mpfs-client.cabal` and ensure the new boot facts verifier code has no `Cardano.Ledger.Api.Tx` imports or transaction-body inspection

**Subagent brief for Slice A**:

```text
Task: T003, T004, T005

Context:
- You are not alone in the codebase. Do not revert edits made by others.
- Make exactly ONE commit. Do not push.
- Owned files:
  - cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs
  - cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs
  - cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs
  - cardano-mpfs-client/cardano-mpfs-client.cabal
  - cardano-mpfs-client/test/Cardano/MPFS/Client/BootFactsSpec.hs
- Forbidden scope: specs/, gate.sh, README, PR metadata, non-boot endpoints, server handler implementation.
- RED proof: add the BootFacts/verifyBootFacts tests first and run the focused test so it fails because the implementation is missing.
- GREEN proof: focused client tests, grep proof that the new boot facts verifier code has no `Cardano.Ledger.Api.Tx` imports or transaction-body inspection, then ./gate.sh.
- Commit subject: feat(client): add boot facts verifier
- Commit body must include: Tasks: T003, T004, T005
```

### Slice B: Local boot cage builder and legacy byte vector

**Commit shape**: One commit closes T006, T007, T008, and T009. Capture
the legacy vector before deleting the legacy server builder in Slice C.

- [X] T006 (commit: c3911e5) [US1] Capture the legacy boot CBOR vector at `specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor` from the pre-deletion boot builder path
- [X] T007 (commit: c3911e5) [P] [US1] RED: add failing `bootCageTx` byte-equivalence and wallet-policy tests in `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/BootSpec.hs`
- [X] T008 (commit: c3911e5) [US1] GREEN: add `WalletPolicy`, `BuildError`, and `bootCageTx` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Policy.hs`, `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/BuildError.hs`, and `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Boot.hs`
- [X] T009 (commit: c3911e5) [US1] Wire cage modules/tests into `cardano-mpfs-client/cardano-mpfs-client.cabal` and reuse the existing boot datum/asset-name logic without adding server/indexer imports to client cage modules

**Subagent brief for Slice B**:

```text
Task: T006, T007, T008, T009

Context:
- You are not alone in the codebase. Do not revert edits made by others.
- Make exactly ONE commit. Do not push.
- Owned files:
  - specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor
  - cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Policy.hs
  - cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/BuildError.hs
  - cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Boot.hs
  - cardano-mpfs-client/cardano-mpfs-client.cabal
  - cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/BootSpec.hs
- Forbidden scope: server hard swap, non-boot endpoints, docs/assets/swagger.json, PR metadata, gate.sh.
- RED proof: add byte-equivalence and policy tests first and run the focused test so it fails.
- GREEN proof: focused cage tests, forbidden-import grep for client cage modules, then ./gate.sh.
- Commit subject: feat(client): add boot cage transaction builder
- Commit body must include: Tasks: T006, T007, T008, T009
```

### Slice C: Server boot facts hard swap

**Commit shape**: One commit closes T010, T011, T012, and T013. The
legacy boot tx route is removed in the same commit that exposes
`POST /facts/boot`.

- [X] T010 (commit: c6cb386) [P] [US1] RED: add failing API/handler tests proving `POST /facts/boot` exists, returns facts without tx CBOR, handles 400/503 cases, and the legacy boot tx route is absent in `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/BootFactsSpec.hs`
- [X] T011 (commit: c6cb386) [US1] GREEN: add `factsBootHandler` using one `runIndexerTx ctx` block in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [X] T012 (commit: c6cb386) [US1] Replace only the boot write route in `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, leaving non-boot write routes unchanged
- [X] T013 (commit: c6cb386) [US1] Remove server-side boot tx construction from `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Inputs.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Transaction.hs`, and `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`

**Subagent brief for Slice C**:

```text
Task: T010, T011, T012, T013

Context:
- You are not alone in the codebase. Do not revert edits made by others.
- Make exactly ONE commit. Do not push.
- Owned files:
  - cardano-mpfs-api/lib/Cardano/MPFS/API.hs
  - cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Inputs.hs
  - cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Transaction.hs
  - cardano-mpfs-offchain/cardano-mpfs-offchain.cabal
  - cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/BootFactsSpec.hs
- Forbidden scope: non-boot endpoint migration, MOOG files, docs/assets/swagger.json unless needed by route compile tests, PR metadata, gate.sh.
- RED proof: add route/handler tests first and run focused tests so they fail.
- GREEN proof: focused server tests, grep showing legacy boot route absent and non-boot routes present, then ./gate.sh.
- Commit subject: feat(server): hard-swap boot facts endpoint
- Commit body must include: Tasks: T010, T011, T012, T013
```

### Slice D: Live-boundary boot proof

**Commit shape**: One commit closes T014 and T015. This is the live HTTP
boundary proof for US1.

**Prerequisite live-valid builder fix**: Slice D exposed that the
client-side boot builder emitted the TxBuild draft's placeholder
minting redeemer budget instead of a submit-valid budget. The old
server builder used the node evaluator to patch ExUnits before
balancing; the facts-only client builder cannot call the server-side
provider and must make its budget/script-integrity choice explicit.

- [X] T021 (commit: f3b9a3e) [US1] Fix `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Boot.hs` so `bootCageTx` emits a submit-valid minting redeemer budget and recomputed script integrity hash, with a focused client regression test proving the boot tx no longer carries placeholder zero ExUnits

- [X] T014 (commit: 0317885) [P] [US1] RED: add failing e2e proof for `POST /facts/boot` client verification, local build, sign, submit, and indexed boot event in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/BootFactsSpec.hs`
- [X] T015 (commit: 0317885) [US1] GREEN: implement e2e helpers and test wiring in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/BootFactsSpec.hs`, `cardano-mpfs-offchain/e2e-test/main.hs`, and `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`

**Subagent brief for Slice D**:

```text
Task: T014, T015

Context:
- You are not alone in the codebase. Do not revert edits made by others.
- Make exactly ONE commit. Do not push.
- Owned files:
  - cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/BootFactsSpec.hs
  - cardano-mpfs-offchain/e2e-test/main.hs
  - cardano-mpfs-offchain/cardano-mpfs-offchain.cabal
- Forbidden scope: unit test rewrites unrelated to boot facts, specs/, PR metadata, gate.sh.
- RED proof: add e2e test first and run focused e2e so it fails against the missing/incomplete live boundary.
- GREEN proof: focused e2e boot facts flow, then ./gate.sh.
- Commit subject: test(e2e): prove boot facts flow
- Commit body must include: Tasks: T014, T015
```

---

## Phase 3: User Story 2 - Legacy boot transaction endpoint is gone (Priority: P1)

**Goal**: The old boot transaction route and server-side boot tx builder
are absent at PR head.

**Independent Test**: Swagger and source grep find no live legacy boot
write path.

- [X] T016 (commit: c6cb386) [US2] Regenerate `docs/assets/swagger.json` with `nix develop --quiet -c just update-swagger` and verify `POST /facts/boot` is present while the legacy boot tx route is absent
- [X] T017 (commit: 4a5f1c6) [US2] Add or update grep-based regression proof in `gate.sh` after Slice C lands so legacy boot route removal stays enforced

**Commit shape**: T016 may ride with Slice C if Swagger changes are part
of the hard swap. T017 is an orchestrator-owned `chore:` gate extension
commit after the grep target is stable.

---

## Phase 4: User Story 3 - Boot verifier is proof-only (Priority: P2)

**Goal**: The boot verifier validates facts only and cannot inspect a
transaction body.

**Independent Test**: Tests in Slice A pass, and source grep confirms no
transaction grammar imports in the boot verifier surface.

- [X] T018 (commit: 4a5f1c6) [US3] Add the verifier forbidden-import grep to `gate.sh` after Slice A lands
- [X] T019 (commit: 0b63f7c) [US3] Migrate the client HTTP boot transport from legacy `/tx/boot` unsigned-tx responses to `/facts/boot`, and remove or quarantine legacy boot tx-shape verifier exports from `cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`, `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs`, and `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs` once downstream boot usage is migrated

**Commit shape**: T018 is an orchestrator-owned `chore:` gate extension.
T019 rides with Slice A or Slice C depending on when the compiler proves
legacy boot tx verifier exports are unused.

---

## Phase 5: User Story 4 - Paired MOOG cutover is controlled (Priority: P2)

**Goal**: The offchain PR remains draft until the paired MOOG-v2
boundary track records a canary-backed boot proof or an explicit
replacement decision.

**Independent Test**: PR metadata names the paired MOOG requirement, and
the child completion record can list both merge SHAs and cutover window,
or the recorded replacement decision.

- [X] T020 (MOOG PR: cardano-foundation/moog#95, commit: 34a5851) [US4] Open or update paired `cardano-foundation/moog` boot PR that migrates boot from legacy tx response to facts verification plus local build/sign/submit
- [X] T022 (PR #272 metadata updated with MOOG PR #95 evidence) [US4] Update PR #272 body with offchain verification evidence, paired MOOG PR link, and explicit non-claims for non-boot endpoints
- [X] T029 (boundary issue: cardano-foundation/moog#96) [US4] Record the MOOG/MPFS-v2 boundary decision and migration track in `moog-boundaries.md`, PR metadata, and the paired MOOG issue
- [ ] T023 [US4] Keep PR #272 draft until the paired MOOG-v2 boundary track has a canary-backed boot proof or an explicit replacement decision and release-window plan

**Paired MOOG implementation notes**:

- Paired MOOG draft PR: https://github.com/cardano-foundation/moog/pull/95
- Paired MOOG boundary issue: https://github.com/cardano-foundation/moog/issues/96
- Paired MOOG implementation commit:
  `34a5851 feat(mpfs): boot token from verified facts`.
- MOOG local proof: focused
  `nix develop --quiet -c just unit "addressBytesForBoot"` passed with
  1 example, 0 failures; `./gate.sh` passed with build, 123 unit
  examples, format checks, and HLint.
- Expected MOOG files include `/code/moog-boot-facts-pivot/src/MPFS/API.hs`,
  `/code/moog-boot-facts-pivot/src/MPFS/Boot.hs`,
  `/code/moog-boot-facts-pivot/test/MPFS/BootSpec.hs`, `moog.cabal`,
  `cabal.project`, `cabal.project.freeze`, and `flake.nix`.
- MOOG code changes land in the MOOG PR, not in this offchain PR.
- Remaining cross-repo non-claim: a live MOOG boot/sign/submit run
  against the paired offchain branch has not been performed from the
  MOOG PR yet, the old MOOG state-machine assumptions have not been
  validated against the new validators, and release-window coordination
  is not recorded.

---

## Phase 6: Finalization

**Purpose**: Align docs, task stamps, gate, and PR metadata before ready
for review.

- [ ] T024 Run `./gate.sh` at HEAD and record the exact passing evidence in PR #272
- [ ] T025 Confirm every closed task in this file is stamped `[X] T### (commit: <short-sha>)`
- [ ] T026 Run the resolve-ticket finalization audit over commits on PR #272
- [ ] T027 Drop `gate.sh` in the final `chore:` commit only after every task is complete and the paired MOOG-v2 boundary condition is satisfied
- [ ] T028 Mark PR #272 ready for external review

---

## Dependencies & Execution Order

1. Phase 1 baseline metadata stays active throughout the PR.
2. Slice A blocks Slice B because `bootCageTx` consumes
   `VerifiedBootFacts`.
3. Slice B blocks Slice C because the server boot builder is deleted
   only after the client helper and legacy vector exist.
4. Slice C blocks Slice D because the e2e proof needs the live
   `POST /facts/boot` endpoint.
5. Swagger and grep gate tasks follow Slice C.
6. Paired MOOG-v2 boundary readiness blocks finalization.

## Parallel Opportunities

- T003 tests can be drafted while T004 design is reviewed, but both
  close in one commit.
- T007 tests can be drafted while T006 vector capture details are
  inspected, but both close in one commit.
- T010 route tests can be drafted while the server handler shape is
  inspected, but both close in one commit.
- MOOG PR preparation can run after the offchain client contract is
  stable, but final MOOG pinning waits for the canary-backed boundary
  proof and the offchain merge SHA.

## Implementation Strategy

1. Finish Slice A and review/stamp it.
2. Finish Slice B and review/stamp it.
3. Finish Slice C and review/stamp it.
4. Extend `gate.sh` with stable grep/focused proof commands.
5. Finish Slice D and review/stamp it.
6. Prepare paired MOOG-v2 boundary proof and keep the offchain PR draft
   until release-window readiness is recorded.
7. Finalization audit, drop `gate.sh`, mark ready.
