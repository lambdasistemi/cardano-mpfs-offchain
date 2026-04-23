---

description: "Task list for feature 178-crypto-proof-replay"
---

# Tasks: Cryptographic CSMT + MPF proof replay in Client.Verify

**Input**: Design documents from `specs/178-crypto-proof-replay/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)
**Issue**: [#226](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/226)

**Tests**: tests are a first-class deliverable of this feature (FR-008, FR-011, SC-003, SC-006). Every user story phase therefore lists its test tasks alongside implementation tasks. No TDD/red-first ordering is enforced across all tasks — Lean theorems land first (Principle X gate), then Haskell replay + DSL, then the forgery / DSL / E2E test suites that consume the DSL.

**Organization**: tasks are grouped by user story to enable independent implementation and testing. Stories are ordered by priority (US1 → US5 in `spec.md`). The Foundational phase (Phase 2) also captures the **Principle X gate** (Lean) because it is a precondition for any code change.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: `[US1]` … `[US5]` maps to user stories in `spec.md`

## Path Conventions

- Client library:
  `cardano-mpfs-client/lib/Cardano/MPFS/Client/…`
- Client tests:
  `cardano-mpfs-client/test/Cardano/MPFS/Client/…`
- Server-side E2E:
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/…`
- Lean model:
  `lean/Phase4/…`
- Project-wide config:
  `cabal.project`, `cardano-mpfs-client.cabal`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: project-wide wiring that must be in place before any client-library edit. No user-story label.

- [ ] T001 Bump the `source-repository-package` for `haskell-mts` in `/code/cardano-mpfs-offchain-issue-226/cabal.project` (lines 45–49) to a `main`-branch commit that ships both `mts:csmt-verify` (PR #141) and `mts:mpf-write` (PR #147) — e.g. `9a51067` or newer. Leave all other source-repo-package pins unchanged.
- [ ] T002 Recompute the `--sha256:` nix32 hash for the bumped `haskell-mts` pin via `nix flake prefetch github:lambdasistemi/haskell-mts/<commit>` + `nix hash convert --to nix32`, and update `cabal.project` line 49. Rule `pins_main_only`: tag must point at a main commit, not a branch.
- [ ] T003 [P] Extend the `library` stanza of `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/cardano-mpfs-client.cabal` to add `build-depends: mts:csmt-verify`, `mts:mpf-write`, and (only if T013 needs it) `cborg`. Keep `base16-bytestring`, `aeson`, `base`, `bytestring`, `text` as-is. Audit: no `cardano-ledger-*`, no `crypton`, no `rocksdb*`, no new C-FFI dep.
- [ ] T004 [P] Add the new exposed-module list to `cardano-mpfs-client.cabal`: `Cardano.MPFS.Client.Verify.Replay`, `Cardano.MPFS.Client.Verify.DSL`, `Cardano.MPFS.Client.Verify.Examples`. Extend `other-modules` under `test-suite unit-tests` to include `Cardano.MPFS.Client.VerifySpec` and `Cardano.MPFS.Client.Verify.DSLSpec`.
- [ ] T005 Run `nix develop --quiet -c cabal build -O0 cardano-mpfs-client` to confirm the new pin and cabal edits compile against the existing (still-structural) `Client.Verify` module. Fix any pin/hash mismatch before moving on.

**Checkpoint**: `haskell-mts` main commit pinned, cabal advertises the new deps and modules, existing tests still compile and pass.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Principle X (Lean as Source of Truth) gate plus the one pure-Haskell primitive layer every user story phase consumes. No user-story label.

**⚠️ CRITICAL**: no user-story work can begin until this phase is complete.

### Lean-first gate (Principle X)

- [ ] T006 Create `/code/cardano-mpfs-offchain-issue-226/lean/Phase4/Verify.lean` with the abstract primitives described in `plan.md` Phase 1.5: `Proof : Type`, `verifyCsmt : Root → Key → Value → Proof → Prop`, `verifyCsmtAbsence : Root → Key → Proof → Prop`, `verifyMpf`, `verifyMpfAbsence`, and a `VerifiedEnvelope` state with `replayWitness` / `replayTrieFact` transitions.
- [ ] T007 In `/code/cardano-mpfs-offchain-issue-226/lean/Phase4/Verify.lean`, prove the three preservation theorems listed in `plan.md`: `replay_binds_key`, `replay_binds_value`, `replay_preserves_root_trust`. No `sorry`, no custom axioms, no `native_decide` on large terms. Verify with `lean_verify` that each theorem name resolves.
- [ ] T008 Register the new module in `/code/cardano-mpfs-offchain-issue-226/lean/Phase4.lean` (add `import Phase4.Verify`). Confirm `lake build` succeeds via `nix develop --quiet -c lake -Clean build` from the repo root.

### Pure replay primitives (Haskell shape mirrors Lean)

- [ ] T009 Create `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Replay.hs` with the two exports from `contracts/replay-primitives.md`: `replayWitnessedUtxo :: Text -> ByteString -> WitnessedUtxo -> Either VerifyError ()` and `replayTrieFact :: Text -> ByteString -> TrieFact -> Either VerifyError ()`. Haddock each export referencing the Lean theorem name it mirrors.
- [ ] T010 In `Verify/Replay.hs`, implement the **CSMT path**: hex-decode `txOut` / `utxoProof`; parse via `CSMT.Core.CBOR.parseProof` (from `mts:csmt-verify`); Blake2b-256 the `TxIn` encoding using `CSMT.Verify.Blake2b.blake2b256` (pure, no C FFI); compare `proofKey`, `proofValue`; call `CSMT.Verify.verifyInclusionProof`. Emit errors in the order **structural → binding → root** per `research.md` R6 and `contracts/verify-error.md`.
- [ ] T011 In `Verify/Replay.hs`, implement the **MPF path**: dispatch on `TrieFact.value` (`Just v` → inclusion, `Nothing` → exclusion); use the Aiken-parity helpers from `mts:mpf-write`'s `MPF.Verify`; confirm proof shape matches the claim (inclusion for Just / exclusion for Nothing) before the root replay; emit the six fixed reason strings listed in `contracts/verify-error.md`.
- [ ] T012 Extend `VerifyError` in `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs` with the two new constructors `CsmtReplayFailed Text Text` and `MpfReplayFailed Text Text`. Preserve the existing four constructors byte-for-byte (downstream-compat). Update the Haddock on `VerifyError`.
- [ ] T013 [P] If T010/T011 need an extra CBOR decode helper (e.g. `parseProofBytes`), decide per `research.md` R4: prefer re-exporting `CSMT.Core.CBOR.parseProof` via `mts:csmt-verify`; only add a tiny local decoder in `Verify/Replay.hs` if the re-export surface is insufficient. If `cborg` gets added, update T003's audit.
- [ ] T014 Wire `replayWitnessedUtxo` into `Cardano.MPFS.Client.Verify.checkWitnessedUtxo` in `Verify.hs`, threading the decoded `snapshot.utxo_root` bytes through each per-endpoint verifier. Order: existing structural checks → new `replayWitnessedUtxo` call. Do not alter the structural-error surface.
- [ ] T015 Wire `replayTrieFact` into `Verify.hs::checkTrieFact`, threading the decoded `UpdateProof.trie_root` bytes. Keep `checkTrieFacts` as the list dispatcher; only `checkTrieFact` grows the replay call.
- [ ] T016 Run `nix develop --quiet -c cabal build -O0 cardano-mpfs-client` and fix any type errors surfaced by T009–T015. No tests run yet.

**Checkpoint**: Lean theorems compile with zero `sorry`; the pure replay primitives exist; `Client.Verify` still returns `Either VerifyError ()` but now with cryptographic replay wired in. No user-story test relies on these being complete — but no user-story test can succeed without them.

---

## Phase 3: User Story 1 — Accept an honest response (P1) 🎯 MVP

**Goal**: the client verifier returns `Right ()` for every honest per-endpoint response, and the E2E spec's first reading path is a happy-path tutorial.

**Independent Test**: `ProofsSpec` runs on devnet; every `response `shouldAccept` verify*TxResponse` assertion passes.

### DSL (tutorial entry points)

- [ ] T017 [P] [US1] Create `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/DSL.hs` exporting `shouldAccept :: (HasCallStack, Show a) => a -> (a -> Either VerifyError ()) -> Expectation` and the matcher-less `shouldRejectWith` stub (implementation in T026). Haddock each combinator with a link to the spec scenario that introduces it (FR-012).
- [ ] T018 [P] [US1] Re-export `shouldAccept`, `shouldRejectWith`, and every verifier (`verifyBootTxResponse`, …, `verifyUpdateTxResponse`) from `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/lib/Cardano/MPFS/Client.hs` so a downstream user needs a single import.

### Unit tests (positive corpus)

- [ ] T019 [US1] Create `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/test/Cardano/MPFS/Client/VerifySpec.hs` with one honest-fixture scenario per response type (`BootTxResponse`, `RequestTxResponse`, `RetractTxResponse`, `EndTxResponse`, `UpdateTxResponse`). Each uses `response \`shouldAccept\` verify*TxResponse`. Fixtures are hand-crafted once (generated by a tiny helper in the module) so tests don't need a running devnet.
- [ ] T020 [US1] Include a **mixed-fact** `UpdateTxResponse` fixture in `VerifySpec.hs`: at least one `trie_read[i]` inclusion (`value = Just _`) and one exclusion (`value = Nothing`); both must replay and `shouldAccept` must pass. Tracks spec US2 acceptance scenarios 1–2.
- [ ] T021 [US1] Include the **empty `trie_read`** edge case in `VerifySpec.hs`: an `UpdateTxResponse` with zero trie-reads returns `Right ()` and performs no MPF replay. Tracks spec US2 acceptance scenario 3 and spec Edge Cases.

### E2E positive path

- [ ] T022 [US1] Extend `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs` so every existing `assertVerify "<endpoint>" verify*TxResponse response` call becomes `response \`shouldAccept\` verify*TxResponse` and reads as tutorial prose. The five live endpoints (boot, request/insert, update, retract, end) are covered; retain the `/tx/reject` deferral comment pointing at issue #224.
- [ ] T023 [US1] Add an E2E `it "accepts an update batch with mixed inclusion/exclusion trie reads" $ …` case in `ProofsSpec.hs` matching US2 scenario 1: boot → requestInsert → updateToken → requestInsert a second key → call `POST /tx/update` → `shouldAccept`.

**Checkpoint**: the happy-path tutorial reads end-to-end. `cabal test cardano-mpfs-client` and `MPFS_BLUEPRINT=… cabal test cardano-mpfs-offchain:e2e-test --match ProofsSpec` both pass. SC-001 / SC-002 met for the positive subset; SC-006's "positive half" (6 scenarios) delivered.

---

## Phase 4: User Story 2 — MPF positive batch (P1)

**Goal**: provide explicit coverage of the mixed-inclusion / exclusion `UpdateProof` batch as a named scenario, separate from the US1 smoke.

**Independent Test**: running the new scenario by name exercises at least one inclusion + one exclusion `trie_read` in a single `UpdateProof` and asserts `shouldAccept`.

- [ ] T024 [US2] Promote the US1 mixed-fact fixture (T020) into a dedicated `describe "UpdateProof trie_read" $ …` block in `VerifySpec.hs`, adding one `it "accepts mixed inclusion and exclusion trie reads" …` and one `it "accepts an empty trie_read" …` case (explicit names mirror spec US2 acceptance scenarios 1 and 3). Rules: re-use the existing fixtures; do not duplicate `shouldAccept` on Boot/Request/etc. here.
- [ ] T025 [US2] Add an E2E scenario in `ProofsSpec.hs`: `it "update accepts a batch with an absent key trie_read" $ …`. The scenario requests insert for key `"k1"`, processes it, then calls `POST /tx/update` and asserts the response's `UpdateProof.trie_read` contains at least one `value = Nothing` entry (exclusion) and `shouldAccept` passes. If the existing `updateToken` flow does not produce absence claims, adjust the fixture via an explicit `POST /tx/request/delete` against a missing key.

**Checkpoint**: the MPF positive story is visible as a stand-alone scenario group and the E2E suite exercises both inclusion and exclusion branches in the same response.

---

## Phase 5: User Story 3 — Reject a forged `utxo_proof` (P1)

**Goal**: every CSMT forgery path listed in spec US3 surfaces as `CsmtReplayFailed <path> <reason>` and the DSL reads as tutorial prose on the negative path.

**Independent Test**: unit tests construct forged CSMT responses; E2E tampers real devnet responses with the DSL helpers; `shouldRejectWith (csmtReplayFailedAt …)` passes in every case.

### DSL negative side

- [ ] T026 [US3] Implement `shouldRejectWith`, the `ErrorMatcher` type, `withReason`, `csmtReplayFailedAt`, and `mpfReplayFailedAt` in `Verify/DSL.hs`. Matcher's `toString` renders the expected-vs-got diff per `contracts/dsl.md`.
- [ ] T027 [P] [US3] Add the CSMT forgery helpers in `Verify/DSL.hs`: `forgingRandomUtxoProofAt`, `forgingWrongRootAt`, `tamperingTxOutAt`. Each returns a deep-copied response with exactly one targeted field tampered. Provide deterministic `'`-suffixed variants taking an explicit `StdGen` (FR-010; `contracts/dsl.md`).
- [ ] T028 [P] [US3] Re-export all DSL symbols added in T026–T027 from `Cardano.MPFS.Client.hs`.

### CSMT forgery unit tests

- [ ] T029 [US3] In `VerifySpec.hs`, add **four CSMT forgery cases** matching spec US3 acceptance scenarios:
  1. Boot funding `utxo_proof` tampered to random bytes → `CsmtReplayFailed "boot.funding[0].utxo_proof" "malformed proof CBOR"`.
  2. Retract `state_ref.utxo_proof` correct against a *different* root (swap the advertised `snapshot.utxo_root`) → `CsmtReplayFailed "retract.state_ref.utxo_proof" "root mismatch"`.
  3. End `state.utxo_proof` correct but `state.tx_out` tampered → `CsmtReplayFailed "end.state.utxo_proof" "value binding mismatch"`.
  4. Update `requests[0].utxo_proof` correct but advertised `tx_in.tx_ix` tampered → `CsmtReplayFailed "update.requests[0].utxo_proof" "key binding mismatch"`.

### CSMT forgery E2E coverage

- [ ] T030 [US3] In `ProofsSpec.hs`, add paired negative E2E scenarios for each of the five live write endpoints using the DSL helpers: `forgingRandomUtxoProofAt "<endpoint>.<role>"` → `shouldRejectWith (csmtReplayFailedAt "<endpoint>.<role>.utxo_proof")`. Endpoints: boot, request/insert, update, retract, end. `/tx/reject` covered by unit test only (see T042).

**Checkpoint**: SC-003's CSMT half (≥ 4 cases) plus SC-006's negative half (5 live endpoint E2E + reject unit fallback) met. Spec US3 acceptance scenarios 1–3 green.

---

## Phase 6: User Story 4 — Reject a forged `mpf_proof` (P1)

**Goal**: every MPF forgery path (inclusion-vs-exclusion shape mismatch, tampered value, wrong root) surfaces as `MpfReplayFailed <path> <reason>`.

**Independent Test**: unit tests construct forged `UpdateProof.trie_read` payloads; E2E tampers real devnet responses; `shouldRejectWith (mpfReplayFailedAt …)` passes.

### DSL MPF-specific helpers

- [ ] T031 [P] [US4] Add `tamperingTrieValueAt`, `dropToExclusionAt`, and `promoteToInclusionAt` to `Verify/DSL.hs`. Each operates on `UpdateTxResponse` by index into `UpdateProof.trie_read` (FR-010; `contracts/dsl.md`). Provide the deterministic `'`-suffixed variants.
- [ ] T032 [P] [US4] Re-export the three new helpers from `Cardano.MPFS.Client.hs`.

### MPF forgery unit tests

- [ ] T033 [US4] In `VerifySpec.hs`, add **four MPF forgery cases** matching spec US4 acceptance scenarios:
  1. `trie_read[0].value` flipped (inclusion claim, correct key-path, tampered value) → `MpfReplayFailed "update.trie_read[0].mpf_proof" "value binding mismatch"`.
  2. `trie_read[0].value = Nothing` but the proof is a real inclusion proof (use `dropToExclusionAt 0` on an honest fixture) → `MpfReplayFailed "update.trie_read[0].mpf_proof" "inclusion proof for absence claim"`.
  3. `trie_read[0].value = Just v` but the proof is a real exclusion proof (use `promoteToInclusionAt 0`) → `MpfReplayFailed "update.trie_read[0].mpf_proof" "exclusion proof for inclusion claim"`.
  4. Correct inclusion proof for the right key/value but against a different root (swap `UpdateProof.trie_root`) → `MpfReplayFailed "update.trie_read[0].mpf_proof" "root mismatch"`.

### MPF forgery E2E coverage

- [ ] T034 [US4] In `ProofsSpec.hs`, add an E2E scenario pair: honest `updateResp \`shouldAccept\` verifyUpdateTxResponse` (already present after T022) + `tampered <- updateResp \`tamperingTrieValueAt\` 0; tampered \`shouldRejectWith\` … mpfReplayFailedAt "update.trie_read[0].mpf_proof"`.
- [ ] T035 [US4] Add a second E2E MPF scenario: `dropToExclusionAt 0` on the same `updateResp` and assert `"inclusion proof for absence claim"`. This covers the shape-mismatch branch on a real devnet response, not just a hand-crafted fixture.

**Checkpoint**: SC-003's MPF half (4 cases) met. Spec US4 acceptance scenarios 1–3 green.

---

## Phase 7: User Story 5 — Cross-target byte identity (P1)

**Goal**: Principle IX stays green with the new replay + DSL wiring; GHC-native, GHC-WASM, and GHC-JS produce byte-identical `Either VerifyError ()` outputs for the same inputs.

**Independent Test**: the existing `cardano-mpfs-client-cross-target` flake check runs on all three targets and diffs outputs.

- [ ] T036 [US5] Extend the cross-target QuickCheck corpus in `/code/cardano-mpfs-offchain-issue-226/cardano-mpfs-offchain/cardano-mpfs-client/test/` (add module `Cardano.MPFS.Client.Verify.CrossTargetSpec.hs` if one does not already exist) so every fixture from `VerifySpec.hs` (honest + forged, CSMT + MPF) is exercised on all three backends. Assert byte identity of the `Either VerifyError a` encoding (JSON or `Show`, whichever the current suite uses).
- [ ] T037 [US5] Add a `prop_matchesLeanReference :: UpdateTxResponse -> Property` QuickCheck property in the cross-target suite that generates random envelopes via Aeson `Arbitrary` instances and asserts the Haskell `verifyUpdateTxResponse` result agrees with the Lean-extracted reference from `Phase4.Verify` (Principle X, `plan.md` Phase 1.5).
- [ ] T038 [US5] Run `nix build .#checks.<system>.cardano-mpfs-client-cross-target` locally on Linux and confirm green. If the check name differs, locate it in `/code/cardano-mpfs-offchain-issue-226/flake.nix` and reference the real name in the PR description.
- [ ] T039 [US5] Add a `build-depends` audit note to the PR description listing the three new pure deps (`mts:csmt-verify`, `mts:mpf-write`, optional `cborg`) and explicitly re-stating no `cardano-ledger-*` / `crypton` / `rocksdb*` / C-FFI deps were added (FR-007, SC-005).

**Checkpoint**: SC-004 + SC-005 met; Principle IX audited.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: documentation, manual-readability validation, CI locally, and backlog follow-ups.

- [ ] T040 [P] Run `nix develop --quiet -c just ci` at the repo root; fix any formatting (fourmolu 70-char), hlint, or `cabal check` finding. Memory rule `lint_before_push` requires a green `just ci` before every push.
- [ ] T041 Update `/code/cardano-mpfs-offchain-issue-226/specs/178-crypto-proof-replay/quickstart.md` if the DSL surface diverged from the plan during implementation (names, argument order) — quickstart is the consumer manual.
- [ ] T042 File or re-reference a tracking note in issue [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224) confirming that `/tx/reject` negative E2E coverage is deferred and the unit-test counterpart is present (matches spec FR-011 escape hatch). Memory rule `drops_need_issues`.
- [ ] T043 Add a "manual readability walk-through" section to the PR description (SC-007): a 3-line bullet list a reviewer can tick off after reading `ProofsSpec.hs` alone — (a) listed every endpoint, (b) listed every rejection kind, (c) listed every forgery helper the suite uses. PR description is load-bearing (`update_pr_description` memory).
- [ ] T044 Run `MPFS_BLUEPRINT=<path> nix develop --quiet -c cabal test -O0 cardano-mpfs-offchain:e2e-test --test-options="--match ProofsSpec"` to confirm the full E2E matrix passes on devnet before pushing the final commit. Memory rule `always_local_ci`.
- [ ] T045 Regenerate Haddock for `cardano-mpfs-client` locally and spot-check that every new combinator has a prose Haddock linking back to the spec scenario that introduces it (FR-012). No auto-publish required here.
- [ ] T046 Open the PR against `main`: labels `feat` and `tx-builder` or the appropriate client-side label already in use; assignee `paolino`; link issue #226; paste the manual-readability walk-through from T043 and the `build-depends` audit from T039 (workflow skill).

**Checkpoint**: PR is mergeable. CI green locally and on GitHub; Haddock current; backlog follow-ups tracked; SC-007 satisfied.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: no dependencies. T001–T002 must run in sequence (pin then hash); T003/T004 can run in parallel; T005 runs last.
- **Phase 2 (Foundational)**: depends on Phase 1. T006 → T007 → T008 (Lean). In parallel with Lean, T009 → T010 → T011 → T012 → T013? → T014 → T015 → T016 can start (Haskell shape is informed by Lean signatures but can be drafted while Lean proofs are being completed). T016 must pass before any user-story task begins.
- **Phase 3 (US1)**: depends on Phase 2 (needs `Verify.Replay` wired into `Verify.hs`).
- **Phase 4 (US2)**: depends on Phase 3 (re-uses the same DSL + fixture scaffolding).
- **Phase 5 (US3)**: depends on Phase 3 (the DSL `shouldRejectWith` etc. build on Phase 3 entry points).
- **Phase 6 (US4)**: depends on Phase 5 (extends the same DSL with MPF helpers).
- **Phase 7 (US5)**: depends on Phases 3–6 (cross-target corpus mirrors the unit fixtures).
- **Phase 8 (Polish)**: depends on Phases 3–7.

### Within Each User Story

- DSL surface additions (plain `[P]` tasks) can run in parallel with each other.
- Unit-test tasks that share a file (`VerifySpec.hs`) must run sequentially.
- E2E changes to `ProofsSpec.hs` must run sequentially.

### Parallel Opportunities

- Phase 1: T003 and T004 in parallel.
- Phase 2: Lean (T006–T008) and Haskell primitives (T009–T015) can overlap once the Lean signatures are drafted.
- Phase 3: T017 and T018 in parallel; T019/T020/T021 all touch `VerifySpec.hs` and therefore run in sequence.
- Phase 5: T027 and T028 in parallel; unit-test cases (T029 sub-items) are one file — sequential.
- Phase 6: T031 and T032 in parallel.
- Phase 7: T036 and T037 in parallel; T038 is a verification step.

---

## Parallel Example: Phase 2 kickoff

```bash
# Lean track (single author):
Task: T006 — lean/Phase4/Verify.lean primitives
Task: T007 — replay_binds_{key,value}, replay_preserves_root_trust
Task: T008 — register module + lake build

# Haskell track (runs concurrently once T006's signatures are drafted):
Task: T009 — Verify/Replay.hs module skeleton
Task: T010 — CSMT path
Task: T011 — MPF path
Task: T012 — extend VerifyError
Task: T013 — [P] local CBOR helper if needed (only if R4 shows upstream gap)
Task: T014 — wire checkWitnessedUtxo
Task: T015 — wire checkTrieFact
Task: T016 — cabal build green
```

---

## Implementation Strategy

### MVP First (US1)

1. Complete Phase 1 (pin + cabal wiring).
2. Complete Phase 2 (Lean theorems + pure replay primitives wired into `Verify.hs`).
3. Complete Phase 3 (DSL `shouldAccept`, positive fixtures, E2E happy path).
4. **STOP and VALIDATE**: `cabal test cardano-mpfs-client` + `MPFS_BLUEPRINT=… ProofsSpec` both green. This is an internally-valid MVP: honest responses now pass a cryptographic replay (not just structural checks), even though the suite cannot yet reject forgeries.
5. Decide whether to ship the MVP alone (tight PR, follow-ups open for US3–US5) or bundle. Given the forgery tests are the load-bearing correctness evidence, **recommended: bundle US1 + US3 + US4 in the same PR**, then US5 audit, then Polish.

### Incremental Delivery

1. Phase 1 + Phase 2 merged as a scaffolding PR? — No. Keep one PR per issue (workflow skill); this whole task list lands as `feat/cryptographic-proof-replay`.
2. Within the branch: each phase is a logical commit (or a small stgit patch stack, per the `stgit_discipline` memory rule). Memory rule `bisect_safe_commits`: every commit compiles.

### Parallel Team Strategy

- Developer A: Lean track (Phase 2, T006–T008).
- Developer B: Haskell replay track (Phase 2, T009–T016), starts once Developer A publishes draft Lean signatures.
- Developer A: DSL (Phase 3 T017–T018) once T016 is green.
- Developer B: unit tests (Phase 3 T019–T021) in parallel.
- Either: E2E (Phase 3 T022–T023) sequentially.
- After US1 green: split Phase 5 (CSMT forgeries) and Phase 6 (MPF forgeries) across two developers; they share `VerifySpec.hs` so coordinate via patch stack.

---

## Notes

- Memory rule `fetch_before_any_work` — Phase 1 starts with `git fetch origin` + rebase onto latest `origin/main` in the worktree.
- Memory rule `lint_before_push` — Phase 8 T040 runs `just ci` locally before any push.
- Memory rule `follow_instructions` — the error ordering (structural → binding → root), the fixed reason vocabulary, and the DSL combinator names are spec/contract outputs; do not improvise variants.
- Memory rule `bisect_safe_commits` — every intermediate commit must compile. Start cabal edits (T003/T004) behind the pin bump (T001/T002) so the build is valid at each step.
- Memory rule `no_push_upstream` — do **not** push anything to `haskell-mts`; only consume its `main` via the `source-repository-package` pin.
- Memory rule `pins_main_only` — the `haskell-mts` tag bump must point at a main commit.
- Memory rule `drops_need_issues` — any scope drop made during implementation opens a tracking issue before the PR is marked ready.
