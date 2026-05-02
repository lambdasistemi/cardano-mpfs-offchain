---
description: "Task list for 249-atomic-boot-handler implementation"
---

# Tasks: Atomic POST /tx/boot

**Input**: Design documents in `/specs/249-atomic-boot-handler/`
**Prerequisites**: spec.md, plan.md, research.md, data-model.md, contracts/atomic-cage-reader.md, quickstart.md

**Tests**: Included. The spec's success criteria (SC-001..SC-005) and
edge-case acceptance scenarios make tests load-bearing for this slice;
landing this without unit + e2e coverage would fail SC-004.

**Organization**: Tasks are grouped by user story (US1..US4 from
spec.md). Each story is independently testable per its "Independent
Test" rubric. The slice's MVP is US1 + US2 (both P1, must ship
together — neither is meaningful without the other).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other [P] tasks in the same phase
  (different files, no shared state).
- **[Story]**: User-story tag (US1..US4) or `FOUND` for foundational.
- Every task description names the exact files it touches.

## Path conventions

This is the speckit "Single project" layout adapted to a Haskell
multi-package repo. All paths are relative to the worktree root
`/code/cardano-mpfs-offchain-atomic-handlers/`.

---

## Phase 1: Setup

**Purpose**: Confirm baseline gate is green on `origin/main` and the
worktree is at the right starting point.

- [ ] T001 [P] Confirm worktree branch is `249-atomic-boot-handler`
  and `git status` is clean. (`git status`, `git branch --show-current`.)
- [ ] T002 [P] Run the full quality gate on the merge-base
  (`origin/main`) and confirm it is green; if red, stop and surface
  the pre-existing failure to the user — do NOT try to fix it in
  this slice. Gate command:
  `nix build .#offchain-tests .#e2e-tests .#cardano-mpfs-offchain
  .#docker-image .#checks.x86_64-linux.swagger-up-to-date && just
  format-check && just hlint && just unit && just unit-offchain &&
  just e2e`.
- [ ] T003 Initialize the StGit patch stack on the branch (`stg init`,
  then create an empty leading patch
  `stg new -m "feat(boot): atomic indexer read for POST /tx/boot"` so
  the slice can be split into bisect-safe commits as work proceeds).

**Checkpoint**: gate green on base, stack ready, no scope creep.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the new `AtomicCageReader` types and wiring
*without* changing any caller yet. Every later phase depends on this.

**⚠️ CRITICAL**: No user-story phase can start until Phase 2 is green.

- [ ] T004 [FOUND] Add `AtomicCageReader`, `AtomicCageRead`, and
  `AtomicReaderError` to `cardano-mpfs-offchain/lib/Cardano/MPFS/Context.hs`
  (per `data-model.md` and `contracts/atomic-cage-reader.md`). Add
  the field `atomicCageReader :: AtomicCageReader m` to `Context`.
  Add Haddock on every constructor. Do NOT call the new field
  anywhere yet.
- [ ] T005 [FOUND] Update
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Context.hs`'s
  `mkMockContext` to set
  `atomicCageReader = error "mkMockContext: atomicCageReader not
  implemented"`. (Mock context is not the boot path.)
- [ ] T006 [FOUND] Add the optional override field
  `atomicCageReaderOverride :: !(Maybe (AtomicCageReader IO))` to
  `AppConfig` in
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs`. Default it
  to `Nothing` everywhere (`Serve.hs`, `DevnetServer.hs`,
  `RunPreprod.hs`, every e2e harness `mkAppConfig` site).
- [ ] T007 [FOUND] Add a Haddock warning to
  `Cardano.MPFS.Provider.Provider.queryUTxOs` documenting that the
  underlying cardano-node `GetUTxOByAddress` is `O(total UTxOs in
  ledger)` and is FORBIDDEN on tx-build paths. Reference issue #252.
- [ ] T008 [FOUND] Run the full gate
  (`just build && just format-check && just hlint && just unit &&
  just unit-offchain && just e2e`). It MUST be green — Phase 2 only
  introduces unused declarations, so semantics are unchanged.
  `stg refresh` into the foundational patch.

**Checkpoint**: `AtomicCageReader` exists as a type and a `Context`
field; `AppConfig.atomicCageReaderOverride` exists; warnings on
`queryUTxOs`. No caller changed yet.

---

## Phase 3: User Story 1 — Honest boot under chain churn (P1) 🎯 MVP

**Goal**: The boot endpoint reads its snapshot, input bytes, and
proofs in one indexer transaction, so the response is verifiable
under chain churn (SC-001).

**Independent Test**: SC-001 — under sustained churn (≥ 1 block/s for
≥ 60 s), 100% of boot responses verify against their own snapshot.

### Tests for User Story 1 (write FIRST; expect FAIL pre-implementation)

- [ ] T009 [US1] Add unit tests for the boot tx-builder in
  `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilder/BootSpec.hs` (NEW).
  Inject a deterministic in-memory `AtomicCageReader IO` whose
  `acrInputs` carry pre-computed `(TxIn, TxOut bytes, proof)` and
  whose `acrSnapshot` carries a known root. Assert: each
  `BootProof.bootFunding` entry mirrors an `acrInputs` entry; the
  envelope's `envSnapshot == acrSnapshot`. Add the test target to
  `cardano-mpfs-offchain.cabal` under the existing offchain test
  suite.
- [ ] T010 [P] [US1] Add an e2e regression in
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
  ("boot under churn — 50 sequential calls"): with the chain
  follower running, drive a background source of dummy traffic, then
  call boot 50× and run the verifier on each response with the
  response's own root as trusted root. Every call MUST verify.

### Implementation for User Story 1

- [ ] T011 [US1] In
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs`, build the
  production `AtomicCageReader IO` closure inside `withApplication`,
  sibling to the existing `exists`/`resolve`/`root`/`proof`
  closures, sharing the same `utxoRt` `RunTransaction`. The body MUST
  be one `CSMT.transact utxoRt $ do { … }` block (SC-005). It MUST:
  read `queryMerkleRoot`, read `latestRollbackPoint` (or its
  per-transaction equivalent — see T012), `collectValues CSMTCol []
  addressKey`, then for each leaf `query KVCol jump` and
  `generateInclusionProof fkv KVCol CSMTCol jump`. Map the four edge
  cases to the corresponding `Left AtomicReaderError`.
- [ ] T012 [US1] Adapt `latestRollbackPoint` so the in-transaction
  variant the reader needs lives inside the `Database.KV.Transaction`
  monad (do not read the checkpoint outside the reader's
  transaction). If a refactor would balloon scope, inline the same
  query the existing `latestRollbackPoint` performs (`CFStore.queryHistory
  InRollbacks`) into the reader transaction instead. Either way: zero
  reads outside the reader's `CSMT.transact utxoRt` block.
- [ ] T013 [US1] Wire the reader: in `withApplication`, set
  `Context.atomicCageReader = fromMaybe atomicReaderProd
  (atomicCageReaderOverride cfg)`. (Override beats production when
  `Just`.)
- [ ] T014 [US1] In
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs`, change
  `bootToken`'s signature from `BundleSnapshot -> Addr -> m
  (ProofEnvelope BootProof)` to `AtomicCageRead -> Addr -> m
  (ProofEnvelope BootProof)`. Update the field's Haddock.
- [ ] T015 [US1] In
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`,
  rewrite `bootTokenImpl` to consume `AtomicCageRead` directly:
  use `acrInputs` to populate the seed input + collateral + funding
  list and to construct `BootProof.bootFunding` (each
  `WitnessedInput` is a direct map from the corresponding
  `acrInputs` triple). The function MUST NOT call
  `queryUTxOs`. It MAY call `queryProtocolParams`, `evaluateTx`, or
  any other Provider field that is not `queryUTxOs`. Set
  `envSnapshot = acrSnapshot rd`.
- [ ] T016 [US1] In
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs`, drop
  the `Provider` argument plumbing that fed the now-removed
  `queryUTxOs` call. `mkRealTxBuilder` keeps the `Provider` for the
  other builders (which still call the protocol-params and
  evaluate paths) but no longer threads `queryUTxOs` into
  `bootTokenImpl`. Adjust the call site to pass the
  newly-changed `bootTokenImpl` shape.
- [ ] T017 [US1] In
  `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`, rewrite
  `txBootHandler` per `data-model.md`: drop
  `requireBundleSnapshot`; call `atomicCageReader ctx addr`;
  pattern-match on `Either AtomicReaderError`; on `Left` map via a
  new local `mapAtomicReaderError` to the documented status codes;
  on `Right` invoke `Tx.bootToken (txBuilder ctx) rd addr`.
- [ ] T018 [US1] Run the full gate. Confirm T009 and T010 are now
  GREEN. `stg refresh` into the patch (or split into 2–3 sub-patches
  if the diff is too large for one reviewable concern).

**Checkpoint**: US1 fully functional. Boot endpoint atomic; SC-001
holds locally; the verifier accepts every churn-test response.

---

## Phase 4: User Story 2 — No `queryUTxOs` on the boot path (P1)

**Goal**: Remove the forbidden cardano-node UTxO query from the boot
path entirely (FR-002, SC-002).

**Independent Test**: SC-002 — `grep -nr 'queryUTxOs' cardano-mpfs-offchain/lib/`
shows zero matches inside the boot tx-builder and zero matches inside
`txBootHandler`.

Note: most of the deletion lands incidentally in T015 / T016 / T017.
Phase 4 is the explicit assertion + the clean-up of any lingering
imports.

### Tests for User Story 2

- [ ] T019 [US2] Add a build-time assertion as a CI-checkable test:
  in `cardano-mpfs-offchain/test/Cardano/MPFS/Forbidden/BootGrepSpec.hs`
  (NEW), use Haskell's `Hspec` with a step that runs `grep -nr
  'queryUTxOs' cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`
  and asserts zero matches. Wire it into the cabal test suite.
  (Cheap and unambiguous; review-grep + CI-grep agree.)

### Implementation for User Story 2

- [ ] T020 [US2] Audit the diff from Phase 3: confirm no remaining
  `queryUTxOs prov` or `queryUTxOs (provider …)` calls in
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`,
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs`, or
  `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`'s boot
  handler. Remove any now-unused imports.
- [ ] T021 [US2] Run T019. It MUST be green. `stg refresh`.

**Checkpoint**: SC-002 verifiable mechanically.

---

## Phase 5: User Story 4 — Test seam for followerEnabled = False (P2)

**Goal**: Restore the `followerEnabled = False` fixture path with a
typed override (FR-006).

**Independent Test**: A fixture with `followerEnabled = False` and a
stub atomic reader installed builds and signs a valid boot tx
accepted on-chain.

(US4 lands before US3 because US3 is observation-only.)

### Tests for User Story 4

- [ ] T022 [US4] Modify
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs`
  (and the analogous `CageSpec.hs` if it sets
  `followerEnabled = False`) to set
  `atomicCageReaderOverride = Just (mkWalletStubAtomicReader walletProv)`.
  Move `mkWalletStubAtomicReader` into a new helper module
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/Helpers/AtomicReaderStub.hs`
  (NEW); the helper uses `Provider.queryUTxOs` on the wallet's own
  LSQ connection (allowed on the wallet side per assumptions),
  synthesises the empty-root snapshot the in-test verifier accepts,
  and emits empty proofs.
- [ ] T023 [US4] Add a unit test
  `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/TokenBootSpec.hs`
  (NEW or extend existing TokenSpec): with the override absent and
  no follower running, calling `txBootHandler` MUST fail with the
  documented `503 Indexer not ready: no chain checkpoint` (FR-004
  edge-case).

### Implementation for User Story 4

- [ ] T024 [US4] Implement
  `Cardano.MPFS.E2E.Helpers.AtomicReaderStub.mkWalletStubAtomicReader`.
  Signature: `Provider IO -> AtomicCageReader IO`. Behaviour: call
  `queryUTxOs prov addr`; on empty list return
  `Left AtomicReaderNoUtxos`; otherwise build the triples (with
  `mempty` proof bytes and an empty-root sentinel snapshot) and
  return `Right`.
- [ ] T025 [US4] Run unit + e2e. The fixture path MUST work with
  the override set and MUST fail with the documented error when the
  override is absent. `stg refresh`.

**Checkpoint**: US4 works; the harness is unblocked.

---

## Phase 6: User Story 3 — Latency curve (P2, observation only)

**Goal**: Confirm SC-003 — boot endpoint median latency at 1M total
indexed UTxOs and K=2 wallet UTxOs is within 2× the median at 1k
total indexed UTxOs.

**Independent Test**: SC-003 — measured ratio.

### Tests for User Story 3

- [ ] T026 [US3] Add a benchmarking script
  `cardano-mpfs-offchain/scripts/bench-boot-latency.sh` (NEW). It
  prepopulates a RocksDB with N total UTxOs and K=2 wallet UTxOs,
  starts a server with `followerEnabled = False` and a synthetic
  override that reads from the prepopulated DB, then issues 100 boot
  requests and reports median latency. Run for N ∈ {1k, 10k, 100k,
  1M}.

### Implementation for User Story 3

- [ ] T027 [US3] Run T026 locally. Capture the four median latencies
  and the `1M / 1k` ratio in a result file
  `specs/249-atomic-boot-handler/bench-results.md`. Ratio MUST be
  ≤ 2 for SC-003 to pass.
- [ ] T028 [US3] If the ratio fails, do NOT continue — return to
  T011 / T015 and find the residual `O(total UTxOs)` cost surface
  before declaring the slice done. Common suspects: a leftover
  `query` that scans, a forgotten `iterating` over `KVCol`, an
  accidental linear scan in the proof generation. (None expected
  given research.md, but the gate exists for defence in depth.)

**Checkpoint**: SC-003 measured and recorded.

---

## Phase 7: Polish & Verification (cross-story finalisation)

- [ ] T029 [P] Run `just format` and `just hlint` over every file
  touched. `stg refresh`.
- [ ] T030 [P] Run `just update-swagger` and confirm the diff is
  empty (the wire contract is unchanged — FR-003). If the diff is
  non-empty, stop: a wire-contract change has snuck in.
- [ ] T031 Run the full quality gate end-to-end on the tip of the
  stack:
  `nix build .#offchain-tests .#e2e-tests .#cardano-mpfs-offchain
  .#docker-image .#checks.x86_64-linux.swagger-up-to-date && just
  format-check && just hlint && just unit && just unit-offchain &&
  just e2e`. ALL targets MUST pass. SC-004 hangs on this step.
- [ ] T032 Walk the StGit stack with `stg goto <each>` and re-run the
  gate at each commit. Every commit MUST be bisect-safe.
- [ ] T033 Push the branch
  (`git push -u origin 249-atomic-boot-handler --force-with-lease`).
  Open / update PR #251 via `gh pr edit` with a description that
  references issues #250 and #252, links the spec, plan, and
  research, and lists the SC-001..SC-005 outcomes.
- [ ] T034 Stop. Do NOT merge. Hand back to the user for review and
  explicit per-PR merge approval (per the `feedback_explicit_merge_per_pr`
  memory).

**Checkpoint**: PR #251 contains a clean, bisect-safe stack
implementing the slice; all five success criteria are met locally; CI
will mirror.

---

## Dependency graph

```text
Phase 1 (Setup)
    ↓
Phase 2 (Foundational) — T004..T008
    ↓
    ├── Phase 3 (US1) — T009..T018
    │       ↓
    │   Phase 4 (US2) — T019..T021       ← depends on Phase 3 (T015 deletes the calls)
    │       ↓
    │   Phase 5 (US4) — T022..T025       ← depends on Phase 2's AppConfig field
    │       ↓
    │   Phase 6 (US3) — T026..T028       ← depends on Phase 4 (no node query) + Phase 5 (override)
    ↓
Phase 7 (Polish & Verification) — T029..T034
```

Within a phase, [P] tasks may run in parallel.

## Acceptance criteria mapping

| Spec ID | Criterion                                                                 | Closing tasks  |
| ------- | ------------------------------------------------------------------------- | -------------- |
| SC-001  | 100% verifier acceptance under churn                                      | T010, T018, T031 |
| SC-002  | Zero matches for `queryUTxOs` in boot tx-builder source                   | T019, T020, T021 |
| SC-003  | Boot latency at 1M UTxOs within 2× of 1k UTxOs                            | T026, T027     |
| SC-004  | Full gate green locally and in CI                                         | T031, T032, T033 |
| SC-005  | Atomicity claim visible in one function                                    | T011, T013     |
| FR-001  | Single coherent point in chain history                                    | T011, T012     |
| FR-002  | No `queryUTxOs` on build path                                             | T015, T016, T017, T019 |
| FR-003  | Wire contract preserved                                                   | T030           |
| FR-004  | Deterministic errors per edge case                                        | T015, T017, T023 |
| FR-005  | Linear in K, not in total UTxOs                                           | T011, T026     |
| FR-006  | Configurable test seam at startup                                         | T006, T024     |
| FR-007  | Verifier accepts purely offline                                           | T010 (validates) |
