---
description: "Tasks for adopting upstream cardano-mpfs-onchain PR #50 in the offchain repo"
---

# Tasks: Adopt split state + request validators (upstream PR #50)

**Input**: Design documents from `/specs/231-split-validators-pr50/`
**Prerequisites**: spec.md, plan.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Test tasks ARE included — the existing repo already has
`OnChainSpec`, `TxBuilderSpec`, and the `e2e-test/Cardano/MPFS/E2E/*`
suite that this feature must keep green. Per Constitution Principle V
(byte-for-byte parity with the upstream cage test vectors at
`cf3a8bdc`) and Principle VI (Test Locally First), the test surface is
load-bearing for acceptance, not optional polish.

**Organization**: Tasks are grouped by user story so each story can
be implemented and validated as a vertical, bisect-safe stgit slice.
The tx-shape contract (`contracts/tx-shapes.md`) is the per-row
acceptance arbiter for every implementation task.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story this task belongs to (US1, US2, US3, US4)
- All paths are relative to the repo root
  `/code/cardano-mpfs-offchain-onchain-bump-50/`.

## Path Conventions

- Library code: `cardano-mpfs-offchain/lib/Cardano/MPFS/...`
- Executables: `cardano-mpfs-offchain/exe/...`
- Unit tests: `cardano-mpfs-offchain/test/Cardano/MPFS/...`
- E2E tests: `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/...`
- Cabal manifest: `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`
- Project pins: `cabal.project`, `flake.nix`, `flake.lock`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Capture the project's quality gate, prove a green
baseline, and lay out the empty stgit patch stack before writing
code.

- [ ] T001 Compose `GATE` from `.github/workflows/*.yml` and the
      `justfile`. Note: `just ci` covers build → unit →
      unit-offchain → format-check → hlint **but not E2E**
      (`just e2e` is a separate recipe at `justfile:75`). Per
      Constitution Principle V, E2E parity is load-bearing for this
      feature, so `GATE` MUST include both: e.g.
      `nix develop --command bash -c 'just ci && just e2e'`. Save
      it as a single re-runnable shell command in
      `specs/231-split-validators-pr50/notes/gate.md` alongside the
      pinned tool versions used by CI.
- [ ] T002 Run `GATE` on `origin/main` (a clean tree at the upstream
      base, NOT this branch) and confirm it is green; record the
      exit status and a short transcript at
      `specs/231-split-validators-pr50/notes/gate-baseline.md`. If
      red, stop and surface to the user — do not try to fix
      pre-existing failures in this PR.
- [ ] T003 [P] Run `stg init` on branch `231-split-validators-pr50`
      and lay out empty patches in topological order, one per
      vertical slice listed in plan.md Phase 2 preview (T010, T015,
      T020, T021, T022, T023, T024, T030, T031, T032, T040, T041,
      T042, T050, T051, T052, T060). Empty patches are placeholders
      — do **not** run
      `stg clean` during active work.

**Checkpoint**: `GATE` is captured, green on `origin/main`, and the
empty stgit stack is laid out. Implementation can begin.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the upstream pin and the cross-cutting type-level
changes that every user story depends on. Each foundational task
ends with `GATE` green.

⚠️ **CRITICAL**: No user-story implementation may begin until this
phase is complete; every later task imports the helpers added here.

- [ ] T010 Land the upstream pin commit (already on disk in the
      working tree): `cabal.project`, `flake.nix`, `flake.lock`, and
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Blueprint.hs`
      (re-exports `applyDataParam`, `applyBytesParam`,
      `applyOutputRef`, `applyRequestParams` and drops
      `applyVersion`). Run `GATE`.
- [ ] T011 Drop `Mint(..)` from exports + import in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Core/OnChain.hs` and
      update the hardcoded `cageScriptHash` literal from the per-token
      PR #48 hash to the global state validator hash
      `c0f05a30f5210d6009ec69923a3969eef40a62429e7d620b66b66e06`.
      Update the matching round-trip + hash literal in
      `cardano-mpfs-offchain/test/Cardano/MPFS/OnChainSpec.hs`. Run
      `GATE`.
- [ ] T012 Add `requestScriptBytes :: ShortByteString` to `CageConfig`
      in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Config.hs`
      (do **not** add `cageSeed` — wallet picks the seed at runtime
      per spec Assumptions). Update every call site that constructs
      `CageConfig`:
      `cardano-mpfs-offchain/exe/*.hs` (server, devnet,
      bootstrap-genesis), and the existing E2E suite under
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/*Spec.hs`,
      to thread the new field through. Run `GATE`.
- [ ] T013 Add the per-cage helpers `mkRequestScript`,
      `requestAddrFromCfg`, `onChainTokenId`, and
      `requestScriptBytesFromCfg` to
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Internal.hs`,
      mirroring the upstream `Internal.hs` at
      [`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f).
      Helpers must apply the unapplied request UPLC to
      `(statePolicyId, cageTokenName)` in upstream order via
      `applyBytesParam`. Run `GATE`.

**Checkpoint**: Library compiles against the pinned upstream and
exposes the per-cage helpers. Every later task can import them.

---

## Phase 3: User Story 1 - Requester pays / retracts at per-cage address (Priority: P1) 🎯 MVP

**Goal**: The offchain TxBuilder routes
`Request{Insert,Delete,Update}` outputs to the per-cage request
address and routes `Retract` spends at that same address while
referencing the state UTxO. (FR-001, FR-002, FR-004)

**Independent Test**: On the devnet, build an Insert tx and observe
the request UTxO is paid to `requestAddrFromCfg cfg tid network`;
build a Retract for that request and observe the spend at the
per-cage address with the state UTxO referenced.

- [ ] T020 [US1] Update
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`:
      drop `Mint` import; mint policy redeemer becomes
      `Minting(seed)` carrying the wallet-chosen seed `OutputRef`
      consumed by the boot tx. The state validator itself is
      unparameterised (`validator state { ... }` upstream); the
      seed is **not** a validator parameter and the global state
      address is the same for every cage in the deployment. Per
      `contracts/tx-shapes.md` "Boot" row.
- [ ] T021 [US1] Update
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs`:
      redeemer becomes `Burning (onChainTokenId tid)`. Per
      `contracts/tx-shapes.md` "End/Burn" row.
- [ ] T022 [US1] In
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs`,
      replace the global cage address payee with
      `requestAddrFromCfg cfg tid (network cfg)` for insert, delete,
      and update flows. Per `contracts/tx-shapes.md`
      "Request{Insert,Delete,Update}" row (FR-002).
- [ ] T023 [US1] In
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs`,
      query the request UTxO at `requestAddrFromCfg cfg tid` and
      reference the state UTxO at the global state address; attach
      the per-cage request validator script as witness. Per
      `contracts/tx-shapes.md` "Retract" row (FR-004).
- [ ] T024 [US1] Extend
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs`
      with assertions that the Insert/Delete/Update tx outputs land
      at the per-cage request address and the Retract tx spends at
      the per-cage address with a referenced state UTxO. (Acceptance
      Story 1.1, 1.2.)

**Checkpoint**: User Story 1 is independently demonstrable on the
devnet — request and retract route to the per-cage address; the
existing oracle path may still be on the old shape until US2.

---

## Phase 4: User Story 2 - Oracle two-validator transaction (Priority: P1)

**Goal**: Update and Reject build a single transaction that spends
the state UTxO at the global state address with `Modify` and the
request UTxOs at the per-cage request address with
`Contribute(stateRef)`, attaching both validator scripts as
witnesses. End/Burn carries the `OnChainTokenId`. (FR-003, FR-006)

**Independent Test**: On the devnet, with at least one pending
request from Story 1, build an Update tx and observe two validator
witnesses + the redeemer pair `Modify` / `Contribute(stateRef)`;
repeat with Reject; build End/Burn and observe
`Burning (onChainTokenId tid)`.

- [ ] T030 [US2] In
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs`,
      split `queryContext` into a state-UTxO query at the global
      state address and a request-UTxOs query at
      `requestAddrFromCfg cfg tid`. Spend the state UTxO with
      `Modify`; spend each request UTxO with `Contribute(stateRef)`.
      Attach **both** the global state validator and the per-cage
      request validator scripts as witnesses. Per
      `contracts/tx-shapes.md` "Update" row (FR-003).
- [ ] T031 [US2] Apply the same split / dual-script-attachment shape
      in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Reject.hs`
      with the upstream Reject redeemer. Per `contracts/tx-shapes.md`
      "Reject" row.
- [ ] T032 [US2] Extend
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs`
      and `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageSpec.hs`
      with assertions for the dual-witness shape and the two
      redeemer payloads (Acceptance Story 2.1, 2.2). Add a passing
      End/Burn case asserting `Burning (onChainTokenId tid)`
      (Acceptance Story 2.3).

**Checkpoint**: Oracle progress works on the devnet with the new
two-validator shape. Together with US1, the legitimate request /
oracle / end loop is fully exercised through the split topology.

---

## Phase 5: User Story 3 - Owner sweep (Priority: P2)

**Goal**: A new owner-only `Sweep` entry point spends a UTxO at the
per-cage request address with redeemer `Sweep(stateRef)` while
referencing (not consuming) the state UTxO. The on-chain validator
reads the owner key hash from the referenced state datum, so a
non-owner sweep MUST fail to validate. (FR-005)

**Independent Test**: On the devnet, pay a junk-datum UTxO from a
non-owner wallet to a cage's per-cage request address; drive Sweep
as the owner and observe the offending UTxO is consumed while the
state UTxO and legitimate request UTxOs are not; repeat as a
non-owner and observe the tx fails.

- [ ] T040 [US3] Create
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Sweep.hs`
      implementing the owner-only sweep entry point per Phase 1
      data-model.md "Sweep entry point" and
      `contracts/tx-shapes.md` "Sweep" row. Re-export it from
      `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs` and
      add `Cardano.MPFS.TxBuilder.Real.Sweep` to
      `exposed-modules` in
      `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`.
- [ ] T041 [US3] Expose the Sweep flow over HTTP so US3 is
      reachable through the offchain service: add a `TxSweepAPI`
      type (`POST /tx/sweep` with `SweepRequest` →
      `SweepTxResponse`) to
      `cardano-mpfs-offchain/cardano-mpfs-api/lib/Cardano/MPFS/API.hs`,
      wire it into `TxWriteAPI` so native Servant clients pick it
      up, define `SweepRequest` / `SweepTxResponse` alongside the
      existing tx-builder request/response types, and wire the
      handler in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
      to call the Sweep entry point from T040. Per
      `contracts/http-endpoints.md` "POST /tx/sweep (NEW)" row
      (FR-005).
- [ ] T042 [US3] Add a `Sweep` round-trip case to
      `cardano-mpfs-offchain/test/Cardano/MPFS/OnChainSpec.hs`
      covering owner-success and non-owner-failure paths against
      the upstream cage test vectors (SC-004, SC-005).

**Checkpoint**: Owner-driven Sweep works on the devnet; non-owner
attempts fail; legitimate request UTxOs at the same address are
unaffected.

---

## Phase 6: User Story 4 - Indexer N+1 + dynamic boot + per-token HTTP listing (Priority: P1)

**Goal**: The indexer follows the global state address plus N
per-cage request addresses, with new addresses added in the same
atomic block batch as the boot mint. The HTTP "list requests for
token T" endpoint resolves to the per-cage request address. (FR-007,
FR-008, FR-009)

**Independent Test**: Run the offchain server against the devnet
through `HTTPLifecycleSpec` and `IndexerSpec` with one cage booted
before server start and a second cage booted while the server is
running; both must list correctly without restart.

- [ ] T050 [US4] Update
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Backend.hs`
      (and the supporting modules
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/CageFollower.hs`,
      `Follower.hs`, `ComposedInv.hs`) so the follower set is
      `(global state address, per-cage request addresses)` and so
      that on every detected boot mint the indexer atomically:
      (a) records the boot, (b) derives the new cage's per-cage
      request address via
      `requestAddrFromCfg cfg tokenName network`, and (c) adds it
      to the follower set within the same RocksDB write batch
      (FR-007, FR-008; Constitution Principle III).
- [ ] T051 [US4] Update the per-token request lookup in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` (and
      any supporting code in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`,
      `Encoding.hs`, `Types.hs`) so the server derives the per-cage
      request address from `(statePolicyId, tokenName)` and queries
      the indexer's per-address index. Public endpoint shape
      MUST stay byte-identical (FR-009; `contracts/http-endpoints.md`).
- [ ] T052 [US4] Extend
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/IndexerSpec.hs`
      with a "boot N+1, then list" assertion confirming the
      follower-set count is 1 + N. Add a "boot while running"
      scenario to
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs`
      that boots a second cage after server start and asserts
      `GET /requests/{newToken}` returns the new request without
      a restart (Acceptance Story 4.1, 4.2, 4.3).

**Checkpoint**: All four user stories pass on the devnet. The full
acceptance surface for spec.md is green.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Sweep up tests, docs, and any cross-cutting tail not
absorbed by the story phases.

- [ ] T060 Update
      `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs`:
      drop the obsolete Mint test, update any hash literals to
      match the global state validator hash from T011, and add the
      request-script identity helper coverage if covered upstream
      (SC-005).
- [ ] T061 Walk the stgit stack with `stg goto <patch>` for every
      patch in the series and run `GATE` on each — confirm
      bisect-safety per the `pr` skill's pre-merge checklist. Record
      a one-line pass/fail per patch in
      `specs/231-split-validators-pr50/notes/gate-walk.md`.
- [ ] T062 Run `quickstart.md` end-to-end on the devnet and
      annotate each step's outcome in
      `specs/231-split-validators-pr50/notes/quickstart-run.md`
      (covers SC-001 through SC-005 in one transcript).
- [ ] T063 [P] Update `CLAUDE.md`'s "Recent Changes" trailer with
      the `231-split-validators-pr50` summary line per the speckit
      agent-context convention. (No new technology, so the rest of
      the file stays untouched.)
- [ ] T064 [P] Mark every applicable acceptance scenario from
      `spec.md` as covered by an E2E or unit test in a short
      coverage table at
      `specs/231-split-validators-pr50/notes/coverage.md` (Acceptance
      Story 1.1–1.3, 2.1–2.3, 3.1–3.3, 4.1–4.3 → test name).

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — runs first.
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user
  stories. Within Phase 2, T010 → T011 → T012 → T013 in order
  (each builds on the previous module's exports).
- **Phase 3 (US1)**: Depends on Phase 2.
- **Phase 4 (US2)**: Depends on Phase 2; benefits from US1 being
  in place to exercise the legitimate-request flow end-to-end, but
  is not strictly blocked by it.
- **Phase 5 (US3)**: Depends on Phase 2 and on the helpers from
  Phase 2 (T013) plus the cabal export added in T040.
- **Phase 6 (US4)**: Depends on Phase 2; benefits from US1 + US2
  for the E2E coverage extensions but the indexer / HTTP code path
  changes are independent.
- **Phase 7 (Polish)**: Depends on Phases 3–6 being green.

### User Story Dependencies

- **US1 (P1)**: Independent after Foundational.
- **US2 (P1)**: Independent after Foundational.
- **US3 (P2)**: Independent after Foundational, but its E2E
  coverage assumes the per-cage address routing from US1 is in
  place.
- **US4 (P1)**: Independent after Foundational; consumes the
  per-cage helpers from T013 only.

### Within Each User Story

- Implementation tasks first, then E2E coverage tasks.
- Every task ends with `GATE` green before `stg refresh` and the
  next `stg push`.

### Parallel Opportunities

- T020, T021, T022, T023 are in different files and can be
  parallelised inside US1 once T013's helpers exist; the E2E
  task T024 must wait for them.
- T030 and T031 can be parallelised across `Update.hs` /
  `Reject.hs`; T032 must wait for both.
- T040 (Sweep) and T050 / T051 (Indexer / HTTP) can be developed
  in parallel by different contributors after T013.
- T063 (CLAUDE.md trailer) and T064 (coverage notes) are
  independent and can be parallelised.

---

## Parallel Example: User Story 1

```bash
# After T013 lands in Phase 2, start US1 in parallel:
Task: "T020 — drop Mint wrapper in TxBuilder/Real/Boot.hs"
Task: "T021 — End.hs Burning(onChainTokenId tid)"
Task: "T022 — Request.hs pay to per-cage request address"
Task: "T023 — Retract.hs split query context, attach request script"

# Then sequentially:
Task: "T024 — extend CageFlowSpec with US1 acceptance assertions"
```

---

## Implementation Strategy

### MVP (User Story 1 + User Story 2 together)

US1 alone delivers requester routing but leaves the oracle on the
old shape — useful for review but not deployable to the devnet as a
working flow. The natural MVP for this feature is **US1 + US2**:
once the two-validator transaction shape is in place, the
legitimate request → oracle progress loop is end-to-end functional
on the devnet. US3 (sweep) and US4 (indexer / HTTP topology) are
incremental from there.

### Incremental Delivery

1. Setup (Phase 1) → `GATE` baseline green.
2. Foundational (Phase 2) → the library compiles against `cf3a8bdc`
   and exposes the per-cage helpers.
3. US1 → Requester routing demonstrable on the devnet.
4. US2 → Oracle two-validator shape demonstrable; minimum viable
   loop on the devnet.
5. US3 → Owner sweep demonstrable.
6. US4 → Indexer / HTTP topology fully aligned with the new
   on-chain layout.
7. Polish → coverage notes, CLAUDE.md trailer, full quickstart
   transcript.

### Parallel Team Strategy

After Phase 2 lands, US1, US3, and US4 can be developed by
different contributors in parallel. US2 is best done immediately
after US1 by the same contributor since its E2E coverage builds
directly on US1's request-routing assertions.

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks.
- [Story] label maps tasks to spec.md user stories for traceability.
- Every implementation task ends with `GATE` green before
  `stg refresh`. Walking the stack at T061 verifies that every
  patch in the series remains bisect-safe.
- The `contracts/tx-shapes.md` table is the byte-for-byte arbiter
  per Constitution Principle V — implementation tasks reference it
  by row name (Boot, Request, Retract, Update, Reject, Sweep,
  End/Burn) rather than restating shapes inline.
- Empty stgit patches laid out in T003 are placeholders. Do **not**
  run `stg clean` during active work — empty patches are intent
  markers.
- `notes/` files referenced in T001 / T002 / T061 / T062 / T064 are
  scratch artefacts for the implementation session; they are not
  load-bearing for review and may be discarded before the final
  PR push if the user prefers a leaner spec folder.
