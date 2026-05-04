---
description: "Task list for 259-fact-provider-pivot implementation"
---

# Tasks: Fact-provider pivot

**Input**: Design documents in `/specs/259-fact-provider-pivot/`
**Prerequisites**: spec.md, plan.md, research.md, data-model.md,
contracts/{facts-api.md, cage-dsl.md, verifier.md}, quickstart.md

**Tests**: Included. SC-001 (end-to-end MOOG flow) and SC-005
(both repo defaults move in lockstep) are gating; without unit +
property + e2e coverage, the pivot would land unverified.

**Organization**: Tasks are grouped by phase, where each phase
corresponds to a coordinated PR landing in one of the three
repositories. Tasks within a phase are tagged by user story
(US1..US4) where applicable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other [P] tasks in the same phase.
- **[Story]**: User-story tag (US1..US4) or `FOUND`/`SETUP` for cross-cutting work.
- Every task description names the exact files / repos it touches.

## Path conventions

- `cardano-mpfs-offchain` worktree: `/code/cardano-mpfs-offchain-fact-provider`
- `cardano-node-clients` worktree: TBD (Phase 2 task creates it)
- `lambdasistemi/moog` worktree: TBD (Phase 4a task creates it)

## Terminology (canonical)

- **facts bundle** — the conceptual entity carrying a snapshot +
  proof-bearing data + protocol parameters for one operation.
- **`XFacts`** — the Haskell type names for each per-endpoint
  bundle (`BootFacts`, `RequestFacts`, …).
- **facts response** — the HTTP-level wire shape of a `POST
  /facts/*` reply.

Use these terms uniformly across all artifacts and source.

---

## Phase 0: Constitution amendment (standalone prior PR)

**Purpose**: Land the constitution amendment (Principle IV
rewritten as "Client-Side Transaction Construction"; Principle IX
waiver for CI-enforcement) BEFORE any pivot implementation work
runs. Without this, the pivot tasks would violate the current
Principle IV (MUST). Per the speckit-analyze rule, constitution
changes happen in a separate workflow.

**⚠️ This is a separate PR from the pivot's implementation.** It
must merge before Phase 1 starts.

- [ ] T_A001 [SETUP] Create branch `259-constitution-v2` (or similar
  short name) from `origin/main` of `cardano-mpfs-offchain`.
- [ ] T_A002 [SETUP] Edit `.specify/memory/constitution.md`:
  - Bump version to **2.0.0**.
  - Replace Principle IV's body with the "Client-Side Transaction
    Construction" wording from plan.md's Constitution Check.
  - Add a top-of-file **Sync Impact Report** comment block
    documenting:
    - Removed: Principle IV literal "API MUST return unsigned
      CBOR transactions".
    - Added: Principle IV "Client-Side Transaction Construction".
    - Waiver: Principle IX's "CI MUST build WASM and JS artifacts"
      clause is operationally deferred to issue
      https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258.
      The verifier remains pure and structurally cross-compilable;
      the CI-enforcement clause is the only operational deferral.
    - Rationale: pivot to fact-provider architecture (issue
      https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257);
      the spirit of Principle IV (no keys on server) is preserved
      and strengthened.
    - Templates / docs requiring updates: spec/plan templates that
      reference Principle IV's old wording (none; the templates
      reference the principle by name, not by body).
- [ ] T_A003 [SETUP] Open PR on
  https://github.com/lambdasistemi/cardano-mpfs-offchain titled
  `chore(constitution): v2.0.0 — Principle IV "Client-Side
  Transaction Construction" + Principle IX CI waiver`.
  Cross-references:
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257
  (pivot) and
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258
  (Principle IX cross-target build infra).
- [ ] T_A004 [SETUP] After CI green and review approves, merge.
  **Phase 0 is complete only when this commit is on
  `cardano-mpfs-offchain` main.**

**Checkpoint**: constitution at v2.0.0; Principle IV's new wording
on main; Principle IX's CI clause explicitly waived (with tracking
ticket #258). The pivot's implementation phases are now legal.

---

## Phase 1: Setup

**Purpose**: Confirm baselines are green; capture byte vectors that the
Principle V byte-equality property will compare against.

- [ ] T001 [SETUP] Verify the full local quality gate is green on
  `origin/main` of `cardano-mpfs-offchain` and on `origin/main` of
  `cardano-node-clients`. Gate: `nix build .#offchain-tests
  .#e2e-tests .#cardano-mpfs-offchain .#docker-image
  .#checks.x86_64-linux.swagger-up-to-date && just format-check &&
  just hlint && nix run .#unit-tests && nix run .#e2e-tests` for
  the offchain repo; equivalent for cardano-node-clients. If red,
  STOP and surface to the user — pre-existing failures are not
  for this slice to fix.
- [ ] T002 [SETUP] Capture golden CBOR vectors of the legacy `Tx`
  output for each operation (boot, three requests, retract, end,
  update with one request, reject with one request) using the
  pre-pivot `Cardano.MPFS.TxBuilder.Real.*Core` modules. Save to
  `specs/259-fact-provider-pivot/test-vectors/legacy-*.cbor` so
  the Principle V byte-equality test (T012) can compare.

  **Pinning discipline**: document at the top of the test-vectors
  directory the exact `cardano-ledger-conway`,
  `cardano-mpfs-offchain`, and `cardano-node-clients` commit SHAs
  the vectors were captured at. Once Phase 3 deletes the
  `Real.*Core` tree (T026), the golden vectors become the only
  reference; breaking byte-equality after the pivot must be a
  deliberate audit point (e.g., pinned-ledger-version bump),
  never a silent drift.
- [ ] T003 [SETUP] Create cardano-node-clients worktree at
  `/code/cardano-node-clients-cage-helpers` from `origin/main`.
  `direnv allow`; confirm `nix develop --command just unit` is
  green.

**Checkpoint**: Baselines green; byte vectors captured.

---

## Phase 2: Foundational — Cage DSL helpers in cardano-node-clients

**Purpose**: Land the cage-protocol DSL helpers (boot, request × 3,
retract, end, update, reject) in `cardano-node-clients` as a
self-contained PR. This must merge before the server pivot can
land.

**⚠️ CRITICAL**: This phase blocks the rest. Until T013 is merged,
the server pivot cannot start.

### Foundational scaffolding

- [ ] T004 [FOUND] Add `WalletPolicy` data type to
  `cardano-node-clients/lib/Cardano/Node/Client/TxBuild/Cage/Policy.hs`
  (NEW). Fields per data-model.md (`wpMaxFee`,
  `wpMaxExUnitsPrice`, `wpMaxMinUtxoCoinPerByte`,
  `wpMaxValidityWindow`). Plus
  `mainnetDefaultWalletPolicy :: WalletPolicy` with sane mainnet
  values. Plus `enforcePolicy :: PParams ConwayEra ->
  WalletPolicy -> Tx ConwayEra -> Either PolicyViolationDetail
  ()` for the post-build check.
- [ ] T005 [FOUND] Add `BuildError` ADT to
  `Cardano.Node.Client.TxBuild.Cage` (NEW common module). Variants:
  `EmptyFunding`, `PolicyViolation PolicyViolationDetail`,
  `MalformedDatum Text`, `DSLBuildFailed TxBuildError`,
  `MalformedFacts Text`.

### Per-endpoint helpers

- [ ] T006 [P] [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.Boot.bootCageTx` per
  contracts/cage-dsl.md. Port the body from
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`
  (the post-#253 pure `bootTokenCore`); replace facts-shape inputs
  with `VerifiedBootFacts`. Decode pp from the bundle's CBOR before
  feeding the DSL.
- [ ] T007 [P] [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.Request.{requestInsertCageTx,
  requestDeleteCageTx, requestUpdateCageTx}` — one shared
  `requestCageTx` parameterised on `Operation`, plus three thin
  wrappers (mirrors today's `Real.Request.requestImpl` shape).
- [ ] T008 [P] [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.Retract.retractCageTx`.
  Consumes the named request UTxO + funding + emits refund to
  requester.
- [ ] T009 [P] [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.End.endCageTx`. Consumes the
  state UTxO + funding; burn -1 of cage policy; refund to
  requester.
- [ ] T010 [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.Update.updateCageTx`. Tier-3:
  decode trie facts → run pure MPF fold → compute new
  `stateRoot` → assemble new state UTxO datum → DSL program
  consumes state UTxO + selected request UTxOs + funding;
  outputs new state UTxO + per-rejected-request refund + tip
  collection at owner. Inherits MPF fold logic from
  `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs`.
- [ ] T011 [FOUND] Implement
  `Cardano.Node.Client.TxBuild.Cage.Reject.rejectCageTx`. Tier-3
  (similar shape to update but for past-retract-window requests).

### Property tests

- [ ] T012 [FOUND] Add property tests for byte-equality between
  each `*CageTx` and its legacy `*Core` counterpart, using the
  golden vectors from T002. Per Principle V — same inputs MUST
  produce byte-equal `Tx ConwayEra` CBOR. Test module:
  `cardano-node-clients/test/Cardano/Node/Client/TxBuild/CageSpec.hs`
  (NEW).
- [ ] T013 [FOUND] Add `WalletPolicy` regression tests: stubbed
  pp with inflated `minFeeA × 100` triggers `PolicyViolation
  FeeBoundExceeded` on every helper.

### Land

- [ ] T014 [FOUND] Run full quality gate locally (build all
  cardano-node-clients targets, format-check, hlint, unit tests
  with new module suites). All MUST be green.
- [ ] T015 [FOUND] Push branch + open PR on
  `lambdasistemi/cardano-node-clients` titled
  `feat(cage): add cage-protocol DSL helpers under TxBuild.Cage.*`
  with cross-references to
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257.
- [ ] T016 [FOUND] After CI green, merge the PR. **Phase 2 is
  complete only when this commit is on cardano-node-clients main.**

**Checkpoint**: Cage DSL helpers landed in cardano-node-clients.
Phase 3 can now start.

---

## Phase 3: Server cutover — cardano-mpfs-offchain

**Purpose**: Replace the server's `transaction/...` endpoints with
`POST /facts/*`; rewrite the verifier to pure proof-validity;
amend the constitution. This is the load-bearing phase of the
pivot.

### Bump pin

- [ ] T017 [US1] Bump the `cardano-node-clients` source-repository-
  package pin in `cardano-mpfs-offchain/cabal.project` to the
  specific merge-commit SHA on `cardano-node-clients` main
  produced by T016 (per `feedback_pins_main_only`: pin to a
  main commit SHA, not the branch ref). `nix flake update` if
  necessary.

### Indexer primitives (server side)

- [ ] T018 [P] [US1] Add
  `Cardano.MPFS.Indexer.Reads.readStateUtxoAt :: TokenId ->
  IndexerTx (Maybe ResolvedStateUtxo)`. Same atomicity discipline
  as PR #253's existing primitives — operates inside one
  `runIndexerTx` block.
- [ ] T019 [P] [US1] Add
  `Cardano.MPFS.Indexer.Reads.readRequestUtxosAt :: TokenId ->
  IndexerTx [ResolvedRequestUtxo]`.
- [ ] T020 [P] [US1] Add
  `Cardano.MPFS.Indexer.Reads.readNamedRequestUtxo :: TxIn ->
  IndexerTx (Maybe ResolvedRequestUtxo)`.
- [ ] T021 [P] [US1] Add
  `Cardano.MPFS.Indexer.Reads.readTrieFact :: TokenId ->
  ByteString -> IndexerTx (Maybe TrieFact)`.

### Facts response types + JSON

- [ ] T022 [US1] Add `Cardano.MPFS.HTTP.Types.{BootFacts,
  RequestFacts, RetractFacts, EndFacts, UpdateFacts, RejectFacts,
  TrieFact, UnverifiedPParams}` per data-model.md. Plus `ToJSON`
  / `FromJSON` instances per contracts/facts-api.md.

### HTTP API rewrite

- [ ] T023 [US1] Replace `Cardano.MPFS.HTTP.API` paths: remove
  every `transaction/{address}/{op}` entry; add the eight
  `POST /facts/{op}` entries.
- [ ] T024 [US1] Add eight new handlers in
  `Cardano.MPFS.HTTP.Server`:
  `factsBootHandler`, `factsRequestInsertHandler`,
  `factsRequestDeleteHandler`, `factsRequestUpdateHandler`,
  `factsRetractHandler`, `factsEndHandler`,
  `factsUpdateHandler`, `factsRejectHandler`. Each is one
  `runIndexerTx ctx $ do { … }` composition + assemble response
  + return.

### Server-side cleanup

- [ ] T025 [US2] Remove `txBootHandler`, `txInsertHandler`,
  `txDeleteHandler`, `txUpdateValueHandler`, `txRejectHandler`,
  `txUpdateHandler`, `txRetractHandler`, `txEndHandler` from
  `Cardano.MPFS.HTTP.Server`.
- [ ] T026 [US2] Delete `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/`
  tree entirely (Boot, Request, Retract, End, Update, Reject,
  Sweep, Internal). Update `Real.hs` re-export module: keep only
  the helpers that other server code still uses (the `Sweep`
  handler stays for the moment as it's owner-only and not
  cage-protocol-shaped — re-evaluate in a follow-up).
- [ ] T027 [US2] Drop the `txBuilder` field from
  `Cardano.MPFS.Context.Context`. Update every Context
  constructor site (mock, test, application). The server has
  no transaction-building responsibility post-pivot.
### Verifier rewrite (cardano-mpfs-client)

- [ ] T029 [US3] Rewrite `Cardano.MPFS.Client.Verify`:
  - Remove every `verifyXTxResponse` function and every
    `Cardano.Ledger.Api.Tx` import.
  - Add `verifyBootFacts`, `verifyRequestFacts`,
    `verifyRetractFacts`, `verifyEndFacts`,
    `verifyUpdateFacts`, `verifyRejectFacts` per
    contracts/verifier.md.
  - Add `VerifiedXFacts` newtypes with constructors
    NOT exported.
- [ ] T029a [US3] For each `verifyXFacts`, add unit tests
  (`cardano-mpfs-client/test/Cardano/MPFS/Client/Verify*Spec.hs`)
  covering exactly the test buckets from contracts/verifier.md
  §"Test plan":
  - **Happy path**: a known-good facts bundle anchored to a known
    root verifies with `Right VerifiedXFacts ...`.
  - **Snapshot tamper** (deterministic, addresses spec edge case
    "trusted root mismatch"): given a known-good bundle, mutate
    one byte in the response's `xfSnapshot.utxoRoot`; verifier
    returns `Left SnapshotMismatch`.
  - **Trusted-root mismatch** (deterministic, dual to the above):
    given a known-good bundle, pass a one-byte-mutated trusted
    root; verifier returns `Left SnapshotMismatch`.
  - **Proof tamper**: flip a byte in any included CSMT or MPF
    proof; verifier returns `Left (CsmtProofInvalid ...)` or
    `Left (MpfProofInvalid ...)`.
  - **Trie fact tamper** (tier-3 only, Update/Reject): flip a byte
    in `tfValue`; verifier returns `Left (MpfProofInvalid ...)`.
- [ ] T030 [US3] Delete `Cardano.MPFS.Client.Verify.Conservation`
  module — obsolete after the pivot (no tx-shape grammar to
  enforce).
- [ ] T031 [US3] Migrate `Cardano.MPFS.Client.Verify.Replay` to
  facts-bundle replay only (removing any `Tx`-aware logic).
- [ ] T032 [US3] Add `Cardano.MPFS.Client.Facts` module: the
  `XFacts` types + JSON instances (re-exported / shared between
  server and client).

### Tests + property suite

- [ ] T033 [US1] Add unit tests for each new IndexerTx primitive
  (T018–T021).
- [ ] T034 [US1] Add unit tests for each new fact handler:
  happy-path returns the expected shape; edge cases per spec.md
  return the documented status codes (404 / 503 / 400 / 500).
- [ ] T035 [US3] Add cross-target QuickCheck for the verifier
  per contracts/verifier.md §"Cross-target byte identity". Test
  property: for random `(root, facts)` pairs, output of
  `verifyXFacts` is byte-identical across native, GHC-WASM,
  GHC-JS.
- [ ] T036 [US1] E2E tests: extend `Cardano.MPFS.E2E.ProofsSpec`
  to exercise `POST /facts/*` for each endpoint; tamper tests
  for snapshot-mismatch and proof invalidity.

### Constitution gate

- [ ] T037 [SETUP] Verify Phase 0 (T_A001–T_A004) has merged on
  `cardano-mpfs-offchain` main BEFORE Phase 3's PR is opened.
  The constitution is at v2.0.0 with the amended Principle IV;
  this Phase 3 PR's diff therefore complies with the active
  constitution. If Phase 0 has not merged, STOP — Phase 3 is
  blocked until it does.

### Swagger + docs

- [ ] T038 [US2] Run `just update-swagger`. Confirm the diff
  shows: removed every `transaction/...` path; added eight
  `/facts/*` paths. Commit the regenerated
  `docs/assets/swagger.json`.
- [ ] T039 [SETUP] Update `docs/architecture/overview.md` and
  sweep every other doc under `docs/`:
  - Document the post-pivot architecture in
    `docs/architecture/overview.md` (server as fact provider;
    client builds tx; verifier as pure proof check; pp gap +
    WalletPolicy). Reference the constitution amendment.
  - Grep all of `docs/` for legacy endpoint mentions:
    `grep -rn 'transaction/{address}\|GET /transaction\|verifyBootTxResponse\|verifyRequestTxResponse\|verifyRetractTxResponse\|verifyEndTxResponse\|verifyUpdateTxResponse\|verifyRejectTxResponse' docs/`.
    Each hit must either be deleted or rewritten to the post-pivot
    shape (`POST /facts/*`, `verifyXFacts`).
  - Grep `docs/` for "unsigned transaction" / "server builds the
    transaction" — same fix-or-delete discipline.
  - Acceptance: after T039, `grep -rn 'transaction/' docs/` returns
    only intentional mentions (e.g., a migration-notes section in
    `docs/architecture/overview.md` that explicitly explains the
    pivot). No prose still describes the legacy shape as the
    current architecture.

### Land

- [ ] T040 [US1/US2/US3] Run full local quality gate (build all
  derivations, format-check, hlint, unit tests, e2e tests). Walk
  the StGit stack with `stg goto` if used; confirm every commit
  is bisect-safe.
- [ ] T041 [US1/US2/US3] Push branch + open PR on
  `lambdasistemi/cardano-mpfs-offchain` titled
  `feat(api): pivot to fact-provider; client builds tx locally`,
  cross-referencing
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257.
  Highlight the Principle IV constitution amendment in the PR
  body.
- [ ] T042 [US1/US2/US3] After CI green and review approves, merge
  the PR. **Phase 3 is complete only when this commit is on
  cardano-mpfs-offchain main.** This is the start of the narrow
  cutover window during which MOOG main is broken — Phase 4
  must follow immediately and no production deploy may use a
  commit in this window.

**Checkpoint**: server cutover landed; legacy endpoints gone;
verifier surface pure; constitution at 2.0.0. Phase 4 must
land within the cutover window.

---

## Phase 4a: MOOG migration — prepare on a branch (parallel with Phase 3)

**Purpose**: Land all of MOOG's code-rewrite work on a feature
branch IN PARALLEL with Phase 3's development. The branch builds
locally against pre-Phase-3 server pins; CI runs against an
unbumped pin until Phase 4b. This shrinks the cutover window from
"full PR review + CI" to "single CI run + merge".

The split honors the analyze pass's H2 (sequencing) finding: only
the pin bump and the integration tests genuinely require Phase 3
on main; the module rewrites do not.

### Worktree

- [ ] T043 [SETUP] Create `lambdasistemi/moog` worktree from
  `origin/main`. **Do not bump pins yet** — work proceeds against
  the current pins (which still see legacy server endpoints).
  The migration is module-level and doesn't need a working server
  to compile.

### MPFS.Facts client

- [ ] T044 [US1] Create `lambdasistemi/moog/src/MPFS/Facts.hs`
  with Servant client functions for the eight `POST /facts/*`
  endpoints + `POST /submit`. Decode response bodies into the
  `XFacts` types from `cardano-mpfs-client`.

### Submit pipeline rewrite

- [ ] T045 [US1] Rewrite `lambdasistemi/moog/src/Submitting.hs`:
  - Replace `signAndSubmitMPFS` with `verifyAndBuildAndSign`:
    pulls facts → verifies via `cardano-mpfs-client.Verify` →
    runs the matching cage helper from
    `cardano-node-clients.TxBuild.Cage.*` → signs → submits.
  - Surface `VerifyError` and `BuildError` clearly to callers.

### Per-callsite migration

- [ ] T046 [P] [US1] Migrate
  `lambdasistemi/moog/src/Cli.hs`: replace `MPFS.API` imports
  with `MPFS.Facts`; replace tx-builder calls with the new
  pipeline.
- [ ] T047 [P] [US1] Migrate
  `lambdasistemi/moog/src/Effects.hs`.
- [ ] T048 [P] [US1] Migrate
  `lambdasistemi/moog/src/Oracle/Process.hs`.
- [ ] T049 [P] [US1] Migrate
  `lambdasistemi/moog/src/Oracle/Config/Cli.hs`.
- [ ] T050 [P] [US1] Migrate
  `lambdasistemi/moog/src/Oracle/Token/Cli.hs`.
- [ ] T051 [P] [US1] Migrate
  `lambdasistemi/moog/src/User/Agent/Cli.hs` and
  `lambdasistemi/moog/src/User/Agent/Lib.hs`.
- [ ] T052 [P] [US1] Migrate
  `lambdasistemi/moog/src/User/Requester/Cli.hs`.

### Cleanup

- [ ] T053 [US2] Delete `lambdasistemi/moog/src/MPFS/API.hs`.
  After this commit `grep -rn 'MPFS.API\b' src/` returns zero
  matches.

### Wallet policy

- [ ] T054 [US4] Define MOOG's default `WalletPolicy` in
  `lambdasistemi/moog/src/Wallet/Policy.hs` (NEW). Sane mainnet
  values; CLI flag for overrides.
- [ ] T055 [US4] Add a regression test using a deterministic test
  seam: server-side `MOCK_MPFS_PP_OVERRIDE` env var (test-only,
  gated behind a build flag) lets the test trigger inflated pp
  without HTTP-layer mocking. Assert MOOG's
  `verifyAndBuildAndSign` returns
  `Left (PolicyViolation FeeBoundExceeded)` before signing.

### Phase 4a checkpoint

- [ ] T056a [SETUP] Push the Phase 4a branch (no PR open yet — the
  branch is a holding place until Phase 4b's cutover). Confirm:
  every callsite migrated to `MPFS.Facts`; `MPFS.API` deleted;
  module compiles against current (pre-Phase-3) pins.

**Checkpoint**: Phase 4a complete. The MOOG migration branch
exists with all module rewrites done; only pin bump and
integration tests remain.

---

## Phase 4b: MOOG cutover — bump pins + run e2e + merge

**Purpose**: The narrow cutover window. Phase 3 must be on
`cardano-mpfs-offchain` main before this phase starts. Pin bump
+ integration tests + merge happen in a single CI run.

**⚠️ CUTOVER WINDOW OPENS HERE.** Production deploys must NOT use
any MOOG main commit between Phase 3's merge (T042) and T059's
merge.

- [ ] T056b [US1] Rebase the Phase 4a branch onto current
  `lambdasistemi/moog` main; bump the
  `cardano-mpfs-offchain` and `cardano-node-clients`
  source-repository-package pins to the specific merge-commit
  SHAs on each repo's main (per `feedback_pins_main_only`).
- [ ] T056 [US1] Migrate MOOG's existing devnet integration
  tests (the ones that exercise the legacy endpoints) to the
  new pipeline. Every test that previously called `MPFS.API`
  now goes through `verifyAndBuildAndSign`. Run the suite
  end-to-end against the post-Phase-3 server.
- [ ] T057 [US1/US2/US4] Run MOOG's full quality gate locally
  (build, lint, unit, integration). All MUST be green.
- [ ] T058 [US1/US2/US4] Push the Phase 4a/b branch and open PR
  on `lambdasistemi/moog` titled
  `feat: migrate to MPFS fact-provider API; build txs locally`,
  cross-referencing
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257.
- [ ] T059 [US1/US2/US4] After CI green and review approves,
  merge. **The cutover window closes when this commit is on
  MOOG main.**

**Checkpoint**: All three repos at the post-pivot state. The
cutover window is closed.

---

## Phase 5: Verification & polish

- [ ] T060 [P] [US2] Cross-repo grep verification — scope each grep
  precisely to "source code that the running binary compiles"
  (typically `lib/` + `exe/` for libraries-and-executables;
  `src/` + `app/` for project-style repos). Doc references and
  migration-note comments in `e2e-test/`, `test/`, `docs/` are
  fine; the grep is about live binary surface.
  - `cardano-mpfs-offchain`: zero `transaction/{address}` paths in
    the server's `lib/Cardano/MPFS/HTTP/{API,Server}.hs`; zero
    `Cardano.Ledger.Api.Tx` imports in
    `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify*`;
    `docs/assets/swagger.json` has zero `transaction` paths.
  - `cardano-node-clients`: cage helpers compile native; all
    byte-equality tests (T012) pass.
  - `lambdasistemi/moog`: zero `MPFS.API` imports in `src/`.
  Additionally: grep `cardano-mpfs-offchain/docs/` for legacy
  endpoint names; update prose where the post-pivot architecture
  contradicts what the docs say (handled in T039 if not earlier).
- [ ] T061 [P] [SC-005] Confirm both `cardano-mpfs-offchain` main
  and `lambdasistemi/moog` main moved within the same release
  window (T042 → T059); no production deploy used a commit in
  the cutover window.
- [ ] T062 [P] [SC-001] Run end-to-end devnet exercise from
  `quickstart.md` §9: boot, three requests, retract, end,
  update, reject, all via MOOG against the new server. Every
  step verifies. Every transaction lands on-chain.
- [ ] T063 [SETUP] Update
  https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257
  with the pivot's completion record:
  - Phase 0 PR merge SHA on `cardano-mpfs-offchain`.
  - Phase 2 PR merge SHA on `cardano-node-clients`.
  - Phase 3 PR merge SHA on `cardano-mpfs-offchain`.
  - Phase 4b PR merge SHA on `lambdasistemi/moog`.
  - Cutover-window timestamps (T042 merge → T059 merge).
  - Link to the post-pivot architecture doc
    (`docs/architecture/overview.md` updated in T039).
  - Confirm SC-001..SC-005 all met.
  - Close
    https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/256
    (verifier-completeness-for-tx-shape; obsolete after pivot).
  - Re-tag
    https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/254
    (multi-band snapshots) against the new fact-provider shape —
    multi-band fits as an optional `snapshot=band` parameter on
    each `/facts/*` call.
  - Issue
    https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258
    (Principle IX cross-target build infra) stays open; the
    pivot's amendment includes the explicit waiver.
- [ ] T064 [SETUP] Update referenced issues:
  - https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/256:
    close (the verifier-completeness-for-tx-shape problem
    evaporates with the pivot).
  - https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/254:
    re-tag against the new architecture (multi-band snapshots
    fit as an optional `snapshot=band` parameter on each
    `/facts/*` call).
  - https://github.com/lambdasistemi/cardano-node-clients/issues/123:
    no change (deferred WASM artifact stays deferred until
    browser-wallet demand materialises).

---

## Dependency graph

```text
Phase 0 (Constitution amendment) — T_A001..T_A004
    ↓ MUST land first; Phase 3 references it
Phase 1 (Setup) — T001..T003
    ↓
Phase 2 (Cage helpers in cardano-node-clients) — T004..T016
    ↓ MUST land before Phase 3
Phase 3 (Server cutover + verifier rewrite) — T017..T042
    ║ ┌────── Phase 4a (MOOG branch prepared in parallel)
    ║ │      — T043..T056a
    ↓ │
    ↓ ↓ Cutover window opens at T042; MOOG main broken
Phase 4b (MOOG cutover) — T056b..T059
    ↓ Cutover window closes at T059
Phase 5 (Verification & polish) — T060..T064
```

Phase 4a runs in parallel with Phase 3's development. Only Phase
4b needs Phase 3 to be on main. This minimises the cutover window
to a single CI run.

Within a phase, [P] tasks may run in parallel.

## Acceptance criteria mapping

| Spec ID | Criterion                                                                               | Closing tasks |
| ------- | --------------------------------------------------------------------------------------- | ------------- |
| FR-001  | Eight `POST /facts/*` endpoints with same parameters as legacy                          | T023, T024 |
| FR-002  | Each response carries snapshot + per-endpoint data + pp                                 | T022, T024 |
| FR-003  | Per-endpoint facts shape matches data-model.md                                          | T022 |
| FR-004  | Every CSMT/MPF proof verifies against snapshot's roots                                  | T029, T035 |
| FR-005  | One `runIndexerTx` per handler; new primitives inside same discipline                   | T018–T021, T024 |
| FR-006  | Legacy endpoints removed; swagger reflects new shape only                                | T025, T026, T038 |
| FR-007  | Verifier surface has zero `Cardano.Ledger.Api.Tx` imports                               | T029, T030, T060 |
| FR-008  | Cage helpers byte-equal to legacy `*Core` for equivalent inputs                         | T012 |
| FR-009  | pp returned with `verified: false`; `WalletPolicy` documented + enforced                 | T022, T013, T054, T055 |
| FR-010  | MOOG's `MPFS.API` removed; every callsite migrated                                       | T044–T053, T060 |
| FR-011  | All three repos move in the same release window; neither broken between landings        | T016 → T042 → T059, T061 |
| SC-001  | MOOG end-to-end exercises all eight ops via new endpoints                                | T056a, T056b, T062 |
| SC-002  | Zero `transaction/{address}` matches; swagger contains zero `transaction` paths         | T060 |
| SC-003  | Zero `Cardano.Ledger.Api.Tx` matches in verifier                                         | T060 |
| SC-004  | Default `WalletPolicy` rejects stubbed inflated pp                                       | T013, T055 |
| SC-005  | Both repo defaults move within the same release window                                   | T061 |

## Production deploy gating

The cutover window opens at T042 (server pivot merges to
`cardano-mpfs-offchain` main) and closes at T059 (MOOG migration
merges to `lambdasistemi/moog` main). During this window:

- `cardano-mpfs-offchain` main is at the post-pivot state and
  publishes only `/facts/*` endpoints.
- `lambdasistemi/moog` main is at the pre-pivot state and calls
  the legacy `transaction/...` endpoints — the production binary
  built from this commit cannot reach the server.

**Discipline**: No production MPFS server deploy may use the
post-T042 commit until T059 merges. No production MOOG deploy
may use the pre-T059 commit against a post-T042 server. The
cutover release deploys both at once.

This is the operational corollary of FR-011 / SC-005 — the
constitutional commitment that both repos move in lockstep.
