# Feature Specification: mpfs-v2 startup replay/recovery before HTTP-ready

**Feature Branch**: `275-mpfs-v2-startup-replay`
**Created**: 2026-05-19
**Status**: Draft
**Input**: Issue #275 — "BUG: mpfs_v2 cannot enter replay phase before serving"

## User Scenarios & Testing

### User Story 1 — Operator runs the full devnet lifecycle, replay is observable, /status only reports ready after CSMT is populated (Priority: P1)

As an MPFS operator, I run `mpfs-serve` against a local devnet from a freshly seeded database. I expect the service to walk every startup phase the production indexer also walks, in this order:

1. **Restoration (KVOnly chain-follow)** — chain-follow from origin up to the stability window, applying cage events and writing UTxO ops to the KV journal *without* populating the CSMT. This phase is the existing `InRestoration` shape in `Cardano.MPFS.Indexer.Backend.composedInit` and is fast because CSMT writes are deferred. The blocks in this window are not empty: the devnet must produce real Conway-era transactions during phase 1 so the KV journal accumulates a non-trivial entry count — otherwise phase 2 has nothing to replay and the test does not exercise the bug.
2. **Replay (journal → CSMT)** — at the stability-window boundary (when `slot + securityParam >= tipSlot`), the indexer transitions out of restoration by calling `toFollowing`/`toFull`, which **replays the KV journal into the CSMT**. This phase emits the existing `TraceReplay ReplayStart` / `ReplayStop` events.
3. **Following (CSMT-enabled chain-follow)** — chain-follow the remaining blocks up to the chain tip, updating the CSMT per block. This phase is the existing `InFollowing` shape.
4. **Service usage** — operator boots a token through the live HTTP API, the cage follower indexes it, operator ends the token, the cage follower indexes the end.

The new contract: the HTTP service MUST NOT signal ready (and external automation MUST NOT see a healthy response) **until phase 2 has run to completion**. Phases 1 and 2 must be externally observable in the structured-log stream so an operator (and the regression test) can assert ordering. Phase 4 is the proof that, by the time `/status` reports ready, the cage state served is the post-replay state — not the pre-replay restoration-phase state.

**Critical invariant — no armageddon on the bug path**: the fix MUST resolve the bug by **completing the in-flight journal replay and continuing chain-follow from where replay left off**. It MUST NOT take the armageddon path (`Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon.setup`, which initialises the UTxO state from scratch and would force a follow from origin). Armageddon stays reserved for its existing roles — initial Origin entry on a truly empty rollbacks store, and the `RollbackImpossible` branch in `CageFollower` when a chain rollback is older than the security parameter. The startup recovery decision on the bug shape (persistent DB with pending journal entries, or persistent DB whose journal is empty but rollback history says the indexer was past the stability window) MUST be a *forward* operation that produces or preserves the post-replay CSMT, not a *reset* operation that throws it away.

The operator command is unchanged:

```bash
mpfs-serve \
  --port 3000 \
  --socket /ipc/node.socket \
  --db /data/db \
  --shelley-genesis /configs/shelley-genesis.json \
  --byron-genesis /configs/byron-genesis.json \
  --blueprint /etc/mpfs/blueprint.json
```

**Why this priority**: this is the bug. On 2026-05-19, production `mpfs-v2` on `mpfs-v2.plutimus.com` (image `ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve:ffc8dfe`, DB `/node/mpfs-v2:/data`, node socket `/node/preprod/ipc`) restarted in a state where the indexer did not enter phase 2 (no `replay_start` events) and the HTTP service began answering `/status` with `checkpoint_slot=3715222` while the chain tip was `123518063`. External automation marked the service healthy and downstream consumers saw stale checkpoint state for the duration of the catch-up window. Without this story, the service cannot be safely operated behind an autohealing or probe-driven supervisor against a persistent preprod database.

**Independent Test**: a devnet-driven regression test that runs `mpfs-serve` against a fresh devnet long enough to walk all four phases, asserts the ordering from the structured-log stream and from `/status` polling, and then boots + ends a token through the HTTP API to prove the post-replay state is the state the service actually serves. The test fails on `ffc8dfe` because, in current code, `/status` returns 200 with a checkpoint payload long before `TraceReplay ReplayStop` fires.

**Acceptance Scenarios**:

1. **Given** a running devnet (`withCardanoNode` style, the same helper the existing `e2e-test` suite uses) that produces a stream of Conway-era transactions before the stability window is reached, and a fresh, empty MPFS database directory, **When** the operator starts `mpfs-serve` against that devnet, **Then** the structured-log stream emits, in order: a phase event for *restoration start*, a stream of restoration-phase per-block events (with non-zero accumulated UTxO ops, observable by the `TraceReplay ReplayStart` `remaining` count being non-trivial), a phase event for *replay start* (the existing `TraceReplay ReplayStart`), a phase event for *replay stop* (the existing `TraceReplay ReplayStop`), and then a phase event for *following start*. No `/status` call returns a healthy/ready response before the *replay stop* event.
2. **Given** the running `mpfs-serve` from scenario 1, **When** an external probe polls the readiness signal during the restoration window (before *replay start*) and during the replay window (between *replay start* and *replay stop*), **Then** the response is "not ready" in both windows — by HTTP status (e.g. 503 on `/status` or on a dedicated readiness path) or by a typed "ready/not-ready" field on the `/status` payload — and the probe does not see a healthy response.
3. **Given** the running `mpfs-serve` from scenario 1, **When** the *replay stop* event has fired and the cage follower has caught up to the devnet's chain tip, **Then** the readiness signal flips to "ready", `/status` reports a `checkpoint_slot` consistent with the indexed chain, and the operator can boot a token through the live HTTP API. The cage follower indexes the boot.
4. **Given** the token from scenario 3, **When** the operator ends the token through the live HTTP API, **Then** the cage follower indexes the end and `/status` reflects the post-end cage state.
5. **Given** the same devnet, a fresh database, and `mpfs-serve` at commit `ffc8dfe`, **When** the regression test asserts the "no healthy `/status` before *replay stop*" ordering from scenario 1, **Then** the assertion fails — proving the test exercises the bug.
6. **Given** the devnet had been running long enough that the stability window has been crossed at least once (so phase 2 has fired at least once and there is meaningful CSMT state on disk), **When** the operator stops `mpfs-serve` cleanly and restarts it against the same database, **Then** the restart re-asserts the same readiness ordering: any startup work required by the on-disk state (whether that is journal-tail replay, cage-rollback reconciliation, or a no-op recovery) completes before `/status` reports ready. This is the 2026-05-19 third-restart shape.

---

### User Story 2 — Fresh-DB startup remains explicit and fast (Priority: P1)

As an MPFS operator, I start `mpfs-serve` against a completely empty database directory (no journal, no checkpoint, no cage rollback history). I expect the service to announce itself as a fresh-DB start in the structured-log stream, perform any genesis seeding that is configured, and then enter phase 1 (restoration) as normal. The classification "fresh-DB" must be observable — not inferred from the absence of other events.

**Why this priority**: the issue's acceptance criteria explicitly require "Fresh empty DB startup remains explicit and tested". The risk this story manages is that the fix for US1 silently regresses the fresh-DB path or makes it indistinguishable from a persistent-DB recovery.

**Independent Test**: scenario 1 in US1 *is* the fresh-DB path (`withSystemTempDirectory` produces an empty database directory). The test asserts the explicit fresh-DB classification event is present and that the existing fresh-DB e2e tests still pass.

**Acceptance Scenarios**:

1. **Given** an empty database directory, **When** `mpfs-serve` starts, **Then** the structured-log stream contains an explicit fresh-DB classification event before phase 1 begins.
2. **Given** the existing `e2e-test` suite (`HTTPLifecycleSpec`, `CrashRecoverySpec`, `BootFactsSpec`, etc., all of which use fresh temp DBs), **When** they run against the fix HEAD, **Then** they pass without modification beyond what is required to read the new readiness signal (if the chosen shape requires it).

---

### User Story 3 — Production log evidence and `autoheal` non-substitution are recorded in the PR (Priority: P1)

As the operator who hit the incident, I read the merged PR description and find the 2026-05-19 production evidence cited verbatim (the three boots) and the explicit non-goal that re-enabling `autoheal=true` on the `/mpfs-v2` container is *not* a substitute for the readiness/recovery contract.

**Why this priority**: the issue's acceptance criteria require both items. P1 because future regressions of this shape are easy to mis-triage as "the indexer is just behind" if the evidence is not anchored to a named contract, and because the next on-call can otherwise undo the non-goal.

**Independent Test**: open the merged PR description; the production-evidence section quotes `13:56:28.599Z`, `14:11:04Z`, `14:33:37Z`, and the non-goal section names `autoheal=true` as out-of-scope as a fix.

**Acceptance Scenarios**:

1. **Given** the merged PR description, **When** an operator searches it for the three restart timestamps, **Then** all three are present with a one-line explanation tying each to the contract clause it would have violated.
2. **Given** the merged PR description, **When** the reader looks for autoheal, **Then** the non-goal is stated and the readiness/recovery contract is named as the durable fix.

---

### Edge Cases

- **Fresh DB with genesis seeding configured**: seeding is part of startup work. Readiness is held until seeding completes, then phase 1 begins. The fresh-DB classification event still fires.
- **Stability window already crossed on disk**: a restart where the on-disk state already represents a post-replay world (the 2026-05-19 third-restart shape). The startup work for this state may be a no-op recovery (no journal entries to replay) — but readiness must still hold until the recovery decision is recorded in the log stream. `/status` must not return a pre-recovery checkpoint as ready.
- **Replay fails mid-stream**: the service must fail closed (exit non-zero or remain "not ready" and surface the failure in structured logs). Under no circumstance does `/status` report healthy with a partially-replayed CSMT, and under no circumstance does the recovery code silently fall back to armageddon to "make the problem go away" — that decision belongs to the operator.
- **Devnet stability window is small enough that phase 1 is brief**: this is *the* property the test exploits. The devnet's `securityParam` is small, so phase 1 → phase 2 → phase 3 can be walked in test time. The test must not depend on a slow devnet to manufacture the ordering — the contract holds regardless.
- **Phase-1 blocks with no transactions**: if the devnet only produces empty blocks during phase 1, the KV journal is empty when phase 2 fires, the replay finds nothing to do, and the test silently degenerates into a no-op-recovery exercise that does NOT prove the bug. The test must fail fast in this case — it asserts a non-zero `remaining` count on `TraceReplay ReplayStart` (see FR-007).
- **Token boot/end submitted before readiness flips**: out of scope here. The contract is about `/status` / readiness, not about HTTP write endpoints. The test boots its token *after* readiness flips and is silent about the pre-ready behavior of write endpoints.
- **Node socket unavailable during startup**: out of scope. The node connection is a separate readiness gate that this spec does not introduce.

## Requirements

### Functional Requirements

- **FR-001**: `mpfs-serve` MUST classify a startup as either *fresh-DB* (no persisted state that requires recovery) or *persistent-DB* (state that does require recovery or reconciliation) and MUST emit a structured-log event recording that classification before phase 1 begins.
- **FR-002**: The four startup phases — restoration, replay, following, ready-to-serve — MUST each be marked by structured-log events (start and end where applicable). The replay phase reuses the existing `TraceReplay ReplayStart` / `ReplayStop`. The restoration and following phases need their own observable boundary events (the existing `TraceBlockReceived` is per-block and does not by itself mark the *phase boundary*).
- **FR-003**: `mpfs-serve` MUST expose a readiness signal that fails closed until the replay phase has run to completion (or has been explicitly determined unnecessary by the recovery decision the spec leaves to the plan). Acceptable shapes: an HTTP status code other than 200 on `/status` or on a dedicated readiness path during the recovery window, or a typed "ready/not-ready" field on the existing `/status` payload. The plan picks one; the spec requires only that external automation can distinguish "recovery pending" from "recovered and serving" with a single HTTP request.
- **FR-004**: For a *fresh-DB* startup, the service MUST run any configured genesis seeding before phase 1 begins, then walk phases 1–3 in order. The readiness signal MUST hold until phase 2 completes, the same way as the persistent-DB case. (There is no special-case "skip recovery because fresh DB" — fresh-DB simply produces a phase-2 that finds nothing to replay; the *contract* is uniform.)
- **FR-005**: When the recovery decision is "no replay needed" — for example, the on-disk state already represents a post-replay world — the service MUST still record that decision in the structured-log stream before flipping readiness to ready. The `/status` checkpoint MUST be consistent with what the indexer would actually serve, not with a stale entry pulled from the cage rollback history alone.
- **FR-006**: When phase 2 cannot complete (journal corruption, CSMT inconsistency the recovery cannot reconcile, etc.), `mpfs-serve` MUST fail closed — exit non-zero or remain in a "not ready" state and surface the failure in structured logs. It MUST NOT report healthy with a partially-replayed CSMT, and it MUST NOT silently fall back to armageddon (state reset) as a workaround. The operator is the actor that decides to wipe state, not the startup code.
- **FR-011**: The fix MUST resolve the bug by completing the in-flight journal replay and resuming chain-follow from where replay left off. The recovery code path introduced by this PR MUST NOT call `Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon.setup` (or any equivalent state-reset entry point) on the bug shape. Armageddon's existing call sites (initial Origin entry for a truly empty rollbacks store, and `RollbackImpossible` rollback handling in `CageFollower`) are out of scope here and MUST remain unchanged.
- **FR-012**: The regression test from FR-007 MUST assert that no `TraceArmageddon` event fires during phases 1–3 of the lifecycle it exercises. If `TraceArmageddon` is observed, the test fails with an assertion that names the reset path as the violation.
- **FR-007**: The release MUST ship a devnet-driven regression test that:
  - boots a devnet with the existing `withCardanoNode` helper,
  - **produces real Conway-era transaction traffic during phase 1** — at minimum one funded payment per N blocks against the devnet's genesis address, sustained until the stability window is reached, so the KV journal accumulates a non-trivial entry count and phase 2 has real work to do. The test MUST assert this directly by checking the `TraceReplay ReplayStart` `remaining` field is non-zero,
  - starts `mpfs-serve` (via `withApplication`) against a fresh temp database,
  - drives the indexer long enough that the stability window is crossed (so phase 2 fires),
  - asserts the ordering contract from FR-002 + FR-003 by combining structured-log capture and `/status` (or readiness-path) polling,
  - then boots a token through the live HTTP API, observes the cage follower indexing it, ends the token, observes the end indexed,
  - asserts the full sequence with deterministic timing primitives (`Tracer`-driven `TMVar` signals — same pattern as `CrashRecoverySpec`) so the test is not flaky.
- **FR-008**: The regression test MUST fail on the pre-fix HEAD (`ffc8dfe`, the image deployed during the 2026-05-19 incident) with a clear assertion message naming the ordering violation, and MUST pass on the fix HEAD.
- **FR-009**: The merged PR description MUST quote the 2026-05-19 production log timestamps from the issue (`13:56:28.599Z`, `14:11:04Z`, `14:33:37Z`) and map each to the contract clause it would have violated.
- **FR-010**: The merged PR description MUST state, as a non-goal, that re-enabling `autoheal=true` on `/mpfs-v2` is not a substitute for the readiness/recovery contract.

### Non-Functional Requirements

- **NFR-001**: The recovery phase MUST NOT silently regress fresh-DB startup latency for existing e2e tests. A fresh-DB devnet startup that previously reached "indexer caught up" in N seconds MUST continue to do so within the existing test timeouts (modulo CI noise).
- **NFR-002**: Structured log output MUST remain a single JSON-line-per-event stream on `stderr`, matching the existing `jsonLinesTracer` shape. New events follow the same envelope (`{ "ts": …, "trace": { "event": "<name>", … } }`); no new sink, no format change to existing events.
- **NFR-003**: The new regression test MUST run in the same CI gate as the existing `e2e-test` suite and MUST complete within a budget consistent with the existing devnet tests (the existing `CrashRecoverySpec` is the upper bound — the new test exercises a similar lifecycle plus one extra `withApplication` cycle for the explicit restart scenario, where applicable).

### Key Entities

- **Startup phase**: an externally-observable phase of `mpfs-serve` startup. Values: *seeding* (optional, only for fresh-DB with genesis configured), *restoration* (phase 1, KVOnly), *replay* (phase 2, journal → CSMT), *following* (phase 3, CSMT-enabled chain-follow), *ready* (HTTP service is accepting state-dependent traffic).
- **Readiness signal**: the externally-observable answer to "is the service ready to serve traffic that depends on indexer state". MUST be derivable from a single HTTP request (no log scraping required by external automation).
- **Startup classification**: a derived property of the on-disk database state, computed at process start and emitted as a structured-log event. Values: *fresh-DB* or *persistent-DB*.
- **Recovery decision**: the conclusion phase 2 reaches on a persistent-DB startup. Values: *replay-ran* (journal entries were replayed into the CSMT) or *no-op* (no replay needed, recorded explicitly in the log stream). Drives whether `/status` is allowed to flip to ready.

## Success Criteria

### Measurable Outcomes

- **SC-001**: The devnet regression test from FR-007 fails on commit `ffc8dfe` with a clear assertion message naming the ordering violation, and passes on the fix HEAD.
- **SC-002**: After the fix, running `mpfs-serve` against a persistent preprod database that requires journal replay (the 2026-05-19 second-restart shape, ~1.3M entries) never produces a window in which the readiness signal reports healthy while replay is still running.
- **SC-003**: After the fix, the 2026-05-19 third-restart shape (clean shutdown → restart → no replay path) cannot serve `/status` as ready with a checkpoint slot that is stale relative to what the indexer would actually pick up. Either the recovery decision reconciles, or readiness fails closed.
- **SC-004**: A fresh-DB devnet startup (the existing `e2e-test` scenarios) continues to walk phases 1–3 within the existing test timeouts. No test in the existing `e2e-test` suite is silently broken or skipped to accommodate the new contract.
- **SC-005**: The merged PR description carries the production evidence (FR-009) and the `autoheal` non-goal (FR-010), verifiable by reading the PR.
- **SC-006**: The regression test, run on the fix HEAD, observes all four phase boundary events (restoration start/end, replay start/end, following start, ready) in order in the structured-log stream, and observes "not ready" then "ready" on the readiness signal across the replay boundary.
- **SC-007**: The regression test, run on the fix HEAD, observes zero `TraceArmageddon` events during phases 1–3. Armageddon is not used as a recovery shortcut on the bug shape.

## Assumptions

- The existing devnet test helpers (`withCardanoNode`, `withApplication`, the `AppTrace` `Tracer IO`-shaped event stream, the `CrashRecoverySpec`-style `TMVar`-driven phase synchronization) are sufficient to drive the full-lifecycle test. The plan may add a thin helper that captures the trace stream into a queue so the test can assert ordering.
- The devnet's `securityParam` (and the size of the chain produced by the devnet during the test window) is small enough that phase 1 → phase 2 → phase 3 can be walked in test time. The existing `CrashRecoverySpec` already achieves this in practice; the plan will confirm.
- "Healthy/ready" today is observed externally as a successful `GET /status` returning 200 with a checkpoint payload. The plan picks whether the readiness signal moves to a dedicated path (e.g. `/ready`), or whether `/status` itself returns a non-2xx during recovery, or whether the response payload gains a typed `ready` field. The spec is agnostic.
- The bug reproduces on `ffc8dfe` with the test described in FR-007. The plan will verify reproducibility on `ffc8dfe` before locking in the assertion message, so FR-008 is concrete.
- The MOOG-v2 boundary work tracked in cardano-foundation/moog#96 is unaffected by this change (no API shape change, only a readiness shape change). The PR is not paired with a MOOG-side PR.
- The `cardano-utxo-csmt` upstream module that owns `openCSMTOps` / `composedInit` / `toFollowing` / `toFull` / `TraceReplay` is treated as a stable dependency; the fix does not require an upstream change. If it does (for example, if the restoration-phase boundary cannot be observed without an upstream trace addition), the plan will surface that as a blocker before any subagent dispatch.
