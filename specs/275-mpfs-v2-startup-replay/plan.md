# Implementation Plan: mpfs-v2 startup replay/recovery before HTTP-ready

**Branch**: `275-mpfs-v2-startup-replay`
**Spec**: [spec.md](spec.md)
**Status**: Draft

## Summary

The bug shape (issue #275, 2026-05-19 production incident on `mpfs-v2.plutimus.com`) is that `mpfs-serve` reports `/status` healthy with a stale `checkpoint_slot` long before the indexer has populated the CSMT to a state consistent with what `/status` returns. The fix is two things:

1. **Make the existing four-phase startup lifecycle observable.** The upstream `ChainFollower.Runner.processBlock` already emits `RunnerEvent { BlockRestored | BlockFollowed | PhaseTransition }`, but `Cardano.MPFS.Indexer.CageFollower.mkCageFollower` discards them by passing `nullTracer` (CageFollower.hs:213). Wire those events into `AppTrace` so phase boundaries are externally observable.
2. **Wire an HTTP readiness signal that fails closed until the indexer has crossed phase 2 (restoration → following).** Hold readiness `NotReady` from process start; flip it to `Ready` on the first `PhaseTransition` event from the runner. Expose the signal so external automation can distinguish "recovery pending" from "ready and serving".

Critically, **the fix MUST NOT take the armageddon path**: it is purely additive observability + a TVar-driven readiness signal that gates HTTP. The existing replay path (`openCSMTOps` `NeedsRecovery → recover`, `composedInit.toFollowing → toFull`) is the one that runs; we only wait for it to finish before flipping readiness.

## Ownership split

| Owner | Scope |
|---|---|
| Orchestrator (you) | `specs/275-mpfs-v2-startup-replay/*`, `gate.sh`, PR metadata, README/docs alignment if needed, post-merge cleanup. |
| Implementation subagent (one slice) | Code + test changes listed in "Slice 1" below. One bisect-safe commit. |

## Architectural decisions

### Readiness signal shape

Options considered:

- **A. Dedicated `/ready` endpoint, body-less, 200 / 503.** Minimum blast radius on existing API (`/status` unchanged). External automation (Docker HEALTHCHECK, k8s readinessProbe, autoheal) gets a stable HTTP-status-only contract.
- **B. `/status` returns 503 during recovery, 200 once ready.** Matches operator mental model (the 2026-05-19 incident was caused by `/status` 200 lying). But breaks any external client that assumes `/status` is always 200 — including the existing `BootFactsSpec.waitForTrustedRoot` polling helper that asserts `status200`.
- **C. Typed `ready: bool` field on `/status` payload.** Backwards-compatible at HTTP-status level; but useless for probes that only read the status code.

**Decision: A + C combined.** Introduce `GET /ready` (200 / 503) for HTTP-level probes, and add `ready :: Bool` to `StatusResponse`. `/status` keeps returning 200; existing tests/clients (including `BootFactsSpec.waitForTrustedRoot`) unaffected. The contract gate uses `/ready`; the operator UX uses `/status`.

### New `AppTrace` events

Add three variants to `Cardano.MPFS.Trace.AppTrace`:

- `TraceRunner RunnerEvent` — lifts the upstream `ChainFollower.Runner.RunnerEvent slot` (`BlockRestored`, `BlockFollowed`, `PhaseTransition`) into `AppTrace`. JSON tags: `runner_block_restored`, `runner_block_followed`, `runner_phase_transition`. This is the load-bearing event; the readiness state machine listens to `PhaseTransition`.
- `TraceStartupClassification { isFresh :: Bool, initialRollbackCount :: Int }` — emitted once in `withApplication` after the `initialCount` is computed, before phase 1 begins. JSON: `{ "event": "startup_classification", "fresh_db": …, "initial_rollback_count": … }`.
- `TraceReady` — emitted when the readiness TVar flips to `Ready`. JSON: `{ "event": "ready" }`. Operator and test can scan for this single event to know recovery is done. This event is the single observable "recovery decision recorded" marker required by spec FR-005: it fires after a `PhaseTransition` on the long restoration path AND after the synchronous `toFollowing` returns on the persistent-DB `initialCount > 0` path (no-op-recovery shape).

Existing events (`TraceArmageddon`, `TraceReplay ReplayStart`/`ReplayStop`, `TraceBlock`, `TraceBlockReceived`, `TraceChainTip`, `TraceSkipProgress`) stay unchanged.

### Readiness state machine

A new `TVar Readiness` initialised to `NotReady` at process start. Two transitions:

- `NotReady → Ready` — on the first `PhaseTransition` event from the runner, **or** on a recovery-decision conclusion of "no-op" (persistent DB whose on-disk state is already past the stability window, so no `PhaseTransition` will fire on this run). The plan covers both with a single rule: flip to `Ready` when the cage follower's first non-restoration step completes, whatever its shape.
- `Ready → Ready` — terminal. No going back. A subsequent rollback that lands in the `RollbackImpossible` branch + `armageddon` reset is its own incident (and out of scope for this PR per FR-011).

The TVar is added to `Context` so the HTTP handlers can read it.

### Where the TVar transition is driven

The cleanest insertion point is a wrapping tracer in `Application.hs`:

```haskell
let readinessTVar = ...  -- TVar Readiness, init NotReady
    readinessTracer = Tracer $ \case
        TraceRunner (PhaseTransition _) ->
            atomically $ writeTVar readinessTVar Ready
        _ -> pure ()
    appTracer' = readinessTracer <> appTracer cfg
```

This keeps the readiness logic localised in `Application.hs` rather than threaded through `CageFollower`. The "no-op recovery" case (persistent DB already past stability window) is handled by detecting `initialCount > 0` at the point we compute `initialPhase`: if we already start in `InFollowing`, no `PhaseTransition` will ever fire, so we flip the TVar to `Ready` immediately after `toFollowing` (which still runs `toFull`/journal replay) returns. This preserves the contract: readiness only flips after the synchronous journal replay completes.

### HTTP wiring

- Add `readiness :: TVar Readiness` to `Cardano.MPFS.Context.Context`.
- Add `GET /ready` to `Cardano.MPFS.HTTP.API` returning a small JSON body `{ "ready": Bool }`, with HTTP 503 when `NotReady` and 200 when `Ready`. Use Servant's `Handler` + `throwError err503` for the negative case.
- Add `ready :: Bool` to `StatusResponse` in `Cardano.MPFS.API.Types`. Default value `False` when the TVar is `NotReady`.
- Update `docs/assets/swagger.json` for both changes (regenerated via `just update-swagger`).

## Vertical slice plan

**One implementation slice** + one finalisation step. Splitting observability from the readiness contract would leave the first commit with no proof and the second commit with an awkward retroactive test, so they ride together.

### Slice 1 — Phase-boundary trace events, `/ready` signal, devnet regression test

Owned files (subagent brief lists these as the writable set):

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trace.hs` — add `TraceRunner`, `TraceStartupClassification`, `TraceReady` and their JSON encoders.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/CageFollower.hs` — replace `nullTracer` (line 213) with a tracer parameter; thread the parameter through `mkCageIntersector` / `mkCageFollower` callers.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs` — add the `Readiness` TVar, the wrapping `readinessTracer`, pass the new tracer down to `mkCageIntersector`, emit `TraceStartupClassification` once, emit `TraceReady` when the TVar flips, populate `ctx.readiness`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Context.hs` — add `readiness :: TVar Readiness` field.
- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` — add `ReadyAPI = "ready" :> Get '[JSON] ReadyResponse` to the API alias.
- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` — add `ReadyResponse { ready :: Bool }` with JSON + Swagger instances; extend `StatusResponse` with `ready :: Bool`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` — implement `readyHandler` (200 / 503), have `statusHandler` populate the `ready` field, wire `readyHandler` into the server.
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/StartupReadinessSpec.hs` — **new** devnet-driven test covering the full lifecycle from spec scenarios US1-1 through US1-4, plus the armageddon-absence assertion.
- `cardano-mpfs-offchain/e2e-test/main.hs` — register the new spec.
- `docs/assets/swagger.json` — regenerated.

Out of scope for this slice (forbidden in subagent brief):

- `specs/275-mpfs-v2-startup-replay/*` — orchestrator owns artifacts.
- `gate.sh` — orchestrator owns it.
- README, package metadata, PR metadata.
- Any change to the upstream `chain-follower` or `cardano-utxo-csmt` source-repository-package pins.
- Any change to `cardano-utxo-csmt` armageddon module behaviour.

**RED proof** (the test must demonstrate the bug before the fix):

The new `StartupReadinessSpec` runs against current code first by:

1. Booting a devnet (`withCardanoNode`).
2. Running a small driver that submits N funded payments from the genesis address (forces phase-1 blocks to contain real Conway-era Tx with UTxO movement, satisfies spec FR-007).
3. Starting `mpfs-serve` (`withApplication`) against a fresh temp DB.
4. Capturing the `AppTrace` stream into a `TBQueue` via a `Tracer` wired to the spec.
5. Polling `/ready` on a tight loop while phase 1 is in progress.
6. Asserting: every `/ready` poll before `TraceRunner (PhaseTransition _)` returns 503; every `/ready` poll after returns 200.
7. Asserting: zero `TraceArmageddon` events fire during phases 1–3.
8. Booting a token via HTTP, observing the cage follower indexing it, ending the token, observing the end indexed.

On `ffc8dfe` (current `main`), steps 5–6 fail because `/ready` does not yet exist (404). The subagent first lands the endpoint returning constant 200 to demonstrate that the readiness contract is what fails — then makes the test pass by wiring the TVar. Both branches of the assertion (503 then 200) must be observed.

**Live-boundary diagnostic** (spec live-boundary playbook): *"What system boundary does this exercise that the unit suite cannot?"* — three boundaries: the cardano-node devnet (blocks + Tx production), the `withApplication` startup state machine (phase transitions), and the Warp HTTP server (status code semantics). A unit test against `Application.hs` with a fake chain follower could pass while the live chain follower never emits `PhaseTransition` in the way `Application.hs` expects. The devnet-driven test is in-gate (`./gate.sh`).

**GREEN proof** (commands the subagent must run on the worktree):

- `nix develop --quiet -c just unit` (existing unit suite stays green; new test runs under e2e-test, not unit, but unit must not regress).
- `nix develop --quiet -c cabal test cardano-mpfs-offchain:e2e-test --test-show-details=streaming --test-options="--match \"Startup readiness\""` — runs only the new spec; must pass on the fix HEAD.
- `nix develop --quiet -c cabal test cardano-mpfs-offchain:e2e-test --test-show-details=streaming` — full e2e-test suite; must not regress.
- `./gate.sh` — gate must pass.

### Slice 2 — Finalisation

Owned by the orchestrator. Two commits:

- `docs: PR description and operator note for readiness/recovery` (if needed; the PR body update + a short paragraph in `cardano-mpfs-offchain/README.md` operator section).
- `chore: drop gate.sh` — the final commit before `gh pr ready`.

## Gate.sh extensions

After slice 1 lands, `gate.sh` is extended with:

- `check_present "TraceRunner JSON tag missing" '"runner_phase_transition"' cardano-mpfs-offchain/lib/Cardano/MPFS/Trace.hs`
- `check_present "/ready route missing" '"/ready"' docs/assets/swagger.json`
- `check_present "ready field missing on StatusResponse" '"ready"' docs/assets/swagger.json`
- A **count assertion** on `setup` calls in `Application.hs`: today there are exactly **two** (line 386 empty-rollbacks bootstrap, line 493 `csmtArmageddon` passed into the chain follower). Gate shape:
  ```bash
  test "$(grep -cE '^[[:space:]]+\$?[[:space:]]*setup$' cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs)" = 2
  ```
  This catches an accidental third call site introduced by this PR. Both existing call sites are line-broken (`$ setup\n…` and `let csmtArmageddon = setup\n…`); a substring pattern with a trailing space would miss them, so the count form is the robust one. Verified against current `main` (`ffc8dfe`).

## Live-boundary smoke

The devnet `StartupReadinessSpec` is the live-boundary smoke. It rides inside `gate.sh` (via the full e2e-test invocation). No deferred operator follow-up.

## Risks and edge cases

- **Devnet `securityParam` and timing**: phase 1 must produce enough real Tx that `TraceReplay ReplayStart`'s `remaining` is non-zero. The driver-side payment loop is the lever; the test asserts `remaining > 0` so a silent degenerate run fails loudly.
- **Persistent-DB-already-past-stability-window**: the `initialCount > 0` branch in `Application.hs` calls `toFollowing` synchronously before `Warp.run`, so `toFull` (the journal replay) runs before HTTP comes up at all. We still flip the readiness TVar to `Ready` only after that synchronous step completes, so a slow replay does not race the HTTP handler. The test does not yet exercise this branch directly (would need a two-`withApplication`-call test like `CrashRecoverySpec`); see operator follow-up note in the PR body.
- **Existing `BootFactsSpec.waitForTrustedRoot`**: polls `/status` and asserts 200. Unaffected by Option-A-plus-C because `/status` keeps returning 200. If plan review picks Option B, this helper must be updated to tolerate 503 during ramp-up.
- **Tracer composition**: the existing `Tracer IO AppTrace` pipeline uses `<>` composition (`metricsTracer`, `jsonLinesTracer`, etc.). Adding `readinessTracer` to the front of the chain must not duplicate downstream tracers; the slice's subagent must verify by checking the JSON output is not doubled.
- **Upstream surface**: `RunnerEvent` is exported by `ChainFollower.Runner` at the pinned SHA `d592a50` — confirmed via `/nix/store/.../lib/ChainFollower/Runner.hs`. No upstream change required.

## Out of scope

- Reconciliation of cage rollback history with the journal-derived CSMT state (the deeper root-cause investigation of why `latestRollbackPoint` returned slot 3.7M on the third 2026-05-19 restart). This PR fixes the user-visible contract; the deeper investigation is a follow-up.
- A health-vs-readiness distinction (k8s style). `/ready` here is binary; future tickets can split readiness from liveness.
- Any change to MOOG-v2 callers; no API-shape contract changes that affect MOOG.
