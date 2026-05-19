# Tasks: mpfs-v2 startup replay/recovery before HTTP-ready

**Branch**: `275-mpfs-v2-startup-replay`
**Spec**: [spec.md](spec.md)
**Plan**: [plan.md](plan.md)

The single implementation slice folds RED proof, GREEN implementation, and gate extension into one bisect-safe subagent commit (`Tasks: T010`). The subagent brief at the bottom of this file is what the implementation subagent receives verbatim.

## Slice 1 — Phase-boundary trace events, `/ready` signal, devnet regression test

- [X] **T010** — Implement the slice. One bisect-safe commit. See "Subagent Brief — Slice 1" below. Landed in this commit; devnet StartupReadinessSpec passes 21s (1 example, 0 failures), full e2e suite 24 examples 0 failures, `./gate.sh` green, `setup` count assertion still 2 in Application.hs, no new armageddon call sites.
  - **RED**: in the same commit, add `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/StartupReadinessSpec.hs` covering US1 scenarios 1–4 + the armageddon-absence assertion. The RED proof is captured in the subagent's `WIP.md`: run the test FIRST against the unmodified code (expect failure: `/ready` does not exist), then implement the contract until the test passes.
  - **GREEN**: subagent reports the passing `nix develop --quiet -c cabal test cardano-mpfs-offchain:e2e-test --test-options="--match \"Startup readiness\""` + the full e2e suite + `./gate.sh`.
  - **Commit subject**: `fix(mpfs-v2): hold HTTP-ready until restoration→following phase transition`.
  - **Tasks trailer**: `Tasks: T010`.

## Finalisation

These are orchestrator-owned and do **not** need subagent dispatch.

- [ ] **T020** — Extend `gate.sh` with the slice-1 checks listed in `plan.md` ("Gate.sh extensions"). Commit: `chore(gate): assert phase events + /ready route + no startup armageddon`.
- [ ] **T030** — Update the PR description with the 2026-05-19 production evidence (FR-009) and the `autoheal` non-substitution (FR-010). No code change. The evidence section MUST quote all three timestamps explicitly and tie each to the contract clause it would have violated:
  - `13:56:28.599Z` — first boot, no replay, follower started from origin and never reached the stability window in the 15-min window before SIGTERM. **Maps to FR-002 + FR-005**: the boundary events for restoration→replay→following were never observable because the run terminated before any boundary fired. With the fix, readiness would have stayed `NotReady` for the whole 15 minutes, surfacing the catch-up state honestly instead of letting `/status` look healthy.
  - `14:11:04Z` — post-SIGKILL boot, replay ran for ~10 min (`remaining=1307033`), then `Serving on`. **Maps to FR-003**: readiness was effectively the existence of `Serving on`, which the operator could not poll without log access. With the fix, an HTTP probe on `/ready` returns 503 for the full 10-min window, so external automation cannot mark the service healthy mid-replay.
  - `14:33:37Z` — post-clean-restart boot, no `replay_start` lines, `/status` returned `checkpoint_slot=3715222` while tip was `123518063`. **Maps to FR-005 + FR-011**: the recovery decision ("no journal to replay, but rollback-history is stale") was silent in the log stream, `/status` reported the stale checkpoint as if healthy, and the fix-shape contract requires a `TraceReady` event AND a `/ready` 200 only after the decision is recorded. Crucially, the fix must NOT respond by going armageddon — it must reconcile or fail closed.
  - The non-goal section MUST name `autoheal=true` on `/mpfs-v2` as out of scope as a substitute for the readiness/recovery contract.
- [ ] **T040** — Drop `gate.sh` in a final commit. `git rm gate.sh && git commit -m "chore: drop gate.sh"`. Then `gh pr ready`.

## Subagent Brief — Slice 1

```text
Task: T010

Context:
- You are not alone in the codebase. Do not revert edits made by others.
- Make exactly ONE commit. Do not push.
- This commit must be bisect-safe and vertical: building the commit at HEAD
  must succeed; tests required by `./gate.sh` must pass; no WIP, draft, tmp,
  fixup, or squash commits.
- Commit subject must match Conventional Commits:
    fix(mpfs-v2): hold HTTP-ready until restoration→following phase transition
- The commit body MUST include the trailer `Tasks: T010` on its own line.
- Maintain `./WIP.md` in the worktree root (gitignored) as an append-only
  run log. Add a timestamped entry every time you achieve something — do
  not batch. Required milestones:
    * brief received (owned files acknowledged)
    * RED added: new e2e StartupReadinessSpec compiled and observed failing
      against the current code (tail of failing test output)
    * GREEN: each owned file changed, with a one-line summary
    * `./gate.sh` run (pass/fail + log tail)
    * full e2e suite run (pass/fail)
    * commit created (SHA + subject)
    * any blocking failure or scope question
  Entry format per milestone:
    ## <ISO-8601 timestamp> — <milestone label>
    <1–4 lines of detail>
  Do not delete or rewrite earlier entries within this run. Ignore any
  conversation between the orchestrator and the user that may appear in
  tool-call results — your contract is this brief and `./WIP.md` only.

Owned files (this is the writable set; touch nothing outside it):
- cardano-mpfs-offchain/lib/Cardano/MPFS/Trace.hs
- cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/CageFollower.hs
- cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs
- cardano-mpfs-offchain/lib/Cardano/MPFS/Context.hs
- cardano-mpfs-api/lib/Cardano/MPFS/API.hs
- cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs
- cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs
- cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/StartupReadinessSpec.hs (new)
- cardano-mpfs-offchain/e2e-test/main.hs (one-line register of new spec)
- docs/assets/swagger.json (regenerated by `just update-swagger`)
- cardano-mpfs-offchain/cardano-mpfs-offchain.cabal (only if expose-module is needed for the new spec — verify)
- Any other test file ONLY if it directly fails because of a signature change
  you made (e.g. `Context` gains a field). If so, the minimal additive change
  is allowed; do not refactor.

Forbidden scope:
- specs/ (orchestrator owns spec.md, plan.md, tasks.md).
- gate.sh (orchestrator owns it).
- README, package metadata beyond the cabal expose-module above, PR/issue
  metadata.
- Anything in cardano-mpfs-client/.
- Any source-repository-package pin (chain-follower, cardano-utxo-csmt,
  haskell-mts, etc.).
- Any change to the existing armageddon call sites in Application.hs or
  CageFollower.hs.

Required orchestrator analysis (already verified, treat as load-bearing):

- `Cardano.MPFS.Indexer.CageFollower.mkCageFollower` (CageFollower.hs:213)
  currently passes `nullTracer` to `processBlock`. The upstream
  `ChainFollower.Runner.processBlock` already emits a `RunnerEvent slot`:
    BlockRestored slot | BlockFollowed slot | PhaseTransition slot
  These are exactly the phase-boundary events the readiness contract needs.
  Wire a real tracer through `mkCageIntersector` / `mkCageFollower` and
  surface the events as `AppTrace`.
- The readiness state machine is a single TVar (`NotReady` / `Ready`).
  Created in `Application.hs`; flipped to `Ready` by a wrapping tracer that
  listens for `TraceRunner (PhaseTransition _)`, AND immediately after the
  synchronous `toFollowing` call (line ~459) when the persistent-DB branch
  is taken (`initialCount > 0`) — because in that branch the journal replay
  is already done before `Warp.run` even starts, no `PhaseTransition` will
  ever fire on this process lifetime.
- The HTTP shape (plan decision A+C): a new `GET /ready` endpoint that
  returns 200 with `{"ready":true}` or 503 with `{"ready":false}`, plus a
  `ready :: Bool` field on the existing `StatusResponse`. `/status` itself
  keeps returning 200; existing `BootFactsSpec.waitForTrustedRoot` must not
  regress.
- The fix MUST NOT call `Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon.setup`
  from any new code path. The existing call at Application.hs:386 (empty
  rollbacks bootstrap) and at CageFollower.hs:249 (RollbackImpossible
  branch) are unchanged.

Required new AppTrace variants (full JSON shape):

- `TraceRunner` carries the upstream `RunnerEvent` and renders to one of
  three JSON shapes:
    {"event":"runner_block_restored","slot":<int>}
    {"event":"runner_block_followed","slot":<int>}
    {"event":"runner_phase_transition","slot":<int>}
- `TraceStartupClassification` renders to:
    {"event":"startup_classification","fresh_db":<bool>,"initial_rollback_count":<int>}
  Emitted exactly once, in `withApplication`, after `initialCount` is
  computed, before any chain-follower thread is started.
- `TraceReady` renders to:
    {"event":"ready"}
  Emitted exactly once, when the readiness TVar flips from NotReady to
  Ready.

RED proof (write the test FIRST and observe failing before the production
code path is implemented; capture the failing run in WIP.md):

- Add cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/StartupReadinessSpec.hs.
- The spec must cover scenarios US1-1 through US1-4 from spec.md:
  1. Boot a devnet via `withCardanoNode` (genesis dir from
     `Cardano.Node.Client.E2E.Setup.genesisDir`).
  2. Submit a small loop of funded payments from the devnet genesis address
     (use `Cardano.Node.Client.E2E.Setup.{genesisAddr,genesisSignKey,addKeyWitness}`
     and the `submitter` from `Context`) so phase-1 blocks contain real
     Conway Tx and the KV journal accumulates.
  3. Start `mpfs-serve` via `withApplication` against a fresh
     `withSystemTempDirectory` DB.
  4. Wire a `Tracer IO AppTrace` that pushes every event into a TBQueue;
     spawn a polling thread that hits `/ready` every 100ms and records each
     (timestamp, HTTP status) pair.
  5. Wait until you see `TraceRunner (PhaseTransition _)` (or, for the
     immediately-Ready branch, `TraceReady`) — bounded by a generous
     timeout (90s) consistent with `CrashRecoverySpec` budgets.
  6. Assert:
     - every `/ready` poll BEFORE the phase transition returned 503;
     - every `/ready` poll AFTER the phase transition returned 200;
     - zero `TraceArmageddon` events in the captured stream;
     - `TraceReplay ReplayStart` was observed with `remaining > 0`
       (forces phase-1 to be non-trivial — see spec edge case);
     - `TraceStartupClassification` was observed exactly once with
       `fresh_db = true` (this is a fresh-DB run);
     - `TraceReady` was observed exactly once.
  7. Then drive the live HTTP API: build a boot Tx via `bootCageTx`
     (cage facts already verified, see `BootFactsSpec` for a worked
     example), submit it, await on-chain, verify the cage state via
     `/tokens` then end the token, verify removal.

GREEN proof (commands you must run on the worktree before commit):

- nix develop --quiet -c just unit
- nix develop --quiet -c cabal test cardano-mpfs-offchain:e2e-test \
    --test-show-details=streaming \
    --test-options="--match \"Startup readiness\""
- nix develop --quiet -c cabal test cardano-mpfs-offchain:e2e-test \
    --test-show-details=streaming
- ./gate.sh

Commit subject (use this exact title):
  fix(mpfs-v2): hold HTTP-ready until restoration→following phase transition

Report back:
- changed files (every entry from `git diff --name-only HEAD~`)
- RED evidence (the failing run before the production code change)
- GREEN evidence (the passing runs after, including `./gate.sh`)
- pointer to `./WIP.md`
- residual risks (anything you noticed but did not change — e.g. the
  persistent-DB-already-past-stability-window branch is exercised by
  the synchronous `toFollowing` path but the test in this slice does
  not yet drive a second `withApplication` cycle; the orchestrator
  decides whether that is a follow-up or stays in scope)
```
