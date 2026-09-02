# Spec — 275 recovery, liveness, readiness

Issue: lambdasistemi/cardano-mpfs-offchain#275
Branch: `fix/275-recovery-readiness`
Base: `origin/main` @ `0f82465f5f828c2ab987a166e9e24c2368228d01`
Milestone contract: M2 `C-SIGNALS` (`/tmp/mpfs/lane/M2-LEDGER.md`), ruling
`A-003`.

Supersedes the closed, unmerged draft PR #276 (branch
`275-mpfs-v2-startup-replay`, head `62df0ba`). That branch is retained as
defect evidence only; none of its code is authoritative here. Its readiness
latch was startup-monotonic, its `/status` stayed 200 during restoration, and
its `StartupReadinessSpec` used in-process `Network.Wai.Test` against a fresh
temporary database — it never started Warp and never reopened a populated
database, so it proved neither recovery path.

## Problem

`mpfs-serve` conflates three distinct operational questions into one
observable surface, and answers two of them wrongly.

1. On an ordinary restart against a retained RocksDB, `withApplication`
   completes CSMT recovery and synchronous journal replay (`toFollowing`)
   **before** invoking the callback in which `Serve.hs` starts Warp. During
   that window no listener exists: a probe is refused or times out. On
   2026-08-24 that window exceeded the 10 s healthcheck timeout and autoheal
   killed the container repeatedly.
2. On a lost/empty database the listener does exist, but `GET /status` returns
   **200** with `utxo_root: null` and progressive checkpoint fields while the
   indexer is still restoring.
3. On a retained-but-stale database the indexer enters `InFollowing`
   immediately and serves a **correct but arbitrarily stale** root. On
   2026-05-19 `/status` reported `checkpoint_slot=3715222` against
   `tip_slot=123518063`.

There is no `/live` and no `/ready`. A supervisor therefore has only
"does an HTTP request succeed", which is false during legitimate recovery —
so wiring one restarts a healthy, recovering process.

## Users and stories

- **US-1 — supervisor.** As the process supervisor I probe exactly one signal
  that distinguishes "this process must be restarted" from "this process is
  recovering", so I never kill a healthy replay.
- **US-2 — moog-v2 canary (M2-T101).** As a dependent service I gate on one
  signal that is true only when MPFS can serve a correct and current root, so
  I never consume a stale or absent root.
- **US-3 — operator.** As an operator I can observe recovery progress
  throughout the not-ready window without any route claiming a root it does
  not have.
- **US-4 — deployer.** As the repository deployment helper I wait for
  readiness, not for "the port answers".

## Functional requirements

- **FR-1** `GET /live` returns 200 whenever the process is alive, from the
  moment the listener binds, throughout recovery, and thereafter. It is the
  only supervisor signal.
- **FR-2** The HTTP listener binds and answers `/live` **before** any retained
  database recovery or journal replay begins.
- **FR-3** `GET /ready` returns 503 until the server can serve a correct and
  current root, and 200 from then on, returning to 503 whenever correctness or
  currency is lost.
- **FR-4** Every gated route returns 503 while `/ready` is 503. No gated route
  ever returns 2xx carrying a null or stale root.
- **FR-5** The 503 body of `/ready` carries recovery diagnostics (reason,
  phase, checkpoint slot, tip slot) so US-3 keeps observability without any
  gated route answering.
- **FR-6** A failure during boot terminates the process with a non-zero exit
  and closes the listener. The service never parks as "alive forever, never
  ready".
- **FR-7** `scripts/deploy-preprod.sh` waits on `/ready`, never on `/status`.
- **FR-8** Repository documentation states that `/live` is the only supervisor
  signal and `/ready` is the dependency gate. No repository-owned artifact
  configures an HTTP healthcheck (the M2 interim ruling).
- **FR-9** `mpfs-serve`'s `main` performs no boot sequencing of its own beyond
  argument parsing and validation; the boot function proven by the recovery
  tests is the exact function production runs.
- **FR-10** `GET /version` returns 200 whenever the process is alive —
  including before the application context exists and throughout replay —
  carrying a mandatory release version, a mandatory full build-time commit,
  and an optional deploy-time image digest.
- **FR-11** Build identity is captured exactly once, during startup and before
  the listener/replay sequence, and is thereafter immutable. No per-request
  environment read and no `unsafePerformIO`.
- **FR-12** A development build may report an unmistakable sentinel, but a
  sentinel can never satisfy the clean-source predicate that qualifies a
  published artifact.

## Route classification

The readiness gate is an explicit allowlist and is default-deny: a route added
later is gated unless someone deliberately exempts it.

Always available — exactly:

- `GET /live`
- `GET /ready`
- `GET /version` (C-IDENTITY, amended into C-SIGNALS by M2 NOTE-001)

Gated (503 while not ready) — everything else, which today means all of
`Cardano.MPFS.API` (`Shared.API`) including `GET /status`, `GET /metrics`,
`GET /metrics/prometheus`, and the static Swagger assets.

Gating the Swagger assets and `/metrics` is deliberate: C-SIGNALS names the
always-available operational surfaces exhaustively, and a default-deny gate
with a three-entry allowlist cannot drift. Operator observability during the
not-ready window is preserved by FR-5 instead of by exempting `/metrics`.

M2 NOTE-001 and NOTE-002 assign this ticket the whole Haskell side of
`/version`: the shared route, the schema, the handler, `Cardano.MPFS.BuildInfo`
and its startup capture. M2-E-PUBLISH owns the later build-system injection of
the compile-time values, the OCI labels, and the release record, and must not
edit these files in parallel.

## Readiness definition

READY ⟺

- the indexer is in the **following** phase (full CSMT, not restoring), **and**
- the latest checkpoint slot is within the genesis **stability window** of the
  most recently observed chain tip slot, **and**
- proof reads are internally consistent.

When the chain follower is disabled (`followerEnabled = False`), currency is
vacuous and readiness follows the internal proof-read flag alone.

Rationale for the currency term: A-003 forbids serving "a stale root", and the
2026-05-19 incident is exactly a correct-but-stale root. The stability window is
already the threshold the follower uses for its restoration→following
transition, so no new tunable is introduced. This is deliberately **not** a
supervisor-visible timeout: nothing here is tuned against an unmeasured,
growing quantity.

## Acceptance criteria

Evidence must be produced over **real Warp/TCP** — a bound socket probed by an
HTTP client — never `Network.Wai.Test`.

- **AC-1** Retained-database reopen: while journal replay is deliberately held
  open for longer than the historical 10 s healthcheck window, a TCP client
  connects and `/live` returns 200 on every probe; `/ready` and every gated
  route return 503 on every probe; no gated route returns 2xx. After replay is
  released, `/ready` reaches 200 and gated routes serve.
- **AC-2** Empty/lost database: the same observations across the restoration
  window.
- **AC-3** Readiness returns to 503 after correctness is lost following a
  period of readiness (restoration reset / armageddon path), and gated routes
  return to 503 with it.
- **AC-4** Readiness is 503 while the checkpoint is outside the stability
  window of the observed tip, even though the phase is following and the root
  is correct.
- **AC-5** A boot failure exits the process non-zero and closes the listener.
- **AC-6** Every assertion behind AC-1..AC-5 and AC-9..AC-11 is demonstrated **able to fail**
  by an explicit negative control that is executed by the gate, not merely
  asserted in prose.
- **AC-7** `scripts/deploy-preprod.sh` polls `/ready`; no repository artifact
  polls `/status` for readiness or configures an HTTP healthcheck.
- **AC-8** `docs/assets/swagger.json` is regenerated and the repository
  `swagger-up-to-date` check passes.
- **AC-9** `GET /version` returns 200 with the mandatory fields during the
  same held-replay window in which every gated route returns 503.
- **AC-10** Mutating `MPFS_IMAGE_DIGEST` after startup does not change any
  `/version` response.
- **AC-11** The clean-source predicate rejects every sentinel value, and the
  digest predicate rejects anything that is not `sha256:` plus 64 hex
  characters. Both are demonstrated able to fail.

## Out of scope

- Build-system injection of the compile-time identity values, OCI labels,
  image publication, and the release record (M2-E-PUBLISH). This ticket ships
  the Haskell interface those consume, and nothing beyond it.
- Any production or external deployment configuration, including compose files
  and autoheal policy. This ticket must not add an HTTP healthcheck anywhere.
- moog-side consumption (M2-T101).
- Recovery *performance*: this ticket makes recovery observable and safe, not
  faster.
- Redesigning the facts API.

## Rejection behavior

- A gated route while not ready: HTTP 503 with a JSON error body. Never 200
  with a null or stale root, and never 404.
- `/ready` while not ready: HTTP 503 with the diagnostic body of FR-5.
- A timeout-tuning workaround, or a second in-process-WAI test, is a rejected
  solution shape, not an acceptable fallback. If listener-before-replay proves
  structurally infeasible, that is an upward blocker with evidence, not a
  licence to weaken the contract.
