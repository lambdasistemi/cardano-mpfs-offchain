# Plan — 275 recovery, liveness, readiness

Base: `origin/main` @ `0f82465f5f828c2ab987a166e9e24c2368228d01`.

## Strategy

Three structural moves, in this order:

1. **Make readiness a pure function of live state, never a latch.** The
   2026-06 draft (#276) latched readiness once per process, so it could not
   return to false when correctness was lost. Computing readiness from current
   inputs on every request makes non-monotonicity structural rather than a
   behavior someone must remember to implement.
2. **Gate data with default-deny middleware, not per-handler calls.** A WAI
   layer in front of the whole application 503s every path outside a two-entry
   allowlist. A route added later is gated by construction; nobody has to
   remember to add a guard. This is the difference between an invariant and a
   convention.
3. **Start the listener before recovery.** `Warp` runs concurrently with
   `withApplication`; the application context is published into a `TVar` when
   boot completes. This deletes the retained-vs-empty special case entirely:
   both paths become "listener up, not ready, then ready".

Nothing here introduces a tuned timeout, a start-period, or a stall detector.
A-003 rejected safety that rests on an unmeasured, growing quantity, and a
liveness stall detector would reintroduce exactly that.

## Why `/live` has no failure mode

`/live` answers 200 whenever it answers at all. Progress is enforced by
termination, not by a probe threshold: if boot or any linked worker thread
fails, the exception propagates out of the server action, `withAsync` closes
the listener, and the process exits non-zero. The supervisor then observes a
dead container rather than a 503.

This is what makes INV-R7 load-bearing. Without it the design has a zombie
state — listener up, `/live` 200 forever, `/ready` 503 forever, supervisor
never acting — which is a worse outage than the one being fixed, because it is
silent. INV-R7 is therefore not a nicety; it is the other half of FR-1.

## Readiness inputs

READY is computed from, and only from:

- follower mode (`followerEnabled`);
- the genesis stability window, in slots;
- boot stage (booting, with a reason) or indexer phase (restoring/following);
- the internal proof-read consistency flag;
- the latest persisted checkpoint slot;
- the most recently observed chain tip slot.

Currency is `checkpoint + stabilityWindow >= tip`. This is the same threshold
the follower already uses for its restoration→following transition, so the
change introduces no new tunable. With the follower disabled there is no tip
and currency is vacuous.

## Live boundary

Diagnostic question — *what system boundary does this exercise that the unit
suite cannot?*

Three, and each is exactly where the previous attempt failed:

| Boundary | Why units cannot see it | Proof |
|---|---|---|
| TCP accept during recovery | in-process WAI needs no socket and no listener; #276's spec passed while the real path had no listener at all | connect and probe from an HTTP client against a bound port |
| boot ordering | ordering is a property of the real boot sequence, not of any handler | hold replay open via the application tracer, probe during the hold |
| retained-database reopen | a fresh temp DB never replays a journal | reopen a database populated by a real devnet run |

Therefore `./gate.sh` carries a live-boundary smoke: a real `cardano-node`
devnet subprocess (`withCardanoNode`, Constitution VI — no Docker), a real
Warp socket, and a real HTTP client. No operator follow-up is deferred.

**The barrier is an existing production seam, not a test hook.** The
application tracer already receives `TraceReplay ReplayStart` from the
replaying thread. A test tracer that blocks there holds replay open for as
long as the test wants — deterministically, with no sleep-and-hope and no
production code that exists only for tests. The server's own progress tracer
runs *before* the configured tracer in the tee, so `/ready` still reports
`replaying` while the configured tracer is blocked.

## Falsifiability

Every assertion ships with a control proving it can fail (AC-6). Specifically:

- **positive control on the probe itself** — the probe helper must report
  failure when pointed at a closed port, so "connect succeeded" is not
  vacuously true;
- **negative control per boundary** — an injected variant that violates the
  invariant must make exactly the corresponding assertion red: readiness
  forced true early (INV-R4 fails), listener started after replay (INV-R1
  fails), readiness latched (INV-R5 fails);
- **quantities in the passing output** — the held window prints the duration
  actually held against the target, so a pass that held 0 s is distinguishable
  from a pass that held 12 s.

The pure readiness function makes most controls cheap: mutate one input, assert
the verdict flips.

## Slices

Both slices are bisect-safe: S1 alone removes the "200 with a null or stale
root" lie; S2 alone would be meaningless without S1's signals.

### S1 — signals surface and default-deny data gating

Adds `/live` and `/ready`, the pure readiness function, the readiness
middleware, the Context fields readiness needs, the regenerated Swagger, the
deployment-helper change, and the documentation of signal roles. Boot order is
untouched.

Provable today over real TCP on the empty-database path, because that path
already has a listener.

Invariants: INV-R3, INV-R4, INV-R5, INV-R8, INV-R9 (for those).

### S2 — listener before recovery

Extracts the boot sequence out of `exe/Serve.hs` into a library module that
starts Warp first and publishes the context when boot completes; `main` keeps
only argument handling. Adds the real-TCP recovery proof across both paths.

Invariants: INV-R1, INV-R2, INV-R6, INV-R7, INV-R10, INV-R9 (for those).

## Invariant mandate

| ID | Must hold | Observable failure |
|---|---|---|
| INV-R1 | the listener accepts TCP and answers `/live` before any retained-database recovery or journal replay begins | during a held replay on a retained DB, a TCP connect to the port is refused or times out |
| INV-R2 | `/live` is 200 on every probe from listener start through the whole recovery window, including a window longer than the historical 10 s healthcheck timeout | any probe in the window is non-200 or fails to connect |
| INV-R3 | `/ready` is 503 unless phase is following **and** the checkpoint is within the stability window of the observed tip **and** proofs are consistent; 200 otherwise | `/ready` 200 while restoring, while the tip is unobserved, or while the checkpoint is outside the window |
| INV-R4 | every route outside the `{/live, /ready}` allowlist returns 503 while `/ready` is 503 | any gated route returns 2xx during a not-ready window, or returns 200 with a null or stale root |
| INV-R5 | readiness returns to 503 when correctness or currency is lost after having been true | readiness stays 200 across a forced restoration/armageddon reset |
| INV-R6 | INV-R1..R4 hold on the retained-reopen path **and** the empty/lost-database path | either path is unproven, or proven only in-process |
| INV-R7 | a boot or linked-thread failure terminates the process non-zero and closes the listener | after an injected boot failure the listener still answers and the process stays alive |
| INV-R8 | no repository-owned artifact treats `/status` or `/ready` as a supervisor signal; the deployment helper waits on `/ready` | `scripts/deploy-preprod.sh` polls `/status`, or any repository artifact configures an HTTP healthcheck |
| INV-R9 | every assertion behind INV-R1..R7 is demonstrated able to fail by a control the gate executes | a control is described in prose but never run, or does not go red when its invariant is violated |
| INV-R10 | the boot function exercised by the recovery proof is the exact function `mpfs-serve` runs | `main` contains boot sequencing the proof does not exercise |

## Constitution check

| Principle | Verdict |
|---|---|
| I Ledger-native types | pass — no new domain types; slots stay `SlotNo` |
| II Records of functions | pass — new readiness inputs are fields on the `Context` record of functions |
| III Atomic block processing | pass — untouched |
| IV Client-side tx construction | pass — the new routes return operational signals, never transactions |
| V Aiken compatibility | pass — untouched |
| VI Test locally first | pass — devnet subprocess plus in-process Warp; no Docker, no external service |
| VII Nix reproducibility | pass — all commands run through `nix develop` / `just` |
| VIII Pure offline verification | pass — no verifier changes; the readiness decision is itself a pure function |
| IX One verifier, many targets | pass — `/live` and `/ready` are declared in the offchain-only `Cardano.MPFS.HTTP.API`, not in the shared `cardano-mpfs-api`, so the WASM/JS client surface is unchanged |

Re-check after design: unchanged. No waiver required.

## Risks

- **Existing specs assert `/status` 200 before readiness.** Updating them is in
  scope for S1 and is a behavior change, not a test weakening; each such edit
  must be justified against FR-4 in the commit body.
- **`followerEnabled = False` fixtures.** Currency must be vacuous in that
  mode or a large part of the e2e suite hangs waiting for a tip that never
  arrives. This is an explicit case in the pure function and needs its own
  test.
- **Devnet replay is fast.** Without the tracer barrier the not-ready window
  would be too short to observe, and the test would pass vacuously. The
  barrier is what makes AC-1 non-vacuous, so a control must prove the barrier
  actually held.
