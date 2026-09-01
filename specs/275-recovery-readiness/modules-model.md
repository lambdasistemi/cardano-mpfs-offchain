# Modules model — 275 recovery, liveness, readiness

Responsibility, dependency direction, and placement only. No bodies, imports,
or algorithms. Signatures live in `functions-model.md`; shapes in
`data-model.md`.

## New

### M-1 `Cardano.MPFS.HTTP.Readiness`

Package `cardano-mpfs-offchain`, library.

Owns the readiness **decision** and nothing else: given the current
observations, is the server able to serve a correct and current root, and if
not, why. Deliberately pure and free of `Context`, HTTP, and `IO`, so the
decision can be exhaustively tested and mutated without a server.

Depends on: core slot/phase types only. Depended on by M-2 and M-3.

Placement rationale: this is the one piece every other piece asks. Keeping it
below the HTTP layer prevents the decision from being re-derived — differently
— in a handler, in the middleware, and in a test.

### M-2 `Cardano.MPFS.HTTP.Gate`

Package `cardano-mpfs-offchain`, library.

Owns the default-deny readiness gate at the WAI layer: the allowlist, the 503
response shape for gated routes, and the `/live` and `/ready` responses, which
it answers itself rather than delegating inward. Reads the published server
phase; asks M-1 for the verdict.

Depends on: M-1, `Cardano.MPFS.HTTP.Types`. Depended on by M-3 and M-4.

Placement rationale: the gate must sit in front of *every* route, including
routes that do not exist yet and routes that exist only while the application
context does not. A middleware is the only placement where "gated by default"
is a property of the architecture rather than a habit.

### M-3 `Cardano.MPFS.Server.Boot`

Package `cardano-mpfs-offchain`, library.

Owns the boot sequence: bind the listener, serve M-2's gate immediately, run
`withApplication` concurrently, tee the application tracer into boot progress,
publish the context when boot completes, and propagate failure so the process
terminates. Owns the server-phase cell.

Depends on: `Cardano.MPFS.Application`, M-2, `Cardano.MPFS.HTTP.Server`.
Depended on by `exe/Serve.hs` and by the recovery e2e spec.

Placement rationale (INV-R10): this exists as a library module precisely so
that the executable and the proof run the same code. Boot logic living in
`main` is unreachable from a test, which is how #276 shipped a green spec for
a path it never executed.

## Changed

### M-4 `Cardano.MPFS.HTTP.Server`

`mkApp` is wrapped by M-2's gate. Handlers keep their existing internal
proof-read guard: that guard is a strictly stronger, instantaneous check and
is not replaced by the readiness gate.

### M-5 `Cardano.MPFS.HTTP.API`

Gains the `/live` and `/ready` route types, beside the existing server-local
metrics routes.

Placement rationale (Constitution IX): these are server operational signals,
not part of the client contract, so they must not enter the shared
`cardano-mpfs-api` package that the WASM and JS verifier targets compile.

### M-6 `Cardano.MPFS.HTTP.Types`

Gains the response shapes for `/live` and `/ready`.

### M-7 `Cardano.MPFS.Context`

Gains the observations readiness needs that the context does not already
expose: the indexer phase, the stability window, and the follower mode. It
already exposes the proof-read flag, the checkpoint, and the metrics snapshot.

Promotion note: these are observations, not services. They are added as plain
fields on the existing record rather than as a new service record, because a
record of functions for three constants would be indirection without a seam.

### M-8 `Cardano.MPFS.Application`

Publishes the indexer phase alongside the existing proof-read flag, so the
phase transitions the follower already performs become observable. No change
to block processing or transaction boundaries.

### M-9 `exe/Serve.hs`

Reduced to argument parsing, validation, blueprint loading, configuration
assembly, and a call into M-3.

### M-10 `scripts/deploy-preprod.sh`

Waits on `/ready` instead of `/status`.

### M-11 `docs/`

States that `/live` is the only supervisor signal and `/ready` the dependency
gate, and records that no HTTP healthcheck is configured in this repository.

## Test modules

### M-12 `Cardano.MPFS.HTTP.ReadinessSpec` (unit)

Exhaustive and property-based coverage of M-1, including the follower-disabled
case and the negative controls that prove each input is load-bearing.

### M-13 `Cardano.MPFS.E2E.RecoverySignalsSpec` (e2e)

The live-boundary proof: real devnet, real Warp socket, real HTTP client, both
recovery paths, the held-replay window, and the controls of AC-6.

Replaces nothing. `CrashRecoverySpec` keeps its own state-survival scope; this
module owns the HTTP boundary during recovery, which no existing spec covers.
