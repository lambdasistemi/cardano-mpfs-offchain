# Tasks — 275 recovery, liveness, readiness

Slice IDs are the unit of dispatch, audit, and commit. A slice is complete
only when every task under it is checked and its invariants are proven by the
frozen gate.

## S0 — ticket setup (ticket owner)

- [x] T001 Ignore `/gate.sh` in `.gitignore` (repository carries no ignore
      entry today; the previous ticket added and dropped a tracked gate).
- [x] T002 Commit the Spec Kit contract for this ticket.

## S1 — signals surface and default-deny data gating

Invariants: INV-R3, INV-R4, INV-R5, INV-R8, INV-R11 (in-context part),
INV-R12, INV-R13, and INV-R9 for those.

- [ ] T101 RED: unit spec for `evalReadiness` covering every reason of D-5,
      the reason precedence of D-9, and the `FollowerDisabled` case; each case
      accompanied by the control that proves the input is load-bearing.
- [ ] T102 RED: real-TCP spec on the empty-database path asserting every
      gated route is 503 while `/ready` is 503, including `/status`,
      `/metrics`, and `/metrics/prometheus`, and that `/live` is 200
      throughout.
- [ ] T103 RED: spec asserting readiness returns to 503 after correctness is
      lost, and that no verdict is cached across requests.
- [ ] T104 RED: spec asserting `/ready` reports `behind` — and gated routes
      503 — when the phase is following and the root correct but the
      checkpoint is outside the stability window of the observed tip.
- [ ] T105 Implement `Cardano.MPFS.HTTP.Readiness` (M-1).
- [ ] T106 Implement `Cardano.MPFS.HTTP.Gate` (M-2), default-deny with the
      three-entry allowlist of `/live`, `/ready` and `/version`.
- [ ] T107 Add the `/live` and `/ready` route types (M-5) and response shapes
      (M-6).
- [ ] T108 Expose the readiness observations on `Context` (M-7) and publish
      the indexer phase from the application (M-8).
- [ ] T109 Wrap `mkApp` with the gate (M-4), leaving the existing internal
      proof-read guard in place.
- [ ] T110 Reconcile existing specs that assert a gated route is 200 before
      readiness; justify each edit against FR-4 in the commit body.
- [ ] T111 Regenerate `docs/assets/swagger.json` and pass the repository
      `swagger-up-to-date` check.
- [ ] T112 Point `scripts/deploy-preprod.sh` at `/ready` (INV-R8).
- [ ] T113 Document the signal roles: `/live` is the only supervisor signal,
      `/ready` is the dependency gate, and this repository configures no HTTP
      healthcheck.

### Added by the C-IDENTITY amendment (M2 NOTE-001, NOTE-002)

Invariants INV-R11 (in-context part), INV-R12, INV-R13; acceptance AC-9
(in-context part), AC-10, AC-11.

- [ ] T115 RED: unit spec for the M-14 predicates covering every sentinel
      value and every malformed digest, each with the control that proves the
      predicate can reject and can accept (AC-11, INV-R13).
- [ ] T116 RED: real-TCP spec on the empty-database path asserting `GET
      /version` returns 200 with its mandatory fields in the same window in
      which `/ready` and every gated route return 503 (AC-9, in-context part).
- [ ] T117 RED: spec asserting that mutating `MPFS_IMAGE_DIGEST` after startup
      changes no `/version` response, accompanied by the control that proves
      the variable is load-bearing when read at startup (AC-10, INV-R12).
- [ ] T118 Implement `Cardano.MPFS.BuildInfo` (M-14): the `BuildInfo` value,
      `loadBuildInfo` in `IO`, and the two pure predicates. No per-request
      environment read and no `unsafePerformIO`.
- [ ] T119 Add the `GET /version` route type and its response shape to the
      shared `Cardano.MPFS.API` (M-15), and register M-14 in the offchain
      cabal file.
- [ ] T120 Capture build identity once during startup, before the
      listener/replay sequence, and hold it in the gate environment (M-2
      addendum, INV-R12).
- [ ] T121 Answer `/version` from the gate itself, never by delegating inward
      to an application built from the context (M-2 addendum, INV-R11).

### Slice close

- [ ] T114 Run the frozen S1 gate green, with every S1 control demonstrated
      red on injection.

## S2 — listener before recovery

Invariants: INV-R1, INV-R2, INV-R6, INV-R7, INV-R10, INV-R11 (pre-context
part), and INV-R9 for those.

- [ ] T201 RED: real-TCP recovery spec on the retained-reopen path — populate
      a database against a real devnet, reopen it with replay held open past
      the historical 10 s window, and assert `/live` 200 on every probe while
      `/ready` and every gated route are 503; then release and assert the
      flip. The passing output must report the duration actually held.
- [ ] T202 RED: the same assertions across the empty/lost-database path
      (INV-R6).
- [ ] T203 RED: control proving the probe helper reports failure against a
      closed port, so "connected" is never vacuous.
- [ ] T204 RED: control proving the held-replay barrier actually held, so
      AC-1 cannot pass on a window of zero length.
- [ ] T205 RED: spec asserting a boot failure terminates and closes the
      listener (INV-R7).
- [ ] T206 Implement `Cardano.MPFS.Server.Boot` (M-3): bind first, serve the
      gate, run the application concurrently, tee the tracer into boot
      progress, publish the context, propagate failure.
- [ ] T207 Reduce `exe/Serve.hs` to argument handling and a `runServer` call
      (M-9, INV-R10).
- [ ] T208 Wire `BootStage` into the readiness decision so the pre-context
      window reports `opening`/`recovering`/`replaying`.
- [ ] T209 Register the recovery spec in the e2e entry point so it is
      executed, not merely compiled.
- [ ] T211 RED: assert `GET /version` returns 200 with its mandatory fields
      during the held-replay window of T201, before the application context
      exists (AC-9, pre-context part, INV-R11).
- [ ] T210 Run the frozen S2 gate green, with every S2 control demonstrated
      red on injection.

## S3 — finalization (ticket owner)

- [ ] T301 Full ticket gate green through a quiet receipt recorder.
- [ ] T302 Finalization audit over the PR range and this task list.
- [ ] T303 PR body records the 2026-05-19 and 2026-08-24 production evidence
      and maps it to the shipped contract, per the issue's acceptance
      criteria.
