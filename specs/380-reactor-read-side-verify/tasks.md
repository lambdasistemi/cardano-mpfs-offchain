# Tasks: Reactor Read-Side Verify Ops

## Slice S1 - Reactor Read-Side Ops

- [X] T380-S1-RED Add failing reactor tests for `verify_tokens`,
  `verify_snapshot`, `verify_fact_inclusion`, and `verify_facts` using
  real non-empty UMPFS response fixtures.
- [X] T380-S1-RED Add tampered fixture assertions that decode
  successfully and return `verify_error`.
- [X] T380-S1-GREEN Add dispatch arms in `Reactor.hs` that decode raw
  response payloads and wrap existing read-side verifier functions.
- [X] T380-S1-GREEN Add only necessary read-side exports/wrappers for
  per-fact inclusion, with no new proof algorithm.
- [X] T380-S1-PROOF Run `nix develop --quiet -c just unit-client
  "runEnvelope"`, `nix build .#wasm-mpfs-verify --fallback`, and
  `./gate.sh`.
- [X] T380-S1-COMMIT Commit as
  `feat(verify): expose read-side reactor ops` with
  `Tasks: T380-S1`.

## Slice S2 - Finalize

- [X] T380-S2-PR Update the PR body with delivered ops, fixture
  provenance, and verification evidence.
- [X] T380-S2-GATE Confirm `./gate.sh` passes at HEAD.
- [X] T380-S2-COMMIT Drop `gate.sh` in
  `chore: drop gate.sh (ready for review)` when the PR is ready.
