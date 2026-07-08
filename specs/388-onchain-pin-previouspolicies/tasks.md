# Tasks: On-chain #76 Pin and State Policy Parameter

## Slice 0 - Orchestration Bootstrap

- [X] T000 Add PR-local `gate.sh`, run the baseline gate, push the branch, and
  open a draft PR.
- [X] T001 Commit this spec, plan, and task contract.

## Slice 1 - Pin Bump and Genesis State Parameter

- [ ] T002 RED: bump `cardano-mpfs-onchain` to resolved `e37e33e`, update
  `flake.lock`, add a focused failing test that proves raw state hash derivation
  no longer matches `applyPreviousPolicies []` genesis state identity, and
  record the failing command/output.
- [ ] T003 GREEN: expose/apply `applyPreviousPolicies []` for every genesis
  state config/hash derivation site in the owned surfaces.
- [ ] T004 Verify focused cage/verifier suites and `./gate.sh`, then commit one
  bisect-safe implementation commit.

## Slice 2 - Final PR Readiness

- [ ] T005 Ticket owner reviews the implementation commit, amends completed
  task checkboxes into that same commit, pushes, and records gate evidence.
- [ ] T006 Drop `gate.sh`, run finalization audit, mark the PR ready, and report
  completion to the epic owner.
