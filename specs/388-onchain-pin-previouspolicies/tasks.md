# Tasks: On-chain #76 Pin and State Policy Parameter

## Slice 0 - Orchestration Bootstrap

- [X] T000 Add PR-local `gate.sh`, run the baseline gate, push the branch, and
  open a draft PR.
- [X] T001 Commit this spec, plan, and task contract.

## Slice 1 - Pin Bump and Genesis State Parameter

- [X] T002 RED: bump `cardano-mpfs-onchain` to resolved `e37e33e`, update
  `flake.lock`, add a focused failing test that proves raw state hash derivation
  no longer matches `applyPreviousPolicies []` genesis state identity, and
  record the failing command/output.
- [X] T003 GREEN: expose/apply `applyPreviousPolicies []` for every genesis
  state config/hash derivation site in the owned surfaces.
- [X] T004 Verify focused cage/verifier suites and `./gate.sh`, then commit one
  bisect-safe implementation commit.

## Slice 2 - CLI and E2E Boot PreviousPolicies Adoption

- [X] T005 RED: add a focused CLI/e2e failing proof that a raw state-byte
  `CageConfig` no longer matches the genesis `applyPreviousPolicies []` script
  identity after the bump, and record the failing command/output.
- [X] T006 GREEN: centralize or apply `applyPreviousPolicies []` in
  `cardano-mpfs-cli/**` and `cardano-mpfs-offchain/e2e-test/**` so all six
  live boot/e2e construction sites use applied state bytes for
  `cageScriptBytes` and `cfgScriptHash`.
- [X] T007 Verify the focused proof, run an e2e boot proof with
  `MPFS_BLUEPRINT` set to the bumped blueprint, run `./gate.sh`, then commit
  one bisect-safe implementation commit.

## Slice 3 - Final PR Readiness

- [ ] T008 Ticket owner reviews the expanded implementation commits, amends
  completed task checkboxes into the second slice commit, pushes, and records
  gate/e2e evidence.
- [ ] T009 Drop `gate.sh`, run finalization audit, mark the PR ready, and report
  completion to the epic owner.
