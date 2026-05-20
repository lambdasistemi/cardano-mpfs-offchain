# Tasks: Retract Fact Provider Pivot

- [x] T001 Add `RetractFacts` wire type, JSON instances, and Swagger
      schema. Wire `FactsRetractAPI` into the shared `Cardano.MPFS.API`
      and the offchain `Cardano.MPFS.HTTP.API`.
- [x] T002 Add `readNamedRequestUtxo :: TxIn -> IndexerTx (Maybe
      ResolvedWalletInput)` in `Cardano.MPFS.Indexer.Reads` if it is
      not already present.
- [x] T003 Add the server `factsRetractHandler` and the
      `mkRetractFacts` assembly helper. The handler runs one atomic
      indexer read for snapshot, named request UTxO, state UTxO, and
      requester wallet UTxOs, then queries protocol parameters and
      derives Phase 2 validity slot bounds.
- [x] T004 Add client `VerifiedRetractFacts`, `verifyRetractFacts`,
      and verifier tests for happy path, snapshot tamper, trusted-root
      mismatch, request UTxO proof tamper, state UTxO proof tamper,
      and wallet UTxO proof tamper.
- [x] T005 Add `retractCageTx` under `cardano-mpfs-client` cage
      helpers with byte-equality proof against
      `legacy-retract.cbor`.
- [x] T006 Remove legacy `POST /tx/retract` from shared API, offchain
      server, client wrapper, active docs, and tests while preserving
      request-update and other not-yet-migrated write routes.
- [x] T007 Regenerate Swagger and prove `/facts/retract` exists while
      `/tx/retract` is absent. Extend `./gate.sh` to assert the
      retract presence/absence pair on top of the existing matrix.
- [x] T008 Extend the #278 local-cluster/devenv facts API coverage
      matrix with a retract row driving `POST /facts/retract ->
      verifyRetractFacts -> retractCageTx -> submit -> indexed
      request consumption` against the local cluster.
- [x] T009 Record MOOG boundary status for retract in spec.md (and on
      issue #267 / parent epic #257 via the PR body). Status: deferred
      to cardano-foundation/moog#96 staged-port or replacement
      decision; no retract-specific MOOG canary exists.
- [x] T010 Run `./gate.sh`, review the branch, then leave `gate.sh`
      in place for parent orchestrator finalization.
- [ ] T011 Fix the final-head `FactsMatrixSpec` boot row collateral
      failure seen on GitHub e2e run 26182733086 / seed 104356220:
      `bootCageTx` must not select an undersized wallet UTxO for
      collateral when the facts wallet set has multiple entries.
- [ ] T012 Rerun the focused facts matrix selector for seed
      104356220, run `./gate.sh`, pass remote PR checks on the fixed
      head, then repeat finalization audit before dropping `gate.sh`.
