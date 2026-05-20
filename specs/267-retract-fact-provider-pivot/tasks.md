# Tasks: Retract Fact Provider Pivot

- [ ] T001 Add `RetractFacts` wire type, JSON instances, and Swagger
      schema. Wire `FactsRetractAPI` into the shared `Cardano.MPFS.API`
      and the offchain `Cardano.MPFS.HTTP.API`.
- [ ] T002 Add `readNamedRequestUtxo :: TxIn -> IndexerTx (Maybe
      ResolvedWalletInput)` in `Cardano.MPFS.Indexer.Reads` if it is
      not already present.
- [ ] T003 Add the server `factsRetractHandler` and the
      `mkRetractFacts` assembly helper. The handler runs one atomic
      indexer read for snapshot, named request UTxO, state UTxO, and
      requester wallet UTxOs, then queries protocol parameters and
      derives Phase 2 validity slot bounds.
- [ ] T004 Add client `VerifiedRetractFacts`, `verifyRetractFacts`,
      and verifier tests for happy path, snapshot tamper, trusted-root
      mismatch, request UTxO proof tamper, state UTxO proof tamper,
      and wallet UTxO proof tamper.
- [ ] T005 Add `retractCageTx` under `cardano-mpfs-client` cage
      helpers with byte-equality proof against
      `legacy-retract.cbor`.
- [ ] T006 Remove legacy `POST /tx/retract` from shared API, offchain
      server, client wrapper, active docs, and tests while preserving
      request-update and other not-yet-migrated write routes.
- [ ] T007 Regenerate Swagger and prove `/facts/retract` exists while
      `/tx/retract` is absent. Extend `./gate.sh` to assert the
      retract presence/absence pair on top of the existing matrix.
- [ ] T008 Extend the #278 local-cluster/devenv facts API coverage
      matrix with a retract row driving `POST /facts/retract ->
      verifyRetractFacts -> retractCageTx -> submit -> indexed
      request consumption` against the local cluster.
- [ ] T009 Record MOOG boundary status for retract in issue #267 and
      parent epic #257.
- [ ] T010 Run `./gate.sh`, review the branch, then leave `gate.sh`
      in place for parent orchestrator finalization.
