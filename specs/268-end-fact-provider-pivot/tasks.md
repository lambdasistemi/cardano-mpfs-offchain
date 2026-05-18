# Tasks: End fact-provider pivot

**Input**: Design documents from `/specs/268-end-fact-provider-pivot/`

## Phase 1: Specification and Baseline

- [x] T001 Record corrected MOOG boundary language in issue #268 and sibling tickets.
- [x] T002 Create `268-end-fact-provider-pivot` worktree from `origin/main`.
- [x] T003 Run baseline `./gate.sh` before edits.
- [x] T004 Add issue #268 spec, plan, contracts, quickstart, checklist, and tasks artifacts.

## Phase 2: End facts verifier

- [ ] T005 RED: add `EndFacts` JSON and `verifyEndFacts` tests in `cardano-mpfs-client/test/Cardano/MPFS/Client/EndFactsSpec.hs`.
- [ ] T006 GREEN: add `EndFacts` wire type in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`.
- [ ] T007 GREEN: implement `VerifiedEndFacts`, `verifyEndFacts`, and request-set completeness replay in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`, `Cardano/MPFS/Client/Verify.hs`, and `Cardano/MPFS/Client/Verify/Completeness.hs`.
- [ ] T008 Wire new modules/tests into `cardano-mpfs-client/cardano-mpfs-client.cabal`.

## Phase 3: End cage builder

- [ ] T009 RED: add `endCageTx` focused tests in `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/EndSpec.hs`.
- [ ] T010 GREEN: add request-address helpers to `Cardano.MPFS.Client.Cage.Config`.
- [ ] T011 GREEN: implement `Cardano.MPFS.Client.Cage.End.endCageTx`.
- [ ] T012 Wire the end cage module/test into `cardano-mpfs-client/cardano-mpfs-client.cabal`.

## Phase 4: Server hard swap

- [ ] T013 RED: add `POST /facts/end` HTTP tests in `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/EndFactsSpec.hs`.
- [ ] T014 GREEN: add end state and request-set `IndexerTx` reads in `Cardano.MPFS.Indexer.Reads`.
- [ ] T015 GREEN: add `factsEndHandler` and `mkEndFacts` in `Cardano.MPFS.HTTP.Server` / `Types`.
- [ ] T016 Remove `TxEndAPI`, `txEndHandler`, and the legacy end transaction route from API/server wiring.
- [ ] T017 Regenerate `docs/assets/swagger.json`.

## Phase 5: Gate and PR

- [ ] T018 Extend `gate.sh` with stable end hard-swap and verifier source checks.
- [ ] T019 Run focused tests and `./gate.sh`.
- [ ] T020 Open/update draft PR for issue #268 with MOOG boundary status and verification evidence.
