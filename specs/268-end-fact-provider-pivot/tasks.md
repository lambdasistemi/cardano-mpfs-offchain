# Tasks: End fact-provider pivot

**Input**: Design documents from `/specs/268-end-fact-provider-pivot/`

## Phase 1: Specification and Baseline

- [x] T001 Record corrected MOOG boundary language in issue #268 and sibling tickets.
- [x] T002 Create `268-end-fact-provider-pivot` worktree from `origin/main`.
- [x] T003 Run baseline `./gate.sh` before edits.
- [x] T004 Add issue #268 spec, plan, contracts, quickstart, checklist, and tasks artifacts.

## Phase 2: API type split

- [X] T005 (commit: 7c1091b) RED: add a compile/import smoke proving new facts DTOs can be imported from `Cardano.MPFS.API.Types.Facts` without adding constructors to `Cardano.MPFS.API.Types`.
- [X] T006 (commit: 7c1091b) GREEN: split common wire primitives into `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Common.hs` and per-operation facts into `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs`; keep `Cardano.MPFS.API.Types` as a temporary compatibility re-export.
- [X] T007 (commit: 7c1091b) Wire the new API modules into `cardano-mpfs-api/cardano-mpfs-api.cabal` and update imports in server/client code opportunistically, without changing behavior.

## Phase 3: End facts verifier

- [X] T008 (commit: 1d9414b) RED: add `EndFacts` JSON and `verifyEndFacts` tests in `cardano-mpfs-client/test/Cardano/MPFS/Client/EndFactsSpec.hs`.
- [X] T009 (commit: 1d9414b) GREEN: add `EndFacts` wire type in `Cardano.MPFS.API.Types.Facts`.
- [X] T010 (commit: 1d9414b) GREEN: implement `VerifiedEndFacts`, `verifyEndFacts`, request-set completeness replay, and the narrowly-scoped request-address helper needed by the verifier in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`, `Cardano/MPFS/Client/Verify.hs`, `Cardano/MPFS/Client/Verify/Completeness.hs`, and `Cardano/MPFS/Client/Cage/Identity.hs`.
- [X] T011 (commit: 1d9414b) Wire new modules/tests into `cardano-mpfs-client/cardano-mpfs-client.cabal`.

## Phase 4: End cage builder

- [X] T012 (commit: a3ac47c) RED: add `endCageTx` focused tests in `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/EndSpec.hs`.
- [X] T013 (commit: a3ac47c) GREEN: reuse the dedicated client cage identity helper when implementing `Cardano.MPFS.Client.Cage.End.endCageTx`; do not grow `Cardano.MPFS.Client.Cage.Config`.
- [X] T014 (commit: a3ac47c) GREEN: implement `Cardano.MPFS.Client.Cage.End.endCageTx`.
- [X] T015 (commit: a3ac47c) Wire the end cage module/test into `cardano-mpfs-client/cardano-mpfs-client.cabal`.

## Phase 5: Server hard swap

- [X] T016 RED: add `POST /facts/end` HTTP tests in `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/EndFactsSpec.hs`.
- [X] T017 GREEN: add end state and request-set `IndexerTx` reads in `Cardano.MPFS.Indexer.Reads`.
- [X] T018 GREEN: add `factsEndHandler` and `mkEndFacts` in `Cardano.MPFS.HTTP.Server` plus a focused facts conversion module; do not grow `Cardano.MPFS.HTTP.Types`.
- [ ] T019 Remove `TxEndAPI`, `txEndHandler`, and the legacy end transaction route from API/server wiring.
- [ ] T020 Regenerate `docs/assets/swagger.json`.

## Phase 6: Gate and PR

- [ ] T021 Extend `gate.sh` with stable end hard-swap and verifier source checks.
- [ ] T022 Run focused tests and `./gate.sh`.
- [ ] T023 Open/update draft PR for issue #268 with MOOG boundary status and verification evidence.
