---
description: "Task list for feature 231-typed-read-side-verifiers"
---

# Tasks: Typed read-side endpoints + verifiers

**Input**: Design documents from `specs/231-typed-read-side-verifiers/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md)
**Issue**: [#231](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/231)

## Phase 1: Setup and Speckit

- [X] T001 Create worktree `cardano-mpfs-offchain-issue-231` and branch `feat/231-typed-read-side-verifiers`.
- [X] T002 Author speckit spec, plan, and tasks.

## Phase 2: Read DTOs

- [X] T003 Add `cardano-mpfs-client/lib/Cardano/MPFS/Client/Read.hs` with `TokenState`, `WitnessedTokenState`, `Request`, `WitnessedRequest`, `FactWitness`, `TokenResponse`, `FactResponse`, `ProofResponse`, `RequestsResponse` and matching `FromJSON`/`ToJSON` instances.
- [X] T004 List `Cardano.MPFS.Client.Read` in `cardano-mpfs-client.cabal:exposed-modules`.

## Phase 3: Read Verifiers

- [X] T005 Extend `Cardano.MPFS.Client.Verify` with `verifyTokenResponse`, `verifyFactResponse`, `verifyProofResponse`, `verifyRequestsResponse` reusing the existing structural and replay helpers.
- [X] T006 Re-export the four new types and four new verifiers from `Cardano.MPFS.Client`.

## Phase 4: Fixtures + Tests

- [X] T007 Extend `Cardano.MPFS.Client.Fixtures` with honest read responses (`honestTokenResponse`, `honestFactResponse`, `honestProofResponse`, `honestRequestsResponse`) plus the queried key constants needed by fact / proof verifiers.
- [X] T008 Add `Cardano.MPFS.Client.ReadSpec` covering positive paths and forgery-DSL coverage for each read verifier; register it in the test suite (`Main.hs`, `cabal:other-modules`).

## Phase 5: Validation and Merge

- [X] T009 Run `nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests` (87 examples, 0 failures).
- [X] T010 Run `nix develop --quiet -c cabal test cardano-mpfs-offchain:unit-tests` (371 examples, 0 failures).
- [X] T011 Run `nix develop --quiet -c just format-check`.
- [X] T012 Run `nix develop --quiet -c just hlint`.
- [ ] T013 Push branch and open PR linked to #231; assign and label.
- [ ] T014 Update PR body after every push; wait for green CI; merge through merge-guard.
