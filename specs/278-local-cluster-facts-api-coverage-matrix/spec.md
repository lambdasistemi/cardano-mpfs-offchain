# Feature Specification: Local-Cluster Facts API Coverage Matrix

**Feature Branch**: `278-facts-api-coverage-matrix`
**Issue**: #278
**Parent**: #257
**Created**: 2026-05-20

## User Story

As an MPFS operator or client implementer, I run a named local-cluster/devenv facts API coverage matrix and observe every migrated facts endpoint prove the same live boundary: real HTTP facts response, local verification, local transaction build, submission, indexing, and absence of the replaced legacy transaction endpoint.

## Acceptance Scenarios

1. **Boot row**: Given a fresh local devnet and MPFS app, when the matrix posts `POST /facts/boot`, then it decodes `BootFacts`, verifies with `verifyBootFacts`, builds with `bootCageTx`, submits the signed transaction, and observes the token indexed.
2. **Request-insert row**: Given a booted token indexed from the boot row, when the matrix posts `POST /facts/request/insert`, then it decodes `RequestInsertFacts`, verifies with `verifyRequestInsertFacts`, builds with `requestInsertCageTx`, submits the signed transaction, and observes the request indexed.
3. **Request-delete row**: Given an inserted fact indexed after a process/update step, when the matrix posts `POST /facts/request/delete`, then it decodes `RequestDeleteFacts`, verifies with `verifyRequestDeleteFacts`, builds with `requestDeleteCageTx`, submits the signed transaction, and observes the delete request indexed and then the fact removal after processing.
4. **End row**: Given an empty indexed token state, when the matrix posts `POST /facts/end`, then it decodes `EndFacts`, verifies with `verifyEndFacts`, builds with `endCageTx`, submits the signed transaction, and observes the token removed from indexed state.
5. **Legacy route absence**: For every migrated row, the matrix fails if the replaced legacy transaction-building route remains reachable.

## Functional Requirements

- **FR-001**: The harness MUST run against the existing real local Cardano devnet/cluster path, not only unit fixtures or pure context values.
- **FR-002**: The harness MUST expose a named operator command or documented test selector for the matrix.
- **FR-003**: The matrix MUST cover `/facts/boot`, `/facts/request/insert`, `/facts/request/delete`, and `/facts/end`.
- **FR-004**: Each row MUST identify the HTTP route, decoded response type, verifier, local cage builder, submit/index proof, and legacy route absence check.
- **FR-005**: The implementation MUST reuse existing devnet/e2e harness pieces where practical.
- **FR-006**: The PR gate MUST run the matrix when feasible; if the matrix remains too expensive for the standard gate, the PR MUST record the exact command and a successful local transcript before leaving draft.

## Non-Goals

- Do not implement retract facts; #267 owns retract and must add its row later.
- Do not implement update, reject, or request-update facts; their tickets must add rows later.
- Do not replace this repository's devnet harness with CLB.
- Do not claim MOOG-v2 or preprod readiness.
- Do not reintroduce legacy transaction-building endpoints.

## Success Criteria

- The named matrix command fails red when a facts route is missing, a verifier/build function is bypassed, submission fails, indexing does not observe the expected state, or a migrated legacy route remains reachable.
- The parent epic can cite #278 as local live-boundary evidence for the currently migrated facts endpoints.
