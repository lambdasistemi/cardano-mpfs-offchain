# Feature Specification: E2E /tx/reject proof verification

**Feature Branch**: `feat/e2e-cover-txreject-in-proofsspec-verifiable-snapsh`
**Created**: 2026-04-25
**Status**: Draft
**Input**: Issue [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224) - "e2e: cover /tx/reject in ProofsSpec verifiable-snapshot scenario"

## User Scenarios & Testing

### User Story 1 - Verify reject tx response in the E2E proof scenario (Priority: P1)

A downstream client reads the proof-oriented E2E scenario as the example
for how to consume every proof-bearing write endpoint. The scenario must
include `POST /tx/reject`, decode its response as a `RejectTxResponse`,
and run the exported offline verifier before any signing step.

**Why this priority**: `/tx/reject` is already part of the proof-bearing
write API, but it is the only write endpoint missing from the HTTP E2E
proof scenario. Without it, regressions in the server response shape or
the client verifier wiring can pass while one endpoint is unexercised.

**Independent Test**: run `ProofsSpec` against the devnet harness with
`MPFS_BLUEPRINT` set. The scenario creates a pending request, waits until
that request is rejectable, posts `/tx/reject`, and asserts
`verifyRejectTxResponse` accepts the response.

**Acceptance Scenarios**:

1. **Given** a booted token with a pending request whose
   `submitted_at + process_time + retract_time` deadline has elapsed,
   **When** the scenario posts `/tx/reject`, **Then** the HTTP response
   decodes as `RejectTxResponse`.
2. **Given** that decoded `RejectTxResponse`, **When**
   `verifyRejectTxResponse` runs through `shouldAccept`, **Then** it
   returns `Right ()`.
3. **Given** that same response, **When** the scenario tampers one reject
   proof field through the exported DSL, **Then** `shouldRejectWith`
   reports `CsmtReplayFailed` at the expected dotted field path.

### Edge Cases

- The reject request is time-gated. The scenario must wait only as long
  as the devnet `CageConfig` requires, not a hard-coded production
  timeout.
- The scenario builds the unsigned reject transaction only; it must not
  submit it, so later assertions in the same scenario are not affected by
  consuming the pending request.
- If the response cannot be built because the request is not rejectable,
  the failure must stay visible as an E2E failure rather than being
  silently skipped.

## Requirements

### Functional Requirements

- **FR-001**: `ProofsSpec` MUST post `/tx/reject` after creating an
  actually rejectable pending request.
- **FR-002**: The response MUST decode as the client package's
  `RejectTxResponse`.
- **FR-003**: The scenario MUST assert the honest response is accepted by
  `verifyRejectTxResponse` through the same `shouldAccept` DSL used for
  other write endpoints.
- **FR-004**: The scenario MUST assert a tampered reject response is
  rejected with `CsmtReplayFailed` at an explicit reject proof path.
- **FR-005**: The scenario MUST call out any runtime increase caused by
  waiting for the reject deadline.
- **FR-006**: The implementation MUST keep the verifier path pure and
  must not change the wire response contract.

### Key Entities

- **Rejectable request**: A pending request UTxO whose request deadline
  has elapsed according to the token state's process and retract windows.
- **RejectTxResponse**: The proof-bearing unsigned transaction response
  returned by `POST /tx/reject`.
- **Verifier DSL assertion**: The exported `shouldAccept` /
  `shouldRejectWith` checks used by `ProofsSpec` as executable client
  documentation.

## Success Criteria

- **SC-001**: `ProofsSpec` covers `BootTxResponse`, `RequestTxResponse`,
  `UpdateTxResponse`, `RetractTxResponse`, `RejectTxResponse`, and
  `EndTxResponse` in one E2E scenario.
- **SC-002**: The focused E2E command for `ProofsSpec` passes with a real
  blueprint.
- **SC-003**: The PR description records the extra wait time added for
  `/tx/reject`.

## Assumptions

- The devnet `CageConfig` in `ProofsSpec` remains intentionally small
  (`defaultProcessTime = 5_000`, `defaultRetractTime = 5_000`) so the
  reject deadline can be reached with a short wait.
- The request created for the `/tokens/:id/requests` read-side check can
  also serve as the pending request used by `/tx/reject`.
- `verifyRejectTxResponse`, `runForgeReject`, and the CSMT matcher DSL
  already exist in `cardano-mpfs-client`.
