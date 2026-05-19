# Feature Specification: Request-Delete Fact Provider Pivot

## User Story

As an MPFS wallet/client, I request delete facts from the server, verify
the facts against my trusted UTxO root, build the request-delete
transaction locally, sign it, submit it, and later observe the pending
request on-chain.

## Scope

- Add `POST /facts/request/delete`.
- Return facts only: snapshot, wallet UTxO witnesses, token, key,
  value, address, and unverified protocol parameters.
- Add client-side `verifyRequestDeleteFacts`.
- Add client-side `requestDeleteCageTx`.
- Remove the legacy `POST /tx/request/delete` route and client wrapper.
- Regenerate Swagger with the new path and without the legacy path.
- Record MOOG boundary status through cardano-foundation/moog#96 or a
  canary/staged-port proof.

## Acceptance Criteria

- Byte equality against `legacy-request-delete.cbor` passes for
  `requestDeleteCageTx`.
- Verifier tests cover happy path, snapshot tamper,
  trusted-root mismatch, and wallet UTxO proof tamper.
- `POST /facts/request/delete` is the only request-delete API path.
- Request-delete facts verifier code imports no transaction grammar.
- Swagger reflects only the new request-delete facts shape.
- MOOG boundary status is linked in the child issue and parent epic.

## Non-Goals

- Do not migrate request-update, retract, reject, update, or end in this
  slice.
- Do not alter the already-landed request-insert facts flow except for
  shared helper reuse that is required for request-delete.
- Do not claim production MOOG readiness from legacy caller behavior.
