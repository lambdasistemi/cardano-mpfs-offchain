# Feature Specification: Request-Insert Fact Provider Pivot

## User Story

As an MPFS wallet/client, I request insert facts from the server, verify
the facts against my trusted UTxO root, build the request-insert
transaction locally, sign it, submit it, and later observe the pending
request on-chain.

## Scope

- Add `POST /facts/request/insert`.
- Return facts only: snapshot, wallet UTxO witnesses, token, key, value,
  address, and unverified protocol parameters.
- Add client-side `verifyRequestInsertFacts`.
- Add client-side `requestInsertCageTx`.
- Remove the legacy `POST /tx/request/insert` route and client wrapper.
- Regenerate Swagger with the new path and without the legacy path.
- Record MOOG boundary status through cardano-foundation/moog#96 or a
  canary/staged-port proof.

## Acceptance Criteria

- Byte equality against `legacy-request-insert.cbor` passes for
  `requestInsertCageTx`.
- Verifier tests cover happy path, snapshot tamper, trusted-root mismatch,
  and wallet UTxO proof tamper.
- `POST /facts/request/insert` is the only request-insert API path.
- Request-insert facts verifier code imports no transaction grammar.
- Swagger reflects only the new request-insert facts shape.
- MOOG boundary status is linked in the child issue and parent epic.

## Non-Goals

- Do not migrate request-delete, request-update, retract, reject, update,
  or end in this slice.
- Do not claim production MOOG readiness from legacy caller behavior.
