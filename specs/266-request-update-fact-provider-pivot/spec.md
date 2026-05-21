# Feature Specification: Request-Update Fact Provider Pivot

## User Story

As an MPFS wallet or operator client, I submit a value-update request by
fetching request-update facts from `POST /facts/request/update`, verifying
those facts against my trusted UTxO root, building the request-update
transaction locally with `requestUpdateCageTx`, signing it, submitting it,
and observing the update request indexed on-chain.

## Deliverables

- `RequestUpdateFacts` wire type, JSON instances, and Swagger schema in
  `cardano-mpfs-api`.
- `POST /facts/request/update` in the shared Servant API and offchain
  server.
- `VerifiedRequestUpdateFacts`, `verifyRequestUpdateFacts`, and
  `requestUpdateCageTx` in `cardano-mpfs-client`.
- `specs/266-request-update-fact-provider-pivot/test-vectors/legacy-request-update.cbor`.
- Focused client verifier tests, cage byte-equality tests, offchain HTTP
  tests, and request-update local-cluster facts matrix coverage.
- Regenerated `docs/assets/swagger.json` showing the facts route and no
  legacy request-update transaction route.
- MOOG boundary-status record for request-update. This is boundary evidence
  only; it is not a production MOOG readiness claim.

## Scope

- Add `POST /facts/request/update` accepting the existing update-value request
  payload: token, key, old value, new value, and requester address.
- Return facts only: snapshot, token, key, old value, new value, requester
  address, server-selected submission timestamp, requester wallet UTxO
  witnesses, and unverified protocol parameters.
- Add client-side `verifyRequestUpdateFacts` returning an opaque
  `VerifiedRequestUpdateFacts` witness.
- Add client-side `requestUpdateCageTx` to
  `Cardano.MPFS.Client.Cage.Request`, reusing the shared request builder with
  `OpUpdate oldValue newValue`.
- Capture a legacy request-update CBOR vector before deleting the route and
  prove byte equality against the new cage helper.
- Remove the legacy `POST /tx/request/update` route and client wrapper in the
  same PR.
- Extend the #278 local-cluster/devenv facts API coverage matrix with a
  request-update row.
- Record MOOG boundary status through cardano-foundation/moog#96 or a
  request-update canary/staged-port proof.

## Acceptance Criteria

- Byte equality against `legacy-request-update.cbor` passes for
  `requestUpdateCageTx`.
- Request-update verifier tests cover happy path, snapshot tamper,
  trusted-root mismatch, and wallet proof tamper.
- `POST /facts/request/update` is the only request-update API path; the
  legacy transaction-building route is absent from API code, server wiring,
  typed client wrappers, Swagger, and live matrix legacy-route checks.
- Verifier surface for request-update has zero
  `Cardano.Ledger.Api.Tx` imports.
- `docs/assets/swagger.json` reflects only the new request-update facts
  shape.
- The #278 local-cluster matrix has a request-update row proving
  `POST /facts/request/update -> verifyRequestUpdateFacts ->
  requestUpdateCageTx -> submit -> request indexed`.
- MOOG boundary status is recorded for request-update as either a
  canary/staged-port proof or the cardano-foundation/moog#96 decision that the
  operation waits for the staged MOOG-v2 port or replacement surface.

## Non-Goals

- Do not migrate reject, update, or sweep in this slice.
- Do not retain a compatibility period for `/tx/request/update`.
- Do not claim production MOOG readiness from this offchain PR. Issue #275
  remains the live-boundary blocker for treating `mpfs-v2` as preprod-ready
  evidence.
- Do not introduce new indexer primitives; request-update is a Tier 1 request
  facts endpoint and mirrors request-insert/request-delete.

## MOOG Boundary Status

Request-update has no operation-specific MOOG canary at planning time. This PR
therefore records boundary status as deferred to the MOOG-v2 staged-port or
replacement surface decision in
[cardano-foundation/moog#96](https://github.com/cardano-foundation/moog/issues/96)
unless a canary/staged-port proof is produced before finalization. This status
is boundary evidence only and must not be described as legacy caller migration
or production readiness.
