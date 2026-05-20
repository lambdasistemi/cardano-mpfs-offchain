# Feature Specification: Retract Fact Provider Pivot

## User Story

As an MPFS wallet/client, I retract a pending request by fetching
retract facts from the server, verifying the facts against my trusted
UTxO root, building the retract transaction locally, signing it,
submitting it, and observing the request UTxO consumed on-chain.

## Scope

- Add `POST /facts/retract`.
- Return facts only: snapshot, token, named request UTxO with CSMT
  inclusion proof, state UTxO (referenced) with CSMT inclusion proof,
  requester wallet UTxO witnesses, server-derived Phase 2 validity
  slot bounds, and unverified protocol parameters.
- Add client-side `verifyRetractFacts` returning
  `VerifiedRetractFacts`.
- Add client-side `retractCageTx` under
  `cardano-mpfs-client` cage helpers.
- Add the `readNamedRequestUtxo` indexer read primitive if it is not
  already present.
- Remove the legacy `POST /tx/retract` route and any client wrapper.
- Regenerate Swagger with the new path and without the legacy path.
- Extend the #278 local-cluster/devenv facts API coverage matrix
  with a retract row.
- Record MOOG boundary status through cardano-foundation/moog#96 or a
  canary/staged-port proof.

## Acceptance Criteria

- Byte equality against `legacy-retract.cbor` passes for
  `retractCageTx`.
- Verifier tests cover happy path, snapshot tamper,
  trusted-root mismatch, and proof tamper.
- `POST /facts/retract` is the only retract API path.
- Retract facts verifier code imports no transaction grammar (zero
  `Cardano.Ledger.Api.Tx` imports in the verifier surface).
- Swagger reflects only the new retract facts shape.
- The #278 local-cluster harness has a retract scenario that proves
  the new retract API end-to-end against the local cluster.
- MOOG boundary status is linked in the child issue and parent epic.

## Non-Goals

- Do not migrate request-update, reject, update, or end in this
  slice.
- Do not alter the already-landed boot/request-insert/request-delete/
  end facts flows except for shared helper reuse that is required
  for retract.
- Do not retain a legacy retract transaction endpoint.
- Do not claim production MOOG readiness from legacy caller behavior.
