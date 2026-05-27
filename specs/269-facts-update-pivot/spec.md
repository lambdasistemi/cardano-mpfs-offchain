# Feature Specification: Update Fact Provider Pivot

## User Story

As an MPFS operator or client, I update a token through
`POST /facts/update`, verify the snapshot-anchored UTxO and MPF facts, build
the unsigned update transaction locally with `updateCageTx`, sign it, submit
it, and observe the update accepted on-chain with the expected new state root.

## Deliverables

- `UpdateFacts` wire type, JSON instances, and Swagger schema.
- Shared `TrieFact` response type and MPF fold helper shaped for reuse by
  reject in #270 without shipping reject behavior in this PR.
- `VerifiedUpdateFacts`, `verifyUpdateFacts`, and `updateCageTx` in
  `cardano-mpfs-client`.
- `readRequestUtxosAt` and `readTrieFact` indexer reads, or clean reuse of
  existing reads if they already satisfy the update shape.
- `POST /facts/update` in the shared Servant API and offchain server.
- Removal of the legacy `POST /tx/update` path in the same PR.
- Legacy update parity proof for `updateCageTx`: structural equality for every
  fact-derived transaction field, excluding provider-runtime validity slot and
  per-redeemer ExUnits, plus same-new-root proof for the MPF fold.
- Focused verifier, cage, HTTP, Swagger, and local-cluster update coverage.
- MOOG boundary-status record for update. This is boundary evidence only; it
  is not a production MOOG readiness claim.

## Scope

- Add a facts-only update endpoint accepting the existing update request
  payload: token and operator funding address.
- Return one atomic snapshot bundle containing the current state UTxO, all
  pending request UTxOs selected for the update, wallet funding UTxOs, the
  state trie root, MPF trie facts for each request fold step, and unverified
  protocol parameters.
- Add a pure client verifier that checks the trusted root against the
  response snapshot, replays every CSMT UTxO proof, checks the state datum's
  trie root binding, and replays each MPF trie fact.
- Add `updateCageTx` under the client cage helpers. It must decode only
  verified facts, apply the same MPF fold as the legacy server-side update
  builder, produce the expected new state root, enforce wallet policy, and
  return an unsigned transaction for local signing.
- Capture legacy update parity evidence before deleting `/tx/update` and prove
  structural equality against the local cage helper for all fields derivable
  from facts.
- Remove legacy update transaction route/type/client wrapper/Swagger entries
  in the same branch that introduces `/facts/update`.
- Extend the facts API local-cluster matrix with an update row that proves
  `POST /facts/update -> verifyUpdateFacts -> updateCageTx -> submit -> new
  state root indexed`.
- Record MOOG boundary status through cardano-foundation/moog#96 or an
  operation-specific canary/staged-port proof.

## Acceptance Criteria

- #248 is present on `main`; PR #284 merged on 2026-05-27 at
  `1761c2c284d40aea7bcb1c3940f6a3950e509a59`, so `TrieRawValues` and raw
  fact bytes are available.
- Per Q-001, `updateCageTx` parity replaces literal byte equality with:
  structural equality against the legacy update transaction for fact-derived
  fields, explicitly excluding validity-upper-slot and per-redeemer ExUnits;
  and same-new-root proof against the legacy server-side MPF fold.
- `verifyUpdateFacts` tests cover happy path, snapshot tamper,
  trusted-root mismatch, CSMT proof tamper, MPF proof tamper, and trie-fact
  value tamper.
- `POST /facts/update` is the only update API path; the legacy transaction
  route is absent from API code, server wiring, typed client wrappers,
  Swagger, and live boundary checks.
- Verifier surface for update has zero `Cardano.Ledger.Api.Tx` imports.
- The MPF fold inside `updateCageTx` produces the same new `stateRoot` as the
  legacy server-side fold for equivalent inputs.
- `docs/assets/swagger.json` reflects only the new update facts shape.
- MOOG boundary status is recorded for this operation as either a
  canary/staged-port proof against the paired offchain branch or the
  cardano-foundation/moog#96 decision that update waits for the MOOG-v2 staged
  port or replacement surface.

## Non-Goals

- Do not migrate reject (#270) or any other endpoint.
- Do not retain a compatibility period for `/tx/update`.
- Do not close #257.
- Do not claim production MOOG readiness from this offchain PR.
- Do not move verifier logic into a transaction-shape verifier; update facts
  verification must stay facts/proof oriented.

## Deviation From Issue Acceptance Criteria

Q-001 replaces the issue's original literal byte-equality requirement for
`updateCageTx`. The legacy update builder derives the validity upper slot via
`Provider.posixMsToSlot` and per-redeemer ExUnits via `Provider.evaluateTx`.
Those are provider-runtime results, not snapshot facts, and adding them to
`UpdateFacts` would violate the server-returns-facts boundary.

This PR therefore proves the parity that is available from verified facts:
structural equality for every fact-derived transaction field and same-new-root
equality for the MPF fold. Validity slot and ExUnits remain client-local build
derivations, not part of the fact contract.

## Architectural Invariants

- The server returns facts, not unsigned transactions.
- The client verifies facts before building locally.
- The update verifier surface must not import `Cardano.Ledger.Api.Tx`.
- The legacy `/tx/update` endpoint is removed in the same PR that adds
  `/facts/update`.
- Tier-3 update work depends on raw trie values from #248/#247; this branch
  starts after that merge.
- MOOG is treated as a boundary/canary or staged-port decision, not as a
  legacy call-site migration.

## MOOG Boundary Status

No update-specific MOOG canary exists at spec time. This PR records boundary
status as deferred to the MOOG-v2 staged-port or replacement decision tracked
in
[cardano-foundation/moog#96](https://github.com/cardano-foundation/moog/issues/96)
unless an update canary/staged-port proof is produced before finalization.
This status is boundary evidence only and must not be described as legacy
caller migration or production readiness.
