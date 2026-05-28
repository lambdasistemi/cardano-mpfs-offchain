# Feature Specification: Reject Fact Provider Pivot

## User Story

As an MPFS operator or client, I reject a pending request through
`POST /facts/reject`, verify the snapshot-anchored request UTxO and MPF
facts, build the unsigned reject transaction locally with
`rejectCageTx`, sign it, submit it, and observe the reject transaction
accepted on-chain and indexed.

## Deliverables

- Forgery DSL port: `runForgeUpdateFacts` (CSMT runner) and a
  trie-level runner over `UpdateFacts` shipped before any reject wire
  type, so the next slices have a stable DSL to reuse. Migrate the
  inline tamper helpers currently sitting in `UpdateFactsSpec.hs` to
  the DSL primitives. Reinstate the negative update-facts e2e
  assertion that #269 S7 dropped.
- `RejectFacts` wire type, JSON instances, and Swagger schema.
- `runForgeRejectFacts` runner added once `RejectFacts` exists.
- `VerifiedRejectFacts`, `verifyRejectFacts`, and `rejectCageTx` in
  `cardano-mpfs-client`.
- A provider-derived reject validity upper slot in `RejectFacts`,
  verified as a facts response field and consumed by `rejectCageTx`
  instead of deriving a slot directly from POSIX milliseconds.
- Indexer reads for reject's named request UTxO (reuse the update
  helpers where shape matches; add reject-specific reads only when
  needed).
- `POST /facts/reject` in the shared Servant API and offchain server.
- Removal of the legacy `POST /tx/reject` path in the same PR.
- Legacy reject parity proof for `rejectCageTx`: structural equality
  for every fact-derived transaction field, excluding provider-runtime
  validity slot and per-redeemer ExUnits, plus same-new-root proof for
  the MPF fold (when reject mutates the trie).
- Focused verifier, cage, HTTP, Swagger, and local-cluster reject
  coverage.
- MOOG boundary-status record for reject. This is boundary evidence
  only; it is not a production MOOG readiness claim.

## Scope

- Add a facts-only reject endpoint accepting the existing reject
  request payload: token id and operator funding address.
- Return one atomic snapshot bundle containing the state UTxO, the
  named request UTxO selected for rejection, wallet funding UTxOs, the
  state trie root, MPF trie facts for the reject fold step (if any),
  unverified protocol parameters, and the provider-derived validity
  upper slot for the reject deadline.
- Add a pure client verifier that checks the trusted root against the
  response snapshot, replays every CSMT UTxO proof, checks the state
  datum's trie root binding, and replays each MPF trie fact.
- Add `rejectCageTx` under the client cage helpers. It must decode
  only verified facts, apply the reject mutation locally, produce the
  expected new state (root unchanged if reject does not alter the
  trie), enforce wallet policy, and return an unsigned transaction for
  local signing. It must consume the verified validity upper slot from
  facts and must not treat POSIX milliseconds as `SlotNo`.
- Capture legacy reject parity evidence before deleting `/tx/reject`
  and prove structural equality against the local cage helper for all
  fields derivable from facts.
- Remove legacy reject transaction route/type/client wrapper/Swagger
  entries in the same branch that introduces `/facts/reject`.
- Extend the facts API local-cluster matrix with a reject row that
  proves `POST /facts/reject -> verifyRejectFacts -> rejectCageTx ->
  submit -> reject indexed`.
- Record MOOG boundary status through cardano-foundation/moog#96 or an
  operation-specific canary/staged-port proof.

## Acceptance Criteria

- #248 is present on `main` (resolved during #269 via PR #284).
- Forgery DSL has `runForgeUpdateFacts`, a trie-level runner over
  `UpdateFacts`, and (after S2 lands `RejectFacts`)
  `runForgeRejectFacts` plus its trie-level counterpart. The
  `UpdateFactsSpec.hs` inline tamperers are gone, replaced by DSL
  primitives.
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
  carries a reinstated update-facts negative assertion using
  `runForgeUpdateFacts` plus the verifier's actual reported field
  path.
- `verifyRejectFacts` tests cover happy path, snapshot tamper,
  trusted-root mismatch, CSMT proof tamper, MPF proof tamper,
  trie-fact value tamper, and validity-slot tamper.
- `POST /facts/reject` is the only reject API path; the legacy
  transaction route is absent from API code, server wiring, typed
  client wrappers, Swagger, and live boundary checks.
- Verifier surface for reject has zero `Cardano.Ledger.Api.Tx`
  imports.
- The cage helper inside `rejectCageTx` agrees with the legacy
  server-side reject behaviour for equivalent inputs (same new state,
  same wallet-policy enforcement).
- `docs/assets/swagger.json` reflects only the new reject facts shape.
- MOOG boundary status is recorded for this operation as either a
  canary/staged-port proof against the paired offchain branch or the
  cardano-foundation/moog#96 decision that reject waits for the
  MOOG-v2 staged port or replacement surface.

## Non-Goals

- Do not migrate update or any non-reject endpoint in this slice.
- Do not retain a compatibility period for `/tx/reject`.
- Do not close #257 in this PR; the epic closes when this PR merges
  through its own metadata.
- Do not claim production MOOG readiness from this offchain PR.
- Do not move verifier logic into a transaction-shape verifier;
  reject facts verification must stay facts/proof oriented.

## Deviation From Issue Acceptance Criteria

The issue's original "byte-equality vs the golden vector" requirement
is replaced with structural equality plus same-new-state proof, by
analogy with #269 Q-001 and Q-002. Reject's legacy builder also
derives provider-runtime fields (validity upper slot, per-redeemer
ExUnits) that are not present in `RejectFacts`; the structural-parity
boundary mirrors update's.

Validity-upper-slot is an era-schedule lookup applied to the request
deadline. By #269 Q-002 it is a verified field of `RejectFacts`, not a
client guess; per-redeemer ExUnits remain client-side evaluator output
and stay excluded from whole-transaction byte equality.

## Architectural Invariants

- The server returns facts, not unsigned transactions.
- The client verifies facts before building locally.
- The reject verifier surface must not import
  `Cardano.Ledger.Api.Tx`.
- The legacy `/tx/reject` endpoint is removed in the same PR that
  adds `/facts/reject`.
- MOOG is treated as a boundary/canary or staged-port decision, not as
  a legacy call-site migration.

## MOOG Boundary Status

No reject-specific MOOG canary exists at spec time. This PR records
boundary status as deferred to the MOOG-v2 staged-port or replacement
decision tracked in
[cardano-foundation/moog#96](https://github.com/cardano-foundation/moog/issues/96)
unless a reject canary/staged-port proof is produced before
finalization. This status is boundary evidence only and must not be
described as legacy caller migration or production readiness.
