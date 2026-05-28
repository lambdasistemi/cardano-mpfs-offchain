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
  assertion that #269 S7 dropped. Shipped as commit bb6cd0a.
- `RejectFacts` wire type, JSON instances, and Swagger schema.
- `runForgeRejectFacts :: CsmtForge () -> RejectFacts -> RejectFacts`
  added once `RejectFacts` exists. No trie-level runner for reject —
  the envelope has no trie facts.
- `VerifiedRejectFacts`, `verifyRejectFacts`, and `rejectCageTx` in
  `cardano-mpfs-client`.
- Provider-derived reject validity **lower** and **upper** slots in
  `RejectFacts`, verified as facts response fields and consumed by
  `rejectCageTx` (lower → `Tx.validFrom`, upper → `Tx.validTo`)
  instead of deriving slots directly from POSIX milliseconds.
- Indexer reads for reject's batch of rejectable request UTxOs (the
  filter `submitted_at + process_time + retract_time < now` lifted
  out of the legacy `queryRejectContext`). Reuse update read helpers
  where shape matches; add reject-specific reads only when needed.
- `POST /facts/reject` in the shared Servant API and offchain server.
- Removal of the legacy `POST /tx/reject` path in the same PR.
- Legacy reject parity proof for `rejectCageTx`: structural equality
  for every fact-derived transaction field, excluding per-redeemer
  ExUnits, plus same-state proof (state root unchanged) against the
  legacy server-side reject builder.
- Focused verifier, cage, HTTP, Swagger, and local-cluster reject
  coverage.
- MOOG boundary-status record for reject. This is boundary evidence
  only; it is not a production MOOG readiness claim.

## Scope

- Add a facts-only reject endpoint accepting the existing reject
  request payload: token id and operator funding address.
- Return one atomic snapshot bundle containing the state UTxO, the
  batch of request UTxOs selected for rejection (matching the legacy
  `rejectRequestsImpl` filter), wallet funding UTxOs, unverified
  protocol parameters, and the server-derived validity lower and
  upper slots. Reject does not mutate the MPF trie, so no trie root
  or trie facts are returned.
- Add a pure client verifier that checks the trusted root against the
  response snapshot, replays every CSMT UTxO proof (state, request,
  wallet), and validates the validity-slot envelope. Reject does not
  carry MPF trie facts, so the verifier has no trie replay step.
- Add `rejectCageTx` under the client cage helpers. It must decode
  only verified facts, replay the reject step locally (state root
  unchanged), enforce wallet policy, and return an unsigned
  transaction for local signing. It must consume the verified
  validity lower slot from facts via `Tx.validFrom` and the verified
  validity upper slot via `Tx.validTo`, and must not treat POSIX
  milliseconds as `SlotNo`.
- Capture legacy reject parity evidence before deleting `/tx/reject`
  and prove structural equality against the local cage helper for all
  fields derivable from facts.
- Remove legacy reject transaction route/type/client wrapper/Swagger
  entries in the same branch that introduces `/facts/reject`.
- Extend the facts API local-cluster matrix with a reject row that
  proves `POST /facts/reject -> verifyRejectFacts -> rejectCageTx ->
  submit -> reject indexed`.
- Add reject to the proof-bearing-envelopes smoke scenario in
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs` as
  a first-class step in the boot → request → wait → reject → refund
  flow, with both a positive (verify + build + submit + assert
  refund) and a negative (`runForgeRejectFacts` + `csmtReplayFailedAt`)
  assertion. See "Addendum 001" below for the contract.
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
  trusted-root mismatch, CSMT proof tamper (state, request_ins[i],
  funding[i]), and validity-slot tamper (lower and upper).
  Trie-fact tamper is documented as vacuous (see deviation note
  below) because reject does not carry trie facts.
- `POST /facts/reject` is the only reject API path; the legacy
  transaction route is absent from API code, server wiring, typed
  client wrappers, Swagger, and live boundary checks.
- The proof-bearing-envelopes smoke (`ProofsSpec.hs` "read and write
  envelopes carry verifiable proofs") includes a reject lifecycle
  flow (positive + negative) as a first-class step alongside boot,
  request, update, and end. See Addendum 001.
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
is replaced with structural equality plus same-state proof, by
analogy with #269 Q-001 and Q-002. Reject's legacy builder derives
provider-runtime fields (validity slots, per-redeemer ExUnits) that
are not present in `RejectFacts`; the structural-parity boundary
mirrors update's.

Validity-slot conversion is an era-schedule lookup applied to request
deadlines. By #269 Q-002 the slot is a verified field of
`RejectFacts`, not a client guess. Reject ships **both bounds**: the
lower slot mirrors the legacy `Tx.validFrom lowerSlot` (latest
Phase-3 deadline among the rejected requests) and the upper slot is
an explicit TTL (every tx needs a finite validity window for replay
protection; relying on node defaults is sloppy). Per-redeemer ExUnits
remain client-side evaluator output and stay excluded from
whole-transaction byte equality.

Per Q-S2-001 (operator decision 2026-05-28), reject ships the
**batch** request-UTxO shape that mirrors the legacy
`rejectRequestsImpl` (`rfRequestUtxos :: [UtxoEntry]`), not a
singular "named" request UTxO. Single-target reject would require
matching changes in the on-chain validator (out of #270 scope).

Per Q-S2-001, reject does **not** carry MPF trie facts.
`prepareRejectState` in `Real/Reject.hs` leaves `stateRoot`
unchanged; `RejectProof` carries only CSMT-layer state/request/
funding witnesses with no trie content; the on-chain reject
validator does not fold the trie. The issue acceptance criterion
"trie-fact-tamper" is therefore vacuous for `verifyRejectFacts`. The
S3 verifier test matrix covers happy / snapshot-tamper /
trusted-root-mismatch / proof-tamper (state, request_ins[i],
funding[i]) / validity-slot-tamper (lower, upper). The S1 DSL still
provides facts-shape trie runners for the cases where trie facts ARE
consumed; reject is not one of those cases.

## Architectural Invariants

- The server returns facts, not unsigned transactions.
- The client verifies facts before building locally.
- The reject verifier surface must not import
  `Cardano.Ledger.Api.Tx`.
- The legacy `/tx/reject` endpoint is removed in the same PR that
  adds `/facts/reject`.
- MOOG is treated as a boundary/canary or staged-port decision, not as
  a legacy call-site migration.

## Addendum 001 — Reject in proof-bearing smoke

Operator instruction (2026-05-28): reject must be a first-class step
in the proof-bearing-envelopes smoke scenario in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`, not
only a matrix row in `FactsMatrixSpec.hs`. The matrix proves the
endpoint works in isolation; the smoke proves it works as part of
the full proof-bearing flow against a live cluster. This is the
#269 S7/S8 lesson: live-flow smoke surfaces what isolated matrix
rows can't.

The smoke flow extension:

1. Insert a request via the existing request path.
2. Drive time past the request's Phase-3 deadline using the e2e
   harness's existing `awaitSlot`/`awaitTime` primitive. If the
   resolution required is not exposed by the e2e helpers, add a
   real time-advance helper as production code, not a test-only
   mutator (the #269 S8 wart-removal rule applies).
3. `POST /facts/reject` → `RejectFacts` (Option C, per
   A-S2-001).
4. `shouldAccept` against `verifyRejectFacts`.
5. Extract `VerifiedRejectFacts` and call `rejectCageTx` to build
   the unsigned tx.
6. Sign + submit + await acceptance on the local cluster.
7. Assert the request UTxO is no longer in the pending set.
8. Assert the wallet received the per-request refund at the
   expected `requestFee` tip.

Plus a negative case in the same scenario:

9. Take the honest `/facts/reject` response, apply
   `runForgeRejectFacts (flipProof "state_utxo")` (or
   `flipProof "request_utxos[0]"`), assert
   `shouldRejectWith verifyRejectFactsUnit $ csmtReplayFailedAt
     "<verifier's actual reported path>"`.

S5 has three legs that must all be present before S7 finalize can
begin: matrix row, smoke positive (steps 1-8), smoke negative
(step 9). No test-only datum mutation to fake Phase-3 reachability;
no skipping the live submit step.

## MOOG Boundary Status

No reject-specific MOOG canary exists at spec time. This PR records
boundary status as deferred to the MOOG-v2 staged-port or replacement
decision tracked in
[cardano-foundation/moog#96](https://github.com/cardano-foundation/moog/issues/96)
unless a reject canary/staged-port proof is produced before
finalization. This status is boundary evidence only and must not be
described as legacy caller migration or production readiness.
