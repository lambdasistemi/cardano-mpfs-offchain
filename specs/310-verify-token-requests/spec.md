# Spec — #310 verifyTokenRequests (RequestsResponse completeness)

Repo: lambdasistemi/cardano-mpfs-offchain. Part of read-side proof work (#243).
Base: `main`. PR target: `main`.

## Goal

`GET /tokens/:id/requests` must be verifiable: a consumer (e.g. moog oracle
validation, cardano-foundation/moog#159) needs to prove the returned pending-
request set is **complete** against the trusted UTxO-CSMT root — a missing
request could hide a conflicting/duplicate pending change. Today `RequestsResponse`
carries only per-request witnesses (`rrRequests :: [WitnessedRequest]`), no
set-level completeness proof, so `verifyUtxoSetCompleteness` cannot be applied.

## P1 user story

As a client of `/tokens/:id/requests`, I verify the response with
`verifyTokenRequests trustedRoot resp` and obtain a `VerifiedTokenRequests`
only if the request set is complete under the per-cage request-address prefix
against the trusted root.

## Functional requirements

- FR1 (API) — `RequestsResponse` carries a request-set completeness witness
  (`UtxoSetWitness`), mirroring `EndFacts.efRequestSet`. (Add a field, e.g.
  `rrRequestSet :: UtxoSetWitness`, alongside or replacing the per-request list
  as the verifiable surface.)
- FR2 (server) — `tokenRequestsHandler` populates it by building the token's
  request-address UTxO set the same way the `end` handler does
  (`utxoSetToJSON requestSet`, `HTTP/Types/Facts.hs`), against the snapshot.
- FR3 (client) — `Cardano.MPFS.Client.Verify.Read` exports
  `VerifiedTokenRequests` (opaque), `verifiedTokenRequests` accessor, and
  `verifyTokenRequests :: TrustedRoot -> RequestsResponse -> Either VerifyError
  VerifiedTokenRequests`, mirroring `verifyTokenFacts`: check
  `snapshot.utxo_root == trustedRoot` and `verifyUtxoSetCompleteness` over the
  request-address prefix. Pure (no fetching).
- FR4 — verification fails closed on a tampered/incomplete set.

## Success criteria

- Client unit test: a complete request-set verifies; an entry-dropped/tampered
  witness fails with a completeness error.
- Server/e2e: `/tokens/:id/requests` for a token with N pending requests returns
  a witness that `verifyTokenRequests` accepts.
- `./gate.sh` green (format-check, hlint, unit-tests, client-unit-tests).
- Swagger/README updated for the new response shape.

## Non-goals

- Changing `/facts` or `/tokens/:id` verifiers (#307 already landed).
- moog-side consumption (that's cardano-foundation/moog#159).
