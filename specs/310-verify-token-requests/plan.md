# Plan — #310 verifyTokenRequests

## Modules
- `cardano-mpfs-api/.../API/Types.hs` — `RequestsResponse` (+ `UtxoSetWitness` field), ToJSON/FromJSON/ToSchema.
- `cardano-mpfs-offchain/.../HTTP/Server.hs` — `tokenRequestsHandler` builds the request-set witness (mirror the `end` handler's `requestSet` + `utxoSetToJSON`, `HTTP/Types/Facts.hs`).
- `cardano-mpfs-verify/.../Client/Verify/Read.hs` — `verifyTokenRequests` + `VerifiedTokenRequests` + accessor (mirror `verifyTokenFacts`; use `Verify/Completeness.verifyUtxoSetCompleteness` over the request-address prefix).
- tests: `cardano-mpfs-client/test/.../Verify/ReadSpec.hs` (client), offchain `test/.../HTTP/RequestsSpec.hs` + e2e completeness.
- `docs/assets/swagger.json`, README.

## Slices (bisect-safe)

### S1 — API + server: request-set completeness witness on /requests
Add the `UtxoSetWitness` to `RequestsResponse`; `tokenRequestsHandler` computes it
like `end` (resolve the token's request-address UTxO set → `utxoSetToJSON`). Update
ToJSON/FromJSON/ToSchema/swagger. Proof: offchain unit/e2e that `/requests` returns a
well-formed completeness witness for N requests; `./gate.sh` green.

### S2 — client verifier: verifyTokenRequests
Add `verifyTokenRequests` + opaque `VerifiedTokenRequests` + `verifiedTokenRequests` to
`Verify/Read`, mirroring `verifyTokenFacts` (snapshot==root + `verifyUtxoSetCompleteness`
over the request-address prefix). RED: client test — complete set verifies, tampered/
dropped fails closed. GREEN: implement. `./gate.sh` green.

## Order
S1 first (the witness must exist to verify). S2 consumes it. The request-address prefix
derivation + `verifyUtxoSetCompleteness` already exist (end-facts); reuse, don't reinvent.
