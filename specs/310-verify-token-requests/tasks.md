# Tasks — #310 verifyTokenRequests

## Slice S1 — API + server request-set completeness witness
- [X] T310-S1 Add `UtxoSetWitness` to `RequestsResponse` (+ JSON/schema/swagger); `tokenRequestsHandler` computes it mirroring the `end` handler (`utxoSetToJSON requestSet`); offchain unit/e2e proof; `./gate.sh` green.

## Slice S2 — client verifyTokenRequests
- [X] T310-S2 Add `verifyTokenRequests` + opaque `VerifiedTokenRequests` + accessor to `Verify/Read` (snapshot==root + `verifyUtxoSetCompleteness` over request-address prefix); RED complete-verifies/tampered-fails-closed; `./gate.sh` green.
