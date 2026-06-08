# Tasks — #310 (restore) verifyTokenRequests regressed by reconcile

## Slice S1 — restore request-set completeness witness + verifier on current main
- [X] T310b-S1 Re-apply #310 on the reconciled tree: add `UtxoSetWitness` (`rrRequestSet`) to `RequestsResponse` (+JSON/schema/swagger); `tokenRequestsHandler` computes it (mirror the `end` request-set build on current main); add `verifyTokenRequests` + opaque `VerifiedTokenRequests` + accessor to `Verify/Read` (snapshot==root + `verifyUtxoSetCompleteness`); client + server tests; `./gate.sh` green.
