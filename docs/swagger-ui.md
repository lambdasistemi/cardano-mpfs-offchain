# API Reference

This Swagger document is generated from the Servant API with
`just update-swagger`.

Script-bearing write operations are facts-first: `/facts/*` returns
proof-bearing material anchored to an indexed UTxO-CSMT snapshot, and
clients build/sign locally before calling `POST /submit`. The API must
not return unsigned transaction CBOR for those flows. `GET /eval-context`
is the current trusted interim source for PlutusV3 cost models,
protocol parameters, `SystemStart`, and live era-history; follow-up #311
will trust-anchor that metadata.

`POST /facts/update` and `POST /facts/reject` accept an exact
`requests: ["txid#ix", ...]` subset. A non-empty subset is fail-closed;
`[]` is the explicit catch-all mode.

For fact lookup, the current Servant shape returns a present
`FactResponse` from `GET /tokens/:id/facts/:key`; absent keys return
404. Exclusion proofs for absent-key verification are served by
`GET /tokens/:id/proofs/:key` and checked with `verifyFactAbsentFacts`.

<swagger-ui src="./assets/swagger.json" />
