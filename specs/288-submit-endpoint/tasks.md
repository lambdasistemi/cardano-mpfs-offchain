# Tasks — #288 POST /submit endpoint

One commit per slice. Each commit body carries `Tasks: T###-S<n>`.

## Slice S1 — reshape endpoint to the locked contract

- [X] T288-S1 Reshape `SubmitRequest` to `{signedTxCbor}`; add
      `SubmitResponse {txId}` and `SubmitError {error, detail}` with
      `ToJSON`/`FromJSON`/`ToSchema` in `API/Types.hs`.
- [X] T288-S1 Change `TxSubmitAPI` to `"submit" :> ReqBody SubmitRequest
      :> Post '[JSON] SubmitResponse`; export new types in `API.hs`;
      re-export from offchain `HTTP/Types.hs`.
- [X] T288-S1 Reshape `txSubmitHandler`: return `SubmitResponse`; 400 and
      502 carry `SubmitError` JSON bodies (application/json). Remove the
      legacy `/tx/submit` shape.
- [X] T288-S1 `just update-swagger`; confirm `swagger-up-to-date` green.
- [X] T288-S1 Proof: `just unit-offchain` + `just ci` green.

## Slice S2 — HTTP-level e2e submit test

- [X] T288-S2 Add an e2e spec that starts the app under `withDevnet`,
      balances+signs a genesis ADA-transfer tx (reuse `balanceTx`,
      `genesisSignKey`, `addKeyWitness`, `genesisAddr`), POSTs
      `{"signedTxCbor": …}` to `/submit`, asserts `200` + `txId`.
- [X] T288-S2 Await the returned `txId` against the node and assert
      success (live-boundary proof the tx reached the mempool).
- [X] T288-S2 Wire the spec into the e2e suite (`main.hs`).
- [X] T288-S2 Proof: `just e2e` green for the new row.
