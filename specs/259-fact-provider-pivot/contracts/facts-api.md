# Server Wire Contract: POST /facts/{op}

The MPFS server's public HTTP surface after the pivot. This is the
only contract external integrators consume from
`cardano-mpfs-offchain`.

## Endpoints

```
POST /facts/boot                   { address }                                         → BootFacts
POST /facts/request/insert         { token, key, value, address }                      → RequestFacts
POST /facts/request/delete         { token, key, value, address }                      → RequestFacts
POST /facts/request/update         { token, key, oldValue, newValue, address }         → RequestFacts
POST /facts/retract                { request_txin, address }                           → RetractFacts
POST /facts/end                    { token, address }                                  → EndFacts
POST /facts/update                 { token, address }                                  → UpdateFacts
POST /facts/reject                 { token, address }                                  → RejectFacts
POST /submit                       { signed_tx_cbor }                                  → { txid }
```

Plus existing read endpoints (`/tokens/:id`, `/utxo/:txin`,
`/status`, etc.) — unchanged.

## Request / response JSON shapes

### Common

Every `XFacts` response embeds:

```json
{
  "snapshot": {
    "utxo_root":  "<32-byte hex>",
    "slot":       1234,
    "block_id":   "<32-byte hex>"
  },
  "protocol_parameters": {
    "verified": false,
    "cbor":     "<conway pparams cbor as hex>"
  },
  ...
}
```

### `BootFacts` and `RequestFacts`

Additional fields:

```json
{
  "wallet_utxos": [
    {
      "ref":         { "tx_id": "<32-byte hex>", "ix": 0 },
      "txout":       "<conway txout cbor as hex>",
      "csmt_proof":  "<csmt inclusion proof bytes as hex>"
    },
    ...
  ]
}
```

### `RetractFacts`

Additional fields:

```json
{
  "request_utxo": { "ref": ..., "txout": "...", "csmt_proof": "..." },
  "wallet_utxos": [ ... ]
}
```

### `EndFacts`

```json
{
  "state_utxo":   { "ref": ..., "txout": "...", "csmt_proof": "..." },
  "wallet_utxos": [ ... ]
}
```

### `UpdateFacts` and `RejectFacts`

```json
{
  "state_utxo":   { "ref": ..., "txout": "...", "csmt_proof": "..." },
  "request_utxos": [
    { "ref": ..., "txout": "...", "csmt_proof": "..." },
    ...
  ],
  "wallet_utxos": [ ... ],
  "trie_facts": [
    {
      "key":        "<bytes hex>",
      "value":      "<bytes hex>",       // null for absence
      "mpf_proof":  "<bytes hex>"
    },
    ...
  ]
}
```

## Error responses

| Status | Body                                                  | When                                                                 |
| ------ | ----------------------------------------------------- | -------------------------------------------------------------------- |
| 400    | `{"error":"no wallet utxos at address"}`              | The named requester address has zero UTxOs in the indexer.           |
| 400    | `{"error":"malformed input"}`                         | Request body fails JSON or hex decoding.                             |
| 404    | `{"error":"token not found", "token": "..."}`         | The named token is not indexed.                                      |
| 404    | `{"error":"request not found", "txin": "..."}`        | The named request UTxO is not indexed (retract path).                |
| 503    | `{"error":"indexer not ready: snapshot unavailable"}` | Chain follower has not produced its first checkpoint yet.            |
| 500    | `{"error":"indexer corruption", "details": "..."}`    | A CSMT leaf has no KV bytes, or a proof generation fails internally. |

The 503 / 404 / 400 cases are deterministic from indexer state;
500 is an invariant violation and should page operators.

## Atomicity invariant

Every facts response observes a single coherent indexer snapshot —
the IndexerTx primitives discipline from #249 (PR #253) is
preserved. Each handler is one `runIndexerTx ctx $ do { … }` block.

## Submit

`POST /submit` accepts a CBOR-encoded signed transaction. The
server forwards it to its connected cardano-node via N2C
`LocalTxSubmission` and returns the resulting `TxId` (or an error
if submission rejects).

The submit endpoint is unchanged from today; mentioned here for
completeness.

## Forbidden patterns

These patterns MUST NOT appear in `cardano-mpfs-offchain` after the
pivot:

- Any `transaction/{address}` path.
- Any `Tx ConwayEra` value emitted in an HTTP response body.
- Any call to `Cardano.MPFS.TxBuilder.Real.*` (the tree is removed).

These are the greppable acceptance criteria for the slice.
