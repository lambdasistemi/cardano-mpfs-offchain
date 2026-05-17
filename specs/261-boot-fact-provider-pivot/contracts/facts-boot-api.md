# Contract: POST /facts/boot

## Endpoint

```http
POST /facts/boot
Content-Type: application/json
Accept: application/json
```

Request:

```json
{
  "address": "<hex-encoded Cardano address>"
}
```

Response:

```json
{
  "snapshot": {
    "utxo_root": "<32-byte hex>",
    "chainpoint": {
      "slot": 1234,
      "block_id": "<block hash hex>"
    }
  },
  "wallet_utxos": [
    {
      "ref": {
        "tx_id": "<32-byte hex>",
        "tx_ix": 0
      },
      "txout_cbor": "<conway TxOut CBOR hex>",
      "inclusion_proof": "<CSMT proof bytes hex>"
    }
  ],
  "protocol_parameters": {
    "verified": false,
    "cbor": "<Conway PParams CBOR hex>"
  }
}
```

## Status Codes

| Status | Meaning |
|--------|---------|
| 200 | Facts returned for the requested address. |
| 400 | Malformed address or no usable wallet UTxOs at the address. |
| 503 | Indexer snapshot/root is not yet available. |
| 500 | Indexer corruption or proof generation failure. |

## Invariants

- The response is assembled by one `runIndexerTx ctx` action.
- `snapshot.utxo_root` anchors every `wallet_utxos[*].inclusion_proof`.
- `protocol_parameters.verified` is `false`.
- The response contains no unsigned transaction CBOR.
- The legacy boot transaction route is absent at PR head.

## Swagger

`docs/assets/swagger.json` must show `POST /facts/boot` and must not
show a live legacy boot transaction endpoint.
