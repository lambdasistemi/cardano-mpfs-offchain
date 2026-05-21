# Server Wire Contract: POST /facts/request/update

## Endpoint

```text
POST /facts/request/update
```

Request body:

```json
{
  "token": "<token asset-name hex>",
  "key": "<key bytes hex>",
  "old_value": "<old value bytes hex>",
  "new_value": "<new value bytes hex>",
  "address": "<serialized requester address hex>"
}
```

Response body:

```json
{
  "snapshot": {
    "utxo_root": "<32-byte CSMT root hex>",
    "slot": 42,
    "block_id": "<32-byte block id hex>"
  },
  "token": "<token asset-name hex>",
  "key": "<key bytes hex>",
  "old_value": "<old value bytes hex>",
  "new_value": "<new value bytes hex>",
  "address": "<serialized requester address hex>",
  "submitted_at": 1700000000000,
  "wallet_utxos": [
    {
      "ref": { "tx_id": "<32-byte tx id hex>", "tx_ix": 0 },
      "tx_out_cbor": "<Conway TxOut CBOR hex>",
      "inclusion_proof": "<CSMT inclusion proof CBOR hex>"
    }
  ],
  "protocol_parameters": {
    "verified": false,
    "cbor": "<Conway PParams CBOR hex>"
  }
}
```

## Invariants

- The response contains no unsigned transaction CBOR.
- The snapshot and wallet UTxOs are read in one indexer transaction.
- The server-selected `submitted_at` and protocol parameters are unverified
  local-builder inputs, not verifier claims.
- The legacy `POST /tx/request/update` route is absent at PR head.
