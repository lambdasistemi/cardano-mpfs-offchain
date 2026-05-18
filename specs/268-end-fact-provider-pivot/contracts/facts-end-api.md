# Contract: POST /facts/end

## Request

```http
POST /facts/end
Content-Type: application/json
```

```json
{
  "token": "<hex token id>",
  "address": "<hex serialized owner/funding address>"
}
```

## Response

```json
{
  "snapshot": {
    "utxo_root": "<hex>",
    "chainpoint": {
      "slot": 0,
      "block_id": "<hex>"
    }
  },
  "token": "<hex token id>",
  "state_utxo": {
    "ref": { "tx_id": "<hex>", "tx_ix": 0 },
    "txout_cbor": "<hex>",
    "inclusion_proof": "<hex>"
  },
  "wallet_utxos": [
    {
      "ref": { "tx_id": "<hex>", "tx_ix": 1 },
      "txout_cbor": "<hex>",
      "inclusion_proof": "<hex>"
    }
  ],
  "request_set": {
    "entries": [],
    "completeness_proof": "<hex>"
  },
  "protocol_parameters": {
    "verified": false,
    "cbor": "<hex>"
  }
}
```

The response contains no unsigned transaction CBOR.

## Errors

- `400`: malformed address or no wallet funding UTxOs at the requested address.
- `404`: token state UTxO is not present in the indexed UTxO set.
- `503`: snapshot unavailable.
- `500`: indexer corruption while loading a CSMT leaf or proof.
