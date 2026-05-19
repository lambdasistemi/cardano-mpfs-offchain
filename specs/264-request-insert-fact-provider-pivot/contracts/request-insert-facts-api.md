# Contract: POST /facts/request/insert

Request body:

```json
{
  "token": "<hex token id>",
  "key": "<hex fact key>",
  "value": "<hex fact value>",
  "address": "<hex serialized requester address>"
}
```

Response body:

```json
{
  "snapshot": {
    "utxo_root": "<hex root>",
    "chain_point": {
      "slot": 0,
      "block_id": "<hex block id>"
    }
  },
  "token": "<hex token id>",
  "key": "<hex fact key>",
  "value": "<hex fact value>",
  "address": "<hex serialized requester address>",
  "wallet_utxos": [
    {
      "ref": { "tx_id": "<hex tx id>", "tx_ix": 0 },
      "tx_out_cbor": "<hex txout cbor>",
      "utxo_proof": "<hex csmt proof>"
    }
  ],
  "protocol_parameters": {
    "verified": false,
    "cbor": "<hex protocol params cbor>"
  }
}
```

The server must not return unsigned transaction CBOR from this endpoint.
