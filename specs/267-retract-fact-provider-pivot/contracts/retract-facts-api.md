# Contract: POST /facts/retract

Request body:

```json
{
  "utxo": "<txhash hex>#<ix>",
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
  "request_utxo": {
    "ref": { "tx_id": "<hex tx id>", "tx_ix": 0 },
    "tx_out_cbor": "<hex txout cbor>",
    "utxo_proof": "<hex csmt proof>"
  },
  "state_utxo": {
    "ref": { "tx_id": "<hex tx id>", "tx_ix": 0 },
    "tx_out_cbor": "<hex txout cbor>",
    "utxo_proof": "<hex csmt proof>"
  },
  "wallet_utxos": [
    {
      "ref": { "tx_id": "<hex tx id>", "tx_ix": 0 },
      "tx_out_cbor": "<hex txout cbor>",
      "utxo_proof": "<hex csmt proof>"
    }
  ],
  "validity_start_slot": 0,
  "validity_end_slot": 0,
  "protocol_parameters": {
    "verified": false,
    "cbor": "<hex protocol params cbor>"
  }
}
```

The server must not return unsigned transaction CBOR from this
endpoint. The `validity_start_slot` and `validity_end_slot` fields
are server-derived Phase 2 bounds. They are unverified and act
fail-closed: bad values cause the locally-built transaction to fail
on-chain validity validation.
