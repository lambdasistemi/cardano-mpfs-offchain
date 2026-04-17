# cardano-mpfs-offchain

Off-chain companion to
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, transaction building, and submission for Cardano Merkle
Patricia Forestry.

## Packages

| Package | Description |
|---|---|
| `merkle-patricia-forestry` | 16-ary hex Patricia trie with Blake2b-256, compatible with the [Aiken on-chain implementation](https://github.com/aiken-lang/merkle-patricia-forestry) |
| `cardano-mpfs-offchain` | Off-chain service layer: N2C node client, UTxO provider, transaction balancing, submission, and skeleton indexer |

## HTTP API

Servant REST API wrapping the internal `Context` record-of-functions.
External signing model --- the API returns unsigned CBOR-encoded
transactions; the client signs and submits via `POST /tx/submit`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/status` | Chain tip and checkpoint |
| GET | `/tokens` | List all token IDs |
| GET | `/tokens/:id` | Token state |
| GET | `/tokens/:id/root` | Trie root hash |
| GET | `/tokens/:id/facts/:key` | Value lookup |
| GET | `/tokens/:id/proofs/:key` | Merkle proof |
| GET | `/tokens/:id/requests` | Pending requests |
| GET | `/tx/:txId?timeout=30` | Block until tx is indexed |
| POST | `/tx/boot` | Build boot transaction |
| POST | `/tx/request/insert` | Build insert request |
| POST | `/tx/request/delete` | Build delete request |
| POST | `/tx/update` | Build update transaction |
| POST | `/tx/retract` | Build retract transaction |
| POST | `/tx/end` | Build end transaction |
| POST | `/tx/submit` | Submit signed transaction |

Swagger UI is served at `/swagger-ui`.

## Building

```bash
nix develop
just build
just unit            # MPF unit tests
just unit-offchain   # offchain unit tests
just e2e             # E2E tests (requires cardano-node in PATH)
just update-swagger  # regenerate docs/assets/swagger.json
```

## Documentation

https://lambdasistemi.github.io/cardano-mpfs-offchain/
