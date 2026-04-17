# cardano-mpfs-offchain

Off-chain service for
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, transaction building, and HTTP API for Cardano Merkle
Patricia Forestry.

Connects to a Cardano node via N2C, indexes cage UTxOs into RocksDB,
and exposes a REST API for building and submitting cage transactions.
On-chain types and proof serialization come from the
[cage library](https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell)
in the onchain repo.

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

## Dependencies

| Package | Source | Role |
|---------|--------|------|
| [cardano-mpfs-cage](https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell) | cardano-mpfs-onchain | On-chain types, proof serialization, blueprint loading |
| [cardano-node-clients](https://github.com/lambdasistemi/cardano-node-clients) | lambdasistemi | TxBuild DSL, N2C provider, fee balancing |
| [haskell-mts](https://github.com/lambdasistemi/haskell-mts) | lambdasistemi | MPF trie (pure + RocksDB backends) |
| [chain-follower](https://github.com/lambdasistemi/chain-follower) | lambdasistemi | ChainSync protocol client |

## Documentation

https://lambdasistemi.github.io/cardano-mpfs-offchain/
