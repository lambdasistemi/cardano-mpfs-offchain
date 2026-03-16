# Cardano MPFS Offchain

Haskell offchain companion to
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, transaction building, and submission for Cardano Merkle
Patricia Forestry.

## Packages

| Package | Description |
|---|---|
| `merkle-patricia-forestry` | 16-ary hex Patricia trie with Blake2b-256, compatible with the [Aiken on-chain implementation](https://github.com/aiken-lang/merkle-patricia-forestry) |
| `cardano-mpfs-offchain` | Off-chain service layer: N2C node client, UTxO provider, transaction balancing, submission, and skeleton indexer |

## Documentation

- [Architecture Overview](architecture/overview.md) --- system diagram, phases, design principles
- [Data Sources](architecture/data-sources.md) --- N2C connection, mini-protocols, data flow
- [Singletons](architecture/singletons.md) --- record-of-functions interfaces
- [Testing](architecture/testing.md) --- unit tests, E2E tests with cardano-node subprocess
- [API Reference](swagger-ui.md) --- Swagger UI for the HTTP API
