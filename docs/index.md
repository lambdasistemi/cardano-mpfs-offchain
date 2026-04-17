# Cardano MPFS Offchain

Off-chain service for
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, transaction building, and HTTP API for Cardano Merkle
Patricia Forestry.

Connects to a Cardano node via N2C, indexes cage UTxOs into RocksDB,
and exposes a REST API for building and submitting cage transactions.
On-chain types and proof serialization come from the
[cage library](https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell).

## Documentation

- [Architecture Overview](architecture/overview.md) --- system diagram, dependency graph, module hierarchy
- [Block Processing](architecture/block-processing.md) --- one block = one RocksDB transaction
- [Data Sources](architecture/data-sources.md) --- N2C connection, mini-protocols, data flow
- [Singletons](architecture/singletons.md) --- record-of-functions interfaces
- [Testing](architecture/testing.md) --- unit tests, E2E tests with cardano-node subprocess
- [API Reference](swagger-ui.md) --- Swagger UI for the HTTP API
