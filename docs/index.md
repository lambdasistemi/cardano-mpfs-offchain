# Cardano MPFS Offchain

Off-chain service for
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, proof-bearing facts, and HTTP API for Cardano Merkle
Patricia Forestry.

Connects to a Cardano node via N2C, indexes cage UTxOs into RocksDB,
and exposes a trust-minimized REST API. The server is a fact provider:
it serves indexed snapshots, UTxOs with CSMT proofs, MPF facts,
protocol parameters, and request-set witnesses. Clients verify the
facts, build and sign locally, then submit signed CBOR through
`POST /submit`; script-bearing endpoints must not return unsigned
transactions.

On-chain types and proof serialization come from the
[cage library](https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell).
The current bounded-refund cage script hash is
`ad0a8eeeec8b0a5ee9930be5d6ea2e80b285fc2f3e9675a13a392dd5`.

## Documentation

- [Architecture Overview](architecture/overview.md) --- system diagram, fact-provider flow, dependency graph, module hierarchy
- [Block Processing](architecture/block-processing.md) --- one block = one RocksDB transaction over UTxO CSMT, cage state, and MPF tries
- [Data Sources](architecture/data-sources.md) --- ChainSync, indexed proof reads, `GET /eval-context`, and submission flow
- [Singletons](architecture/singletons.md) --- record-of-functions interfaces
- [Testing](architecture/testing.md) --- unit tests, E2E tests, and verifier/reactor coverage
- [API Reference](swagger-ui.md) --- Swagger UI for the HTTP API
