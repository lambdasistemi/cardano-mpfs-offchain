# Implementation Plan: Proof-Carrying API Responses

**Branch**: `177-proof-carrying-api` | **Date**: 2026-04-17 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/177-proof-carrying-api/spec.md`

## Summary

Upgrade the affected query and transaction-building endpoints from bare
state/hex/CBOR payloads to proof-bearing JSON objects. Query endpoints
can mostly assemble bundles in the HTTP layer using existing `Context`
proof hooks, while transaction endpoints likely require a richer
`TxBuilder` return type so the server can return the unsigned
transaction plus the exact UTxO and MPF witnesses the builder relied on.

## Technical Context

**Language/Version**: Haskell (GHC 9.8.4)
**Primary Dependencies**: servant, swagger2, cardano-ledger,
cardano-utxo-csmt, cardano-mpfs-onchain
**Storage**: RocksDB-backed index/state plus persistent trie manager
**Testing**: hspec unit tests, HTTP endpoint specs, E2E devnet tests,
Swagger freshness check via `just update-swagger`
**Target Platform**: Linux server (Nix-built service and Docker image)
**Project Type**: web-service for unsigned transaction building and trie
queries
**Performance Goals**: Preserve current synchronous request model while
adding proof generation bounded by the number of bundled UTxOs and trie
facts in the response
**Constraints**: One indexed snapshot per response, unsigned txs only,
direct `/utxo/*` debugging endpoints remain valid, Swagger JSON must stay
fresh
**Scale/Scope**: 13 affected endpoints (`GET /status`, 4 token query
endpoints, 8 tx-building endpoints) plus shared schemas, docs, and
tests

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | Pass | Witness bundles should reuse existing `TxIn`, `TxOut`, and serialized ledger CBOR rather than invent shadow chain types |
| II. Records of Functions | Pass | `TxBuilder` may return a richer product type, but remains a record-of-functions boundary |
| III. Atomic Block Processing | N/A | No block application or RocksDB write-batch changes |
| IV. External Signing | Pass | Responses remain unsigned; richer metadata helps clients verify before signing |
| V. Aiken Compatibility | Pass | MPF proofs reuse existing trie proof encoding; UTxO proofs come from the indexed CSMT state |
| VI. Test Locally First | Pass | HTTP, unit, E2E, and Swagger freshness checks all run locally |
| VII. Nix Reproducibility | Pass | No new non-flake tooling required |

## Project Structure

### Documentation (this feature)

```text
specs/177-proof-carrying-api/
├── spec.md
├── research.md
└── plan.md
```

### Source Code Changes

```text
cardano-mpfs-offchain/lib/Cardano/MPFS/
├── Context.hs                 # Existing UTxO proof hooks used by HTTP/query layer
├── TxBuilder.hs               # Builder interface likely returns proof-bearing bundles
├── TxBuilder/Real.hs          # Wire richer builder results
├── TxBuilder/Real/Boot.hs     # Boot witness bundle (wallet inputs only)
├── TxBuilder/Real/Request.hs  # Request insert/delete/update witness bundles
├── TxBuilder/Real/Update.hs   # Update bundle with request/state MPF proofs
├── TxBuilder/Real/Retract.hs  # Retract bundle with request/state witnesses
├── TxBuilder/Real/Reject.hs   # Reject bundle for batched Phase 3 requests
├── TxBuilder/Real/End.hs      # End bundle with consumed inputs
├── HTTP/API.hs                # Response types for affected endpoints change from scalars to objects
├── HTTP/Types.hs              # Shared proof-bearing response schemas
├── HTTP/Server.hs             # Assemble query bundles and serialize tx bundles
└── HTTP/Swagger.hs            # Swagger description updates if schema naming/notes need tightening

cardano-mpfs-offchain/test/Cardano/MPFS/
├── HTTP/StatusSpec.hs         # `GET /status` root + snapshot contract
├── HTTP/TokenSpec.hs          # `GET /tokens/:id`
├── HTTP/TrieSpec.hs           # `GET /facts/:key`, `GET /proofs/:key`
├── HTTP/RequestsSpec.hs       # `GET /requests`
└── TxBuilderSpec.hs           # Rich tx bundle shape and proof association

cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/
├── HTTPLifecycleSpec.hs       # Inline proof contracts and direct `/utxo/*` cross-checks
└── CageFlowSpec.hs            # Update/request/reject/end flows with proof-bearing tx responses

docs/
└── assets/swagger.json        # Regenerated API contract
```

**Structure Decision**: Keep the implementation inside the existing
single Haskell service. Query proof bundling lives primarily in
`HTTP.*`; transaction proof bundling crosses `TxBuilder` and `HTTP.*`
because the builder is the only layer that knows which inputs and trie
facts justified the unsigned transaction.

## Implementation Phases

### Slice 1: Shared response model + status root

Add the shared proof-bearing response types in `HTTP.Types` and extend
`StatusResponse` with the current UTxO-CSMT root. Wire `statusHandler`
to surface the root from `Context.utxoRoot` alongside existing chain tip
and checkpoint metadata.

**Files**: `HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`test/Cardano/MPFS/HTTP/StatusSpec.hs`

**Goal**: Establish the verification snapshot contract before changing
the token and tx endpoints.

### Slice 2: Proof-bearing query endpoints

Convert the affected token query endpoints from scalar payloads to
structured objects:

- `GET /tokens/:id`
- `GET /tokens/:id/facts/:key`
- `GET /tokens/:id/proofs/:key`
- `GET /tokens/:id/requests`

Use the existing `Context.resolveUtxo`, `Context.utxoProof`, and trie
proof code to attach witnessed state/request UTxOs and MPF proofs to the
business payloads.

**Files**: `HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`test/Cardano/MPFS/HTTP/TokenSpec.hs`,
`test/Cardano/MPFS/HTTP/TrieSpec.hs`,
`test/Cardano/MPFS/HTTP/RequestsSpec.hs`

**Goal**: Clients can verify all read-side responses offline against one
reported snapshot.

### Slice 3: Rich transaction builder boundary

Replace the current `TxBuilder` return type of bare `Tx ConwayEra` with
a proof-bearing bundle type that can carry:

- the unsigned transaction
- the set of consumed inputs as witnessed UTxOs
- snapshot metadata
- optional trie proof payloads for trie-dependent operations

Update mocks and the top-level wiring in `TxBuilder/Real.hs`.

**Files**: `TxBuilder.hs`, `TxBuilder/Real.hs`,
`Mock/TxBuilder.hs`, `test/Cardano/MPFS/TxBuilderSpec.hs`

**Why this slice exists**: Parsing the final tx in `HTTP.Server` is not
enough to reconstruct MPF proof intent or stable proof-to-request
association for batched flows.

### Slice 4: Simple tx endpoints on the new bundle type

Migrate the tx endpoints that only need witnessed consumed inputs and,
at most, minimal state context:

- `POST /tx/boot`
- `POST /tx/request/insert`
- `POST /tx/request/delete`
- `POST /tx/request/update`
- `POST /tx/retract`
- `POST /tx/reject`
- `POST /tx/end`

Boot omits MPF sections entirely; the others include them only when the
builder actually relies on trie/state facts that the client must trust.

**Files**: `TxBuilder/Real/Boot.hs`,
`TxBuilder/Real/Request.hs`,
`TxBuilder/Real/Retract.hs`,
`TxBuilder/Real/Reject.hs`,
`TxBuilder/Real/End.hs`,
`HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`test/Cardano/MPFS/TxBuilderSpec.hs`,
`e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs`

### Slice 5: `POST /tx/update` proof bundle

Handle the hardest endpoint separately. `POST /tx/update` batches
pending requests and applies trie changes, so the response must carry:

- witnessed state input
- witnessed request inputs
- MPF proofs for every request key/value the update relied on
- deterministic association between each request and its proof data

**Files**: `TxBuilder/Real/Update.hs`, `HTTP/Types.hs`,
`HTTP/Server.hs`, `test/Cardano/MPFS/TxBuilderSpec.hs`,
`e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs`

**Goal**: A client can inspect a batched update transaction and verify
every contributing request before signing.

### Slice 6: Swagger + end-to-end contract checks

Regenerate `docs/assets/swagger.json`, make the freshness check pass,
and add contract-style tests that cross-check inline proof data against
the existing direct `/utxo/*` endpoints for the same indexed snapshot.

**Files**: `docs/assets/swagger.json`,
`HTTP/Types.hs`, `HTTP/Swagger.hs`,
`e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs`

## Risks

- **Snapshot drift during response assembly**: If the indexer advances
  between reading data and building proofs, the response can mix roots.
  The implementation must capture one snapshot root per response and
  reject or retry inconsistent bundles.
- **TxBuilder boundary expansion**: Changing the builder return type
  touches every tx endpoint and the mock builder. The migration should be
  staged so each commit still compiles.
- **Batch response size**: `update` and `reject` can include many
  witnessed inputs. The API contract must keep proof-to-item association
  explicit even when payloads get large.
- **Swagger churn**: Several endpoints currently return `Hex`; changing
  them to objects affects schema names, examples, and the freshness
  check in one sweep.
