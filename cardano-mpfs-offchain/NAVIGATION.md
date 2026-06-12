# cardano-mpfs-offchain — Package Navigation

A map of the `cardano-mpfs-offchain` package: the production server
(indexer + proof-bearing HTTP API). For the repo-wide map covering all
packages see [`../NAVIGATION.md`](../NAVIGATION.md); for prose
architecture see [`../docs/architecture/`](../docs/architecture/).

Base module: `Cardano.MPFS`

---

## Table of Contents

1. [Core Domain](#core-domain)
2. [Service Interfaces](#service-interfaces)
3. [HTTP Layer](#http-layer)
4. [Chain Indexer](#chain-indexer)
5. [Trie Backends](#trie-backends)
6. [Node Integration](#node-integration)
7. [Mock Implementations](#mock-implementations)
8. [Application Wiring](#application-wiring)
9. [Executables](#executables)

---

## Core Domain

Pure types and logic with no IO dependencies.

| Module | Purpose |
|--------|---------|
| [`Core.Types`][s-core-types] | `TokenId`, `Root`, `Request`, `TokenState`, `Operation`, `BlockId`, ledger re-exports |
| [`Core.OnChain`][s-core-onchain] | Re-exports from [`cardano-mpfs-cage`][cage]: `CageDatum`, `MintRedeemer`, `UpdateRedeemer`, `ProofStep`; offchain-specific `cageScriptHash`, `cageAddr` |
| [`Core.Proof`][s-core-proof] | Re-exports from [`cardano-mpfs-cage`][cage]: `serializeProof`, `toProofSteps` |
| [`Core.Blueprint`][s-core-blueprint] | Re-exports from [`cardano-mpfs-cage`][cage]: CIP-57 blueprint loading, schema validation |

[s-core-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Types%22&type=code
[s-core-onchain]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.OnChain%22&type=code
[s-core-proof]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Proof%22&type=code
[s-core-blueprint]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Blueprint%22&type=code
[cage]: https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell

---

## Service Interfaces

Every major component is a **record of functions**, polymorphic in
`m`. No typeclasses — dependencies are explicit values.

### Provider

Ledger metadata and evaluation via N2C LocalStateQuery.

| Field | Purpose |
|-------|---------|
| `queryProtocolParams` | Current `PParams ConwayEra` |
| `evaluateTx` | Script ex-unit evaluation |
| `posixMsToSlot` / `posixMsCeilSlot` | POSIX-ms to slot conversion |
| `queryUTxOs` | LSQ address scan — **forbidden on tx-build paths** (cost is O(total UTxO on chain); the indexed CSMT is the fact source, see #252) |

Real: [`mkNodeClientProvider`][s-mkNodeClientProvider] ·
Mock: [`mkMockProvider`][s-mkMockProvider]

[s-mkNodeClientProvider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkNodeClientProvider&type=code
[s-mkMockProvider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockProvider&type=code

### State

Indexed token and request state with three sub-records: `Tokens`,
`Requests`, `Checkpoints`, plus `hoistState` natural transformations.

Real: [`mkPersistentState`][s-mkPersistentState] (RocksDB) ·
Transactional: [`mkTransactionalState`][s-mkTransactionalState] ·
Mock: [`mkMockState`][s-mkMockState] (IORef maps)

[s-mkPersistentState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPersistentState&type=code
[s-mkTransactionalState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkTransactionalState&type=code
[s-mkMockState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockState&type=code

### TrieManager

Per-token MPF trie access. Each cage token has an isolated trie.

| Field | Purpose |
|-------|---------|
| `withTrie` | Run an action with a token's trie |
| `withSpeculativeTrie` | Read-your-writes session, discarded on exit |
| `createTrie` / `deleteTrie` | Lifecycle |
| `hideTrie` / `unhideTrie` | Soft-delete for burn forward/rollback |

The [`Trie m`][s-Trie] record exposes insert/delete/lookup, root, and
proof operations.

[s-Trie]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Trie+m%22&type=code

### Submitter

`submitTx :: Tx ConwayEra -> m SubmitResult` — returns
`Submitted TxId` or `Rejected reason`.

Real: [`mkN2CSubmitter`][s-mkN2CSubmitter] (N2C LocalTxSubmission) ·
Mock: [`mkMockSubmitter`][s-mkMockSubmitter] (rejects all)

[s-mkN2CSubmitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkN2CSubmitter&type=code
[s-mkMockSubmitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockSubmitter&type=code

### TxBuilder

Internal proof-envelope builders. Each field consumes a
`BundleSnapshot` plus indexed UTxO facts and returns a
`ProofEnvelope` — the builders never query the node for UTxO state.

| Field | Cage operation |
|-------|----------------|
| `bootToken` | **Errors** — server-side boot was removed; clients use `POST /facts/boot` and build locally |
| `requestInsert` / `requestDelete` / `requestUpdate` | Submit request UTxO |
| `updateToken` | Consume requests, update trie root |
| `retractRequest` | Cancel a pending request |
| `rejectRequests` | Owner rejection of expired requests |
| `endToken` | Burn cage token |

The owner-only sweep transaction is not a `TxBuilder` field:
[`sweepUtxoImpl`][s-sweepUtxoImpl] from `TxBuilder.Real` is invoked
directly by the `POST /tx/sweep` handler.

Real: [`mkRealTxBuilder`][s-mkRealTxBuilder] ·
Mock: [`mkMockTxBuilder`][s-mkMockTxBuilder] (throws on all ops)

[s-mkRealTxBuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkRealTxBuilder&type=code
[s-mkMockTxBuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockTxBuilder&type=code
[s-sweepUtxoImpl]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=sweepUtxoImpl&type=code

### Context

Facade record bundling the singletons plus indexed-read primitives:
`utxoExists`, `resolveUtxo`, `awaitUtxo`, `utxoRoot`, `utxoProof`,
`indexerProofsReady`, `evalContext`, `runIndexerTx`, `readMetrics`,
and the static `cfgCage`.

[`runIndexerTx`][s-runIndexerTx] runs a composed `IndexerTx` read
inside one underlying transaction — the atomicity anchor for all
proof-bearing handlers.

[s-runIndexerTx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=runIndexerTx&type=code

---

## HTTP Layer

Servant REST API with Swagger UI. `mkApp` from `HTTP.Server` wraps a
`Context IO` into a WAI `Application`. The canonical API type and
wire DTOs live in the `cardano-mpfs-api` package; this layer adds the
handlers and the server-local metrics endpoints.

| Module | Purpose |
|--------|---------|
| [`HTTP.API`][s-http-api] | Server-local wrapper around the shared Servant API plus `/metrics` |
| [`HTTP.Types`][s-http-types] | Server compatibility re-exports and ledger conversion helpers |
| [`HTTP.Types.Facts`][s-http-facts] | Assembly helpers for facts-only responses |
| [`HTTP.Encoding`][s-http-enc] | `Hex` newtype for binary-as-hex JSON transport |
| [`HTTP.Server`][s-http-server] | WAI wiring, `mkApp`, all handlers |
| [`HTTP.SubmitScope`][s-http-scope] | `txTouchesMpfs` — the `POST /submit` MPFS-scope gate |
| [`HTTP.Swagger`][s-http-swagger] | OpenAPI generation, Swagger UI at `/swagger-ui` |

[s-http-api]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.API%22&type=code
[s-http-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Types%22&type=code
[s-http-facts]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Types.Facts%22&type=code
[s-http-enc]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Encoding%22&type=code
[s-http-server]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Server%22&type=code
[s-http-scope]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.SubmitScope%22&type=code
[s-http-swagger]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Swagger%22&type=code

---

## Chain Indexer

Block-by-block processing of cage transactions with rollback support.
One block = one RocksDB transaction — see
[block processing](../docs/architecture/block-processing.md).

| Module | Purpose |
|--------|---------|
| [`Indexer.Event`][s-idx-event] | `detectCageEvents` — classify cage-relevant txs into `CageEvent`; `CageInverseOp`; `/submit` scope predicates |
| [`Indexer.Follower`][s-idx-follower] | `detectCageBlockEvents`, `applyCageBlockEvents`, `applyCageInverses` — generic over `Monad m` |
| [`Indexer.CageFollower`][s-idx-cage] | `rollForward`/`rollBackward` via the chain-follower `Runner` (`processBlock`, `rollbackTo`); phase threading, armageddon reset |
| [`Indexer.Backend`][s-idx-backend] | `composedInit` — composed UTxO + cage backend (restore/follow/applyInverse callbacks) |
| [`Indexer.ComposedInv`][s-idx-inv] | `ComposedInv` — combined UTxO + cage inverse ops, one per rollback point |
| [`Indexer.Reads`][s-idx-reads] | `IndexerTx` composable atomic reads: `readSnapshot`, `readUtxoWitness`, `readTrieFacts`, `readRequestUtxosAt`, … |
| [`Indexer.Columns`][s-idx-columns] | `AllColumns` + `UnifiedColumns` GADTs — the 14-column DB schema |
| [`Indexer.Codecs`][s-idx-codecs] | CBOR codecs for column key-value types |
| [`Indexer.Persistent`][s-idx-persist] | `mkTransactionalState` (composable) + `mkPersistentState` (IO) |

[s-idx-event]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Event%22&type=code
[s-idx-follower]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Follower%22&type=code
[s-idx-cage]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.CageFollower%22&type=code
[s-idx-backend]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Backend%22&type=code
[s-idx-inv]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.ComposedInv%22&type=code
[s-idx-reads]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Reads%22&type=code
[s-idx-columns]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Columns%22&type=code
[s-idx-codecs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Codecs%22&type=code
[s-idx-persist]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Persistent%22&type=code

---

## Trie Backends

`TrieManager` implementations sharing the same interface.

| Module | Purpose |
|--------|---------|
| [`Trie.Pure`][s-trie-pure] | In-memory trie backed by an `IORef`, for tests |
| [`Trie.PureManager`][s-trie-pm] | `mkPureTrieManager` — in-memory manager keyed by `TokenId` |
| [`Trie.Persistent`][s-trie-pers] | `mkUnifiedTrieManager` (transactional, composes into the block transaction) + `mkPersistentTrieManager` (IO, token-prefixed keys, speculative sessions) |

[s-trie-pure]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.Pure%22&type=code
[s-trie-pm]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.PureManager%22&type=code
[s-trie-pers]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.Persistent%22&type=code

---

## Node Integration

Thin wrappers over
[cardano-node-clients](https://github.com/lambdasistemi/cardano-node-clients),
which owns the N2C connection, channels, and protocol state machines.

| Module | Purpose |
|--------|---------|
| [`Provider.NodeClient`][s-prv-nc] | `mkNodeClientProvider` — N2C LocalStateQuery for PParams, evaluation, slot conversion, eval context |
| [`Submitter.N2C`][s-sub-n2c] | `mkN2CSubmitter` — N2C LocalTxSubmission |

[s-prv-nc]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Provider.NodeClient%22&type=code
[s-sub-n2c]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Submitter.N2C%22&type=code

---

## Mock Implementations

Test doubles. No node connection required.

| Module | Constructor | Behavior |
|--------|-------------|----------|
| [`Mock.Context`][s-mock-ctx] | `mkMockContext` | Wires all mocks into `Context IO` |
| [`Mock.State`][s-mock-st] | `mkMockState` | `IORef (Map k v)` per sub-record |
| [`Mock.Provider`][s-mock-prv] | `mkMockProvider` | Empty UTxO sets, empty evaluation |
| [`Mock.Submitter`][s-mock-sub] | `mkMockSubmitter` | Rejects all transactions |
| [`Mock.TxBuilder`][s-mock-txb] | `mkMockTxBuilder` | Throws on all operations |

[s-mock-ctx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Context%22&type=code
[s-mock-st]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.State%22&type=code
[s-mock-prv]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Provider%22&type=code
[s-mock-sub]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Submitter%22&type=code
[s-mock-txb]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.TxBuilder%22&type=code

---

## Application Wiring

[`withApplication`][s-withApplication] creates the full `Context IO`
with bracket lifecycle:

1. Read the Shelley genesis (network magic, stability window,
   security parameter `k`)
2. Open RocksDB over all 14 column families behind one unified
   transaction runner
3. Project cage and UTxO column subsets; check schema migration
4. Build persistent State + TrieManager; seed genesis UTxOs on a
   fresh DB
5. Start the `CageFollower` on its own ChainSync N2C connection
6. Wire Provider + Submitter on a second N2C connection (LSQ + LTxS)
7. Bundle everything into `Context IO`; tear down on exit

`AppConfig` fields: `epochSlots`, `shelleyGenesisPath`, `socketPath`,
`dbPath`, `channelCapacity`, `cageConfig`, `byronGenesisPath`,
`followerEnabled`, `appTracer`.

[`Trace`][s-trace] defines the structured `AppTrace` type and
`jsonLinesTracer` for stderr JSON-lines logging.

[s-withApplication]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=withApplication&type=code
[s-trace]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trace%22&type=code

---

## Executables

Under `exe/`:

| Executable | Source | Purpose |
|------------|--------|---------|
| `mpfs-serve` | `Serve.hs` | Production server: `--socket`, `--db`, `--port`, `--shelley-genesis`, `--blueprint`; optional `--byron-genesis`, `--epoch-slots`, `--mainnet` |
| `mpfs-devnet-server` | `DevnetServer.hs` | Single-node devnet + MPFS API, for E2E and SPA testing |
| `mpfs-run-preprod` | `RunPreprod.hs` | Minimal runner to smoke-test the follower against a preprod node |
| `cardano-mpfs-swagger` | `swagger/Main.hs` | Print the OpenAPI spec (drives `just update-swagger`) |
| `mpfs-inspect-db` | `InspectDb.hs` | Report per-column statistics of an MPFS RocksDB database |
| `mpfs-tx-vectors` | `TxVectors.hs` | Generate CBOR transaction test vectors |
