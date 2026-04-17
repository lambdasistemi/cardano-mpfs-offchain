# cardano-mpfs-offchain — Semantic Navigation

A human-readable map of the offchain library. Each entry has a
one-line description and a GitHub search link to the source.

Base module: `Cardano.MPFS`

---

## Table of Contents

1. [Core Domain](#core-domain)
2. [Service Interfaces](#service-interfaces)
3. [HTTP API](#http-api)
4. [Indexer Pipeline](#indexer-pipeline)
5. [Trie Management](#trie-management)
6. [Transaction Building](#transaction-building)
7. [Node Integration](#node-integration)
8. [Application Wiring](#application-wiring)
9. [Testing](#testing)

---

## Core Domain

Pure types and logic with no IO dependencies.

| Module | Description |
|--------|-------------|
| [`Core.Types`][s-core-types] | `TokenId`, `Root`, `Request`, `TokenState`, `Operation`, `BlockId`, ledger re-exports |
| [`Core.OnChain`][s-core-onchain] | Re-exports from [`cardano-mpfs-cage`][cage] + offchain-specific `cageScriptHash`, `cageAddr` |
| [`Core.Proof`][s-core-proof] | Re-exports from [`cardano-mpfs-cage`][cage]: `serializeProof`, `toProofSteps` |
| [`Core.Blueprint`][s-core-blueprint] | Re-exports from [`cardano-mpfs-cage`][cage]: blueprint loading, schema validation |

[s-core-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Types%22&type=code
[s-core-onchain]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.OnChain%22&type=code
[s-core-proof]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Proof%22&type=code
[s-core-blueprint]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Blueprint%22&type=code
[cage]: https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell

---

## Service Interfaces

Every major component is a record of functions, polymorphic in `m`.
No typeclasses -- dependencies are explicit values.

| Module | Description |
|--------|-------------|
| [`Provider`][s-provider] | `queryUTxOs`, `queryProtocolParams`, `evaluateTx` |
| [`Submitter`][s-submitter] | `submitTx :: Tx ConwayEra -> m SubmitResult` |
| [`TxBuilder`][s-txbuilder] | Cage protocol operations: boot, request, update, retract, end |
| [`State`][s-state] | `Tokens`, `Requests`, `Checkpoints` sub-records for indexed state |
| [`Indexer`][s-indexer] | Chain follower lifecycle: `start`, `stop`, `pause`, `resume`, `getTip` |
| [`Trie`][s-trie] | `TrieManager` -- per-token MPF trie access (`withTrie`, `createTrie`, `deleteTrie`) |
| [`Context`][s-context] | Facade bundling all singletons + `utxoExists` into one record |

[s-provider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Provider%22+path%3Alib%2FCardano%2FMPFS%2FProvider.hs&type=code
[s-submitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Submitter%22+path%3Alib%2FCardano%2FMPFS%2FSubmitter.hs&type=code
[s-txbuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder%22+path%3Alib%2FCardano%2FMPFS%2FTxBuilder.hs&type=code
[s-state]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.State%22&type=code
[s-indexer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer%22+path%3Alib%2FCardano%2FMPFS%2FIndexer.hs&type=code
[s-trie]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie%22+path%3Alib%2FCardano%2FMPFS%2FTrie.hs&type=code
[s-context]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Context%22&type=code

---

## HTTP API

Servant REST API with Swagger UI. `mkApp` from `HTTP.Server` wraps
a `Context IO` into a WAI `Application`.

| Module | Description |
|--------|-------------|
| [`HTTP.API`][s-http-api] | Servant type-level API definition (all endpoint types) |
| [`HTTP.Types`][s-http-types] | JSON wire types: `StatusResponse`, `TokenIdJSON`, `RequestJSON`, request bodies |
| [`HTTP.Encoding`][s-http-enc] | `Hex` newtype for binary-as-hex JSON transport |
| [`HTTP.Server`][s-http-server] | WAI application wiring, `mkApp`, all Servant handlers |
| [`HTTP.Swagger`][s-http-swagger] | OpenAPI spec generation, `swaggerDoc`, Swagger UI serving |

[s-http-api]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.API%22&type=code
[s-http-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Types%22&type=code
[s-http-enc]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Encoding%22&type=code
[s-http-server]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Server%22&type=code
[s-http-swagger]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.HTTP.Swagger%22&type=code

---

## Indexer Pipeline

Block-by-block processing of cage transactions with rollback
support. All state changes in one block commit in a single RocksDB
WriteBatch.

| Module | Description |
|--------|-------------|
| [`Indexer.Event`][s-idx-event] | `detectCageEvents` -- classify cage-relevant transactions into `CageEvent` |
| [`Indexer.Follower`][s-idx-follower] | `processCageBlock`, `applyCageEvent`, `applyCageInverses` -- generic over `Monad m` |
| [`Indexer.CageFollower`][s-idx-cage] | Unified `rollForward`/`rollBackward` -- one block = one RocksDB transaction |
| [`Indexer.Columns`][s-idx-columns] | `AllColumns` + `UnifiedColumns` GADTs -- full DB schema |
| [`Indexer.Codecs`][s-idx-codecs] | CBOR serialization for column key-value types |
| [`Indexer.Persistent`][s-idx-persist] | `mkTransactionalState` (composable) + `mkPersistentState` (IO) |
| [`Indexer.Rollback`][s-idx-rollback] | `storeRollback`, `rollbackToSlot` -- slot-based rollback via inverse ops |

[s-idx-event]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Event%22&type=code
[s-idx-follower]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Follower%22&type=code
[s-idx-cage]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.CageFollower%22&type=code
[s-idx-columns]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Columns%22&type=code
[s-idx-codecs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Codecs%22&type=code
[s-idx-persist]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Persistent%22&type=code
[s-idx-rollback]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Rollback%22&type=code

---

## Trie Management

Per-token MPF trie backends. Each cage token has an isolated trie.

| Module | Description |
|--------|-------------|
| [`Trie`][s-trie] | `TrieManager` and `Trie m` record interfaces |
| [`Trie.Pure`][s-trie-pure] | In-memory `IORef MPFInMemoryDB` trie for testing |
| [`Trie.PureManager`][s-trie-pm] | `mkPureTrieManager` -- in-memory `TrieManager` backed by `Map TokenId` |
| [`Trie.Persistent`][s-trie-pers] | `mkPersistentTrieManager` -- RocksDB with token-prefixed keys, speculative sessions |

[s-trie-pure]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.Pure%22&type=code
[s-trie-pm]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.PureManager%22&type=code
[s-trie-pers]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.Persistent%22&type=code

---

## Transaction Building

Real `TxBuilder` implementations for all cage protocol operations.
Each builds a full transaction with PlutusV3 script witnesses.

| Module | Description |
|--------|-------------|
| [`TxBuilder.Config`][s-txb-cfg] | `CageConfig` -- script bytes, hash, time windows, network, slot params |
| [`TxBuilder.Real`][s-txb-real] | `mkRealTxBuilder` entry point wiring Config + Provider + State + TrieManager |
| [`TxBuilder.Real.Boot`][s-txb-boot] | Mint cage token (pick seed UTxO, derive asset name, build +1 mint) |
| [`TxBuilder.Real.Request`][s-txb-req] | Submit insert/delete request (pay to cage address with `RequestDatum`) |
| [`TxBuilder.Real.Update`][s-txb-upd] | Consume requests, compute proofs speculatively, update root |
| [`TxBuilder.Real.Retract`][s-txb-ret] | Cancel pending request (spend with `Retract` redeemer) |
| [`TxBuilder.Real.End`][s-txb-end] | Burn cage token (consume state with `End`, mint -1 with `Burning`) |
| [`TxBuilder.Real.Internal`][s-txb-int] | Shared helpers: POSIX-to-slot, UTxO finders, datum extraction, redeemer indexing |

[s-txb-cfg]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Config%22&type=code
[s-txb-real]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real%22+path%3AReal.hs&type=code
[s-txb-boot]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.Boot%22&type=code
[s-txb-req]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.Request%22&type=code
[s-txb-upd]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.Update%22&type=code
[s-txb-ret]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.Retract%22&type=code
[s-txb-end]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.End%22&type=code
[s-txb-int]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real.Internal%22&type=code

---

## Node Integration

N2C protocol clients for Cardano node communication.

| Module | Description |
|--------|-------------|
| [`Provider.NodeClient`][s-prv-nc] | `mkNodeClientProvider` -- N2C LocalStateQuery for UTxOs and PParams |
| [`Submitter.N2C`][s-sub-n2c] | `mkN2CSubmitter` -- N2C LocalTxSubmission for transaction submission |

[s-prv-nc]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Provider.NodeClient%22&type=code
[s-sub-n2c]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Submitter.N2C%22&type=code

---

## Application Wiring

| Module | Description |
|--------|-------------|
| [`Application`][s-app] | `withApplication` -- creates and wires all components with bracket lifecycle |
| [`Trace`][s-trace] | `AppTrace` structured tracing, `jsonLinesTracer` for stderr JSON-lines logging |

[s-app]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Application%22&type=code
[s-trace]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trace%22&type=code

---

## Testing

### Mock Implementations

Test doubles for all interfaces. No node connection required.

| Module | Description |
|--------|-------------|
| [`Mock.Context`][s-mock-ctx] | `mkMockContext` -- wires all mocks into `Context IO` |
| [`Mock.State`][s-mock-st] | `mkMockState` -- `IORef (Map k v)` per sub-record |
| [`Mock.Provider`][s-mock-prv] | `mkMockProvider` -- returns empty UTxO sets |
| [`Mock.Submitter`][s-mock-sub] | `mkMockSubmitter` -- rejects all transactions |
| [`Mock.TxBuilder`][s-mock-txb] | `mkMockTxBuilder` -- throws on all operations |
| [`Mock.Indexer`][s-mock-idx] | `mkMockIndexer` -- no-op lifecycle |
| [`Mock.Skeleton`][s-mock-skel] | `mkSkeletonIndexer` -- IORef/MVar tracking, no chain sync |

[s-mock-ctx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Context%22&type=code
[s-mock-st]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.State%22&type=code
[s-mock-prv]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Provider%22&type=code
[s-mock-sub]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Submitter%22&type=code
[s-mock-txb]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.TxBuilder%22&type=code
[s-mock-idx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Indexer%22&type=code
[s-mock-skel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Skeleton%22&type=code

### Unit test structure

Tests under `test/Cardano/MPFS/`:

- `BalanceSpec`, `OnChainSpec`, `ProofSpec` -- core logic
- `StateSpec`, `TrieSpec`, `TrieManagerSpec`, `TxBuilderSpec` -- interface specs
- `Indexer/EventSpec`, `Indexer/FollowerSpec`, `Indexer/CodecsSpec`, `Indexer/PersistentSpec`, `Indexer/RollbackSpec`, `Indexer/InverseSpec` -- indexer pipeline
- `Trie/PersistentSpec` -- RocksDB trie backend
- `HTTP/StatusSpec`, `HTTP/TokensSpec`, `HTTP/TokenSpec`, `HTTP/RequestsSpec`, `HTTP/TrieSpec` -- REST API via WAI test sessions

### E2E test structure

Tests under `e2e-test/Cardano/MPFS/E2E/`:

- `ProviderSpec` -- N2C LocalStateQuery
- `SubmitterSpec` -- build, balance, sign, submit ADA transfer
- `CageSpec` -- cage event detection
- `CageFlowSpec` -- full cage flow with CageFollower
- `IndexerSpec` -- detectFromTx + applyCageEvent
- `ChainSyncSpec` -- chain sync protocol
- `HTTPLifecycleSpec` -- full token lifecycle via HTTP API

---

## Directory Tree

```
cardano-mpfs-offchain/
  lib/Cardano/MPFS/
    Core/
      Types.hs          Blueprint.hs
      OnChain.hs        Proof.hs
    HTTP/
      API.hs            Encoding.hs
      Types.hs          Server.hs
      Swagger.hs
    Indexer/
      Event.hs          Follower.hs
      CageFollower.hs   Columns.hs
      Codecs.hs         Persistent.hs
      Rollback.hs
    Mock/
      Context.hs        State.hs
      Provider.hs       Submitter.hs
      TxBuilder.hs      Indexer.hs
      Skeleton.hs
    Provider/
      NodeClient.hs
    Submitter/
      N2C.hs
    Trie/
      Pure.hs           PureManager.hs
      Persistent.hs
    TxBuilder/
      Config.hs         Real.hs
      Real/
        Boot.hs         Request.hs
        Update.hs       Retract.hs
        End.hs          Internal.hs
    Application.hs      Context.hs
    Indexer.hs          Provider.hs
    State.hs            Submitter.hs
    Trace.hs            Trie.hs
    TxBuilder.hs
  test/Cardano/MPFS/
    HTTP/               Indexer/
    Trie/               BalanceSpec.hs
    OnChainSpec.hs      ProofSpec.hs
    StateSpec.hs        TrieSpec.hs
    TrieManagerSpec.hs  TxBuilderSpec.hs
  e2e-test/Cardano/MPFS/E2E/
    ProviderSpec.hs     SubmitterSpec.hs
    CageSpec.hs         CageFlowSpec.hs
    IndexerSpec.hs      ChainSyncSpec.hs
    HTTPLifecycleSpec.hs
```
