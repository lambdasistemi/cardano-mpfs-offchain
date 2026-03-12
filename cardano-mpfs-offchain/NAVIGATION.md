# cardano-mpfs-offchain — Semantic Navigation

A human-readable map of the offchain library. Each section explains
*what* the code does and *why*, with links to the source.

Base module: `Cardano.MPFS`

[lib]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS

---

## Table of Contents

1. [Core Domain](#core-domain)
2. [Service Interfaces](#service-interfaces)
3. [Node-to-Client Protocol](#node-to-client-protocol)
4. [Transaction Builders](#transaction-builders)
5. [Chain Indexer](#chain-indexer)
6. [Trie Backends](#trie-backends)
7. [Mock Implementations](#mock-implementations)
8. [Application Wiring](#application-wiring)

---

## Core Domain

Pure types and logic with no IO dependencies. Everything else imports
from here.

### Types

Ledger re-exports and MPFS-specific domain types.

| Type | Purpose |
|------|---------|
| [`TokenId`][s-TokenId] | Newtype over `AssetName` — identifies a cage token |
| [`Root`][s-Root] | 32-byte Blake2b-256 Merkle root hash |
| [`Request`][s-Request] | Pending insert/delete/update request with fee and timestamp |
| [`TokenState`][s-TokenState] | On-chain token state: owner, root, fee, time windows |
| [`Operation`][s-Operation] | `Insert` / `Delete` / `Update` — what a request asks for |
| [`Fact`][s-Fact] | Key-value pair stored in a trie |
| [`BlockId`][s-BlockId] | Block identifier (hash bytes) |

-> [Core/Types.hs:L69-L136][f-types]

[s-TokenId]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22newtype+TokenId%22&type=code
[s-Root]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22newtype+Root%22+path%3ACore&type=code
[s-Request]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Request%22+path%3ACore%2FTypes&type=code
[s-TokenState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+TokenState%22+path%3ACore%2FTypes&type=code
[s-Operation]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Operation%22+path%3ACore&type=code
[s-Fact]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Fact%22&type=code
[s-BlockId]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22newtype+BlockId%22&type=code
[f-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Types.hs#L69-L136

### OnChain

Plutus-level datum/redeemer types matching the Aiken validator
layout byte-for-byte. Manual `ToData`/`FromData` instances.

| Type | Purpose |
|------|---------|
| [`CageDatum`][s-CageDatum] | `RequestDatum` or `StateDatum` — inline datum on cage UTxOs |
| [`MintRedeemer`][s-MintRedeemer] | `Minting` / `Migrating` / `Burning` — cage policy redeemer |
| [`UpdateRedeemer`][s-UpdateRedeemer] | `End` / `Contribute` / `Modify` / `Retract` / `Reject` — spending redeemer |
| [`ProofStep`][s-ProofStep] | `Branch` / `Fork` / `Leaf` — on-chain Merkle proof step |

Key functions:

- **[`cageScriptHash`][s-cageScriptHash]**: Cage validator script hash (hardcoded from blueprint)
  -> [Core/OnChain.hs:L668-L710][f-onchain-hash]
- **[`cageAddr`][s-cageAddr]**: Derive cage script address for a network
  -> [Core/OnChain.hs:L713-L718][f-onchain-addr]
- **[`deriveAssetName`][s-deriveAssetName]**: SHA2-256(txId ++ bigEndian16(idx)) — deterministic token name
  -> [Core/OnChain.hs:L728-L747][f-onchain-derive]

[s-CageDatum]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+CageDatum%22&type=code
[s-MintRedeemer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+MintRedeemer%22&type=code
[s-UpdateRedeemer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+UpdateRedeemer%22&type=code
[s-ProofStep]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+ProofStep%22+path%3AOnChain&type=code
[s-cageScriptHash]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=cageScriptHash+path%3AOnChain&type=code
[s-cageAddr]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=cageAddr+path%3AOnChain&type=code
[s-deriveAssetName]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=deriveAssetName&type=code
[f-onchain-hash]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/OnChain.hs#L668-L710
[f-onchain-addr]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/OnChain.hs#L713-L718
[f-onchain-derive]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/OnChain.hs#L728-L747

### Blueprint

CIP-57 Plutus blueprint loading and schema validation.

- **[`loadBlueprint`][s-loadBlueprint]**: Parse blueprint JSON from file
  -> [Core/Blueprint.hs:L198-L202][f-blueprint-load]
- **[`extractCompiledCode`][s-extractCompiledCode]**: Extract hex-decoded PlutusV3 script bytes
  -> [Core/Blueprint.hs:L262-L274][f-blueprint-extract]
- **[`validateData`][s-validateData]**: Validate `Data` against `Schema` with `$ref` resolution
  -> [Core/Blueprint.hs:L210-L236][f-blueprint-validate]
- **[`applyVersion`][s-applyVersion]**: Apply version parameter to UPLC script
  -> [Core/Blueprint.hs:L312-L346][f-blueprint-version]

[s-loadBlueprint]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=loadBlueprint&type=code
[s-extractCompiledCode]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=extractCompiledCode&type=code
[s-validateData]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=validateData+path%3ABlueprint&type=code
[s-applyVersion]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=applyVersion&type=code
[f-blueprint-load]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Blueprint.hs#L198-L202
[f-blueprint-extract]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Blueprint.hs#L262-L274
[f-blueprint-validate]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Blueprint.hs#L210-L236
[f-blueprint-version]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Blueprint.hs#L312-L346

### Proof

Aiken-compatible proof serialization — converts MPF proofs to
on-chain `ProofStep` lists and raw CBOR bytes.

- **[`serializeProof`][s-serializeProof]**: MPF proof -> CBOR bytes (byte-identical to TypeScript reference)
  -> [Core/Proof.hs:L54-L63][f-proof-ser]
- **[`toProofSteps`][s-toProofSteps]**: MPF proof -> `[ProofStep]` (reverses leaf-to-root to root-to-leaf)
  -> [Core/Proof.hs:L147-L150][f-proof-steps]

[s-serializeProof]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=serializeProof&type=code
[s-toProofSteps]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=toProofSteps&type=code
[f-proof-ser]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Proof.hs#L54-L63
[f-proof-steps]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Proof.hs#L147-L150

### Balance

Fee estimation fixpoint loop. Adds a fee-paying UTxO and change
output, iterates `setMinFeeTx` until stable (max 10 rounds).

- **[`balanceTx`][s-balanceTx]**: Pure transaction balancer
  -> [Core/Balance.hs:L64-L153][f-balance]

[s-balanceTx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=balanceTx+path%3ABalance&type=code
[f-balance]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Balance.hs#L64-L153

### Bootstrap

CBOR bootstrap file for UTxO seeding on fresh databases.
Stream-decodes entries without loading the full map into memory.

- **[`encodeBootstrapFile`][s-encodeBootstrap]**: Write header + key-value entries to CBOR file
  -> [Core/Bootstrap.hs:L69-L90][f-bootstrap-enc]
- **[`foldBootstrapEntries`][s-foldBootstrap]**: Stream-decode with callbacks (header, then each entry)
  -> [Core/Bootstrap.hs:L96-L135][f-bootstrap-fold]

[s-encodeBootstrap]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=encodeBootstrapFile&type=code
[s-foldBootstrap]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=foldBootstrapEntries&type=code
[f-bootstrap-enc]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Bootstrap.hs#L69-L90
[f-bootstrap-fold]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Bootstrap.hs#L96-L135

---

## Service Interfaces

Every major component is a **record of functions**, polymorphic in
`m`. No typeclasses — dependencies are explicit values.

### Provider

Read-only blockchain queries.

| Field | Signature |
|-------|-----------|
| `queryUTxOs` | `Addr -> m [(TxIn, TxOut ConwayEra)]` |
| `queryProtocolParams` | `m (PParams ConwayEra)` |
| `evaluateTx` | `ByteString -> m ExUnits` |

-> [Provider.hs:L24-L36][f-provider]

Real: [`mkNodeClientProvider`][s-mkNodeClientProvider] (N2C LSQ)
Mock: [`mkMockProvider`][s-mkMockProvider] (empty results)

[f-provider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Provider.hs#L24-L36
[s-mkNodeClientProvider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkNodeClientProvider&type=code
[s-mkMockProvider]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockProvider&type=code

### State

Token and request state with three sub-records.

| Sub-record | Key operations |
|------------|----------------|
| [`Tokens`][s-Tokens] | `getToken`, `putToken`, `removeToken`, `listTokens` |
| [`Requests`][s-Requests] | `getRequest`, `putRequest`, `removeRequest`, `requestsByToken` |
| [`Checkpoints`][s-Checkpoints] | `getCheckpoint`, `putCheckpoint` |

-> [State.hs:L30-L77][f-state]

Real: [`mkPersistentState`][s-mkPersistentState] (RocksDB)
Mock: [`mkMockState`][s-mkMockState] (IORef maps)

[s-Tokens]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Tokens%22&type=code
[s-Requests]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Requests%22&type=code
[s-Checkpoints]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Checkpoints%22&type=code
[f-state]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/State.hs#L30-L77
[s-mkPersistentState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPersistentState&type=code
[s-mkMockState]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockState&type=code

### TrieManager

Per-token MPF trie access. Each cage token has an isolated trie.

| Field | Purpose |
|-------|---------|
| `withTrie` | Run action with token's trie |
| `withSpeculativeTrie` | Speculative session (copied DB, rolled back on exit) |
| `createTrie` / `deleteTrie` | Lifecycle |
| `hideTrie` / `unhideTrie` | Soft-delete for burn forward/rollback |

The [`Trie m`][s-Trie] record exposes: `insert`, `delete`, `lookup`,
`getRoot`, `getProof`, `getProofSteps`.

-> [Trie.hs:L32-L88][f-trie]

Real: [`mkPersistentTrieManager`][s-mkPersistentTrieManager] (RocksDB, token-prefixed keys)
Pure: [`mkPureTrieManager`][s-mkPureTrieManager] (IORef maps)

[s-Trie]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+Trie+m%22&type=code
[f-trie]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie.hs#L32-L88
[s-mkPersistentTrieManager]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPersistentTrieManager&type=code
[s-mkPureTrieManager]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPureTrieManager&type=code

### Submitter

Transaction submission. Takes a `Tx ConwayEra`, returns `Submitted TxId`
or `Rejected reason`.

-> [Submitter.hs:L20-L35][f-submitter]

Real: [`mkN2CSubmitter`][s-mkN2CSubmitter] (N2C LocalTxSubmission)
Mock: [`mkMockSubmitter`][s-mkMockSubmitter] (rejects all)

[f-submitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Submitter.hs#L20-L35
[s-mkN2CSubmitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkN2CSubmitter&type=code
[s-mkMockSubmitter]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockSubmitter&type=code

### TxBuilder

Constructs transactions for all cage protocol operations.

| Field | Cage operation |
|-------|----------------|
| `bootToken` | Mint new cage token |
| `requestInsert` / `requestDelete` | Submit request UTxO |
| `updateToken` | Consume requests, update trie root |
| `retractRequest` | Cancel pending request |
| `endToken` | Burn cage token |

-> [TxBuilder.hs:L24-L56][f-txbuilder]

Real: [`mkRealTxBuilder`][s-mkRealTxBuilder]
Mock: [`mkMockTxBuilder`][s-mkMockTxBuilder] (throws on all ops)

[f-txbuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs#L24-L56
[s-mkRealTxBuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkRealTxBuilder&type=code
[s-mkMockTxBuilder]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockTxBuilder&type=code

### Indexer

Chain sync follower with lifecycle control.

| Field | Purpose |
|-------|---------|
| `start` / `stop` | Follower lifecycle |
| `pause` / `resume` | Temporary suspension |
| `getTip` | Current [`ChainTip`][s-ChainTip] (slot + block hash) |

-> [Indexer.hs:L16-L36][f-indexer]

Real: [`Indexer.Follower`][s-Follower] (block processing)
Mock: [`mkSkeletonIndexer`][s-mkSkeletonIndexer] (no-op) / [`mkMockIndexer`][s-mkMockIndexer]

[s-ChainTip]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+ChainTip%22&type=code
[f-indexer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer.hs#L16-L36
[s-Follower]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Follower%22&type=code
[s-mkSkeletonIndexer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkSkeletonIndexer&type=code
[s-mkMockIndexer]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockIndexer&type=code

### Context

Facade record bundling all singletons into one environment.

-> [Context.hs:L19-L32][f-context]

[f-context]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Context.hs#L19-L32

---

## Node-to-Client Protocol

Multiplexed N2C connection over a Unix socket to `cardano-node`.
Two mini-protocols share one connection.

### Connection

- **[`runNodeClient`][s-runNodeClient]**: Establish multiplexed connection, launch protocol threads
  -> [NodeClient/Connection.hs:L122-L143][f-nc-conn]
- **[`newLSQChannel`][s-newLSQChannel]** / **[`newLTxSChannel`][s-newLTxSChannel]**: Create `TBQueue`-backed channels
  -> [NodeClient/Connection.hs:L234-L244][f-nc-chan]

[s-runNodeClient]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=runNodeClient+path%3AConnection&type=code
[s-newLSQChannel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=newLSQChannel&type=code
[s-newLTxSChannel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=newLTxSChannel&type=code
[f-nc-conn]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/NodeClient/Connection.hs#L122-L143
[f-nc-chan]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/NodeClient/Connection.hs#L234-L244

### Channel Types

| Type | Purpose |
|------|---------|
| [`LSQChannel`][s-LSQChannel] | `TBQueue` of `(Query, TMVar result)` pairs |
| [`LTxSChannel`][s-LTxSChannel] | `TBQueue` of `(GenTx, TMVar result)` pairs |
| [`SomeLSQQuery`][s-SomeLSQQuery] | Existential wrapper hiding the query result type |

-> [NodeClient/Types.hs:L36-L75][f-nc-types]

[s-LSQChannel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22newtype+LSQChannel%22&type=code
[s-LTxSChannel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22newtype+LTxSChannel%22&type=code
[s-SomeLSQQuery]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+SomeLSQQuery%22&type=code
[f-nc-types]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/NodeClient/Types.hs#L36-L75

### LocalStateQuery

Protocol client driven by `LSQChannel`. Acquires volatile tip per
query (no long-lived acquired state).

- **[`queryLSQ`][s-queryLSQ]**: Submit query through channel, block on `TMVar` result
  -> [NodeClient/LocalStateQuery.hs:L124-L133][f-lsq]

[s-queryLSQ]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=queryLSQ&type=code
[f-lsq]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/NodeClient/LocalStateQuery.hs#L124-L133

### LocalTxSubmission

- **[`submitTxN2C`][s-submitTxN2C]**: Submit `GenTx` through channel, block on result
  -> [NodeClient/LocalTxSubmission.hs:L77-L89][f-ltxs]

[s-submitTxN2C]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=submitTxN2C&type=code
[f-ltxs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/NodeClient/LocalTxSubmission.hs#L77-L89

---

## Transaction Builders

Real implementations for all cage protocol operations. Each builds
a full `Tx ConwayEra` with PlutusV3 script witnesses, inline datums,
and validity intervals, then balances via [`balanceTx`][s-balanceTx].

### Config

[`CageConfig`][s-CageConfig]: Script bytes, script hash, time windows,
network, slot parameters.

-> [TxBuilder/Config.hs:L24-L41][f-txb-cfg]

[s-CageConfig]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+CageConfig%22&type=code
[f-txb-cfg]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Config.hs#L24-L41

### Entry Point

- **[`mkRealTxBuilder`][s-mkRealTxBuilder]**: Wire Config + Provider + State + TrieManager into `TxBuilder IO`
  -> [TxBuilder/Real.hs:L56-L74][f-txb-real]

[f-txb-real]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs#L56-L74

### Per-Operation Builders

| Module | Operation | Key steps |
|--------|-----------|-----------|
| [`Real.Boot`][f-txb-boot] | Mint cage token | Pick seed UTxO, derive asset name, build +1 mint, create state datum |
| [`Real.Request`][f-txb-req] | Submit request | Pay to cage address with `RequestDatum`, no script execution |
| [`Real.Update`][f-txb-upd] | Process requests | Find state + request UTxOs, compute proofs speculatively, build `Modify` redeemer with proof lists |
| [`Real.Retract`][f-txb-ret] | Cancel request | Spend request UTxO with `Retract` redeemer, reference state UTxO, Phase 2 validity window |
| [`Real.End`][f-txb-end] | Burn token | Consume state with `End` spending, mint -1 with `Burning` minting |

[f-txb-boot]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs#L80-L200
[f-txb-req]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs#L56-L161
[f-txb-upd]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs#L95-L284
[f-txb-ret]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs#L83-L224
[f-txb-end]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs#L75-L178

### Internal Helpers

Shared helpers used by all builders.

| Function | Purpose |
|----------|---------|
| [`posixMsToSlot`][s-posixMsToSlot] / [`posixMsCeilSlot`][s-posixMsCeilSlot] | POSIX ms -> `SlotNo` (floor/ceiling) |
| [`findStateUtxo`][s-findStateUtxo] | Find state UTxO by policy ID + token ID in MultiAsset |
| [`findRequestUtxos`][s-findRequestUtxos] | Find request UTxOs by decoding inline datums |
| [`extractCageDatum`][s-extractCageDatum] | Extract `CageDatum` from inline datum in `TxOut` |
| [`mkRequestDatum`][s-mkRequestDatum] | Build `CageDatum` for request submission |
| [`spendingIndex`][s-spendingIndex] | Compute redeemer index of `TxIn` in sorted input set |
| [`computeScriptIntegrity`][s-computeScriptIntegrity] | Compute `ScriptIntegrityHash` for tx body |

-> [TxBuilder/Real/Internal.hs][f-txb-int]

[s-posixMsToSlot]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=posixMsToSlot&type=code
[s-posixMsCeilSlot]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=posixMsCeilSlot&type=code
[s-findStateUtxo]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=findStateUtxo&type=code
[s-findRequestUtxos]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=findRequestUtxos&type=code
[s-extractCageDatum]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=extractCageDatum&type=code
[s-mkRequestDatum]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkRequestDatum&type=code
[s-spendingIndex]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=spendingIndex+path%3AInternal&type=code
[s-computeScriptIntegrity]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=computeScriptIntegrity&type=code
[f-txb-int]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Internal.hs

---

## Chain Indexer

Block-by-block processing of cage transactions with rollback support.
All state changes in one block commit in a single RocksDB WriteBatch.

### Event Detection

[`CageEvent`][s-CageEvent] classifies cage-relevant transactions:

| Constructor | Signal |
|-------------|--------|
| `CageBoot` | Mint +1 under cage policy |
| `CageRequest` | Output to cage address with `RequestDatum` |
| `CageUpdate` | Cage input with `Modify` redeemer |
| `CageRetract` | Cage input with `Retract` redeemer |
| `CageBurn` | Mint -1 under cage policy |

- **[`detectCageEvents`][s-detectCageEvents]**: Extract events from a transaction
  -> [Indexer/Event.hs:L146-L154][f-idx-detect]
- **[`inversesOf`][s-inversesOf]**: Compute [`CageInverseOp`][s-CageInverseOp] for rollback
  -> [Indexer/Event.hs:L346-L380][f-idx-inverse]

[s-CageEvent]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+CageEvent%22&type=code
[s-detectCageEvents]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=detectCageEvents&type=code
[s-inversesOf]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=inversesOf&type=code
[s-CageInverseOp]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+CageInverseOp%22&type=code
[f-idx-detect]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Event.hs#L146-L154
[f-idx-inverse]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Event.hs#L346-L380

### Block Processor (Follower)

Processes each block: extract Conway txs, detect cage events, apply
to state and trie, record inverse ops for rollback.

- **[`processCageBlock`][s-processCageBlock]**: Process full block for cage events
  -> [Indexer/Follower.hs:L86-L107][f-follower-block]
- **[`applyCageEvent`][s-applyCageEvent]**: Apply single event to State + TrieManager
  -> [Indexer/Follower.hs:L200-L253][f-follower-apply]
- **[`applyCageInverses`][s-applyCageInverses]**: Replay inverse ops for rollback
  -> [Indexer/Follower.hs:L310-L342][f-follower-inv]

[s-processCageBlock]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=processCageBlock&type=code
[s-applyCageEvent]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=applyCageEvent&type=code
[s-applyCageInverses]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=applyCageInverses&type=code
[f-follower-block]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Follower.hs#L86-L107
[f-follower-apply]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Follower.hs#L200-L253
[f-follower-inv]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Follower.hs#L310-L342

### Column Families

[`AllColumns`][s-AllColumns] GADT — type-safe column family selectors for
the shared RocksDB instance.

| Column | Key | Value |
|--------|-----|-------|
| `CageTokens` | `TokenId` | `TokenState` |
| `CageRequests` | `TxIn` | `Request` |
| `CageCfg` | `()` | [`CageCheckpoint`][s-CageCheckpoint] |
| `CageRollbacks` | `SlotNo` | `[CageInverseOp]` |
| `TrieNodes` | prefixed bytes | trie node data |
| `TrieKV` | prefixed bytes | key-value data |

-> [Indexer/Columns.hs:L49-L95][f-columns]

[s-AllColumns]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+AllColumns%22&type=code
[s-CageCheckpoint]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=%22data+CageCheckpoint%22&type=code
[f-columns]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Columns.hs#L49-L95

### Codecs

CBOR serialization for all column key-value types. Uses
`rocksdb-kv-transactions` `Prism` pattern.

- **[`allCodecs`][s-allCodecs]**: `DMap` of codecs keyed by `AllColumns`
  -> [Indexer/Codecs.hs:L93-L126][f-codecs]

[s-allCodecs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=allCodecs+path%3ACodecs&type=code
[f-codecs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Codecs.hs#L93-L126

### Rollback

Slot-based rollback by replaying stored inverse operations.

- **[`storeRollback`][s-storeRollback]**: Persist inverse ops for a slot
  -> [Indexer/Rollback.hs:L46-L56][f-rollback-store]
- **[`rollbackToSlot`][s-rollbackToSlot]**: Replay inverses from current tip back to target slot
  -> [Indexer/Rollback.hs:L80-L113][f-rollback-to]

[s-storeRollback]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=storeRollback&type=code
[s-rollbackToSlot]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=rollbackToSlot&type=code
[f-rollback-store]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Rollback.hs#L46-L56
[f-rollback-to]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Rollback.hs#L80-L113

### Persistent State

RocksDB-backed `State IO` implementation.

- **[`mkPersistentState`][s-mkPersistentState]**: Build State from RocksDB column families
  -> [Indexer/Persistent.hs:L48-L56][f-persistent]

[f-persistent]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Persistent.hs#L48-L56

---

## Trie Backends

Three `TrieManager` implementations sharing the same interface.

### Pure (in-memory)

`IORef MPFInMemoryDB` per token. Good for testing.

- **[`mkPureTrie`][s-mkPureTrie]**: New empty trie backed by fresh IORef
  -> [Trie/Pure.hs:L51-L54][f-trie-pure]
- **[`mkPureTrieManager`][s-mkPureTrieManager]**: Manager backed by `Map TokenId (IORef MPFInMemoryDB)`
  -> [Trie/PureManager.hs:L39-L57][f-trie-pm]

[s-mkPureTrie]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPureTrie&type=code
[f-trie-pure]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs#L51-L54
[f-trie-pm]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/PureManager.hs#L39-L57

### Persistent (RocksDB)

Token-prefixed keys in shared column families. Supports speculative
sessions via transactional snapshots.

- **[`mkPersistentTrieManager`][s-mkPersistentTrieManager]**: Manager from DB + two column families (nodes, kv)
  -> [Trie/Persistent.hs:L117-L162][f-trie-pers]
- **[`mkPrefixedTrieDB`][s-mkPrefixedTrieDB]**: Prefixed `Database` with transparent key wrapping
  -> [Trie/Persistent.hs:L365-L409][f-trie-prefixed]
- **[`tokenPrefix`][s-tokenPrefix]**: Serialize `TokenId` to prefix bytes
  -> [Trie/Persistent.hs:L206-L210][f-trie-prefix]

[s-mkPrefixedTrieDB]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkPrefixedTrieDB&type=code
[s-tokenPrefix]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=tokenPrefix+path%3APersistent&type=code
[f-trie-pers]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs#L117-L162
[f-trie-prefixed]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs#L365-L409
[f-trie-prefix]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs#L206-L210

---

## Mock Implementations

Test doubles for all interfaces. No IO side effects, no node
connection required.

| Module | Constructor | Behavior |
|--------|-------------|----------|
| [`Mock.Context`][f-mock-ctx] | [`mkMockContext`][s-mkMockContext] | Wires all mocks into `Context IO` |
| [`Mock.State`][f-mock-st] | [`mkMockState`][s-mkMockState] | `IORef (Map k v)` per sub-record |
| [`Mock.Provider`][f-mock-prv] | [`mkMockProvider`][s-mkMockProvider] | Returns empty UTxO sets |
| [`Mock.Submitter`][f-mock-sub] | [`mkMockSubmitter`][s-mkMockSubmitter] | Rejects all transactions |
| [`Mock.TxBuilder`][f-mock-txb] | [`mkMockTxBuilder`][s-mkMockTxBuilder] | Throws on all operations |
| [`Mock.Indexer`][f-mock-idx] | [`mkMockIndexer`][s-mkMockIndexer] | No-op lifecycle |
| [`Mock.Skeleton`][f-mock-skel] | [`mkSkeletonIndexer`][s-mkSkeletonIndexer] | IORef/MVar tracking, no chain sync |

[s-mkMockContext]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=mkMockContext&type=code
[f-mock-ctx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Context.hs
[f-mock-st]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/State.hs
[f-mock-prv]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Provider.hs
[f-mock-sub]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Submitter.hs
[f-mock-txb]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/TxBuilder.hs
[f-mock-idx]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Indexer.hs
[f-mock-skel]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/Skeleton.hs

---

## Application Wiring

[`withApplication`][s-withApplication] creates the full `Context IO`:

1. Open RocksDB with all [column families][f-app-cfs]
2. Create persistent State + TrieManager
3. Seed from bootstrap file if fresh DB
4. Open N2C connection (LSQ + LTxS channels)
5. Wire Provider, Submitter, TxBuilder
6. Bundle into `Context IO`
7. Bracket: tear down on exit

-> [Application.hs:L123-L190][f-app]

| Config field | Purpose |
|--------------|---------|
| `networkMagic` | Mainnet / testnet identifier |
| `socketPath` | cardano-node Unix socket |
| `dbPath` | RocksDB directory |
| `cageConfig` | Script bytes, time windows, slot params |
| `bootstrapFile` | Optional CBOR file for fresh DB seeding |

-> [Application.hs:L76-L89][f-app-cfg]

[s-withApplication]: https://github.com/lambdasistemi/cardano-mpfs-offchain/search?q=withApplication&type=code
[f-app]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs#L123-L190
[f-app-cfg]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs#L76-L89
[f-app-cfs]: https://github.com/lambdasistemi/cardano-mpfs-offchain/blob/main/cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs#L107-L115
