# Phase 4 — Indexer + Persistence

## Goal

Replace in-memory state (Mock.State, PureManager, Skeleton
Indexer) with persistent, crash-safe storage backed by a
single RocksDB instance shared with cardano-utxo-csmt. Every
block is processed in one atomic WriteBatch covering UTxO
index, cage state, and trie mutations.

## Design principle: one WriteBatch per block

The central invariant is **atomicity at the block boundary**.
When a block arrives via ChainSync:

1. Extract UTxO changes (cardano-utxo-csmt)
2. Detect cage transactions (mpfs-offchain)
3. Apply trie mutations (merkle-patricia-forestry)
4. Commit all of the above in a single RocksDB WriteBatch

If the process crashes at any point before the WriteBatch
commits, no partial state is visible on recovery. If it
crashes after the commit, all state is consistent.

This is possible because `rocksdb-kv-transactions` was
designed for exactly this: composable, multi-column-family
atomic transactions over a shared RocksDB instance.

## Architecture

```
Block (via N2C ChainSync)
  │
  ▼
┌──────────────────────────────────────────────────┐
│  Single RocksDB Transaction (WriteBatch)         │
│                                                  │
│  ┌─────────────┐  ┌──────────┐  ┌────────────┐  │
│  │ UTxO CFs    │  │ Cage CFs │  │ Trie CFs   │  │
│  │ kv          │  │ tokens   │  │ trie-nodes │  │
│  │ csmt        │  │ requests │  │ trie-kv    │  │
│  │ rollbacks   │  │ cage-cfg │  │            │  │
│  │ config      │  │          │  │            │  │
│  └─────────────┘  └──────────┘  └────────────┘  │
│                                                  │
│  applyOps → single WriteBatch → atomic commit    │
└──────────────────────────────────────────────────┘
```

### Column families

| Column family | Owner | Key | Value |
|---------------|-------|-----|-------|
| `kv` | cardano-utxo-csmt | CBOR TxIn | CBOR TxOut |
| `csmt` | cardano-utxo-csmt | CSMT Key | CSMT Indirect hash |
| `rollbacks` | cardano-utxo-csmt | WithOrigin SlotNo | RollbackPoint |
| `config` | cardano-utxo-csmt | ConfigKey | ByteString |
| `tokens` | mpfs-offchain | TokenId | TokenState |
| `requests` | mpfs-offchain | TxIn | Request |
| `cage-cfg` | mpfs-offchain | () | CageCheckpoint |
| `trie-nodes` | merkle-patricia-forestry | HexKey | HexIndirect hash |
| `trie-kv` | merkle-patricia-forestry | ByteString | ByteString |

### Combined Columns GADT

The GADT lives in mpfs-offchain since it is the application
that owns the full schema. It extends cardano-utxo-csmt's
columns with cage-specific and trie-specific ones:

```haskell
data AllColumns x where
    -- cardano-utxo-csmt columns
    UTxOKV        :: AllColumns (KV LBS LBS)
    UTxOCSMT      :: AllColumns (KV CSMTKey CSMTIndirect)
    UTxORollbacks :: AllColumns (KV RollbackKey RollbackVal)
    UTxOConfig    :: AllColumns (KV ConfigKey LBS)
    -- cage state columns
    CageTokens    :: AllColumns (KV TokenId TokenState)
    CageRequests  :: AllColumns (KV TxIn Request)
    CageCfg       :: AllColumns (KV () CageCheckpoint)
    -- trie columns (per-token, prefixed)
    TrieNodes     :: AllColumns (KV LBS LBS)
    TrieKV        :: AllColumns (KV LBS LBS)
```

All column families are created when the RocksDB instance is
opened. The `Database` record from `rocksdb-kv-transactions`
provides a unified handle. Transactions compose operations on
any subset of columns and commit atomically.

## Block processing pipeline

### On rollForward

```
rollForward :: Fetched -> SlotNo -> IO (Follower Fetched)
  1. utxoOps   = uTxOs block          -- [Change]
  2. cageOps   = detectCage block      -- [CageEvent]
  3. runTransaction $ do
       -- UTxO index (delegated to cardano-utxo-csmt)
       applyUTxOOps slot utxoOps
       -- Cage state
       for_ cageOps $ \case
         CageBoot tokenId tokenState ->
           insert CageTokens tokenId tokenState
         CageRequest txIn request ->
           insert CageRequests txIn request
         CageUpdate tokenId newRoot removedReqs ->
           update CageTokens tokenId (setRoot newRoot)
           for_ removedReqs (delete CageRequests)
           applyTrieMutations tokenId ...
         CageRetract txIn ->
           delete CageRequests txIn
         CageBurn tokenId ->
           delete CageTokens tokenId
       -- Checkpoint
       insert CageCfg () (CageCheckpoint slot blockId)
```

### On rollBackward

```
rollBackward :: Point -> IO (ProgressOrRewind Fetched)
  1. Delegate to cardano-utxo-csmt's rollbackTipApply
     (reverts UTxO index via inverse operations)
  2. In the same transaction, revert cage state:
     - Restore tokens/requests from inverse operations
     - Restore trie state from checkpoint
  3. Atomic commit
```

### Cage event detection

A `CageEvent` is extracted by inspecting each transaction in
the block for interactions with the cage script address and
policy ID:

```haskell
data CageEvent
    = CageBoot TokenId TokenState
      -- ^ Mint with cage policyId → new token
    | CageRequest TxIn Request
      -- ^ Output to cage address with RequestDatum
    | CageUpdate TokenId Root [TxIn]
      -- ^ Input from cage address with Modify redeemer
    | CageRetract TxIn
      -- ^ Input from cage address with Retract redeemer
    | CageBurn TokenId
      -- ^ Burn with cage policyId → token removed
```

Detection rules:

| Event | Signal |
|-------|--------|
| **Boot** | Tx mints exactly +1 token under `cagePolicyId`. Decode the `StateDatum` from the new cage output. |
| **Request** | Tx creates output at `cageAddr` with `RequestDatum` inline datum. No script input required. |
| **Update** | Tx consumes cage UTxO with `Modify` spending redeemer. Decode new `StateDatum` from continuing output. Consumed `RequestDatum` inputs are the processed requests. |
| **Retract** | Tx consumes cage UTxO with `Retract` spending redeemer. The consumed `RequestDatum` input is removed. |
| **Burn** | Tx mints -1 token under `cagePolicyId`. |

The detection function needs access to:
- The transaction's minted values (for boot/burn)
- The transaction's outputs with inline datums (for request)
- The transaction's inputs + redeemers (for update/retract)
- The previous UTxO set (to resolve spent inputs)

Since we process blocks sequentially and the UTxO index is
updated in the same transaction, we can resolve spent inputs
from the UTxO index **before** applying the current block's
UTxO changes, or we can use cardano-utxo-csmt's
`Query.getValue` within the transaction workspace.

## Trie persistence

### Per-token trie isolation

Each cage token has its own MPF trie. On chain, the trie root
is stored in the `StateDatum`. Off chain, the trie nodes and
key-value pairs are stored in RocksDB.

**Key prefix strategy**: Trie column families are shared
across all tokens. Keys are prefixed with the `TokenId` to
isolate per-token data:

```
trie-nodes key = tokenId ++ originalKey
trie-kv key    = tokenId ++ originalKey
```

This avoids creating per-token column families (RocksDB
column families are heavyweight, not meant to be dynamic).

### Trie mutations in the transaction

When an `Update` cage event is detected, the block processor:

1. Reads the consumed `RequestDatum` inputs to get the
   operations (insert/delete key-value pairs)
2. Applies each operation to the token's trie within the
   same RocksDB transaction
3. The resulting root hash must match the `stateRoot` in the
   new `StateDatum` (this is enforced on-chain by the Aiken
   validator, but we verify it off-chain as a sanity check)

### TrieManager replacement

The current `TrieManager` interface uses in-memory
`IORef MPFInMemoryDB`. The persistent version wraps the
shared RocksDB transaction:

```haskell
mkPersistentTrieManager
    :: RunTransaction IO CF AllColumns BatchOp
    -> TrieManager IO
```

Operations (`insert`, `delete`, `lookup`, `getRoot`,
`getProof`) are translated to prefixed reads/writes on the
`TrieNodes` and `TrieKV` column families.

## Rollback strategy

### The problem

Rollbacks must revert three domains atomically:
1. UTxO index (handled by cardano-utxo-csmt's inverse ops)
2. Cage state (tokens, requests)
3. Trie state (per-token Merkle tries)

### The solution: inverse operations

Following cardano-utxo-csmt's pattern, we record **inverse
operations** for cage state changes at each block:

```haskell
data CageInverseOp
    = InvInsertToken TokenId TokenState
      -- ^ To undo a delete: re-insert
    | InvDeleteToken TokenId
      -- ^ To undo an insert: delete
    | InvInsertRequest TxIn Request
      -- ^ To undo a delete: re-insert
    | InvDeleteRequest TxIn
      -- ^ To undo an insert: delete
    | InvSetRoot TokenId Root
      -- ^ To undo a root change: restore old root
```

These are stored alongside cardano-utxo-csmt's rollback
points in the same `rollbacks` column family (or a parallel
`cage-rollbacks` column family).

On rollback to slot S:
1. Replay inverse operations from the rollback point
2. Restore trie state (see below)

### Trie rollback

Trie mutations are harder to invert because the MPF trie is a
Merkle structure — you can't just "undo" a hash. Two options:

**A) Store trie inverse ops** — For each trie mutation,
store the old key-value pair. On rollback, replay inverses.
Since the trie is deterministic (same inputs → same root),
replaying in reverse restores the correct state.

**B) Snapshot trie roots** — Store periodic snapshots of the
full trie. On rollback, restore from the nearest snapshot and
replay forward. More expensive in storage but simpler.

**Choice: A** — Store trie inverse operations. The trie is
deterministic, so replaying inverses in order produces the
correct root. This is consistent with how cardano-utxo-csmt
handles UTxO rollbacks.

## cardano-utxo-csmt integration

### Dependency setup

Add `cardano-utxo-csmt` as a `source-repository-package` in
`cabal.project`, along with its transitive dependencies:

```cabal
source-repository-package
  type: git
  location: https://github.com/lambdasistemi/cardano-utxo-csmt
  tag: <commit-hash>

source-repository-package
  type: git
  location: https://github.com/lambdasistemi/haskell-mts
  tag: <commit-hash>

source-repository-package
  type: git
  location: https://github.com/lambdasistemi/cardano-read-ledger
  tag: <commit-hash>

source-repository-package
  type: git
  location: https://github.com/lambdasistemi/contra-tracer-contrib
  tag: <commit-hash>
```

In `flake.nix`, no new inputs are needed — these are pure
Haskell packages resolved by haskell.nix from the cabal
project file.

### What we import

From `cardano-utxo-csmt` (library):
- `Cardano.UTxOCSMT.Ouroboros.ConnectionN2C` — N2C socket connection
- `Cardano.UTxOCSMT.Ouroboros.Types` — `Follower`, `Intersector`, `Block`, `Point`, `TipOf`
- `Cardano.UTxOCSMT.Application.ChainSyncN2C` — ChainSync client
- `Cardano.UTxOCSMT.Application.Database.Interface` — `Update`, `State`, `Query`, `Operation`
- `Cardano.UTxOCSMT.Application.Database.RocksDB` — DB setup
- `Cardano.UTxOCSMT.Application.UTxOs` — `uTxOs`, `Change`

### What we replace

| Before (Phase 3) | After (Phase 4) |
|-------------------|-----------------|
| `NodeClient.Connection` (custom N2C) | `ConnectionN2C.runLocalNodeApplication` |
| `NodeClient.LocalStateQuery` (LSQ for UTxOs) | `Query.getValue` on UTxO column |
| `Mock.State` (IORef maps) | RocksDB columns via `RunTransaction` |
| `Trie.PureManager` (IORef) | RocksDB columns with token-prefixed keys |
| `Mock.Skeleton` (no-op) | Real `Follower` processing blocks |

### What we keep

- `NodeClient.LocalTxSubmission` — still needed for tx
  submission (cardano-utxo-csmt doesn't submit)
- `NodeClient.LocalStateQuery` — still needed for
  `queryProtocolParams` (epoch-cached)
- `TxBuilder.Real.*` — transaction construction unchanged
- `Balance` — fee estimation unchanged

### Connection strategy

Two N2C connections to the same node socket:

1. **cardano-utxo-csmt connection** — ChainSync only
   (blocks arrive here, processed by our Follower)
2. **mpfs-offchain connection** — LocalTxSubmission +
   LocalStateQuery (tx submission + protocol params)

Both connect to the same Unix socket. The node handles
multiple client connections.

## Application wiring (updated)

```haskell
withApplication :: AppConfig -> (Context IO -> IO a) -> IO a
withApplication cfg action = do
    -- 1. Open shared RocksDB with all column families
    withSharedDB dbPath allColumnFamilies $ \db -> do
        -- 2. Create RunTransaction (MVar-serialized)
        runner <- newRunTransaction db

        -- 3. Initialize cardano-utxo-csmt state machine
        (utxoUpdate, utxoQuery) <- initUTxOCSMT runner

        -- 4. Create persistent cage state interface
        let cageState = mkPersistentState runner

        -- 5. Create persistent trie manager
        let trieManager = mkPersistentTrieManager runner

        -- 6. Build the Follower (block processor)
        let follower = mkCageFollower runner utxoUpdate
                cageState trieManager cagePolicyId

        -- 7. Start ChainSync N2C (blocks → follower)
        async $ runLocalNodeApplication magic socket
            (mkN2CChainSyncApplication ... follower ...)

        -- 8. Start LSQ + TxSubmission N2C connection
        (lsqChan, ltxsChan) <- startN2CConnection magic socket

        -- 9. Wire Provider (UTxO from RocksDB, params from LSQ)
        let provider = Provider
                { queryUTxOs = queryByAddress utxoQuery
                , queryProtocolParams = queryLSQ lsqChan ...
                , evaluateTx = ...
                }

        -- 10. Wire remaining components
        let submitter = mkN2CSubmitter ltxsChan
            txBuilder = mkRealTxBuilder cageConfig provider
                cageState trieManager
            context = Context { provider, trieManager,
                state = cageState, indexer, submitter,
                txBuilder }

        action context
```

## Invariants (to be formalized in Lean 4)

### INV-1: Atomic block processing

For any block B at slot S, the state transition from
`state(S-1)` to `state(S)` is atomic. Either all of these
hold simultaneously or none:
- UTxO set reflects all spends and creates in B
- Cage tokens reflect all boot/burn events in B
- Cage requests reflect all request/retract/update events in B
- Trie roots match the on-chain StateDatum roots after
  processing all updates in B

### INV-2: Rollback correctness

For any rollback to slot S, the resulting state is identical
to the state that existed at slot S. Formally:

```
rollback(process(state(S), blocks(S+1..S+n)), S) = state(S)
```

This must hold for UTxO index, cage state, and trie state.

### INV-3: Cage event completeness

For any block B, the set of detected `CageEvent`s is exactly
the set of cage-relevant transactions in B. No cage
transaction is missed, no non-cage transaction is
misidentified.

### INV-4: Trie-state consistency

After processing any block, for every token T with
`TokenState.root = R`, the actual Merkle root of T's
persisted trie equals R.

### INV-5: Inverse operation correctness

For any forward operation `op` producing inverse `inv(op)`,
applying `inv(op)` after `op` restores the original state:

```
apply(inv(op), apply(op, state)) = state
```

## Design decisions (resolved)

1. **Protocol params source** — Keep LSQ. Params change at
   most once per epoch and are only queried when building
   transactions, not per-block.

2. **Provider.queryUTxOs source** — Use cardano-utxo-csmt's
   local UTxO index (`Query.getByAddress`). LSQ-based
   `queryUTxOs` is unusable. The `Provider` gains a
   dependency on cardano-utxo-csmt's query interface. The
   N2C LSQ connection is only needed for protocol params
   and tx evaluation.

3. **Trie prefix efficiency** — Deferred. Prefixed keys in
   shared column families work correctly. RocksDB prefix
   bloom filter configuration is a future optimization
   (see #40).

4. **Transaction serialization** — Keep the MVar. Atomicity
   is required. Block processing is well within the block
   interval on both mainnet (20s) and devnet (0.1s).
