# Singletons

Every major component is a **record of functions**, polymorphic in
the monad `m`. No typeclasses are used — dependencies are explicit
and passed as values. This makes testing trivial: swap a record for
a mock.

## Provider

Blockchain queries (read-only). Queries a Cardano node via N2C
LocalStateQuery.

**Implementation:** [`mkNodeClientProvider`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkNodeClientProvider&type=code) (real N2C, in [`Provider.NodeClient`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Provider.NodeClient%22&type=code))

```haskell
data Provider m = Provider
    { queryUTxOs
        :: Addr
        -> m [(TxIn, TxOut ConwayEra)]
    -- ^ Look up UTxOs at an address
    , queryProtocolParams
        :: m (PParams ConwayEra)
    -- ^ Fetch current protocol parameters
    , evaluateTx
        :: ByteString -> m ExUnits
    -- ^ Evaluate execution units for a serialised
    -- CBOR transaction (not yet implemented)
    }
```

```mermaid
graph LR
    PRV["Provider"]
    NODE["Cardano Node<br/>(LocalStateQuery)"]

    PRV -->|queryUTxOs| NODE
    PRV -->|queryProtocolParams| NODE
```

---

## TrieManager

Manages a map of token identifiers to MPF tries. Each token has
its own isolated trie, sharing the same RocksDB column families
with per-token `HexKey` prefix scoping.

**Transactional:** [`mkUnifiedTrieManager`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkUnifiedTrieManager&type=code) — composes into the caller's `Transaction` (used by `CageFollower` for atomic block processing)
**IO:** [`mkPersistentTrieManager`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkPersistentTrieManager&type=code) — auto-commits + IORef caches (used by `TxBuilder` and speculative sessions)
**Test:** [`mkPureTrieManager`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkPureTrieManager&type=code) — in-memory (in [`Trie.PureManager`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Trie.PureManager%22&type=code))

```haskell
data TrieManager m = TrieManager
    { withTrie
        :: forall a
         . TokenId
        -> (Trie m -> m a)
        -> m a
    -- ^ Run an action with access to a token's trie
    , createTrie :: TokenId -> m ()
    -- ^ Create a new empty trie for a token
    , deleteTrie :: TokenId -> m ()
    -- ^ Delete a token's trie
    }

data Trie m = Trie
    { insert
        :: ByteString -> ByteString -> m Root
    , delete :: ByteString -> m Root
    , lookup :: ByteString -> m (Maybe ByteString)
    , getRoot :: m Root
    , getProof :: ByteString -> m (Maybe Proof)
    }
```

---

## State

Token and request state tracking. Three sub-records for tokens,
requests, and chain sync checkpoints.

**Transactional:** [`mkTransactionalState`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkTransactionalState&type=code) — composes into the caller's `Transaction` (used by `CageFollower`)
**IO:** [`mkPersistentState`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkPersistentState&type=code) — auto-commits via `hoistState` (in [`Indexer.Persistent`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.Persistent%22&type=code))
**Test:** [`mkMockState`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkMockState&type=code) — in-memory (in [`Mock.State`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.State%22&type=code))

```haskell
data State m = State
    { tokens :: Tokens m
    , requests :: Requests m
    , checkpoints :: Checkpoints m
    }

data Tokens m = Tokens
    { getToken :: TokenId -> m (Maybe TokenState)
    , putToken :: TokenId -> TokenState -> m ()
    , removeToken :: TokenId -> m ()
    , listTokens :: m [TokenId]
    }

data Requests m = Requests
    { getRequest :: TxIn -> m (Maybe Request)
    , putRequest :: TxIn -> Request -> m ()
    , removeRequest :: TxIn -> m ()
    , requestsByToken :: TokenId -> m [Request]
    }

data Checkpoints m = Checkpoints
    { getCheckpoint :: m (Maybe (SlotNo, BlockId))
    , putCheckpoint :: SlotNo -> BlockId -> m ()
    }
```

---

## Indexer

Chain sync follower with lifecycle control.

**Real:** [`CageFollower`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Indexer.CageFollower%22&type=code) — processes each block in a [single atomic transaction](block-processing.md) covering UTxO, cage state, tries, and rollback
**Mock:** [`mkSkeletonIndexer`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkSkeletonIndexer&type=code) (lifecycle-only skeleton, in [`Mock.Skeleton`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.Skeleton%22&type=code))

```haskell
data ChainTip = ChainTip
    { tipSlot :: SlotNo
    , tipBlockId :: BlockId
    }

data Indexer m = Indexer
    { start :: m ()
    , stop :: m ()
    , pause :: m ()
    , resume :: m ()
    , getTip :: m ChainTip
    }
```

---

## Submitter

Transaction submission via N2C LocalTxSubmission. Takes a full
ledger `Tx ConwayEra` and returns a `SubmitResult`.

**Implementation:** [`mkN2CSubmitter`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkN2CSubmitter&type=code) (real N2C, in [`Submitter.N2C`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Submitter.N2C%22&type=code))

```haskell
data SubmitResult
    = Submitted TxId
    | Rejected ByteString

newtype Submitter m = Submitter
    { submitTx :: Tx ConwayEra -> m SubmitResult
    }
```

---

## TxBuilder

Constructs transactions for all MPFS protocol operations. Returns
full ledger `Tx` values ready for signing.

**Mock:** [`mkMockTxBuilder`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkMockTxBuilder&type=code) (in [`Mock.TxBuilder`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Mock.TxBuilder%22&type=code))
**Real:** [`mkRealTxBuilder`](https://github.com/paolino/cardano-mpfs-offchain/search?q=mkRealTxBuilder&type=code) (in [`TxBuilder.Real`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.TxBuilder.Real%22+path%3AReal.hs&type=code))

```haskell
data TxBuilder m = TxBuilder
    { bootToken
        :: Addr -> m (Tx ConwayEra)
    -- ^ Create a new MPFS token
    , requestInsert
        :: TokenId -> ByteString -> ByteString
        -> Addr -> m (Tx ConwayEra)
    -- ^ Request inserting a key-value pair
    , requestDelete
        :: TokenId -> ByteString
        -> Addr -> m (Tx ConwayEra)
    -- ^ Request deleting a key
    , updateToken
        :: TokenId -> Addr -> m (Tx ConwayEra)
    -- ^ Process pending requests for a token
    , retractRequest
        :: TxIn -> Addr -> m (Tx ConwayEra)
    -- ^ Cancel a pending request
    , endToken
        :: TokenId -> Addr -> m (Tx ConwayEra)
    -- ^ Retire an MPFS token
    }
```

---

## Balance

Pure transaction balancing function (not a singleton record), in
[`Core.Balance`](https://github.com/paolino/cardano-mpfs-offchain/search?q=%22module+Cardano.MPFS.Core.Balance%22&type=code).
Adds a fee-paying UTxO and change output, finding the fee via
a fixpoint loop over [`estimateMinFeeTx`](https://github.com/paolino/cardano-mpfs-offchain/search?q=estimateMinFeeTx&type=code).

```haskell
balanceTx
    :: PParams ConwayEra
    -> (TxIn, TxOut ConwayEra)   -- fee-paying UTxO
    -> Addr                       -- change address
    -> Tx ConwayEra               -- unbalanced tx
    -> Either BalanceError (Tx ConwayEra)
```

The fee estimation iterates until stable (max 10 rounds, crashes
if not converged). One key witness is assumed for the fee input.

---

## Context

Facade record that bundles all singletons into a single environment.

```haskell
data Context m = Context
    { provider :: Provider m
    , trieManager :: TrieManager m
    , state :: State m
    , indexer :: Indexer m
    , submitter :: Submitter m
    , txBuilder :: TxBuilder m
    }
```

```mermaid
graph TD
    CTX["Context"]
    PRV["Provider"]
    TM["TrieManager"]
    ST["State"]
    IDX["Indexer"]
    SUB["Submitter"]
    TXB["TxBuilder"]

    CTX --> PRV
    CTX --> TM
    CTX --> ST
    CTX --> IDX
    CTX --> SUB
    CTX --> TXB
```
