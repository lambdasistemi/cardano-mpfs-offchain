# Phase 1 Data Model: Atomic POST /tx/boot

## Scope

This feature introduces one new internal seam (`IndexerTx`, with a
small library of read primitives) and exposes it on `Context` via
`runIndexerTx`. Every other entity it touches already exists; this
document records how each existing entity is consumed or produced by
the new seam.

## New entities

### IndexerTx (read-action monad)

```haskell
newtype IndexerTx a = IndexerTx
    { unIndexerTx
        :: forall cf op
         . L.Transaction IO cf
              ( UnifiedColumns
                    Point
                    Hash
                    BSL.ByteString
                    BSL.ByteString
              )
              op
              a
    }
    deriving (Functor, Applicative, Monad)  -- hand-written
```

**Location**: `Cardano.MPFS.Indexer.Reads` (new module).

**Purpose**: Carry an action over the unified-columns database
transaction across module boundaries while hiding the `cf` and `op`
existentials (which are bound at use-site by `Context.runIndexerTx`).
Composing two `IndexerTx` values via `Monad` keeps both sub-actions
inside the same single underlying RocksDB transaction — that is what
makes atomicity hold.

### Read primitives

```haskell
readCheckpoint :: IndexerTx (Maybe (SlotNo, BlockId))
readMerkleRoot :: IndexerTx (Maybe ByteString)
readSnapshot   :: IndexerTx (Maybe BundleSnapshot)
readWalletInputsAt :: Addr -> IndexerTx [ResolvedWalletInput]
```

**Location**: `Cardano.MPFS.Indexer.Reads` (new module).

**Purpose**: Each primitive performs one logical read against the
indexer. Handlers compose them inside `do { … }` and discharge through
`Context.runIndexerTx`. New primitives (e.g. for state-UTxO reads,
trie facts, request-UTxO reads needed by `update` / `reject`) are
added in this module — never as new transactions.

### `Context.runIndexerTx`

```haskell
data Context m = Context
    { … existing fields …
    , runIndexerTx :: forall a. IndexerTx a -> m a
    }
```

**Mock**: `mkMockContext` initializes the field to a constant
`error "mkMockContext: runIndexerTx not implemented"` — boot is
not exercised through the mock context.

## Changed entities

### `Cardano.MPFS.Context.Context m`

**Change**: add `runIndexerTx :: forall a. IndexerTx a -> m a`.
Requires `RankNTypes` on the module.

### `Cardano.MPFS.Application.AppConfig`

**Change**: no new fields. The earlier `atomicCageReaderOverride`
seam was dropped — handlers compose primitives, so a per-endpoint
override is no longer the right shape. Tests that need to bypass
the indexer entirely use the e2e helper `walletBootInputs` to feed
`bootToken` directly.

### `Cardano.MPFS.TxBuilder.TxBuilder.bootToken`

**Change**: signature.

| Before                                                                                | After                                                                                             |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `BundleSnapshot -> Addr -> m (ProofEnvelope BootProof)`                               | `BundleSnapshot -> [ResolvedWalletInput] -> Addr -> m (ProofEnvelope BootProof)`                  |

**Why**: the snapshot now arrives bundled with the resolved inputs
and proofs. The HTTP handler reads both inside one `IndexerTx` and
hands the bundle to the builder.

### `Cardano.MPFS.TxBuilder.Real.Boot.bootTokenImpl`

**Change**: stop calling `queryUTxOs`; consume the snapshot and the
input list directly. The orchestrator delegates step-by-step to the
new `Boot.Components` module (asset name, mint value, datum, output,
script + redeemers, body, tx assembly) and to `Boot.Inputs` for
input decoding and witness conversion.

### `Cardano.MPFS.HTTP.Server.txBootHandler`

**Change**: composes the read primitives directly:

```haskell
txBootHandler ctx (BootRequest addrHex) = do
    addr <- requireAddr addrHex
    (mSnap, inputs) <-
        liftIO $ runIndexerTx ctx $ do
            snap <- readSnapshot
            ins <- readWalletInputsAt addr
            pure (snap, ins)
    case mSnap of
        Nothing -> throwError err503 …
        Just snap
            | null inputs -> throwError err400 …
            | otherwise -> do
                bundle <-
                    liftIO
                        $ Tx.bootToken
                            (txBuilder ctx)
                            snap
                            inputs
                            addr
                pure (mkBootTxResponse bundle)
```

## Unchanged entities (used as-is)

- `Cardano.MPFS.TxBuilder.BundleSnapshot` — produced by `readSnapshot`.
- `Cardano.MPFS.TxBuilder.WitnessedInput` — emitted by the builder.
- `Cardano.MPFS.TxBuilder.BootProof`, `ProofEnvelope` — unchanged.
- `Cardano.MPFS.TxBuilder.ResolvedWalletInput` — produced by
  `readWalletInputsAt`.
- `Cardano.MPFS.API.Types.BootRequest` — unchanged. Wire contract
  remains `POST /tx/boot { address }` (FR-003).
- `Cardano.MPFS.Provider.Provider`'s `queryUTxOs` field — kept for
  test-side wallet simulation. Haddock warning added; zero call sites
  on tx-build paths inside `cardano-mpfs-offchain/lib/`.

## State transitions

The boot endpoint is stateless from the server's perspective (no DB
write). The state observed by the handler is the indexer's snapshot
at transaction-open time, captured by `runIndexerTx ctx (do …)`.

```text
Request → runIndexerTx ctx (readSnapshot >>= readWalletInputsAt addr)
                              ↓ one transaction, coherent reads
                          (snap, inputs)
                              ↓
                          bootToken
                              ↓
                          ProofEnvelope BootProof
                              ↓
                          UnsignedTxResponse → wallet
```

Errors are pure values:

- `mSnap == Nothing` → `503 Indexer not ready: snapshot unavailable`
- `mSnap == Just _` && `null inputs` → `400 No wallet UTxOs at address`

## Validation rules

| Rule                                                                                                                                                                                                                          | Source FR(s) |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `runIndexerTx` MUST open exactly one underlying RocksDB transaction; the reads composed inside it MUST observe a coherent snapshot.                                                                                          | FR-001 |
| Every `WitnessedInput` proof in the response verifies against the response's snapshot's CSMT root.                                                                                                                            | FR-001, FR-007 |
| The boot tx-builder source contains zero call sites of `queryUTxOs` on the build path inside `cardano-mpfs-offchain/lib/`.                                                                                                    | FR-002, SC-002 |
| `BootRequest` retains exactly one field, `brAddr :: Hex`.                                                                                                                                                                     | FR-003 |
| The handler emits the documented status codes deterministically for each error case.                                                                                                                                          | FR-004 |
| `readWalletInputsAt`'s wall-clock cost is bounded by the number of UTxOs at the address (not by total chain UTxOs).                                                                                                          | FR-005, SC-003 |
| Test fixtures with `followerEnabled = False` build boot transactions by calling the e2e helper `walletBootInputs` (a wallet-side `Provider.queryUTxOs` allowance) and feeding the result into `bootToken` directly.          | FR-006 |
| The verifier accepts the response purely offline.                                                                                                                                                                              | FR-007, SC-001 |
