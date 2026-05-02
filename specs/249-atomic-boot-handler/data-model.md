# Phase 1 Data Model: Atomic POST /tx/boot

## Scope

This feature introduces one new internal seam (`AtomicCageReader`) and
one new error sum type (`AtomicReaderError`). Every other entity it
touches already exists; this document records how each existing entity
is consumed or produced by the new seam.

## New entities

### AtomicCageReader (record-of-functions seam)

```haskell
type AtomicCageReader m =
    Addr
    -> m (Either AtomicReaderError AtomicCageRead)
```

**Location**: `Cardano.MPFS.Context` (added).

**Purpose**: Given an owner address, return the `BundleSnapshot` and a
list of `(TxIn, TxOut CBOR bytes, CSMT inclusion proof)` triples for
every wallet UTxO at that address — all from a single coherent indexer
read.

**Construction**:

- Production: `Cardano.MPFS.Application.withApplication` constructs a
  closure over the `utxoRt :: CSMT.RunTransaction` value; the closure
  body is one `CSMT.transact utxoRt $ do { … }` block.
- Override: `AppConfig.atomicCageReaderOverride` (new, optional).

**Effect parameter**: `m` is `IO` in production and tests; the seam is
not used by `mkMockContext` (which short-circuits `bootToken`).

### AtomicCageRead (success payload)

```haskell
data AtomicCageRead = AtomicCageRead
    { acrSnapshot :: BundleSnapshot
    , acrInputs   :: [(TxIn, ByteString, ByteString)]
        -- ^ (input ref, TxOut CBOR bytes, CSMT inclusion proof)
    }
    deriving Show
```

**Location**: `Cardano.MPFS.Context` (added).

**Invariant**: every entry in `acrInputs` was read from the same
indexer transaction that produced `acrSnapshot`. The CSMT root inside
`acrSnapshot` is the root the inclusion proofs verify against.

**Order**: input order is the order `collectValues` returns. The
builder may pick the first as the asset-name seed (matching the
existing `bootTokenImpl` shape).

### AtomicReaderError (failure variants)

```haskell
data AtomicReaderError
    = AtomicReaderNoCheckpoint
        -- ^ Indexer has no chain checkpoint yet
        --   (Edge case: server just started, no block applied).
    | AtomicReaderRootMissing
        -- ^ CSMT has no Merkle root yet
        --   (Edge case: empty / un-bootstrapped CSMT).
    | AtomicReaderNoUtxos
        -- ^ Address has zero UTxOs in the indexer
        --   (Edge case: unfunded or fully-spent address).
    | AtomicReaderKvMissing TxIn
        -- ^ A leaf was found in the CSMT but its
        --   resolved TxOut bytes are absent from KVCol
        --   (Edge case: indexer corruption — fail loud).
    deriving (Show, Eq)
```

**Location**: `Cardano.MPFS.Context` (added).

**HTTP mapping**: see `contracts/atomic-cage-reader.md`. The handler
maps each variant to a deterministic 4xx / 503 response (FR-004).

## Changed entities

### `Cardano.MPFS.Context.Context m`

**Change**: add one field.

```haskell
data Context m = Context
    { … existing fields …
    , atomicCageReader :: AtomicCageReader m
    }
```

**Mock**: `mkMockContext` initializes the field to a constant
`error "mkMockContext: atomicCageReader not implemented"` — boot is
not exercised through the mock context (mock has its own
`mkMockTxBuilder`).

### `Cardano.MPFS.Application.AppConfig`

**Change**: add one field.

```haskell
data AppConfig = AppConfig
    { … existing fields …
    , atomicCageReaderOverride
        :: !(Maybe (AtomicCageReader IO))
    }
```

**Default**: `Serve.hs` sets `atomicCageReaderOverride = Nothing`.
Tests with `followerEnabled = False` set it to `Just …`.

### `Cardano.MPFS.TxBuilder.TxBuilder.bootToken`

**Change**: signature.

| Before                                                   | After                                                |
| -------------------------------------------------------- | ---------------------------------------------------- |
| `BundleSnapshot -> Addr -> m (ProofEnvelope BootProof)`  | `AtomicCageRead -> Addr -> m (ProofEnvelope BootProof)` |

**Why**: the snapshot now arrives bundled with the resolved inputs
and proofs. Splitting them is the bug.

### `Cardano.MPFS.TxBuilder.Real.Boot.bootTokenImpl`

**Change**: stop calling `queryUTxOs`; consume `AtomicCageRead`
directly.

```haskell
bootTokenImpl
    :: CageConfig
    -> Provider IO   -- still used for protocol params, evaluate, balance
    -> AtomicCageRead
    -> Addr
    -> IO (ProofEnvelope BootProof)
```

The body picks the seed input from `acrInputs`, derives the asset
name from it, builds mint + body + redeemer, balances using
`Provider`'s protocol params and `evaluateTx` (those are not the
forbidden `queryUTxOs` call), and constructs each `WitnessedInput`
straight from the corresponding `acrInputs` triple — no extra IO.

### `Cardano.MPFS.HTTP.Server.txBootHandler`

**Change**: drops `requireBundleSnapshot`; calls `atomicCageReader ctx`.

```haskell
txBootHandler ctx (BootRequest addrHex) = do
    addr <- requireAddr addrHex
    er   <- liftIO (atomicCageReader ctx addr)
    case er of
        Left e   -> throwError (mapAtomicReaderError e)
        Right rd -> do
            bundle <-
                liftIO
                    $ Tx.bootToken
                        (txBuilder ctx) rd addr
            pure (mkBootTxResponse bundle)
```

`mapAtomicReaderError` is local to `Server.hs`:

| Variant                  | HTTP status | Body                                                           |
| ------------------------ | ----------- | -------------------------------------------------------------- |
| `AtomicReaderNoCheckpoint` | 503       | `"Indexer not ready: no chain checkpoint"`                     |
| `AtomicReaderRootMissing`  | 503       | `"Indexer not ready: no CSMT root"`                            |
| `AtomicReaderNoUtxos`      | 400       | `"No wallet UTxOs at address"`                                 |
| `AtomicReaderKvMissing _`  | 500       | `"Indexer corruption: missing KV for indexed leaf"`            |

## Unchanged entities (used as-is)

- `Cardano.MPFS.TxBuilder.BundleSnapshot` — produced by the reader.
- `Cardano.MPFS.TxBuilder.WitnessedInput` — emitted by the builder.
- `Cardano.MPFS.TxBuilder.BootProof`, `ProofEnvelope` — unchanged.
- `Cardano.MPFS.API.Types.BootRequest` — unchanged. Wire contract
  remains `POST /tx/boot { address }` (FR-003).
- `Cardano.MPFS.Provider.Provider`'s `queryUTxOs` field — kept for
  test-side wallet simulation. Haddock warning added; zero call sites
  on tx-build paths inside `cardano-mpfs-offchain/lib/` after this
  slice.

## State transitions

The boot endpoint is stateless from the server's perspective (no DB
write). The state observed by the reader is the indexer's snapshot at
transaction-open time. After the transaction closes, no state is
mutated by boot (the on-chain mutation, if the wallet signs and
submits, is observed asynchronously by the chain follower in the
normal block processing loop and is not part of this feature).

```text
Request → atomicCageReader (1 indexer transaction) → Right rd
                                                 ↓
                                         bootToken builder
                                                 ↓
                                         ProofEnvelope BootProof
                                                 ↓
                                       UnsignedTxResponse → wallet
```

If `atomicCageReader` returns `Left`, the handler short-circuits
before invoking the builder; the response is the deterministic error
payload above.

## Validation rules

Each rule is testable; all map to one or more functional requirements.

| Rule                                                                                                                                                                                                                            | Source FR(s) |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| The reader's success payload's CSMT root and chain checkpoint match the indexer's tip at transaction-open time.                                                                                                                  | FR-001       |
| Every triple's CSMT proof verifies against the success payload's CSMT root.                                                                                                                                                      | FR-001, FR-007 |
| The boot tx-builder source contains zero call sites of `queryUTxOs` on the build path inside `cardano-mpfs-offchain/lib/`.                                                                                                       | FR-002, SC-002 |
| `BootRequest` retains exactly one field, `brAddr :: Hex`.                                                                                                                                                                        | FR-003 |
| For each variant of `AtomicReaderError`, the handler emits the documented status code and body deterministically.                                                                                                                | FR-004 |
| The reader's wall-clock cost on the address-prefix walk is bounded by the number of UTxOs at the address (not by total chain UTxOs); validated by the latency curve in SC-003.                                                  | FR-005, SC-003 |
| `AppConfig.atomicCageReaderOverride` is configurable only at startup time (no CLI / runtime path sets it).                                                                                                                       | FR-006 |
| The verifier (`cardano-mpfs-client`), invoked on the response with the response's own snapshot as trusted root, accepts on 100% of attempts under sustained chain churn.                                                         | FR-007, SC-001 |
