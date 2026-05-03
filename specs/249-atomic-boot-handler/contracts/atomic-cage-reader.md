# Internal Contract: IndexerTx primitives

## Purpose

Every proof-bearing tx-build handler needs to produce a response
whose snapshot, resolved input bytes, and CSMT inclusion proofs all
reflect a single coherent point in the indexer's chain history.
`Cardano.MPFS.Indexer.Reads` exposes the underlying transaction as a
small monadic DSL plus a library of read primitives; handlers
compose the primitives they need inside one `IndexerTx` value and
discharge it through `Cardano.MPFS.Context.runIndexerTx`. The
atomicity of a handler's reads follows from the discipline that all
reads for one HTTP request live in one `runIndexerTx` call.

This is an **internal** contract: there is no HTTP surface. The
wire contract `POST /tx/boot { address }` is preserved unchanged.

## Signature

```haskell
newtype IndexerTx a = IndexerTx
    { unIndexerTx
        :: forall cf op
         . L.Transaction IO cf UnifiedColumns op a
    }
    deriving (Functor, Applicative, Monad)  -- hand-written

readCheckpoint     :: IndexerTx (Maybe (SlotNo, BlockId))
readMerkleRoot     :: IndexerTx (Maybe ByteString)
readSnapshot       :: IndexerTx (Maybe BundleSnapshot)
readWalletInputsAt :: Addr -> IndexerTx [ResolvedWalletInput]

-- on Context:
runIndexerTx :: forall a. IndexerTx a -> m a
```

Module: `Cardano.MPFS.Indexer.Reads`.

## Semantics

The implementation MUST satisfy the following:

1. **One transaction per dispatch.** Each call to `runIndexerTx ctx`
   MUST open exactly one underlying RocksDB transaction. The reads
   composed inside the action observe a coherent snapshot of the
   indexer at the moment that transaction opens.

2. **Coherent snapshot.** When a handler reads `readSnapshot >>= …`
   followed by other primitives in the same `IndexerTx`, the CSMT
   root inside the returned `BundleSnapshot` MUST be the root every
   subsequent read in that same `IndexerTx` observes.

3. **Address scoping.** `readWalletInputsAt` MUST limit its leaf
   walk to the subtree of the CSMT keyed by the input `Addr` (using
   `collectValues CSMTCol [] addrKey`). Its cost MUST grow with the
   number of UTxOs at the address, not with the total UTxOs on
   chain.

4. **No node query.** No primitive in `Cardano.MPFS.Indexer.Reads`
   MUST consult `Cardano.MPFS.Provider.queryUTxOs` (or any other
   cardano-node UTxO query) for UTxO state. The Provider is only
   queried for protocol parameters and tx evaluation, which happens
   in the tx-builder, not in the indexer reader.

5. **Pure values for missing data.** When the indexer is not ready
   (no checkpoint, no root) `readSnapshot` returns `Nothing` and
   the handler maps that to a 503. When an address has no UTxOs,
   `readWalletInputsAt` returns `[]` and the handler maps that to
   a 400. Implementations MUST NOT throw exceptions for these cases.
   Truly unexpected failures (RocksDB IO error, decoder mismatch on
   indexer-internal bytes) propagate as IO exceptions in the usual
   way.

6. **Composability.** Two `IndexerTx` actions composed with `>>=`
   MUST execute inside the same single underlying transaction.
   The Monad instance is hand-written to preserve this invariant
   under the rank-2 newtype wrapper.

## Production wiring

`Cardano.MPFS.Application.withApplication` provides the runner:

```haskell
let unifiedDb = … in do
    L.RunTransaction run <- newRunTransaction unifiedDb
    let ctx = Context { …, runIndexerTx = \(IndexerTx body) -> run body, … }
    action ctx
```

The runner is a single line: `\(IndexerTx body) -> run body`. Every
caller goes through it; no parallel runners exist.

## Caller composition

`txBootHandler` is the canonical caller:

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
                        $ Tx.bootToken (txBuilder ctx)
                            snap inputs addr
                pure (mkBootTxResponse bundle)
```

Future handlers (`requestInsert`, `retract`, `update`, `reject`,
`end`) will follow the same pattern: compose the primitives they
need inside one `runIndexerTx ctx $ do { … }` block. New primitives
are added to `Cardano.MPFS.Indexer.Reads` — never as new
transactions.

## Caller obligations

Callers MUST:

- Call `runIndexerTx ctx` at most once per HTTP request — never
  open a second transaction inside the same request handler.
- Treat `Nothing` / `[]` as terminal — no fallback to the Provider,
  no retry inside the same request.
- Compose all reads they need *before* calling `runIndexerTx`. If a
  read can't be expressed yet, add a primitive to
  `Cardano.MPFS.Indexer.Reads` rather than reaching for the
  underlying `L.Transaction`.

The `bootToken` field of `TxBuilder` accepts the
`(BundleSnapshot, [ResolvedWalletInput], Addr)` triple directly; it
does not invoke any indexer reader itself. This keeps "did the
single-transaction read actually happen?" reviewable in one place
(the handler).

## Forbidden patterns

The following patterns MUST NOT appear in
`cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`,
`Cardano.MPFS.TxBuilder.Real.Boot.*`, or
`Cardano.MPFS.HTTP.Server`'s boot handler after this slice:

- Any call to `queryUTxOs (provider ctx)` or `queryUTxOs prov` on
  the build path. (FR-002, SC-002.)
- A second `runIndexerTx ctx` call for the same HTTP request. (FR-001.)
- A call to the underlying `L.RunTransaction` directly, bypassing
  the `IndexerTx` newtype.

These are the greppable acceptance criteria for the slice.
