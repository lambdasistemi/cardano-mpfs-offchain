# Internal Contract: AtomicCageReader

## Purpose

The boot endpoint must produce a proof-bearing response whose snapshot,
resolved input bytes, and CSMT inclusion proofs all reflect a single
coherent point in the indexer's chain history. `AtomicCageReader` is
the seam that owns "produce that coherent reading" responsibility on
behalf of any handler that needs it. This slice wires it for
`POST /tx/boot`; subsequent slices in the #250 family wire it for
`tokens-list`, `requestInsert/Delete/Update`, `retract`, `reject`,
`update`, and `end`.

This is an **internal** contract: there is no HTTP surface. The wire
contract `POST /tx/boot { address }` is preserved unchanged.

## Signature

```haskell
type AtomicCageReader m =
    Addr
    -> m (Either AtomicReaderError AtomicCageRead)

data AtomicCageRead = AtomicCageRead
    { acrSnapshot :: BundleSnapshot
    , acrInputs   :: [(TxIn, ByteString, ByteString)]
    }

data AtomicReaderError
    = AtomicReaderNoCheckpoint
    | AtomicReaderRootMissing
    | AtomicReaderNoUtxos
    | AtomicReaderKvMissing TxIn
```

Module: `Cardano.MPFS.Context`.

## Semantics

The implementation MUST satisfy the following:

1. **One transaction.** The reader MUST perform every read it needs
   inside a single `RunTransaction` call over `UnifiedColumns`,
   projected to `InUtxo`. No call site may interleave a second
   transaction or any IO that observes the indexer outside this
   transaction.

2. **Coherent snapshot.** The CSMT root inside `acrSnapshot` MUST be
   the root the inclusion proofs in `acrInputs` verify against. The
   chain checkpoint inside `acrSnapshot` MUST be the indexer's
   checkpoint at the moment the transaction opened.

3. **Address scoping.** The reader MUST limit its leaf walk to the
   subtree of the CSMT keyed by the input `Addr` (using
   `collectValues CSMTCol [] addressKey` or an equivalent prefix
   walk). Its cost MUST grow with the number of UTxOs at the address,
   not with the total UTxOs on chain.

4. **No node query.** The reader MUST NOT call any
   `Cardano.MPFS.Provider` field for UTxO state. (Calls to the
   Provider for protocol parameters and tx evaluation/balance happen
   *outside* the reader, in the builder, and are unaffected by this
   contract.)

5. **Deterministic errors.** On any of the four documented failure
   modes, the reader MUST return the corresponding `Left` variant.
   The reader MUST NOT throw exceptions for these cases. Unexpected
   failures (e.g. a RocksDB IO error) propagate as IO exceptions in
   the usual way.

6. **Order preservation.** The order of `acrInputs` is the order the
   underlying `collectValues` walk produces. Callers may rely on this
   order being stable across calls against the same indexer state.

## Production wiring

`Cardano.MPFS.Application.withApplication` builds the closure:

```haskell
let atomicReaderProd :: AtomicCageReader IO
    atomicReaderProd addr =
        CSMT.transact utxoRt $ do
            mRoot <-
                queryMerkleRoot (hashing context)
            case mRoot of
                Nothing ->
                    pure (Left AtomicReaderRootMissing)
                Just rootHash -> do
                    mCheckpoint <- … latestRollbackPoint …
                    case mCheckpoint of
                        Nothing ->
                            pure (Left AtomicReaderNoCheckpoint)
                        Just (slot, blockId) -> do
                            let snap =
                                    BundleSnapshot
                                        { snapshotUtxoRoot =
                                            renderHash rootHash
                                        , snapshotSlot = slot
                                        , snapshotBlockId = blockId
                                        }
                                addrKey = encodeAddrAsCsmtKey addr
                            indirects <-
                                collectValues CSMTCol [] addrKey
                            case indirects of
                                [] -> pure (Left AtomicReaderNoUtxos)
                                xs -> readEach snap xs
  where
    readEach snap xs = do
        ms <- traverse (readOne (fromKV context)) xs
        case sequenceA ms of
            Left tin -> pure (Left (AtomicReaderKvMissing tin))
            Right rs -> pure (Right (AtomicCageRead snap rs))

    readOne fkv Indirect{jump} = do
        let key = jump
        mTxOut <- query KVCol key
        case mTxOut of
            Nothing ->
                pure (Left (decodeTxInFromCsmtKey key))
            Just txOut -> do
                proof <-
                    generateInclusionProof
                        fkv KVCol CSMTCol key
                pure
                    $ Right
                        ( decodeTxInFromCsmtKey key
                        , BSL.toStrict txOut
                        , maybe mempty BSL.toStrict
                            (fmap snd proof)
                        )
```

The exact code is not the point of this contract — the point is that
the entire body lives inside one `CSMT.transact utxoRt $ do { … }`
block. SC-005 is the gate.

## Test seam wiring

`AppConfig.atomicCageReaderOverride :: Maybe (AtomicCageReader IO)`.
`Serve.hs` sets `Nothing`. Test fixtures with `followerEnabled = False`
construct an override that:

- queries `Provider.queryUTxOs` (wallet-side allowance — the test
  *is* a wallet on its own LSQ connection),
- synthesises a `BundleSnapshot` whose `snapshotUtxoRoot` is the
  empty-root constant the on-chain validator already accepts at boot,
  and whose `snapshotSlot` / `snapshotBlockId` reflect a sentinel
  value the verifier in those fixtures is configured to accept,
- emits an empty proof bytestring per input (those fixtures do not
  exercise the verifier on the boot response).

The override is constructed in the harness module that already wires
the manual indexer driver — it is not exported from
`cardano-mpfs-offchain` proper.

## Caller obligations

Callers (currently only `txBootHandler` and `bootTokenImpl`) MUST:

- Treat each `AtomicReaderError` variant as terminal — no fallback
  to the Provider, no retry inside the same request.
- Not re-read the snapshot or the inputs from any other source after
  invoking the reader.

The `bootToken` field of `TxBuilder` accepts an `AtomicCageRead`
directly; it does not invoke the reader itself. This keeps "did the
single-transaction read actually happen?" reviewable in one place
(the handler).

## Forbidden patterns

The following patterns MUST NOT appear in
`cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs` or
`cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`'s boot
handler after this slice:

- Any call to `queryUTxOs (provider ctx)` or
  `queryUTxOs prov` on the build path. (FR-002, SC-002.)
- A `requireBundleSnapshot` call inside `txBootHandler`. (FR-001.)
- A second `RunTransaction` call inside the boot reader for the same
  request. (FR-001.)

These are the greppable acceptance criteria for the slice.
