# Phase 1 Data Model: Atomic POST /tx/request/{insert,delete,update}

## Scope

This feature introduces one new pure record (`RequestCore`), one new
error sum (`RequestCoreError`), one new public function with three
thin operation-specific wrappers (`requestCore`, `requestInsertCore`,
`requestDeleteCore`, `requestUpdateCore`), and one IO orchestrator
(`runRequestBuilder`). It also moves the existing input-decoding
helpers from `Boot/Inputs.hs` to a shared `Wallet/Inputs.hs`. Every
other entity it touches already exists.

## New entities

### `RequestCore`

```haskell
data RequestCore = RequestCore
    { rcProgram :: TxBuild (Const ()) Void ()
    , rcInputs :: [(TxIn, TxOut ConwayEra)]
    , rcAddr :: Addr
    , rcFunding :: [WitnessedInput]
    , rcSnapshot :: BundleSnapshot
    }
```

**Location**: `Cardano.MPFS.TxBuilder.Real.Request` (existing
module — replaces the imperative `requestImpl` body).

**Purpose**: Mirrors `BootCore`. All the data the IO layer needs to
drive the DSL's `build` loop and emit a proof-bearing
`ProofEnvelope RequestProof`.

### `RequestCoreError`

```haskell
data RequestCoreError
    = RequestCoreDecodeFailed DecoderError
    | RequestCoreEmptyInputs
    deriving (Show)
```

**Location**: same module.

**Purpose**: Mirrors `BootCoreError` — the two pre-IO failure modes
the pure constructor surfaces.

### `requestCore` (pure shared constructor)

```haskell
requestCore
    :: CageConfig
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> TokenId
    -> ByteString          -- key
    -> Operation           -- insert / delete / update payload
    -> Addr                -- requester
    -> Either RequestCoreError RequestCore
```

**Behaviour**: Decode inputs → pick wallet UTxOs (one funding input
plus one collateral, mirroring boot) → derive the per-token request
address from `cfg + tid` (pure) → describe the tx as a
`TxBuild (Const ()) Void ()` program (`spend` + `payTo'` for the
pending-request output with inline datum + `collateral`) → return
`RequestCore`.

### `requestInsertCore`, `requestDeleteCore`, `requestUpdateCore`

Three thin wrappers that pre-fill the operation discriminator:

```haskell
requestInsertCore cfg snap inputs tid key value addr =
    requestCore cfg snap inputs tid key (OpInsert value) addr

requestDeleteCore cfg snap inputs tid key oldVal addr =
    requestCore cfg snap inputs tid key (OpDelete oldVal) addr

requestUpdateCore cfg snap inputs tid key oldVal newVal addr =
    requestCore cfg snap inputs tid key
        (OpUpdate oldVal newVal) addr
```

These three are the public surface of the module.

### `runRequestBuilder` (IO orchestrator)

**Location**: `Cardano.MPFS.TxBuilder.Real` (alongside
`runBootBuilder`).

```haskell
runRequestBuilder
    :: CageConfig
    -> Provider IO
    -> ( CageConfig
         -> BundleSnapshot
         -> [ResolvedWalletInput]
         -> TokenId
         -> ByteString
         -> Addr
         -> Either RequestCoreError RequestCore
       )
    -- ^ One of the three *Core wrappers, partially applied
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> TokenId
    -> ByteString
    -> Addr
    -> IO (ProofEnvelope RequestProof)
```

**Behaviour**: Call the supplied core constructor → on `Left`,
`error` with the variant → on `Right`, fetch `pp`, run DSL `build`
with the request's `evalAdapter` (same shape as boot) → return
the envelope.

## Changed entities

### `Cardano.MPFS.TxBuilder.Real.Request`

- Remove `requestImpl` (the imperative IO function).
- Remove `requestInsertImpl`, `requestDeleteImpl`,
  `requestUpdateImpl` (their wrappers around `requestImpl`).
- Add `requestCore`, `requestInsertCore`, `requestDeleteCore`,
  `requestUpdateCore` — pure functions returning
  `Either RequestCoreError RequestCore`.
- Drop imports of `Provider`, `State`, `IO`,
  `evaluateAndBalance`, `mkBasicTxBody`, etc.
- Module is structurally pure: no `IO`, no `Provider`.

### `Cardano.MPFS.TxBuilder.Real.mkRealTxBuilder`

Wire the three request fields to `runRequestBuilder` partial-
applications (one per operation):

```haskell
{ requestInsert = \snap inputs tid k v addr ->
    runRequestBuilder cfg prov
        (\c s i t k' a -> requestInsertCore c s i t k' v a)
        snap inputs tid k addr
, requestDelete = \snap inputs tid k oldV addr ->
    runRequestBuilder cfg prov
        (\c s i t k' a -> requestDeleteCore c s i t k' oldV a)
        snap inputs tid k addr
, requestUpdate = \snap inputs tid k oldV newV addr ->
    runRequestBuilder cfg prov
        (\c s i t k' a -> requestUpdateCore c s i t k' oldV newV a)
        snap inputs tid k addr
}
```

Or, if cleaner, three small private orchestrators (one per
operation) calling a common `runRequestBuilder'` that accepts a
pre-built `RequestCore` builder. Both shapes equivalent;
implementation chooses the less awkward.

### `Cardano.MPFS.HTTP.Server.txInsertHandler` / `txDeleteHandler` / `txUpdateValueHandler`

Each handler today reads the snapshot in the HTTP layer (separate
from the per-input proofs read inside `requestImpl`). After this
slice, each handler:

```haskell
txInsertHandler ctx req = do
    addr <- requireAddr (irAddr req)
    let tid = tokenIdFromJSON (irToken req)
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
                        $ Tx.requestInsert
                            (txBuilder ctx)
                            snap
                            inputs
                            tid
                            (k req)
                            (v req)
                            addr
                pure (mkRequestTxResponse bundle)
```

The `Tx.requestInsert` field's signature gains a
`[ResolvedWalletInput]` argument before the `TokenId`, mirroring
the change `Tx.bootToken` got in PR #253. Same for the other two.

### `Cardano.MPFS.TxBuilder.TxBuilder` (signature changes)

```haskell
data TxBuilder m = TxBuilder
    { bootToken
        :: BundleSnapshot -> [ResolvedWalletInput] -> Addr
        -> m (ProofEnvelope BootProof)
    , requestInsert
        :: BundleSnapshot -> [ResolvedWalletInput] -> TokenId
        -> ByteString -> ByteString -> Addr
        -> m (ProofEnvelope RequestProof)
    , requestDelete
        :: BundleSnapshot -> [ResolvedWalletInput] -> TokenId
        -> ByteString -> ByteString -> Addr
        -> m (ProofEnvelope RequestProof)
    , requestUpdate
        :: BundleSnapshot -> [ResolvedWalletInput] -> TokenId
        -> ByteString -> ByteString -> ByteString -> Addr
        -> m (ProofEnvelope RequestProof)
    , …  -- updateToken, retractRequest, rejectRequests, endToken
        -- unchanged for this slice
    }
```

The three request fields gain a `[ResolvedWalletInput]` argument
in the same position boot already has (after `BundleSnapshot`).

### `Cardano.MPFS.TxBuilder.Real.Boot.Inputs` → `Wallet.Inputs`

Module move only. Same exports; importers in `Boot.hs` and
`Request.hs` update.

## Unchanged entities (used as-is)

- `Cardano.MPFS.TxBuilder.BundleSnapshot` — produced by
  `readSnapshot` (from PR #253).
- `Cardano.MPFS.TxBuilder.WitnessedInput` — emitted by the
  builder; constructed via `Wallet.Inputs.rowToWitness`.
- `Cardano.MPFS.TxBuilder.RequestProof` — unchanged.
- `Cardano.MPFS.TxBuilder.ResolvedWalletInput` — produced by
  `readWalletInputsAt`.
- `Cardano.MPFS.TxBuilder.Real.Internal.requestAddrFromCfg`,
  `mkRequestDatum`, `toPlcData`, `mkInlineDatum`,
  `requestLockedAda` — reused.
- `Cardano.MPFS.HTTP.Types.InsertRequest` /
  `DeleteRequest` / `UpdateRequest` — wire contracts unchanged
  (FR-003).
- `Cardano.MPFS.Provider.queryUTxOs` — kept for wallet-side test
  use; zero call sites in `lib/` after this slice.

## State transitions

Each request endpoint is stateless on the server side (no DB write).
The state observed is the indexer's snapshot at transaction-open
time (one `runIndexerTx`).

```text
Request → runIndexerTx ctx (readSnapshot >>= readWalletInputsAt addr)
                              ↓ one transaction, coherent reads
                          (snap, inputs)
                              ↓
                          requestInsert / Delete / Update
                              ↓
                          ProofEnvelope RequestProof
                              ↓
                          UnsignedTxResponse → wallet
```

Errors map deterministically:

- `mSnap == Nothing` → `503 Indexer not ready: snapshot unavailable`
- `mSnap == Just _` && `null inputs` → `400 No wallet UTxOs at address`

## Validation rules

| Rule                                                                                                                                             | Source FR(s) |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ |
| `runIndexerTx` opens exactly one underlying RocksDB transaction per HTTP request.                                                                | FR-001 |
| Every `WitnessedInput` proof verifies against the response's snapshot's CSMT root.                                                               | FR-001, FR-007 |
| The three request impl modules contain zero call sites of `queryUTxOs`.                                                                           | FR-002, SC-002 |
| `InsertRequest`, `DeleteRequest`, `UpdateRequest` retain their existing fields exactly.                                                          | FR-003 |
| Each handler emits the documented status codes deterministically per error case.                                                                  | FR-004 |
| `readWalletInputsAt`'s wall-clock cost is bounded by the number of UTxOs at the address (already true after PR #253; reused unchanged).          | FR-005 |
| The Request module imports neither `Provider` nor any IO-typed function; the transaction body is a `TxBuild` program.                            | FR-006, SC-003 |
| The verifier accepts request responses purely offline.                                                                                           | FR-007, SC-001 |
