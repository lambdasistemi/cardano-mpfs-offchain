# Internal Contract: requestCore + runRequestBuilder

## Purpose

Exact application of the boot-slice contract
(`specs/249-atomic-boot-handler/contracts/atomic-cage-reader.md`)
to the three request endpoints. The transaction is described as a
pure `TxBuild` program in
`Cardano.MPFS.TxBuilder.Real.Request.requestCore`; the IO
orchestration (fetch `pp`, run DSL `build`, assemble envelope)
lives in `Cardano.MPFS.TxBuilder.Real.runRequestBuilder`.
Handlers compose `IndexerTx` primitives at the HTTP boundary and
hand the resulting (snapshot, inputs) to the orchestrator.

This is an **internal** contract: there is no HTTP surface
change. The three wire contracts
(`POST /tx/request/{insert,delete,update}`) are preserved
unchanged.

## Signature

```haskell
-- Cardano.MPFS.TxBuilder.Real.Request

data RequestCore = RequestCore
    { rcProgram :: TxBuild (Const ()) Void ()
    , rcInputs :: [(TxIn, TxOut ConwayEra)]
    , rcAddr :: Addr
    , rcFunding :: [WitnessedInput]
    , rcSnapshot :: BundleSnapshot
    }

data RequestCoreError
    = RequestCoreDecodeFailed DecoderError
    | RequestCoreEmptyInputs
    deriving (Show)

requestCore
    :: CageConfig
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> TokenId
    -> ByteString
    -> Operation
    -> Addr
    -> Either RequestCoreError RequestCore

requestInsertCore
    :: CageConfig -> BundleSnapshot
    -> [ResolvedWalletInput] -> TokenId
    -> ByteString -> ByteString -> Addr
    -> Either RequestCoreError RequestCore

requestDeleteCore
    :: CageConfig -> BundleSnapshot
    -> [ResolvedWalletInput] -> TokenId
    -> ByteString -> ByteString -> Addr
    -> Either RequestCoreError RequestCore

requestUpdateCore
    :: CageConfig -> BundleSnapshot
    -> [ResolvedWalletInput] -> TokenId
    -> ByteString -> ByteString -> ByteString -> Addr
    -> Either RequestCoreError RequestCore
```

## Semantics

The implementation MUST satisfy:

1. **Pure construction.** `requestCore` and the three wrappers
   MUST be pure values. Their module imports neither
   `Cardano.MPFS.Provider` nor `IO`. (FR-006, SC-003.)

2. **TxBuild program shape.** The `TxBuild` program in `rcProgram`
   uses only `spend`, `payTo'`, and `collateral` from the DSL.
   No `mint`, no `spendScript`, no `attachScript`. (Q0-4.)

3. **Pending request output.** The `payTo'` call MUST emit a
   single output at the per-token request address
   (`requestAddrFromCfg cfg tid (network cfg)`) with the
   request datum inline. Value is the requester's tip, computed
   via `requestLockedAda` (or equivalent).

4. **Input picking.** The first decoded `InputRow` is the
   funding input (`spend` it). The collateral is the last one
   (per the boot-slice convention; reused for symmetry).

5. **No `queryUTxOs` reachable from the build path.** A grep of
   `Cardano.MPFS.TxBuilder.Real.Request` MUST return zero
   matches for `queryUTxOs`. The provider is not in scope here
   at all. (FR-002, SC-002.)

6. **Snapshot pass-through.** `rcSnapshot` is the
   `BundleSnapshot` the caller obtained from
   `readSnapshot`. The orchestrator places it in `envSnapshot`
   verbatim.

7. **Funding pass-through.** `rcFunding` is the list of
   `WitnessedInput` rows emitted via
   `Wallet.Inputs.rowToWitness`. Same shape as
   `BootProof.bootFunding`.

## IO orchestrator

```haskell
-- Cardano.MPFS.TxBuilder.Real

runRequestBuilder
    :: CageConfig
    -> Provider IO
    -> ( CageConfig -> BundleSnapshot
         -> [ResolvedWalletInput] -> TokenId
         -> ByteString -> Addr
         -> Either RequestCoreError RequestCore
       )
    -- ^ The pre-applied core constructor (closes over the
    --   operation kind via partial application from one of the
    --   three wrappers).
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> TokenId
    -> ByteString
    -> Addr
    -> IO (ProofEnvelope RequestProof)
```

**Behaviour**:

```haskell
runRequestBuilder cfg prov mkCore snap inputs tid k addr =
    case mkCore cfg snap inputs tid k addr of
        Left e -> error $ "requestBuilder: " <> show e
        Right core -> do
            pp <- queryProtocolParams prov
            let evalAdapter tx =
                    fmap (fmap (either (Left . show) Right))
                        (evaluateTx prov tx)
            result <-
                build pp noCtxInterpretIO evalAdapter
                    (rcInputs core) (rcAddr core)
                    (rcProgram core)
            case result of
                Left e -> error $ "requestBuilder: DSL build \
                                  \failed: " <> show e
                Right tx ->
                    pure ProofEnvelope
                        { envTx = tx
                        , envSnapshot = rcSnapshot core
                        , envProof = RequestProof
                            { requestFunding = rcFunding core }
                        }
```

The orchestrator mirrors `runBootBuilder` exactly. The DSL's
`build` is called once per request; the IO surface is one
function.

## Caller composition (HTTP handlers)

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
                bundle <- liftIO
                    $ Tx.requestInsert (txBuilder ctx)
                        snap inputs tid (k req) (v req) addr
                pure (mkRequestTxResponse bundle)
```

Symmetric handlers for `txDeleteHandler` and
`txUpdateValueHandler`. Every handler is one
`runIndexerTx ctx $ do { … }` followed by one
`Tx.requestX (txBuilder ctx) …` call.

## Forbidden patterns

These patterns MUST NOT appear after this slice:

- Any call to `queryUTxOs` in
  `Cardano.MPFS.TxBuilder.Real.Request*` or in the three
  request handlers.
- `requireBundleSnapshot` calls in any of the three request
  handlers (the snapshot is now read inside the same
  `runIndexerTx` as the wallet inputs — same fix as the boot
  handler in PR #253).
- A second `runIndexerTx` call inside one HTTP request.
- `Provider` or `IO` imports inside the
  `Cardano.MPFS.TxBuilder.Real.Request` module.

These are the greppable acceptance criteria for the slice.
