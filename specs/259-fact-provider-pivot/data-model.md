# Phase 1 Data Model: Fact-provider pivot

## Scope

Eight per-endpoint `XFacts` records (server-side wire shapes), eight
`VerifiedXFacts` newtype wrappers (verifier output), one client-side
`WalletPolicy`, eight cage-protocol DSL helpers (in
`cardano-mpfs-client`, the in-repo client library; the helpers
compose `Cardano.Node.Client.TxBuild` primitives imported from
upstream `cardano-node-clients`). All other entities reused
unchanged.

## New entities (server side)

### `BootFacts`

```haskell
data BootFacts = BootFacts
    { bfSnapshot           :: BundleSnapshot
    , bfWalletUtxos        :: [(TxIn, ByteString, ByteString)]
                              -- (ref, txOut bytes, csmtProof bytes)
    , bfProtocolParameters :: UnverifiedPParams
    }
```

**Location**: `Cardano.MPFS.Client.Facts` (in `cardano-mpfs-client`,
shared between server and client).

### `RequestFacts`

Same shape as `BootFacts` — request endpoints need exactly the same
indexer reads (snapshot + wallet UTxOs at requester address + pp).
Operation discriminator + payload travel in the request body, not
the response.

### `RetractFacts`

```haskell
data RetractFacts = RetractFacts
    { rfSnapshot           :: BundleSnapshot
    , rfRequestUtxo        :: (TxIn, ByteString, ByteString)
                              -- the named pending-request UTxO
    , rfWalletUtxos        :: [(TxIn, ByteString, ByteString)]
                              -- funding inputs at requester
    , rfProtocolParameters :: UnverifiedPParams
    }
```

### `EndFacts`

```haskell
data EndFacts = EndFacts
    { efSnapshot           :: BundleSnapshot
    , efStateUtxo          :: (TxIn, ByteString, ByteString)
                              -- the cage state UTxO for the token
    , efWalletUtxos        :: [(TxIn, ByteString, ByteString)]
                              -- funding inputs at owner address
    , efProtocolParameters :: UnverifiedPParams
    }
```

### `UpdateFacts`

```haskell
data UpdateFacts = UpdateFacts
    { ufSnapshot           :: BundleSnapshot
    , ufStateUtxo          :: (TxIn, ByteString, ByteString)
    , ufRequestUtxos       :: [(TxIn, ByteString, ByteString)]
                              -- pending requests in the batch
    , ufWalletUtxos        :: [(TxIn, ByteString, ByteString)]
                              -- funding at owner
    , ufTrieFacts          :: [TrieFact]
                              -- one per affected key
    , ufProtocolParameters :: UnverifiedPParams
    }

data TrieFact = TrieFact
    { tfKey         :: ByteString
    , tfValue       :: Maybe ByteString
                       -- Nothing for inclusion-of-absence
    , tfMpfProof    :: ByteString
    }
```

### `RejectFacts`

Same shape as `UpdateFacts` — batched processing of past-retract-time
requests has identical fact shape.

### `UnverifiedPParams`

```haskell
data UnverifiedPParams = UnverifiedPParams
    { uppCbor     :: ByteString
                     -- full Conway PParams CBOR
    , uppVerified :: Bool
                     -- always False today; reserved for future signed-pp
    }
```

JSON encoding includes both fields explicitly so integrators see
the unverified status without reading the spec.

## New entities (client-verifier side)

### `Verified*Facts` (eight newtypes)

```haskell
newtype VerifiedBootFacts    = VerifiedBootFacts    BootFacts
newtype VerifiedRequestFacts = VerifiedRequestFacts RequestFacts
newtype VerifiedRetractFacts = VerifiedRetractFacts RetractFacts
newtype VerifiedEndFacts     = VerifiedEndFacts     EndFacts
newtype VerifiedUpdateFacts  = VerifiedUpdateFacts  UpdateFacts
newtype VerifiedRejectFacts  = VerifiedRejectFacts  RejectFacts
```

**Constructor exports**: kept *internal* to the verifier module —
the only legitimate way to obtain a `Verified*Facts` value is by
calling the corresponding `verify*Facts` function. The cage-protocol
DSL helpers consume `Verified*Facts` only.

**Location**: `Cardano.MPFS.Client.Verify`.

### `verify*Facts` (eight functions)

```haskell
verifyBootFacts        :: TrustedRoot -> BootFacts        -> Either VerifyError VerifiedBootFacts
verifyRequestFacts     :: TrustedRoot -> RequestFacts     -> Either VerifyError VerifiedRequestFacts
verifyRetractFacts     :: TrustedRoot -> RetractFacts     -> Either VerifyError VerifiedRetractFacts
verifyEndFacts         :: TrustedRoot -> EndFacts         -> Either VerifyError VerifiedEndFacts
verifyUpdateFacts      :: TrustedRoot -> UpdateFacts      -> Either VerifyError VerifiedUpdateFacts
verifyRejectFacts      :: TrustedRoot -> RejectFacts      -> Either VerifyError VerifiedRejectFacts
```

Each function:

1. Asserts `xfSnapshot.utxoRoot == trustedRoot`.
2. Asserts every CSMT proof in the bundle verifies against
   `xfSnapshot.utxoRoot` for the corresponding (TxIn, TxOut bytes)
   leaf.
3. (Tier-3 only) Asserts every MPF proof in `tfMpfProof` verifies
   against the trie root encoded in the consumed state UTxO's
   datum.
4. Returns the `Verified*Facts` newtype on success.

`VerifyError` enumerates: `SnapshotMismatch`,
`CsmtProofInvalid TxIn`, `MpfProofInvalid ByteString`,
`MalformedFactsBundle Text`.

## New entities (client-side, in MOOG / any wallet)

### `WalletPolicy`

```haskell
data WalletPolicy = WalletPolicy
    { wpMaxFee                 :: Coin
    , wpMaxExUnitsPrice        :: ExUnitsPrices  -- max acceptable
    , wpMaxMinUtxoCoinPerByte  :: Coin
    , wpMaxValidityWindow      :: SlotNo
    }
```

Documented per FR-009. Wallets that don't override get sensible
mainnet defaults from a dedicated `Cardano.MPFS.Client.WalletPolicy`
module (no upstream dependency on `cardano-node-clients` for
defaults).

## New entities (cage DSL host, in `cardano-mpfs-client`)

### Eight cage-protocol helpers

```haskell
-- Cardano.MPFS.Client.Cage.Boot
bootCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Either BuildError (Tx ConwayEra)

-- Cardano.MPFS.Client.Cage.Request (three exports)
requestInsertCageTx :: CageConfig -> WalletPolicy -> RequestPayload -> VerifiedRequestFacts -> Either BuildError (Tx ConwayEra)
requestDeleteCageTx :: CageConfig -> WalletPolicy -> RequestPayload -> VerifiedRequestFacts -> Either BuildError (Tx ConwayEra)
requestUpdateCageTx :: CageConfig -> WalletPolicy -> RequestPayload -> VerifiedRequestFacts -> Either BuildError (Tx ConwayEra)

-- Cardano.MPFS.Client.Cage.Retract
retractCageTx :: CageConfig -> WalletPolicy -> VerifiedRetractFacts -> Either BuildError (Tx ConwayEra)

-- Cardano.MPFS.Client.Cage.End
endCageTx     :: CageConfig -> WalletPolicy -> VerifiedEndFacts     -> Either BuildError (Tx ConwayEra)

-- Cardano.MPFS.Client.Cage.Update
updateCageTx  :: CageConfig -> WalletPolicy -> VerifiedUpdateFacts  -> Either BuildError (Tx ConwayEra)

-- Cardano.MPFS.Client.Cage.Reject
rejectCageTx  :: CageConfig -> WalletPolicy -> VerifiedRejectFacts  -> Either BuildError (Tx ConwayEra)
```

Each helper composes the generic `Cardano.Node.Client.TxBuild`
operational-monad primitives (`spend`, `payTo'`, `attachScript`,
`mint`, `collateral`) imported from upstream `cardano-node-clients`.
The MPFS-specific cage protocol logic (datums, redeemers, asset-name
derivation, MPF fold) is supplied by helper-private modules under
`Cardano.MPFS.Client.Cage.Internal`.

`BuildError` enumerates: `EmptyFunding`, `PolicyViolation
PolicyViolationDetail`, `MalformedDatum`, `DSLBuildFailed
TxBuildError`.

## Changed entities

### `Cardano.MPFS.HTTP.API` (server)

- Removed: every `transaction/{address}/{op}` path.
- Added: eight `POST /facts/{op}` paths.
- Unchanged: `GET /tokens/...`, `GET /utxo/...`, `GET /status`,
  `POST /submit` (already present), all metrics endpoints.

### `Cardano.MPFS.HTTP.Server` (server)

- Removed: `txBootHandler`, `txInsertHandler`, etc. (and
  `mapAtomicError` if any remained).
- Added: `factsBootHandler`, `factsInsertHandler`, … one per
  operation. Each is a single `runIndexerTx ctx $ do { … }`
  composition + assemble response.

### `Cardano.MPFS.Indexer.Reads` (server)

- Added: `readStateUtxoAt`, `readRequestUtxosAt`,
  `readNamedRequestUtxo`, `readTrieFact`. All inside the existing
  `IndexerTx` monad — same atomicity discipline.

### `Cardano.MPFS.Application` (server)

- The `txBuilder` field on `Context` is removed. It's no longer
  needed because there is no server-side transaction building.
- The `Provider` field's `evaluateTx` becomes unused on the server
  side (no DSL `build` call). Keep the field for now to avoid
  rippling Provider changes; mark it as candidate-for-removal in
  a follow-up.

### `Cardano.MPFS.TxBuilder.Real.*` (server)

**Entire tree removed.** Content moves to
`cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/*` (in-repo
move; same monorepo, different cabal package). The post-pivot
server has no transaction-building code at all. `Real.Boot` is
already a pure cage builder on `main` (`bootTokenCore` returns
`BootCore { bcProgram :: TxBuild ... }`) and is the easiest
relocation; the other five operations get the same purify-then-
relocate treatment per stgit patch.

### `Cardano.MPFS.Client.Verify.*` (verifier)

- Removed: `verifyBootTxResponse`, `verifyRequestTxResponse`, etc.,
  all conservation/template-enforcement helpers, all imports of
  `Cardano.Ledger.Api.Tx`.
- Added: `verifyBootFacts`, etc. — the eight pure proof-validity
  functions defined above.
- Cross-target QuickCheck suite asserts byte-identical outputs
  between native, GHC-WASM, and GHC-JS (Principle IX).

### `MPFS.API` (MOOG) → `MPFS.Facts` (MOOG)

- Module renamed; HTTP client functions renamed
  `bootToken` → `factsBoot`, etc.
- Each function now returns `Either ServantError XFacts`.
- Caller responsibility shifts: the existing `signAndSubmitMPFS`
  closure becomes `verifyAndBuildAndSign` — it pulls facts, runs
  the verifier, runs the cage helper, signs, submits.

## Validation rules

| Rule                                                                                                                                                                       | Source FR(s) |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| Every facts response contains a `BundleSnapshot` and the proof-bearing data the named operation needs at that snapshot.                                                  | FR-002       |
| Per-endpoint shapes match FR-003 of the spec.                                                                                                                              | FR-003       |
| Every CSMT proof in a response verifies against the response's snapshot's UTxO root.                                                                                       | FR-004       |
| Every MPF fact in a response verifies against the trie root encoded in the consumed state UTxO's datum at the same snapshot.                                              | FR-004       |
| Each handler is one `runIndexerTx ctx $ do { … }` block; new tier-2/tier-3 primitives sit inside the same block.                                                          | FR-005       |
| Server source after the pivot contains zero `transaction/{address}/...` path entries; swagger.json reflects only the new shape.                                           | FR-006, SC-002 |
| `Cardano.MPFS.Client.Verify.*` after the pivot imports neither `Cardano.Ledger.Api.Tx` nor any transaction-grammar type.                                                   | FR-007, SC-003 |
| Cage-protocol DSL helpers in `cardano-mpfs-client` produce byte-equal `Tx ConwayEra` to the legacy server-side `Real.*` modules for equivalent inputs (Principle V check). | FR-008, SC-001 |
| Each facts response carries `protocol_parameters` as `{cbor: <hex>, verified: false}`; spec and integrator docs name the wallet-policy mitigation.                        | FR-009       |
| MOOG's main after the pivot imports no module named `MPFS.API`; every legacy callsite migrated.                                                                            | FR-010, SC-002 |
| Both repository defaults move in the same release window; neither broken between landings.                                                                                  | FR-011, SC-005 |
