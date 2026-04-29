# Data Model: Post-Split Proof Redesign

This document inventories every type that crosses the API boundary or the verifier boundary in this feature. Field names below correspond directly to JSON wire keys; Haskell record names follow the convention used in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`.

## Foundational types (kept from existing API, not changed)

### `VerificationSnapshot`

```haskell
data VerificationSnapshot = VerificationSnapshot
    { vsUtxoRoot   :: Hex          -- "utxo_root"   — CSMT root at chainpoint
    , vsChainPoint :: ChainPoint   -- "chainpoint"  — { slot, block_id }
    }
```

Identical to today. Acts as the single anchor for every proof in a response. Verifiers reject responses whose embedded snapshots disagree across nested fields.

### `UtxoRef`

```haskell
data UtxoRef = UtxoRef
    { urTxId :: Hex      -- "tx_id"
    , urTxIx :: Word64   -- "tx_ix"
    }
```

### `ChainPoint`

Existing type, unchanged.

## Read-side payload primitives (new)

### `UtxoEntry`

```haskell
data UtxoEntry = UtxoEntry
    { ueRef            :: UtxoRef   -- "ref"
    , ueTxOutCbor      :: Hex       -- "txout_cbor"
    , ueInclusionProof :: Hex       -- "inclusion_proof"  — CSMT inclusion against the snapshot's utxo_root
    }
```

The leaf type that appears wherever a single UTxO is being attested.

### `UtxoEntryRefOnly`

```haskell
data UtxoEntryRefOnly = UtxoEntryRefOnly
    { ueRef       :: UtxoRef  -- "ref"
    , ueTxOutCbor :: Hex      -- "txout_cbor"
    }
```

The leaf type that appears inside a `UtxoSetWitness` — no per-entry inclusion proof, because the set's completeness proof attests every leaf at once.

### `UtxoSetWitness`

```haskell
data UtxoSetWitness = UtxoSetWitness
    { uswEntries           :: [UtxoEntryRefOnly]   -- "entries"
    , uswCompletenessProof :: Hex                  -- "completeness_proof"
    }
```

Carries an enumerated set of UTxOs together with one CSMT prefix-completeness proof attesting the set is exactly the leaves under a script-hash prefix in the CSMT under the snapshot's `utxo_root`. The script-hash prefix itself is *not* in the witness — the verifier derives it locally from the trusted blueprint and the per-cage parameters.

## Read-side response types

### `StatusResponse` (changed)

```haskell
data StatusResponse = StatusResponse
    { srTipSlot    :: Word64    -- "tip_slot"
    , srTipBlockId :: Hex       -- "tip_block_id"
    }
```

Removed fields vs today's version: `checkpoint_slot`, `checkpoint_block_id`, `utxo_root`. The server stops being authoritative for indexed checkpoints and CSMT roots.

### `TokensListResponse` (changed)

```haskell
data TokensListResponse = TokensListResponse
    { tlrSnapshot :: VerificationSnapshot   -- "snapshot"
    , tlrTokens   :: UtxoSetWitness         -- "tokens"
    }
```

Replaces today's bare `[TokenIdJSON]`. The witness is a CSMT prefix-completeness proof at the **global state validator script address**, derived client-side. Clients decode each `entries[i].txout_cbor` to recover the token id from the value's NFT and classify legitimate vs garbage.

### `TokenResponse` (changed)

```haskell
data TokenResponse = TokenResponse
    { trSnapshot  :: VerificationSnapshot   -- "snapshot"
    , trStateUtxo :: UtxoEntry              -- "state_utxo"
    , trRequests  :: UtxoSetWitness         -- "requests"
    }
```

Both the state UTxO and the per-cage requests set anchored to the same snapshot. The requests witness's prefix is the **per-cage request validator script address**, derived client-side from `(state_policy_id, asset_name)` via the trusted blueprint.

### `FactPresentResponse` (new — HTTP 200 from `GET /tokens/:id/facts/:key`)

```haskell
data FactPresentResponse = FactPresentResponse
    { fprSnapshot          :: VerificationSnapshot   -- "snapshot"
    , fprStateUtxo         :: UtxoEntry              -- "state_utxo"
    , fprValue             :: Hex                    -- "value"
    , fprMpfInclusionProof :: Hex                    -- "mpf_inclusion_proof"
    }
```

### `FactAbsentResponse` (new — HTTP 404 with body from `GET /tokens/:id/facts/:key`)

```haskell
data FactAbsentResponse = FactAbsentResponse
    { farSnapshot          :: VerificationSnapshot   -- "snapshot"
    , farStateUtxo         :: UtxoEntry              -- "state_utxo"
    , farMpfExclusionProof :: Hex                    -- "mpf_exclusion_proof"
    }
```

HTTP 404 *without* a body remains the third arm: indexer has no state UTxO for the token. That case is unverified by design (the client falls back to `GET /tokens` for cryptographic absence at the global level).

### `ConfirmResponse` (new — HTTP 200 from `GET /tx/:txId?timeout=N`)

```haskell
data ConfirmResponse = ConfirmResponse
    { crSnapshot       :: VerificationSnapshot   -- "snapshot"
    , crRef            :: UtxoRef                -- "ref"   (always { txId, 0 })
    , crTxOutCbor      :: Hex                    -- "txout_cbor"
    , crInclusionProof :: Hex                    -- "inclusion_proof"
    }
```

HTTP 408 with no body remains the timeout arm.

## Write-side response type (uniform)

### `UnsignedTxResponse`

```haskell
data UnsignedTxResponse = UnsignedTxResponse
    { utrUnsignedTxCbor             :: Hex                       -- "unsigned_tx_cbor"
    , utrSnapshot                   :: VerificationSnapshot      -- "snapshot"
    , utrInputs                     :: [UtxoEntry]               -- "inputs"
    , utrRequestsCompletenessProof  :: Maybe Hex                 -- "requests_completeness_proof"
    }
```

Used by every `POST /tx/...` endpoint. The `inputs` list is *flat*: it contains an entry for every input the unsigned tx mentions, whether spent or referenced. The role of each entry (consumed vs read-only reference) is recovered by the client from the decoded `unsigned_tx_cbor`'s redeemer attachments. The `requests_completeness_proof` field is present only on `POST /tx/oracle/update` and `POST /tx/oracle/end`; absent (= JSON `null` or omitted) elsewhere.

JSON encoding rule: when `requests_completeness_proof` is absent, omit the key entirely (do not emit `null`) for cleaner Swagger.

## Existing request body types (unchanged)

`BootRequest`, `InsertRequest`, `DeleteRequest`, `UpdateValueRequest`, `RetractRequest`, `RejectRequest`, `UpdateRequest`, `SweepRequest`, `EndRequest`, `SubmitRequest` keep their current shapes. Only response shapes change.

## Verifier-side types

### `TrustedRoot`

```haskell
newtype TrustedRoot = TrustedRoot { unTrustedRoot :: Hex }
```

A nominal newtype carried by the verifier API. Makes the trust boundary visible at type level: every verifier accepts `TrustedRoot` rather than a bare `Hex`, so it is always clear at the call site that this value comes from outside the offchain server.

### `Blueprint`

```haskell
data Blueprint = Blueprint
    { bpStatePolicyId       :: Hex
    , bpStateScriptAddress  :: Address
    , bpRequestScriptAddress :: AssetName -> Address
    }
```

The trusted Aiken blueprint's verifier-relevant projections. Distributed out of band per `research.md` §4. Carried as a value through the verifier entry points.

### `VerifyError`

Extended in `contracts/verify-error.md`. Carries dotted field paths in the failure (continuing the convention from #226).

## Removed types

The following types are removed from `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` together with the endpoints that returned them:

- `BootTxResponse`, `RequestTxResponse`, `RetractTxResponse`, `RejectTxResponse`, `UpdateTxResponse`, `SweepTxResponse`, `EndTxResponse` — all collapse into the uniform `UnsignedTxResponse`.
- `ProofResponse` — endpoint removed.
- The bare `Hex` returned by `GET /tokens/:id/root` and `GET /utxo/root` — endpoints removed.
- `RequestsResponse` — folded into `TokenResponse`.

## State transitions

This feature does not introduce server-side state transitions beyond what the existing indexer already maintains. The indexer's CSMT and per-cage UTxO sets evolve as blocks arrive; response construction is a pure read against an immutable snapshot.

The verifier-side state machine (Lean) is documented in `lean/Phase4/ProofRedesign.lean` and `lean/Phase4/Completeness.lean`. The plan's Phase 1 lists the predicates and preservation theorems; their full bodies land in the implementation phase as prerequisites for the Haskell modules (constitution X).
