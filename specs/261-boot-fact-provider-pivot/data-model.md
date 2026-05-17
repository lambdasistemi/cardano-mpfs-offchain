# Data Model: Boot fact-provider pivot

## BootFacts

Boot facts are the server response for `POST /facts/boot`.

```haskell
data BootFacts = BootFacts
    { bfSnapshot :: VerificationSnapshot
    , bfWalletUtxos :: [UtxoEntry]
    , bfProtocolParameters :: UnverifiedPParams
    }
```

Validation:

- `bfSnapshot.utxo_root` must be 32 bytes.
- Every `bfWalletUtxos` entry must have a valid TxIn reference,
  non-empty TxOut CBOR, and non-empty CSMT inclusion proof.
- The endpoint may return an error for an empty wallet UTxO set; if it
  returns a bundle, the builder must also reject empty funding.

## UnverifiedPParams

Protocol parameters are included so clients can build locally, but they
are not proof-verified by this API.

```haskell
data UnverifiedPParams = UnverifiedPParams
    { uppVerified :: Bool
    , uppCbor :: Hex
    }
```

Validation:

- `uppVerified` is `False` for this slice.
- `uppCbor` must decode as Conway protocol parameters before the local
  builder uses it.
- Wallet policy must cap any value that can increase loss or denial of
  service risk.

## TrustedRoot

Existing client-side trusted UTxO root. It is compared byte-for-byte
with `bfSnapshot.utxo_root`.

Validation:

- Must be exactly 32 bytes.
- Mismatch causes verification failure before construction.

## VerifiedBootFacts

Opaque verifier output.

```haskell
newtype VerifiedBootFacts = VerifiedBootFacts BootFacts
```

Validation:

- Constructor is not exported.
- Can only be obtained from `verifyBootFacts`.
- Carries no extra runtime data; the type is the proof token.

## WalletPolicy

Client-side caps enforced before signing.

```haskell
data WalletPolicy = WalletPolicy
    { wpMaxFee :: Coin
    , wpMaxExUnitsPrice :: ExUnitsPrices
    , wpMaxMinUtxoCoinPerByte :: Coin
    , wpMaxValidityWindow :: SlotNo
    }
```

Validation:

- `bootCageTx` rejects protocol parameters or constructed transactions
  exceeding policy.
- Defaults are suitable for MOOG's first native integration but can be
  overridden by wallet operators in the paired MOOG PR.

## BuildError

Client-side transaction construction failures.

```haskell
data BuildError
    = EmptyFunding
    | MalformedTxOut Text
    | MalformedPParams Text
    | PolicyViolation PolicyViolationDetail
    | DSLBuildFailed Text
```

Validation:

- `EmptyFunding` for no wallet UTxOs.
- `MalformedTxOut` or `MalformedPParams` before running the TxBuild DSL.
- `PolicyViolation` before signing.
- `DSLBuildFailed` preserves the underlying pure build failure.

## Legacy Boot CBOR Vector

Checked-in binary vector used by the byte-equivalence proof.

Path:

```text
specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor
```

Validation:

- Captured from the legacy boot path before deletion.
- The new helper's serialized `Tx ConwayEra` must match this vector for
  equivalent inputs.
