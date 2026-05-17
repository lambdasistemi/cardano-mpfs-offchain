# Contract: Boot verifier and local builder

## Verifier

```haskell
verifyBootFacts
    :: TrustedRoot
    -> BootFacts
    -> Either VerifyError VerifiedBootFacts
```

Checks:

1. Trusted root is 32 bytes.
2. `BootFacts.snapshot.utxo_root` is 32 bytes.
3. Snapshot root equals trusted root.
4. Every wallet UTxO entry replays as a valid CSMT inclusion proof
   against the snapshot root.

Forbidden:

- `IO`
- HTTP/network/filesystem/time access
- `Cardano.Ledger.Api.Tx` imports
- transaction body inspection

## VerifiedBootFacts

```haskell
newtype VerifiedBootFacts
```

The constructor is not exported. Public callers can obtain this value
only through `verifyBootFacts`.

## Local Builder

```haskell
bootCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Either BuildError (Tx ConwayEra)
```

Builder obligations:

1. Decode wallet UTxO TxOut CBOR to ledger-native `TxOut ConwayEra`.
2. Decode unverified protocol parameters.
3. Enforce `WalletPolicy`.
4. Build the boot transaction locally with the cage TxBuild program.
5. Return unsigned `Tx ConwayEra` or `BuildError`.

Forbidden:

- `IO` in `bootCageTx`.
- Server-side indexer/provider queries.
- Accepting raw `BootFacts`.
- Rebuilding boot logic in the HTTP handler.

## Required Proofs

- Verifier tests: happy path, snapshot tamper, trusted-root mismatch,
  proof tamper.
- Builder test: serialized transaction CBOR matches
  `specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor`
  for equivalent inputs.
- Grep proof: boot verifier surface has no transaction grammar imports.
