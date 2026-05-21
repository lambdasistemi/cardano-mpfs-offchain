# Client Contract: Request-Update Facts Verification And Cage Helper

## Verifier

```haskell
verifyRequestUpdateFacts
    :: TrustedRoot
    -> RequestUpdateFacts
    -> Either VerifyError VerifiedRequestUpdateFacts
```

The verifier checks:

- trusted root length is 32 bytes,
- `snapshot.utxo_root` length is 32 bytes,
- `snapshot.utxo_root` equals the caller's trusted root,
- every `wallet_utxos[*]` CSMT proof replays against the trusted root.

The verifier does not import `Cardano.Ledger.Api.Tx` and does not validate
protocol parameters, `submitted_at`, or transaction grammar.

## Cage Helper

```haskell
requestUpdateCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRequestUpdateFacts
    -> Either BuildError (Tx ConwayEra)
```

The helper decodes protocol parameters and wallet UTxOs from verified facts,
enforces `WalletPolicy`, then builds the unsigned request transaction using
`OpUpdate oldValue newValue`. It must produce byte-identical transaction CBOR
to `legacy-request-update.cbor` for the deterministic equivalent legacy input.
