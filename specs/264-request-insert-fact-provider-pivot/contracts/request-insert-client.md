# Contract: requestInsertCageTx

`requestInsertCageTx` takes:

- `CageConfig`
- `WalletPolicy`
- `VerifiedRequestInsertFacts`

It returns either a `BuildError` or an unsigned `Tx ConwayEra`.

The caller must obtain `VerifiedRequestInsertFacts` from
`verifyRequestInsertFacts`; public APIs must not expose a constructor that
lets callers bypass fact verification.

The builder must:

- decode protocol parameters from `protocol_parameters.cbor`,
- enforce wallet protocol-parameter caps,
- decode wallet UTxOs from `wallet_utxos`,
- build an insert request with the supplied token/key/value/address,
- enforce transaction policy caps before returning.
