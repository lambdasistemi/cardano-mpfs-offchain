# Contract: retractCageTx

`retractCageTx` takes:

- `CageConfig`
- `WalletPolicy`
- `VerifiedRetractFacts`

It returns either a `BuildError` or an unsigned `Tx ConwayEra`.

The caller must obtain `VerifiedRetractFacts` from
`verifyRetractFacts`; public APIs must not expose a constructor
that lets callers bypass fact verification.

The builder must:

- decode protocol parameters from `protocol_parameters.cbor`,
- enforce wallet protocol-parameter caps,
- decode the request UTxO, state UTxO, and wallet UTxOs from
  facts,
- extract the request owner key hash from the request UTxO's
  inline datum,
- build a retract transaction that:
  - spends the named request UTxO with the `Retract` redeemer
    pointing at the state UTxO ref,
  - references the state UTxO as a reference input,
  - uses the requester's wallet UTxOs for fees and collateral,
  - requires the request owner as a signer,
  - applies the server-derived Phase 2 validity slot interval,
- enforce transaction policy caps before returning.
