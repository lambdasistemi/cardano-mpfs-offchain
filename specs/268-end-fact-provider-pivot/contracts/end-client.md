# Contract: End Client Surface

```haskell
verifyEndFacts
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> Either VerifyError VerifiedEndFacts

verifiedEndFacts :: VerifiedEndFacts -> EndFacts

endCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedEndFacts
    -> Either BuildError (Tx ConwayEra)
```

Verification checks:

- trusted root is 32 bytes,
- `snapshot.utxo_root` is 32 bytes and equals the trusted root,
- `state_utxo.inclusion_proof` replays against the snapshot root,
- every `wallet_utxos[*].inclusion_proof` replays against the snapshot root,
- `request_set.entries` is empty,
- `request_set.completeness_proof` verifies against the locally-derived per-cage request address prefix.

Builder checks:

- protocol parameters decode,
- wallet policy accepts protocol parameters and built transaction,
- state UTxO decodes to a state datum with an owner key hash,
- at least one funding UTxO exists,
- returned transaction consumes the state UTxO, burns exactly one token, has spending and minting redeemers, attaches the cage script, and requires the owner signer.
