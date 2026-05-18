# Quickstart: End fact-provider pivot

Baseline:

```bash
./gate.sh
```

Focused commands expected during implementation:

```bash
nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests --test-options '--match "/verifyEndFacts/"'
nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests --test-options '--match "/endCageTx/"'
nix develop --quiet -c cabal test cardano-mpfs-offchain:unit-tests --test-options '--match "/POST /facts/end/"'
nix develop --quiet -c just update-swagger
./gate.sh
```

Source checks:

```bash
rg 'POST /tx/end|\"/tx/end\"|TxEndAPI|txEndHandler' cardano-mpfs-api cardano-mpfs-offchain docs/assets/swagger.json
rg 'Cardano.Ledger.Api.Tx' cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs
```
