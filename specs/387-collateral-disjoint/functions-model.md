# Functions model — 387

Only new or changed signatures.

## Changed

```
Cardano.MPFS.Client.Cage.Retract.selectFeeRow
```

Its result type changes from a single row to the triple already used by
`selectBootRows`: the funding rows, the row that pays the fee, and the
reserved collateral row. Argument stays the decoded wallet row list; the
result stays in `Either BuildError`. The name should follow the responsibility
once it selects more than a fee row.

```
Cardano.MPFS.Client.Cage.Retract.buildRetractTx
```

Gains an explicit reserved-collateral row argument alongside the existing
funding rows, mirroring `buildBootTx`. The collateral row is passed to the
evaluator as the collateral UTxO set and is not unioned into the spent inputs.

## Unchanged

`evaluateAndBalancePure`, `evaluateAndBalancePureAtFee`, and
`ensureCollateralInputsResolved` keep their current signatures. `retractCageTx`
and `retractCageTxWithEval` keep their exported signatures — the `moog-v2`
consumer surface does not change.
