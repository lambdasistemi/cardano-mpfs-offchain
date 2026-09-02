# Data model — 387

No persisted data and no wire format changes.

## State invariants on the built body

- `collateralInputs ∩ inputs = ∅` for every builder that sets collateral.
- Every element of `collateralInputs` is a key of the collateral UTxO set
  handed to the evaluator.
- The reserved collateral row is not a member of the funding row set.
- The funding row set is exactly the wallet rows minus the reserved collateral
  row.

## Validation

| Input | Condition | Outcome |
|---|---|---|
| wallet rows | empty | `EmptyFunding` |
| wallet rows | exactly one | `InsufficientCollateralUtxos` |
| collateral inputs | not resolvable in the collateral UTxO set | `MissingBalancedInput` |
