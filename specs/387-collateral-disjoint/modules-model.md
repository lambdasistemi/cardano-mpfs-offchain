# Modules model — 387

Only changed responsibilities are listed.

| Module | Change |
|---|---|
| `Cardano.MPFS.Client.Cage.Retract` | Gains the responsibility of separating the collateral row from the funding rows. It stops treating one wallet row as both fee source and collateral. Dependency direction unchanged: it continues to depend on `Cage.Eval` and `Cage.BuildError` and is depended on by `Cardano.MPFS.Client`. |
| `Cardano.MPFS.Client.Cage.Eval` | No responsibility change. Its existing collateral-resolution guard becomes independently observable. |
| `Cardano.MPFS.Client.Cage.BuildError` | No change; `InsufficientCollateralUtxos` already exists and gains a fifth producer. |

No new module and no abstraction promotion: the selection rule is already
stated four times in sibling builders, but consolidating it is a separate
refactor with its own bisect risk and is deliberately not bundled with a
defect fix.

Test modules: `RetractSpec` (re-authored to the post-change world) and
`EvalSpec` (gains the R-3 example).
