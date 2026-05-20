# Quickstart: Facts API Coverage Matrix

## Goal

Run the local-cluster facts API matrix for the currently migrated MPFS facts endpoints.

## Command

```bash
just e2e-facts-matrix
```

Equivalent forms:

```bash
just e2e "facts API coverage matrix"
nix run .#e2e-tests -- --match "facts API coverage matrix"
```

The recipe wraps `nix run .#e2e-tests` with the hspec
`--match` selector that targets the single matrix scenario
in `Cardano.MPFS.E2E.FactsMatrixSpec`. The harness boots a
real local `cardano-node` devnet plus the MPFS app with the
follower enabled, so `MPFS_BLUEPRINT` must point at the
PlutusV3 blueprint just like the rest of the E2E suite.

## Expected Rows

- `/facts/boot -> verifyBootFacts -> bootCageTx -> submit -> token indexed`
- `/facts/request/insert -> verifyRequestInsertFacts -> requestInsertCageTx -> submit -> request indexed`
- `/facts/request/delete -> verifyRequestDeleteFacts -> requestDeleteCageTx -> submit -> delete request indexed -> fact removal observed`
- `/facts/end -> verifyEndFacts -> endCageTx -> submit -> token removed`

The command must also fail if a migrated legacy transaction endpoint remains reachable. `Cardano.MPFS.E2E.FactsMatrixSpec.assertLegacyRoutesGone` exercises this at the live WAI boundary by POSTing to `/tx/boot`, `/tx/request/insert`, `/tx/request/delete`, and `/tx/end` and asserting a non-200 response.

## Gate Integration

`./gate.sh` retains the cheap source-level and Swagger checks (legacy routes absent from server, API package, client, Swagger) and adds a static check that `Cardano.MPFS.E2E.FactsMatrixSpec` stays wired into the E2E suite. The live matrix command itself is too slow for the standard PR gate, so it must be run manually via `just e2e-facts-matrix` and the resulting transcript captured in the PR body before the PR leaves draft.
