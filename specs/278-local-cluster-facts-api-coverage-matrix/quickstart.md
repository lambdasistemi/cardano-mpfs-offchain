# Quickstart: Facts API Coverage Matrix

## Goal

Run the local-cluster facts API matrix for the currently migrated MPFS facts endpoints.

## Command

The implementation must provide a named command or documented selector here before the PR leaves draft. Acceptable forms include:

```bash
just e2e "facts API coverage matrix"
```

or:

```bash
nix run .#e2e-tests -- --match "facts API coverage matrix"
```

## Expected Rows

- `/facts/boot -> verifyBootFacts -> bootCageTx -> submit -> token indexed`
- `/facts/request/insert -> verifyRequestInsertFacts -> requestInsertCageTx -> submit -> request indexed`
- `/facts/request/delete -> verifyRequestDeleteFacts -> requestDeleteCageTx -> submit -> delete request indexed -> fact removal observed`
- `/facts/end -> verifyEndFacts -> endCageTx -> submit -> token removed`

The command must also fail if a migrated legacy transaction endpoint remains reachable.
