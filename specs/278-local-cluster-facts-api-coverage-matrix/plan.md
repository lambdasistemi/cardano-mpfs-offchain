# Implementation Plan: Local-Cluster Facts API Coverage Matrix

## Technical Context

The repository already has a Nix-backed E2E runner that starts a local `cardano-node` devnet and runs MPFS with the follower enabled. Existing coverage is uneven:

- `BootFactsSpec` already proves the full boot facts path.
- `ProofsSpec` exercises `/facts/end` but needs parity with boot for submit/index evidence.
- `CageFlowSpec` and `HTTPLifecycleSpec` prove devnet state transitions, but request-insert and request-delete currently use direct tx-builder calls rather than the HTTP facts -> verifier -> local cage builder boundary.

## Approach

Add a focused E2E matrix spec or refactor the existing facts E2E code so all currently migrated facts endpoints share the same proof shape:

1. Start the existing local devnet and MPFS app.
2. Build a token through `/facts/boot`.
3. Insert a request through `/facts/request/insert`.
4. Process that request using the existing update path to create an inserted fact when needed.
5. Delete that fact through `/facts/request/delete`.
6. Process the delete request using the existing update path to observe removal.
7. End the token through `/facts/end`.
8. Assert every migrated legacy transaction route is absent at the live HTTP boundary.

Prefer one focused spec with small helpers over spreading the matrix across many files. The row helpers should make missing parity obvious when later endpoints are added.

## Files Expected To Change

- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/...`
- `cardano-mpfs-offchain/e2e-test/main.hs`
- `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`
- `justfile` or docs if a named command/test selector is added
- `specs/278-local-cluster-facts-api-coverage-matrix/tasks.md`
- `gate.sh`

## Verification

- Run the focused matrix command.
- Run `./gate.sh`.
- If the full local matrix is too expensive for the gate, keep the route/static checks in `gate.sh`, record the exact matrix command and transcript in the PR body, and leave the PR draft until that transcript exists.

## Risk Notes

- Request-delete depends on an inserted fact, so the matrix may need an update/process step between request-insert and request-delete.
- End depends on an empty token state, so the matrix may need to process all pending requests before ending.
- Keep the row helper names explicit so later retract/update/reject tickets can extend the matrix mechanically.
