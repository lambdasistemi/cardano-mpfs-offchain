# GATE — quality gate for branch `231-split-validators-pr50`

Captured per `pr` skill Setup step 1 (T001).

## Single-command form

```bash
just ci && just e2e
```

CI (`/.github/workflows/ci.yml`) uses flake outputs directly for all
non-Docker verification commands:

```bash
nix run --quiet .#unit-tests
nix run --quiet .#format-check
nix run --quiet .#hlint
nix run --quiet .#e2e-tests
```

Docker remains a package build (`nix build .#docker-image`) because
running Docker is a separate host-level boundary.

Run from the repo root. This is the full per-patch gate — invoke it
unchanged after every `stg refresh` and at every step of the stack
walk (T061).

## What it covers

`just ci` (`justfile:67`) decomposes into:

```
build package/check  # nix build .#cardano-mpfs-offchain + swagger check
just unit            # nix run .#unit-tests
just unit-offchain   # nix run .#unit-tests
just format-check    # nix run .#format-check
just hlint           # nix run .#hlint
```

`just e2e` (`justfile:75`) runs `nix run --quiet .#e2e-tests`.
The flake app wraps the built E2E test executable with
`MPFS_BLUEPRINT`, `E2E_GENESIS_DIR`, `cardano-node`, `cardano-cli`,
and `aiken` on the runtime path. This matches the GitHub `e2e` job.
The suite covers the offchain E2E modules under
`cardano-mpfs-offchain/e2e-test/` (CageSpec, CageFlowSpec,
ChainSyncSpec, HTTPLifecycleSpec, IndexerSpec, ProofsSpec) against a
`cardano-node` subprocess devnet.

E2E is **not** part of `just ci` — it is a separate recipe. Per
Constitution Principle V (byte-for-byte parity with the upstream
cage test vectors at `cf3a8bdc`), E2E coverage is load-bearing for
this feature, so the gate must include both.

## Tool versions

Pinned by the project's flake apps. `format-check` and `hlint` use
the same haskell.nix tool pins as the development shell. `unit-tests`
and `e2e-tests` wrap the built test executables with the required
blueprint/devnet runtime environment.

## Ordering

`just ci` runs first so that a broken build short-circuits before
spinning up a cardano-node subprocess for E2E. `&&` enforces the
short-circuit.

## Notes

- Whitespace and typo workflows are not separate jobs in this repo —
  fourmolu + cabal-fmt (`just format-check`) and hlint (`just hlint`)
  are the only style gates CI runs.
- Path-scope: `just format-check` covers the entire workspace, not
  a subset, so no per-path scoping is needed.
