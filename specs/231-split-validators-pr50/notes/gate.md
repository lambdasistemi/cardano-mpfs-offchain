# GATE — quality gate for branch `231-split-validators-pr50`

Captured per `pr` skill Setup step 1 (T001).

## Single-command form

```bash
nix develop --command bash -c '
  just ci \
  && find . -name "*.cabal" -not -path "./dist-newstyle/*" | xargs cabal-fmt -c \
  && just e2e
'
```

CI (`/.github/workflows/ci.yml`) runs `cabal-fmt -c` on all `.cabal`
files; `just ci` does not. Adding it here matches CI exactly.

Run from the repo root. This is the full per-patch gate — invoke it
unchanged after every `stg refresh` and at every step of the stack
walk (T061).

## What it covers

`just ci` (`justfile:67`) decomposes into:

```
just build           # cabal build all -O0 across the multi-package project
just unit            # MPF / client unit tests
just unit-offchain   # offchain interface + unit tests (incl. OnChainSpec, TxBuilderSpec)
just format-check    # fourmolu --mode check
just hlint
```

`just e2e` (`justfile:75`) builds `.#e2e-tests` through Nix and runs
the resulting `./result/bin/e2e-tests` executable under `nix develop`.
This matches the GitHub `e2e` job. The suite covers the offchain E2E
modules under `cardano-mpfs-offchain/e2e-test/` (CageSpec,
CageFlowSpec, ChainSyncSpec, HTTPLifecycleSpec, IndexerSpec,
ProofsSpec) against a `cardano-node` subprocess devnet.

E2E is **not** part of `just ci` — it is a separate recipe. Per
Constitution Principle V (byte-for-byte parity with the upstream
cage test vectors at `cf3a8bdc`), E2E coverage is load-bearing for
this feature, so the gate must include both.

## Tool versions

Pinned by the project's `flake.nix` development shell (entered via
`nix develop`). No system-level tools are used; the gate must be run
through `nix develop`.

## Ordering

`just ci` runs first so that a broken build short-circuits before
spinning up a cardano-node subprocess for E2E. `&&` enforces the
short-circuit.

## Notes

- Whitespace and typo workflows are not separate jobs in this repo —
  fourmolu (`just format-check`), hlint (`just hlint`), and
  `cabal-fmt -c` are the only style gates CI runs.
- Path-scope: `just format-check` covers the entire workspace, not
  a subset, so no per-path scoping is needed. `cabal-fmt -c` runs
  against every `*.cabal` outside `dist-newstyle/`.
