# Implementation Plan: Retire `mpfs-spa`

## Context

`cardano-mpfs-browser` now has parity with the legacy PureScript SPA. The
offchain repository still carries the SPA source directory, PureScript Nix
packaging, Playwright e2e shell apps, CI build entries, and README references.
Those can be retired, but `wasm-mpfs-verify` and `csmt-verify-wasm` must remain
because the browser consumes those artifacts directly.

## Keep Invariant

Do not edit `nix/wasm-targets.nix`. In `flake.nix`, preserve:

```nix
inherit (wasmTargets) wasm-mpfs-verify csmt-verify-wasm;
```

Every slice must prove:

- `nix build .#wasm-mpfs-verify .#csmt-verify-wasm --fallback`
- `nix flake check`

## Slices

### S1 - Remove SPA Playwright e2e wiring

Owned files:

- `scripts/e2e-spa-devnet.sh`
- `scripts/e2e-spa-preprod.sh`
- `flake.nix` (only `test-playwright-spa` shell apps and flake apps)
- `justfile` (only `e2e-spa` recipes)

Proof:

- RED-skip: removal-only slice with no dedicated test harness.
- GREEN: the keep-invariant build and `nix flake check` pass; no
  `test-playwright-spa` or `e2e-spa` live references remain.

Commit:

- `chore(spa): remove retired playwright e2e wiring`
- Trailer: `Tasks: T372-S1`

### S2 - Remove SPA package and source

Owned files:

- `mpfs-spa/`
- `nix/mpfs-spa.nix`
- `flake.nix` (only the `mpfs-spa` package and dev shell wiring)
- `justfile` (only the `.#mpfs-spa` build token)
- `.github/workflows/ci.yml` (only the `.#mpfs-spa` build token)
- `nix/clean-src.nix`
- `README.md`
- `docs/architecture/testing.md` if README cleanup exposes stale SPA testing
  docs during reference checks
- `docs/architecture/dependencies.md` only if the dep-graph drift gate proves
  it stale

Proof:

- RED-skip: removal-only slice with no dedicated test harness.
- GREEN: the full gate passes; `grep` finds no live `mpfs-spa` references in
  source/config/docs outside release history.

Commit:

- `chore(spa): retire legacy mpfs spa`
- Trailer: `Tasks: T372-S2`

## Gate Notes

The gate mirrors the offchain CI build/test surface and adds retirement-specific
checks:

- keep-invariant wasm build
- full flake check
- CI build derivations and unit/e2e apps
- Cabal version/manifest parity
- live-reference checks for `mpfs-spa`

The GitHub `dep-graph-drift` job is action-based. If CI reports drift, refresh
`docs/architecture/dependencies.md` using the same `paolino/dev-assets/dep-graph`
action in `staleness=false` mode and include the generated doc in S2.
