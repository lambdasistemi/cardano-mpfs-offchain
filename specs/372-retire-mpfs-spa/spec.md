# Feature Specification: Retire `mpfs-spa`

## User Story

As a maintainer, I need the legacy `mpfs-spa` bundle and offchain packaging
removed now that `cardano-mpfs-browser` has parity, while keeping the
`wasm-mpfs-verify` cage reactor and `csmt-verify-wasm` artifacts available for
downstream browser consumers.

## Acceptance Criteria

- `mpfs-spa/` and `nix/mpfs-spa.nix` are removed.
- `flake.nix`, `justfile`, `.github/workflows/ci.yml`, and
  `nix/clean-src.nix` no longer expose or build `mpfs-spa`.
- The Playwright SPA e2e shell apps, scripts, and `just` recipes are removed.
- README references to the retired SPA are removed.
- `nix/wasm-targets.nix` is untouched.
- The flake package inherit for `wasm-mpfs-verify` and `csmt-verify-wasm`
  remains intact.
- `nix build .#wasm-mpfs-verify .#csmt-verify-wasm --fallback` succeeds.
- `nix flake check` succeeds and there are no live `mpfs-spa` references
  outside release history.

## Requirements

- FR-001: Remove only the legacy SPA source, packaging, e2e wiring, and
  documentation references.
- FR-002: Keep both browser-consumed wasm artifacts exposed from the offchain
  flake.
- FR-003: Leave release-please-managed `CHANGELOG.md` untouched.
- FR-004: Do not change `.cabal` versions or any browser repository files.
- FR-005: Preserve bisectability: every slice must leave the flake evaluating
  and the wasm keep-invariant buildable.

## Non-Goals

- Browser integration changes.
- Removing or changing `wasm-mpfs-verify` or `csmt-verify-wasm`.
- Haskell package version changes.
- Hand-written changelog edits.
