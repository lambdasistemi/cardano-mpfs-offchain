# Feature Specification: Neutral CSMT Verifier WASM Package

## User Story

As a downstream browser flake consumer, I need a neutral `csmt-verify-wasm`
package from `cardano-mpfs-offchain` so the browser can use the generic CSMT
inclusion verifier without depending on MPFS reactor package names.

## Acceptance Criteria

- `nix build .#csmt-verify-wasm` succeeds.
- The package output contains exactly `csmt-verify-wasm.wasm`.
- The artifact is built from the already pinned `haskell-mts` fork used by the
  existing wasm reactor build.
- The existing `wasm-mpfs-verify` package still builds.

## Requirements

- FR-001: Add `csmt-verify-wasm` to the existing wasm target package list so it
  shares the `cardano-ledger-wasm` build closure.
- FR-002: Expose a top-level flake package attribute named `csmt-verify-wasm`.
- FR-003: Keep the public package neutral by selecting only
  `csmt-verify-wasm.wasm` into its output.
- FR-004: Do not bump or duplicate the pinned `haskell-mts` source.
- FR-005: Do not add a second full `mkCardanoLedgerWasm` derivation.

## Non-Goals

- Browser integration.
- MPFS reactor behavior changes.
- Haskell source changes in this repository or in `haskell-mts`.
