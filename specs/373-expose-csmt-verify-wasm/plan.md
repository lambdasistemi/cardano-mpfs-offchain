# Implementation Plan: Neutral CSMT Verifier WASM Package

## Context

`nix/wasm-targets.nix` builds the existing reactor wasm artifacts through one
`libWasm.mkCardanoLedgerWasm` derivation named `wasm-mpfs-verify`. The pinned
`haskell-mts` source already contains the executable target
`csmt-verify-wasm`, so this ticket should extend the existing wasm build rather
than introduce another dependency closure.

## Design

1. Add `"csmt-verify-wasm"` to the `packages` list in
   `wasm-mpfs-verify`.
2. Add a thin `csmt-verify-wasm` derivation in `nix/wasm-targets.nix` that
   copies only `${wasm-mpfs-verify}/csmt-verify-wasm.wasm` to `$out`.
3. Expose that derivation from `flake.nix` in `packages`.
4. If Nix reports a fixed-output dependency hash mismatch, recompute the
   `dependenciesHash` mechanically using `pkgs.lib.fakeHash` and the printed
   replacement hash.

## Slices

### S1 - Expose `csmt-verify-wasm`

Owned files:

- `nix/wasm-targets.nix`
- `flake.nix`

Proof:

- RED: `nix build .#csmt-verify-wasm` fails because the flake attribute is
  missing.
- GREEN: `./gate.sh` passes, including builds for `.#csmt-verify-wasm` and
  `.#wasm-mpfs-verify`, plus a single-file output assertion.

Commit:

- `feat(wasm): expose neutral csmt verifier wasm`
- Trailer: `Tasks: T373`
