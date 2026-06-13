# Research: Shared Ledger WASM Kernel for MPFS Verify

## Decision: Align CHaP to the released kernel

Use `cardano-ledger-wasm` v0.1.1's CHaP revision
`00c90c10812a98ef9680f4bfa269d42366d46d89` and Cabal index-state
`cardano-haskell-packages 2026-04-15T11:20:53Z` for this migration.

Rationale: Issue #359 explicitly says to verify the CHaP pin aligns before
proceeding. The current MPFS branch starts at CHaP `2026-05-25T13:25:40Z`
with flake lock rev `9a187ad488ecf19944d6d119c41eff683a1ff54f`, so it
diverges from the released kernel.

Alternatives considered: keep the newer MPFS CHaP pin and only import the
builder. Rejected because it preserves the exact mismatch the issue calls out
as a first-order risk.

## Decision: Import the released flake as a dependency

Add `cardano-ledger-wasm` as a flake input pinned to
`845877fde0907b58b150a2c8302033b4e73e9061` and consume
`cardano-ledger-wasm.lib.wasm.mkCardanoLedgerWasm`.

Rationale: The released flake exports `lib.wasm = import ./nix/wasm`, and that
library exposes `mkCardanoLedgerWasm`, `forks`, and
`cabalWasmProjectFragment`. Importing that surface lets MPFS delete its local
copy of `nix/wasm/{forks.json,mkCardanoLedgerWasm.nix,cabal-project-fragment.nix}`
as the issue requests.

Alternatives considered: copy the v0.1.1 files into MPFS. Rejected because
that would keep a duplicate source of truth.

## Decision: Keep only MPFS-specific wasm project content locally

Retain local `cabal-wasm.project` content only for packages and SRPs that are
not part of the ledger kernel: `cardano-mpfs-verify`,
`cardano-mpfs-cage-tx`, `cardano-mpfs-api`, `cardano-mpfs-onchain`,
`haskell-mts`, `aiken-codegen`, `cardano-tx-tools`, and
`rocksdb-kv-transactions`.

Rationale: `cardano-ledger-wasm` owns the ledger closure forks, but it cannot
know MPFS application packages. The shared builder supports
`extraCabalProject` and `srpForks`; MPFS can use those to pass only the
MPFS-specific layer.

Alternatives considered: move all MPFS-specific SRPs into the released kernel.
Rejected because it would make the shared kernel depend on MPFS-specific code.

## Decision: Verification gate

Use the PR-local `gate.sh` plus focused commands:

- `nix develop --quiet -c just unit-client "Verify"`
- `nix build --quiet .#wasm-mpfs-verify`
- `nix build --quiet .#mpfs-spa`
- `nix develop --quiet -c just e2e-spa`
- `nix develop --quiet -c just ci`

Rationale: These commands cover native verifier behavior, wasm reactor output,
SPA bundling, browser reactor smoke, and the repository CI mirror.

Alternatives considered: only run full `just ci`. Rejected because `just ci`
does not currently build `wasm-mpfs-verify` or run the SPA reactor smoke.
