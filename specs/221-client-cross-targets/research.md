# Research: Cross-Target Client Verifier Builds

## Current client dependency surface

`cardano-mpfs-client` library dependencies are currently pure Haskell
and verifier-oriented:

- `aeson`
- `base`
- `base16-bytestring`
- `bytestring`
- `cborg`
- `hspec`
- `mts:csmt-core`
- `mts:csmt-verify`
- `mts:mpf-write`
- `operational`
- `text`

The library does not depend on `cardano-ledger-*`, `crypton`, `unix`,
`process`, RocksDB, network clients, or the offchain service package.
This is the expected starting point for Principle IX.

## Current Nix surface

The flake imports `nix/project.nix`, which defines a single native
haskell.nix project:

- `compiler-nix-name = "ghc984"`
- native packages include `cardano-mpfs-offchain`, `mpfs-serve`,
  `offchain-tests`, `e2e-tests`, `docker-image`, and Haddock outputs
- no current `cardano-mpfs-client` package output
- no current WASM/JS package output

## Open research questions

| Question | Initial Finding | Next Action |
|----------|-----------------|-------------|
| Can haskell.nix build the client library with GHC-WASM directly from this project? | Unknown. | Add or prototype a WASM project/output and run `nix build`. |
| Can haskell.nix build the client library with GHC-JS directly from this project? | Unknown. | Add or prototype a JS project/output and run `nix build`. |
| Do `mts` public sublibraries expose correctly in cross package databases? | Risk from prior dev-shell behavior around public sublibraries. | Treat missing sublibrary units as a first-class blocker/fix. |
| Should the `mpfs-verify` executable be part of the cross proof? | Not for the hard gate. | Start with the library; revisit CLI only if library builds are green. |
| How should parity tests run across targets? | Larger conformance suite belongs to #233. | Use a minimal existing verifier corpus if artifacts are runnable. |

## Blocker Log

No cross-target attempts have run yet in this worktree.
