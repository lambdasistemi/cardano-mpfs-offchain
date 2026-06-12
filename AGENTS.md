# Repository Agent Guide

## What this repo is

Off-chain service for
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain):
it indexes cage UTxOs from a Cardano node into RocksDB and serves a
trust-minimized, proof-bearing REST API for Merkle Patricia Forestry
tokens. The server is a fact provider — it returns indexed snapshots,
UTxOs with CSMT proofs, MPF facts, and evaluation metadata; clients
verify the facts, build and sign transactions locally, and submit
signed CBOR via `POST /submit`.

## Project structure

```text
cardano-mpfs-offchain/   server: indexer, HTTP API, e2e tests
cardano-mpfs-api/        shared Servant API + JSON wire types
cardano-mpfs-verify/     pure cross-target proof verifiers
cardano-mpfs-cage-tx/    pure client-side cage tx builders + wasm reactor
cardano-mpfs-client/     native HTTP client, re-exports verifiers
cardano-mpfs-workflows/  verified read/write workflow helpers
cardano-mpfs-cli/        mpfs-cli executable
mpfs-spa/                PureScript browser SPA (HTTP + CIP-30)
lean/                    Lean 4 formal model (Phase 4)
docs/                    mkdocs site (architecture, CLI manual, swagger)
specs/                   speckit feature specs, plans, and tasks
```

The MPF/CSMT trie implementation lives in the external
[haskell-mts](https://github.com/lambdasistemi/haskell-mts) repository,
pinned in `cabal.project`.

## How to work here

Use `just` recipes from `nix develop` (GHC 9.10.1 toolchain, Fourmolu,
HLint):

```bash
just build           # cabal build all components (-O0)
just unit            # offchain unit tests
just unit-client     # client unit tests
just unit-workflows  # workflows unit tests
just unit-cli        # CLI unit tests
just e2e             # E2E tests (spawns a cardano-node subprocess)
just format          # apply Fourmolu formatting
just format-check    # check formatting
just hlint           # run HLint
just ci              # full local CI mirror
just update-swagger  # regenerate docs/assets/swagger.json
just docs            # serve the mkdocs site locally
```

Source of truth:

- Project constitution: `.specify/memory/constitution.md`
  (authoritative for architectural decisions)
- Architecture: `docs/architecture/overview.md`
- Testing guide: `docs/architecture/testing.md`
- Public API surface: `docs/assets/swagger.json`
- Code maps: `NAVIGATION.md` (repo-wide),
  `cardano-mpfs-offchain/NAVIGATION.md` (server package)

## Core constraints

- Use ledger-native Cardano types; do not introduce shadow ledger
  representations.
- Service boundaries use records of functions, not typeclasses.
- Block processing must be atomic across RocksDB column families
  (one block = one transaction).
- The server is a fact provider: it MUST NOT return unsigned
  transactions for script-bearing operations; clients build and sign
  locally. The only server-built transaction is the owner-only
  `/tx/sweep`.
- `LocalStateQuery` UTxO scans are forbidden on tx-build and proof
  paths; UTxO facts come from the indexed CSMT.
- Proof encoding, trie hashing, and datum/redeemer construction must
  stay compatible with the Aiken validators in `cardano-mpfs-onchain`.
- Client verifiers are pure offline functions: no `IO`, networking,
  filesystem, time, or non-determinism, and their dependency closure
  must cross-compile to GHC-WASM and GHC-JS.

## Spec Kit workflow

Every issue starts with speckit artifacts under `specs/<issue>-<slug>/`
before implementation: `spec.md` (requirement + acceptance criteria),
`plan.md` (technical approach + Constitution Check), `tasks.md`
(ordered, testable work items).

## Skills

Activatable procedures live under `skills/`. Load the one whose
description matches your task:

- `skills/cardano-mpfs-offchain-guide/` — repository map, build/test
  commands, code navigation, CLI and HTTP API usage, and where to find
  answers for user questions about this repo.
