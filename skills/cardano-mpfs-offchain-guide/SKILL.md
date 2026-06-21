---
name: cardano-mpfs-offchain-guide
description: Guide to the lambdasistemi/cardano-mpfs-offchain repository — the off-chain fact-provider service for Cardano Merkle Patricia Forestry (MPFS). Load when working in this repo or answering questions about it. Covers the seven Haskell packages (cardano-mpfs-offchain server, cardano-mpfs-api, cardano-mpfs-verify, cardano-mpfs-cage-tx, cardano-mpfs-client, cardano-mpfs-workflows, cardano-mpfs-cli), the mpfs-spa PureScript browser SPA, executables (mpfs-serve, mpfs-cli, mpfs-devnet-server, mpfs-cage-reactor, mpfs-verify-reactor), the proof-bearing HTTP API (GET /tokens, /utxo, POST /facts/boot, /facts/update, /submit, /tx/sweep, swagger-ui), the RocksDB cage indexer (CageFollower, one block = one transaction, CSMT, MPF tries), CIP-30 wallet signing, environment variables MPFS_SERVER, MPFS_SIGNER_WALLET, MPFS_BLUEPRINT, and the just/nix/cabal build and test commands (just unit, just e2e, just ci).
---

# cardano-mpfs-offchain guide

## Repository map

| Path | Purpose |
|------|---------|
| `cardano-mpfs-offchain/` | Server package: RocksDB cage indexer, Servant HTTP API, executables (`exe/`), unit tests (`test/`), E2E tests (`e2e-test/`) |
| `cardano-mpfs-api/` | Shared Servant API type and JSON wire DTOs |
| `cardano-mpfs-verify/` | Pure proof verifiers (`verifyTokenState`, `verify*Facts`); compiles native, wasm32-wasi, GHC-JS |
| `cardano-mpfs-cage-tx/` | Pure client-side cage transaction builders (`*WithEval`) and the `mpfs-cage-reactor` executable |
| `cardano-mpfs-client/` | Native HTTP client wrappers; re-exports the verifier surface; `mpfs-verify` executable |
| `cardano-mpfs-workflows/` | Verified read/write workflows over the client surface |
| `cardano-mpfs-cli/` | `mpfs-cli` executable (owner/requester lifecycle) |
| `mpfs-spa/` | PureScript + Halogen browser SPA, talks HTTP + CIP-30, runs the wasm cage reactor |
| `lean/` | Lean 4 formal model of the Phase 4 proof design |
| `docs/` | mkdocs site: installation, CLI manual, architecture, swagger |
| `specs/` | Speckit feature specs/plans/tasks, one directory per issue |
| `nix/`, `flake.nix` | haskell.nix project, wasm targets, SPA derivation, docker image |
| `deploy/` | SPA nginx/Docker deployment |
| `scripts/` | E2E and deployment shell scripts |

Code maps with per-module detail: `NAVIGATION.md` (repo root) and
`cardano-mpfs-offchain/NAVIGATION.md` (server package).

## Build, test, run

Everything runs from `nix develop` via `just`:

```bash
just build           # cabal build all (-O0)
just unit            # offchain unit tests (nix run .#unit-tests)
just unit-client     # client unit tests
just unit-workflows  # workflows unit tests
just unit-cli        # CLI unit tests
just e2e             # E2E: spawns a cardano-node devnet subprocess
just e2e-spa         # Playwright SPA test against a local devnet
just format          # fourmolu
just hlint           # hlint
just ci              # full local CI mirror
just update-swagger  # regenerate docs/assets/swagger.json
just docs            # mkdocs serve
```

Pass a test pattern with `just unit "pattern"` / `just e2e "pattern"`.
Direct flake apps: `nix run .#mpfs-devnet-server -- --port 3000`,
`nix run .#mpfs-cli -- --help`, `nix build .#mpfs-serve`.

## Navigating the code

- **Server entry point**: `cardano-mpfs-offchain/exe/Serve.hs`
  (`mpfs-serve` flags) → `Cardano.MPFS.Application.withApplication`
  (full wiring: RocksDB, two N2C connections, Context) →
  `Cardano.MPFS.HTTP.Server.mkApp` (all handlers).
- **HTTP API type**: `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
  (canonical Servant API); wire types in `Cardano.MPFS.API.Types*`.
- **Indexer**: `Cardano.MPFS.Indexer.CageFollower` (chain-follower
  Runner integration), `Indexer.Backend` (`composedInit` business
  logic), `Indexer.Follower` (event detection/application/inverses),
  `Indexer.Columns` (14-column DB schema), `Indexer.Reads` (atomic
  `IndexerTx` reads used by handlers via `Context.runIndexerTx`).
- **Service records**: `Provider`, `State`, `Trie`, `Submitter`,
  `TxBuilder`, `Context` — records of functions, no typeclasses;
  mocks under `Cardano.MPFS.Mock.*`.
- **Verifiers**: `cardano-mpfs-verify/lib/Cardano/MPFS/Client/`
  (`Verify.Read`, `Facts`, `Verify.Completeness`, `Verify.MPF`).
- **Client builders**: `cardano-mpfs-cage-tx/lib/Cardano/MPFS/Client/Cage/`
  (one module per operation; `Reactor` is the JSON-envelope
  dispatcher compiled to wasm).
- **CLI**: `cardano-mpfs-cli/lib/Cardano/MPFS/CLI/Options.hs` is the
  authoritative command/flag tree; `Run.hs` drives the workflows.
- To find a feature: grep `cardano-mpfs-api` for the endpoint type,
  then the handler in `HTTP/Server.hs`, then the verifier in
  `cardano-mpfs-verify` and the builder in `cardano-mpfs-cage-tx`.

## Using the service and CLI

Server (needs a synced cardano-node socket):

```bash
mpfs-serve --socket node.socket --db ./db --port 3000 \
  --shelley-genesis shelley-genesis.json --blueprint cage.json
```

CLI configuration: `MPFS_SERVER` (base URL), `MPFS_SIGNER_WALLET`
(Bech32 `ed25519_sk1...` key file), `MPFS_BLUEPRINT` (cage blueprint
JSON). Lifecycle: `register-token` → `fact insert/update/delete`
(requester) → `token process` (owner folds requests) → `fact get` /
`fact list` (verified reads) → `fact retract` / `fact reject` →
`token end`. All write commands verify server facts locally, build,
sign, and submit; `--json` gives machine-readable output.

Live deployment: <https://umpfs.plutimus.com> (API) and
<https://umpfs.plutimus.com/spa/> (browser SPA). Swagger UI at
`/swagger-ui` on any running server.

## Answering questions

- "What is this / how does it work?" — README **What is this** and
  `docs/architecture/overview.md` (fact-provider model, submit gate).
- "How do I install / run it?" — `docs/installation.md`; README
  **Install** and **Quickstart**.
- "What endpoints exist?" — README **HTTP API** table;
  `docs/assets/swagger.json` is generated from the code
  (`just update-swagger`) and is always current.
- "How do I use the CLI?" — `docs/cli/index.md` (manual),
  `docs/cli/walkthrough.md` (full lifecycle),
  `docs/cli/troubleshooting.md` (409s, key formats, timing windows).
- "How is block processing atomic?" —
  `docs/architecture/block-processing.md`.
- "Which node protocols are used?" —
  `docs/architecture/data-sources.md` (ChainSync, LSQ, LTxS; no
  Ogmios).
- "How is it tested?" — `docs/architecture/testing.md`.
- "Why is the API facts-first / why no unsigned txs?" —
  `.specify/memory/constitution.md` and README **What is this**.
- Validator identity questions: the current cage script hash is in
  README **What is this**; identity comes from the pinned
  `cardano-mpfs-onchain` blueprint (flake input + cabal.project pin).
