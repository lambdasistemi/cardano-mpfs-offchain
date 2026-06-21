# cardano-mpfs-offchain

Off-chain service for
[cardano-mpfs-onchain](https://github.com/cardano-foundation/cardano-mpfs-onchain)
--- indexing, proof-bearing facts, and HTTP API for Cardano Merkle
Patricia Forestry.

## What is this

The server connects to a Cardano node via node-to-client (N2C),
indexes cage UTxOs into RocksDB, and exposes a trust-minimized REST
API. It is a fact provider: it serves indexed snapshots, UTxOs with
CSMT proofs, MPF facts, request sets, and ledger evaluation metadata.
Clients verify those facts, build transactions locally, sign with
their wallet, and submit signed CBOR via `POST /submit` --- the server
never returns unsigned transactions for script-bearing operations.

The browser SPA is available at <https://umpfs.plutimus.com/spa/>. It
uses HTTP plus CIP-30 only; protocol logic runs inside the pure Haskell
wasm cage reactor, not in ad hoc browser code.

The #62 bounded-refund validator cutover uses cage script hash
`ad0a8eeeec8b0a5ee9930be5d6ea2e80b285fc2f3e9675a13a392dd5`. The old
`c0f05a30...` exact-refund hash is legacy. The SPA and server both take
validator identity from the pinned `cardano-mpfs-onchain` blueprint so
the browser reactor and offchain indexer agree on state and per-cage
request validator addresses.

## Architecture

The repository hosts seven Haskell packages plus the PureScript SPA:

```mermaid
graph TD
    OFFCHAIN["cardano-mpfs-offchain<br/><i>server: indexer + fact API</i>"]
    API["cardano-mpfs-api<br/><i>wire types</i>"]
    VERIFY["cardano-mpfs-verify<br/><i>proof verifiers</i>"]
    CAGETX["cardano-mpfs-cage-tx<br/><i>client builders</i>"]
    CLIENT["cardano-mpfs-client<br/><i>native HTTP client</i>"]
    WORKFLOWS["cardano-mpfs-workflows<br/><i>workflow helpers</i>"]
    CLI["cardano-mpfs-cli<br/><i>mpfs-cli executable</i>"]
    ONCHAIN["cardano-mpfs-onchain/haskell<br/><i>cage library</i>"]
    CLIENTS["cardano-node-clients"]
    MTS["haskell-mts"]
    FOLLOWER["chain-follower"]
    NODE["cardano-node<br/><i>Unix socket</i>"]
    SPA["mpfs-spa<br/><i>HTTP + CIP-30</i>"]

    OFFCHAIN -->|"Servant API<br/>JSON DTOs"| API
    VERIFY -->|"wire DTOs"| API
    CAGETX -->|"verifies facts first"| VERIFY
    CLIENT -->|"HTTP wrappers"| API
    CLIENT -->|"re-exports verifiers"| VERIFY
    CLIENT -->|"local cage builders"| CAGETX
    WORKFLOWS -->|"read + write flows"| CLIENT
    CLI -->|"owner/requester<br/>workflows"| WORKFLOWS
    CLI --> CLIENT
    OFFCHAIN -->|"on-chain types<br/>proof serialization<br/>blueprint loading"| ONCHAIN
    CAGETX -->|"on-chain types<br/>scripts"| ONCHAIN
    OFFCHAIN -->|"N2C provider<br/>LocalTxSubmission<br/>eval context"| CLIENTS
    CAGETX -->|"TxBuild DSL<br/>fee balancing"| CLIENTS
    OFFCHAIN -->|"CSMT + MPF trie<br/>pure + RocksDB"| MTS
    VERIFY -->|"CSMT/MPF replay"| MTS
    OFFCHAIN -->|"ChainSync<br/>block streaming"| FOLLOWER
    CLIENTS --> NODE
    FOLLOWER --> NODE
    SPA -->|"facts + eval context"| OFFCHAIN
    SPA -->|"wasm reactor"| CAGETX

    click ONCHAIN "https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell"
    click CLIENTS "https://github.com/lambdasistemi/cardano-node-clients"
    click MTS "https://github.com/lambdasistemi/haskell-mts"
    click FOLLOWER "https://github.com/lambdasistemi/chain-follower"
```

- **[cardano-mpfs-onchain/haskell](https://github.com/cardano-foundation/cardano-mpfs-onchain/tree/main/haskell)** ---
  canonical Haskell types matching the Aiken validator (`CageDatum`, `UpdateRedeemer`, `RequestAction`, `ProofStep`),
  MPF proof serialization, CIP-57 blueprint parsing, and asset-name derivation.
  The offchain re-exports these via `Core.OnChain`, `Core.Proof`, and `Core.Blueprint`.

- **[cardano-node-clients](https://github.com/lambdasistemi/cardano-node-clients)** ---
  operational-monad DSL for local transaction construction and
  convergent fee balancing (`Tx.build` + `Peek`), N2C protocol
  parameters, script evaluation, slot conversion, and
  LocalTxSubmission. `LocalStateQuery` UTxO scans are not used on the
  proof-bearing write path; UTxO facts come from the indexed CSMT.

- **[haskell-mts](https://github.com/lambdasistemi/haskell-mts)** ---
  CSMT/MPF primitives: sparse UTxO set proofs, 16-ary Merkle Patricia
  Forestry with Blake2b-256 hashing, pure in-memory backends for tests,
  RocksDB-backed production storage, and proof generation compatible
  with the [Aiken MPF library](https://github.com/aiken-lang/merkle-patricia-forestry).

- **[chain-follower](https://github.com/lambdasistemi/chain-follower)** ---
  ChainSync protocol client that streams blocks from a Cardano node.
  The offchain's `CageFollower` processes each block in a single atomic
  RocksDB transaction.

- **`cardano-mpfs-verify`** ---
  pure cross-target verifier package. It exposes snapshot, facts, read,
  CSMT completeness, and MPF replay verifiers such as
  `verifyTokenState`, `verifyTokenFacts`, `verifyTokenRequests`, and
  `verify*Facts`. The same code is compiled native, wasm32-wasi, and
  GHC-JS.

- **`cardano-mpfs-cage-tx`** ---
  pure client-side cage transaction builders and the
  `mpfs-cage-reactor` executable. Script-bearing operations are
  `*WithEval` builders that consume verified facts plus
  `GET /eval-context` metadata and return local unsigned transactions
  for wallet signing.

- **`cardano-mpfs-client`** ---
  native HTTP client wrappers re-exporting the verifier surface, plus
  the `mpfs-verify` snapshot-checking executable.

- **`cardano-mpfs-workflows`** ---
  higher-level verified read/write workflows over the client surface,
  used by the CLI.

See [docs/architecture](https://lambdasistemi.github.io/cardano-mpfs-offchain/architecture/overview/)
for the server-internal architecture.

## Install

- **Release tarballs** --- each
  [GitHub release](https://github.com/lambdasistemi/cardano-mpfs-offchain/releases)
  ships self-contained `mpfs-cli` bundles for `x86_64-linux` and
  `aarch64-darwin` (`mpfs-cli-<version>-<platform>.tar.gz`; unpack and
  run `bin/mpfs-cli`).
- **Nix** --- the flake exposes the server and tools as packages:

  ```bash
  nix build .#mpfs-serve   # production server
  nix build .#mpfs-cli     # command-line client
  ```

- **Docker** --- `just build-docker` builds the server image via Nix
  and loads it into the local Docker daemon as
  `ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve`.

## Quickstart

Query the live preprod deployment:

```bash
curl -s https://umpfs.plutimus.com/status
```

Or run everything locally --- a single-node devnet with the MPFS API
on port 3000, then the CLI against it:

```bash
nix run .#mpfs-devnet-server -- --port 3000
nix run .#mpfs-cli -- --help
```

The [CLI walkthrough](https://lambdasistemi.github.io/cardano-mpfs-offchain/cli/walkthrough/)
drives the full token lifecycle (register, insert, process, get,
delete, end) against the devnet server.

## Usage

### HTTP API

Servant REST API wrapping the internal `Context` record-of-functions.
The public model is facts-first: script-bearing write endpoints return
proof-bearing facts, never unsigned transaction CBOR. The browser and
other clients verify those facts, run the pure cage builder locally, add
wallet witnesses, and submit via `POST /submit`. The remaining
server-built transaction endpoint is the owner-only `/tx/sweep` cleanup
path.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/status` | Chain tip and checkpoint |
| GET | `/eval-context` | Trusted interim PlutusV3 PParams, cost models, SystemStart, and era-history for wallet-side ex-unit evaluation |
| GET | `/tokens` | Enumerate token-state UTxOs (token id decoded from each entry's `txout_cbor`) with a completeness witness |
| GET | `/tokens/:id` | Token state with UTxO witness and verification snapshot |
| GET | `/tokens/:id/root` | Trie root hash |
| GET | `/tokens/:id/facts` | Enumerate facts with witnessed state |
| GET | `/tokens/:id/facts/:key` | Present fact value with state witness and MPF proof; absent keys currently return 404 |
| GET | `/tokens/:id/proofs/:key` | MPF proof for a key, including exclusion proofs used by `verifyFactAbsentFacts` |
| GET | `/tokens/:id/requests` | Pending requests with UTxO witnesses and request-set completeness |
| GET | `/utxo/root` | Current indexed UTxO-CSMT root |
| GET | `/utxo/:txId/:txIx` | Resolve an indexed UTxO |
| GET | `/utxo/:txId/:txIx/proof` | UTxO CSMT inclusion proof |
| GET | `/tx/:txId?timeout=30` | Block until tx is indexed |
| GET | `/metrics`, `/metrics/prometheus` | Server metrics (JSON / Prometheus exposition) |
| POST | `/facts/boot` | Return boot facts for wallet-side transaction construction |
| POST | `/facts/request/insert` | Return insert-request facts for wallet-side transaction construction |
| POST | `/facts/request/delete` | Return delete-request facts for wallet-side transaction construction |
| POST | `/facts/request/update` | Return update-request facts for wallet-side transaction construction |
| POST | `/facts/update` | Return update facts for an exact request subset; `requests: []` means catch-all |
| POST | `/facts/retract` | Return retract facts for a named request UTxO |
| POST | `/facts/reject` | Return reject facts for an exact request subset; `requests: []` means catch-all |
| POST | `/facts/end` | Return end facts for wallet-side transaction construction |
| POST | `/tx/sweep` | Build owner-only sweep transaction for non-legitimate UTxO cleanup |
| POST | `/submit` | Submit signed transaction CBOR |

`POST /facts/update` and `POST /facts/reject` accept
`requests: ["txid#ix", ...]` as a fail-closed subset. An omitted or
empty list is the catch-all mode. A non-empty list that does not match
processable/rejectable indexed requests fails instead of silently
changing the batch.

Swagger UI is served at `/swagger-ui`.

### CLI

`mpfs-cli` drives the full owner/requester lifecycle against a server,
signing locally with a Bech32 ed25519 key. Configuration comes from
`MPFS_SERVER`, `MPFS_SIGNER_WALLET`, and `MPFS_BLUEPRINT` (or the
corresponding `--server`, `--owner-key`, `--cage-config` flags):

- owner: `register-token`, `token process`, `fact reject`, `token end`
- requester: `fact insert`, `fact update`, `fact delete`, `fact retract`
- verified reads: `token list`, `token get`, `fact list`, `fact get`,
  `requests list`

See the [CLI manual](https://lambdasistemi.github.io/cardano-mpfs-offchain/cli/)
for flags, defaults, and troubleshooting.

### Verification client

The `cardano-mpfs-verify` package is the pure verifier. It is carved out
from `cardano-mpfs-client` so the verification closure is free of IO,
networking, Servant, and HTTP client dependencies. `cardano-mpfs-client`
still re-exports the verifier surface for native clients.

Read-side verifiers include:

- `verifyTokenState` for `GET /tokens/:id`
- `verifyTokenFacts` for `GET /tokens/:id/facts`
- `verifyTokenRequests` for `GET /tokens/:id/requests`
- `verifyFactPresentFacts` and `verifyFactAbsentFacts` for key proofs

Facts-side verifiers include `verifyBootFacts`,
`verifyRequestInsertFacts`, `verifyRequestUpdateFacts`,
`verifyRequestDeleteFacts`, `verifyUpdateFacts`, `verifyRetractFacts`,
`verifyRejectFacts`, and `verifyEndFacts`.

The older `mpfs-verify` CLI in `cardano-mpfs-client` verifies the
snapshot carried by a proof-bearing JSON response:

```bash
curl -s https://host/status | cabal run mpfs-verify
```

For browser and cross-target use, the native/wasm reactors read JSON
envelopes on stdin and return deterministic one-line verdicts:
`mpfs-verify-reactor` verifies envelopes and `mpfs-cage-reactor` verifies
facts before building cage transactions.

## Documentation

The full manual lives at
<https://lambdasistemi.github.io/cardano-mpfs-offchain/> (CLI manual,
architecture, API reference).

For AI agents, start at [AGENTS.md](AGENTS.md).

## Development

```bash
nix develop
just build           # cabal build all components
just unit            # unit tests via nix run .#unit-tests
just unit-client     # client unit tests
just unit-workflows  # workflows unit tests
just unit-cli        # CLI unit tests
just e2e             # E2E tests via nix run .#e2e-tests
just format          # fourmolu
just hlint           # lint
just ci              # full local CI mirror
just update-swagger  # regenerate docs/assets/swagger.json
just docs            # serve the mkdocs site locally
```

## License

[Apache-2.0](LICENSE)
