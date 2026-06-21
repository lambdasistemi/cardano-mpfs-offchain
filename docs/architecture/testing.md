# Testing

## Unit Tests

The repository has several focused unit suites:

- **`cardano-mpfs-offchain:unit-tests`** — indexer persistence,
  atomic block processing, on-chain encodings, trie managers, state
  tracking, HTTP facts/read endpoints, and server-side proof envelopes.
- **`cardano-mpfs-client:unit-tests`** — native client facade,
  read/facts verifiers, HTTP wrappers, and cage builder integration.
- **`cardano-mpfs-workflows:unit-tests`** — higher-level workflow
  helpers over the API/client surface.
- **`cardano-mpfs-cli:unit-tests`** — CLI behavior. CLI documentation is
  owned separately and is intentionally not covered here.
- **`cardano-mpfs-cage-tx:byte-identity`** — native/wasm cage reactor
  output parity for deterministic envelopes.

```bash
just unit
just unit-client
just unit-workflows
just unit-cli
```

Unit tests use mock implementations (`mkMockState`,
`mkPureTrieManager`, `mkMockTxBuilder`) and don't require a
running Cardano node.

### HTTP API unit tests

The REST API is exercised through WAI test sessions against a mock
`Context`. Current coverage includes:

| Spec | What it covers |
|------|----------------|
| `StatusSpec` | `GET /status` field validation and UTxO root exposure |
| `TokensSpec` | `GET /tokens` completeness witness and explicit `token_id` entries |
| `TokenSpec` | `GET /tokens/:id` witnessed token-state lookup and 404s |
| `TokenFactsSpec` | `GET /tokens/:id/facts` enumeration and witnessed state |
| `RequestsSpec` | `GET /tokens/:id/requests` filtering and request witnesses |
| `TrieSpec` | `GET /tokens/:id/root`, facts, and MPF proofs |
| `BootFactsSpec` | `POST /facts/boot` facts-only response |
| `RequestInsertFactsSpec` | `POST /facts/request/insert` |
| `RequestDeleteFactsSpec` | `POST /facts/request/delete` |
| `RequestUpdateFactsSpec` | `POST /facts/request/update` |
| `UpdateFactsSpec` | `POST /facts/update`, including request subset selection |
| `RetractFactsSpec` | `POST /facts/retract` |
| `RejectFactsSpec` | `POST /facts/reject`, including request subset selection |
| `EndFactsSpec` | `POST /facts/end` |
| `SubmitSpec` | `POST /submit` signed transaction submission |
| `SubmitScopeSpec` | `POST /submit` MPFS-scope gate — non-MPFS transactions are rejected |

Client tests cover `verifyTokenState`, `verifyTokenFacts`,
`verifyTokenRequests`, the `verify*Facts` family, CSMT replay failures,
MPF fact present/absent checks, and the `*WithEval` cage APIs. The eval
context tests assert the reactor derives `EpochInfo` from live
era-history, covering the former 502 era-history failure mode.

## E2E Tests

End-to-end tests run against a real `cardano-node` subprocess.
No Docker, no external services.

```bash
nix run .#e2e-tests  # direct flake app
just e2e             # same app, with optional --match support
```

### How It Works

The test harness (`Cardano.Node.Client.E2E.Devnet` from the
[cardano-node-clients](https://github.com/lambdasistemi/cardano-node-clients)
package, used as `withCardanoNode`) manages a single-node devnet:

1. **Genesis files** ship with the `devnet-genesis` package of
   `cardano-node-clients`; the flake apps export their location as
   `E2E_GENESIS_DIR`. They define a testnet with magic 42, 0.1s
   slots, all hard forks at epoch 0 (instant Conway).

2. **At startup**, the harness copies genesis files to a temp
   directory and patches `systemStart` to current UTC time + 5
   seconds (giving the node time to initialize before the first
   slot).

3. **`cardano-node run`** starts as a subprocess with delegate keys
   for block production. The harness polls for the socket file
   (up to 30 seconds).

4. **Tests use genesis UTxO funds** directly — a deterministic
   signing key derived from a hardcoded seed matches the
   `initialFunds` address in `shelley-genesis.json`. No faucet
   or top-up mechanism needed.

5. **After tests complete**, the node is terminated and the temp
   directory cleaned. No root-owned files.

### Test Specs

- **ProviderSpec** — N2C LocalStateQuery for protocol parameters
  and evaluation metadata
- **SubmitterSpec** — build, balance, sign, and submit an ADA
  transfer via N2C LocalTxSubmission
- **CageSpec** — cage event detection from real transactions
- **CageFlowSpec** — full cage flow (boot, request, update,
  retract) with `CageFollower` auto-indexing
- **IndexerSpec** — `detectFromTx` + `applyCageEvent` against
  real Plutus transactions
- **ChainSyncSpec** — chain sync protocol with a real node
- **HTTPLifecycleSpec** — full token lifecycle via HTTP API
  (boot, insert, update, insert, retract, end), confirmed via
  `GET /tx/:txId` blocking endpoint
- **ProofsSpec** — proof-bearing reads and facts checked against the
  indexed trusted root
- **BootFactsSpec** — `POST /facts/boot`: verify facts, build the
  boot transaction locally, submit, and confirm indexing
- **CrashRecoverySpec** — kill the service during following and
  verify cage state survives the restart
- **TokensCompletenessSpec** — token set completeness witnesses
- **TokenFactsCompletenessSpec** — fact enumeration completeness
- **FactsMatrixSpec** — local-cluster matrix for facts endpoints,
  request subsets, update/retract/reject/end, and bounded-refund cases
- **SubmitEndpointSpec** — signed submit endpoint behavior
- **WorkflowsIntegrationSpec** — workflow helpers over facts plus local
  construction

### Prerequisites

The flake app wraps the E2E executable with `cardano-node`,
`cardano-cli`, `MPFS_BLUEPRINT`, and `E2E_GENESIS_DIR`, so a dev shell
is not required for the E2E command itself:

```bash
just e2e
```

### Genesis Configuration

| Parameter | Value | Why |
|-----------|-------|-----|
| `testnetMagic` | 42 | Arbitrary devnet identifier |
| `slotLength` | 0.1s | Fast block production for tests |
| `activeSlotsCoeff` | 1.0 | Every slot produces a block |
| `epochLength` | 500 | Short epochs |
| `securityParam` | 10 | 1-second stability window (10 × 0.1s) |
| `systemStart` | patched at runtime | UTC now + 5 seconds |
| All hard forks | epoch 0 | Instant Conway |

## Browser SPA Tests

The PureScript SPA is exercised with Playwright:

```bash
just e2e-spa          # local devnet
just e2e-spa-preprod  # https://umpfs.plutimus.com
```

The preprod smoke points the browser at the live server, injects a
CIP-30 signer, fetches facts and `GET /eval-context`, runs the wasm cage
reactor, signs locally, and submits via `POST /submit`. The SPA itself
is served at `https://umpfs.plutimus.com/spa/`.
