# Testing

## Unit Tests

Two unit test suites:

- **`merkle-patricia-forestry:unit-tests`** — MPF trie operations,
  hashing, proofs, insertion/deletion round-trips
- **`cardano-mpfs-offchain:unit-tests`** — balance, on-chain type
  encoding, proof serialization, trie manager, state tracking,
  HTTP API

```bash
just unit            # MPF unit tests
just unit-offchain   # offchain unit tests
```

Unit tests use mock implementations (`mkMockState`,
`mkPureTrieManager`, `mkMockTxBuilder`) and don't require a
running Cardano node.

### HTTP API unit tests

16 tests exercise the REST API via WAI test sessions against a
mock `Context`:

| Spec | Tests | What it covers |
|------|-------|----------------|
| `StatusSpec` | 3 | `GET /status` field validation |
| `TokensSpec` | 2 | `GET /tokens` list |
| `TokenSpec` | 2 | `GET /tokens/:id` lookup + 404 |
| `RequestsSpec` | 4 | `GET /tokens/:id/requests` filtering |
| `TrieSpec` | 5 | `GET /tokens/:id/root`, facts, proofs |

## E2E Tests

End-to-end tests run against a real `cardano-node` subprocess.
No Docker, no external services.

```bash
just e2e             # E2E tests (requires cardano-node in PATH)
```

### How It Works

The test harness (`Cardano.MPFS.E2E.Devnet`) manages a
single-node devnet:

1. **Genesis files** are checked into `e2e-test/genesis/`. They
   define a testnet with magic 42, 0.1s slots, all hard forks at
   epoch 0 (instant Conway).

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
  and UTxOs
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

### Prerequisites

`cardano-node` and `cardano-cli` must be in `PATH`. The nix dev
shell provides them:

```bash
nix develop    # adds cardano-node 10.5.4 to PATH
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
