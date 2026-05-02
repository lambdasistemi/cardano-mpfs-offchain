# Feature Specification: Atomic POST /tx/boot

**Feature Branch**: `249-atomic-boot-handler`
**Created**: 2026-05-02
**Status**: Draft
**Input**: User description: "atomic-boot-handler — make `POST /tx/boot` read its `BundleSnapshot`, the resolved `TxOut` bytes for every wallet UTxO at the owner address, and a CSMT inclusion proof per UTxO, all in ONE database transaction over the local indexer. Drop `queryUTxOs prov addr` from the boot path entirely (cardano-node's `GetUTxOByAddress` is O(total UTxOs on chain) — see #252). The indexer is the single source of truth on tx-build paths (#250). Wire-level API stays unchanged: `POST /tx/boot { address }` — the server picks UTxOs at the address from its own CSMT (which already indexes them), not from cardano-node and not from the request body. Tests for fixtures that run with `followerEnabled = False` need a test seam (AppConfig override) to substitute a wallet-backed stub atomic reader."

## User Scenarios & Testing

### User Story 1 — Honest boot of a token under realistic chain churn (Priority: P1)

A wallet client sends `POST /tx/boot { address }` against an MPFS server whose chain follower is actively processing blocks (the realistic production case). The server returns an unsigned boot transaction together with a `VerificationSnapshot` and a CSMT inclusion proof for every wallet input the transaction spends. The wallet signs, submits, and the on-chain validator accepts the transaction. An offline verifier given the response and an independently-obtained trusted UTxO root checks the response without performing any further query: every proof verifies against the snapshot's `utxo_root`, and the snapshot's `utxo_root` equals the externally-supplied trusted root.

**Why this priority**: This is the core promise of the proof-bearing API. If the response cannot be verified offline against the embedded snapshot, the proof-bearing contract is broken and trust collapses. Today the response is racy (#250, #252) — fixing this is the unblocker for every other proof-bearing slice.

**Independent Test**: Run the boot end-to-end against a devnet under load: submit boot while the chain follower is processing concurrent blocks, then run the verifier on the response with the snapshot's own root as the trusted root. The verifier MUST accept. Repeat with a tampered root and the verifier MUST reject with a snapshot mismatch. The test holds independently of every other handler.

**Acceptance Scenarios**:

1. **Given** an MPFS server actively following the chain with multiple blocks per second, **When** a client sends `POST /tx/boot { address }` for an address that has wallet UTxOs in the indexer, **Then** the response carries a `VerificationSnapshot { utxo_root, chainpoint }` and a CSMT inclusion proof for every wallet input in the unsigned transaction, every proof verifies against `utxo_root`, and the response is accepted by the offline verifier.
2. **Given** the same conditions, **When** the chain follower applies a new block while the server is building the boot response, **Then** the snapshot, the resolved input bytes, and the proofs all reflect the same single point in chain history (no torn read) — the verifier accepts the response regardless of which block landed during the build.
3. **Given** an offline verifier given the same response and a trusted root tampered with one byte, **When** the verifier runs, **Then** it rejects with a snapshot-root mismatch at the snapshot's `utxo_root` field — never with an inclusion-proof failure or a missing-input failure.

---

### User Story 2 — Server stops querying cardano-node for UTxO state (Priority: P1)

The MPFS server's tx-build path is fully self-sufficient: it does not consult cardano-node's `LocalStateQuery` for UTxO state at any point. All UTxO knowledge needed to build a boot transaction comes from the local indexer, which already maintains a CSMT keyed by address-prefix.

**Why this priority**: cardano-node's `GetUTxOByAddress` is implemented as a linear scan over the entire ledger UTxO set. Its cost is O(total UTxOs on chain), not O(UTxOs at the address). At production scale (millions of UTxOs on mainnet) the server becomes unusable per-request, and a high-traffic server can effectively DoS its own node. The local indexer answers the same question in O(M) where M is the number of UTxOs at the address.

**Independent Test**: After a full boot is exercised through the system, `grep` the boot tx-builder source. The forbidden call MUST be absent. Operationally, run the boot endpoint while disabling cardano-node's `GetUTxOByAddress` (or running against a node that rejects it) and confirm boot still succeeds.

**Acceptance Scenarios**:

1. **Given** the boot tx-builder source, **When** a reviewer searches for the forbidden UTxO-by-address query call, **Then** the search returns no matches in the boot path.
2. **Given** an MPFS server whose connection to cardano-node has been configured to reject `GetUTxOByAddress`, **When** a client sends `POST /tx/boot`, **Then** boot still succeeds because the indexer is the single source of UTxO state.

---

### User Story 3 — Hot-path latency is independent of chain size (Priority: P2)

A boot endpoint serving a wallet that has K UTxOs at its address responds in time proportional to K, not in time proportional to the total number of UTxOs on the chain.

**Why this priority**: This is the operational consequence of User Story 2 — without it the server cannot survive at mainnet scale. It is P2 because it is observed, not directly built: it follows from Story 2 plus the indexer's existing CSMT prefix walk.

**Independent Test**: Measure boot endpoint latency against indexers populated with 1k, 10k, 100k, and 1M total UTxOs (with the same K UTxOs at the wallet's address). Latency MUST stay within a small constant factor across the four scales. The test holds independently of the proof-verification logic.

**Acceptance Scenarios**:

1. **Given** an indexer populated with 1M total UTxOs of which K are at the requesting wallet address, **When** the boot endpoint is called, **Then** the median latency is within 2× of the same call against an indexer populated with only those K UTxOs.

---

### User Story 4 — Test fixtures that don't run a chain follower can still build boot transactions (Priority: P2)

Some test fixtures exercise the indexer's event-application path manually (without a real chain follower). They submit transactions to a devnet, then drive the indexer with the resulting blocks themselves. These fixtures need to build a valid boot transaction even though their indexer never wrote a CSMT root or chain checkpoint, and even though their CSMT does not contain the wallet's UTxOs.

**Why this priority**: P1 work here would block these tests entirely — they were the harness used during the `#243` slice work. A clean test seam preserves their independence from the chain-follower wiring.

**Independent Test**: Run a fixture with `followerEnabled = false` that calls boot, signs, and submits. The test passes if the transaction is accepted on-chain and the indexer's manual application of the resulting block produces the expected events.

**Acceptance Scenarios**:

1. **Given** a test fixture with `followerEnabled = false` and a stub atomic reader installed via configuration, **When** the fixture calls boot, **Then** the boot transaction is built using `TxOut` bytes the stub looked up via cardano-node `LocalStateQuery` (test-side wallet simulation is allowed; server-side is forbidden) and is accepted on-chain.
2. **Given** the same fixture without the stub installed, **When** the fixture calls boot, **Then** the call fails fast with a clear error pointing at the missing checkpoint — not with a CBOR-decode error or a silent empty response.

### Edge Cases

- The wallet address has zero UTxOs in the indexer (unfunded address): the server MUST fail with a deterministic 4xx error explicitly identifying "no wallet UTxOs at address" as the cause; it MUST NOT fall back to cardano-node and MUST NOT silently emit an empty response.
- The chain follower has not yet written a checkpoint (server just started, no block processed yet): the server MUST fail with a deterministic 503 explicitly identifying "indexer not ready" as the cause; it MUST NOT emit a synthetic snapshot.
- The indexer is behind cardano-node (a UTxO exists on chain but is not yet indexed): the server MUST treat that UTxO as not-present and either succeed against the indexed UTxOs or, if there are none, surface the "no wallet UTxOs at address" error from the case above. Wallets that need confirmation that a specific UTxO is indexed MUST poll a dedicated endpoint, not the boot endpoint.
- A block lands during the boot build: the response MUST reflect a single, coherent point in chain history (the indexer's tip at the moment the build's read transaction opened); the verifier MUST accept the response regardless of which block landed during the build.
- The indexer CSMT contains UTxOs at the address but their KV column is missing the resolved `TxOut` bytes (corrupted database): the server MUST fail loudly rather than emit a partial response. This is an indexer-corruption error, not an expected boot failure mode.

## Requirements

### Functional Requirements

- **FR-001**: The boot endpoint MUST emit a response whose snapshot, resolved-input bytes, and CSMT inclusion proofs all reflect a single coherent point in the indexer's chain history. Block application by the chain follower MUST NOT be able to interleave between any two reads contributing to a single boot response.
- **FR-002**: The boot tx-builder MUST source every UTxO it spends from the local indexer's CSMT, walked at the requested address's prefix. It MUST NOT call cardano-node's `LocalStateQuery` `GetUTxOByAddress` at any point on the build path.
- **FR-003**: The boot tx-builder MUST NOT depend on the request body for input selection. The wire contract `POST /tx/boot { address }` is unchanged.
- **FR-004**: When the indexer cannot produce a coherent snapshot (no checkpoint yet, no CSMT root yet, or no UTxOs at the requested address), the boot endpoint MUST fail with a distinct, deterministic error that names the cause. It MUST NOT silently emit a partial or synthetic response.
- **FR-005**: The boot tx-builder's wallet-input lookup time MUST grow with the number of UTxOs at the requested address, not with the total number of UTxOs on the chain.
- **FR-006**: A test seam MUST exist that lets fixtures running without a chain follower substitute an alternate "atomic reader" implementation. The seam MUST be configurable at server-startup time (not a runtime API parameter) so production builds cannot accidentally route through it.
- **FR-007**: The verifier component, given a boot response and a trusted root that matches the response's snapshot, MUST accept the response without making any further chain or indexer call. Verification MUST be a pure offline check.

### Key Entities

- **Wallet UTxO**: A UTxO at the user's address that the boot transaction can spend. The indexer holds (input reference, transaction-output bytes, inclusion proof) for each one.
- **BundleSnapshot**: The (CSMT root, chain checkpoint slot, chain checkpoint block id) tuple the response is anchored to. Every proof in the response must verify against this snapshot's CSMT root.
- **AtomicCageReader (interface)**: The server-internal seam that, given an address, returns a `BundleSnapshot` and the resolved-bytes-and-proof triple for every wallet UTxO at that address, all from a single coherent indexer read. Production wires it to the indexer; tests can wire a stub.
- **Test seam (configuration)**: A startup-time configuration value that swaps the production atomic reader for a stub. Tests with no chain follower set it; production never sets it.

## Success Criteria

### Measurable Outcomes

- **SC-001**: The boot endpoint accepts proof-bearing verification by the offline verifier (using the response's own snapshot as trusted root) on 100% of attempts under sustained chain churn (≥ 1 block per second sustained for ≥ 60 seconds before the boot call). On the previous racy implementation this rate is observably below 100% (we have reproduced rejections).
- **SC-002**: A reviewer searching the boot tx-builder source for the forbidden UTxO-by-address query call returns zero matches.
- **SC-003**: Boot endpoint median latency at 1M total indexed UTxOs and K = 2 wallet UTxOs at the requesting address stays within 2× the median latency at 1k total indexed UTxOs (same K). The current implementation (going through the node-side query) does not meet this criterion: its latency grows linearly with total chain UTxOs.
- **SC-004**: The full automated test suite — the standard build of all derivations the project ships, formatter and linter, unit tests, and the devnet-backed end-to-end suite — passes locally and in CI on the change. No fixture is left in a "skipped" or "pending" state to side-step the change.
- **SC-005**: A reviewer can verify the atomicity claim by reading a single function in the application wiring layer: that function MUST visibly open one indexer read transaction and perform every read needed for a boot response inside it. Reviews must not require chasing reads across multiple functions or modules.

## Assumptions

- The local indexer's CSMT is keyed by address-prefix and exposes a primitive that walks the subtree at a prefix in time proportional to the leaves under that prefix. This primitive already exists in the underlying storage library and is the basis for the no-linear-scan claim.
- The indexer's chain follower writes its CSMT mutations and its chain checkpoint inside the same database transaction per block. Without this invariant the atomicity claim cannot be made; with it, an indexer reader that opens one transaction sees a coherent snapshot.
- The indexer-side database transactions provide snapshot-isolation semantics for readers — a reader is not blocked by, and does not block, a concurrent writer; the reader simply sees the snapshot at the moment it opened. The implementation chosen by the project meets this; if a future migration loses that property, this feature's atomicity claim has to be re-validated.
- Wallets calling the boot endpoint accept that the server picks UTxOs from the address (currently this is the existing behavior of the endpoint). A different contract — wallet-supplies-inputs in the request body — is a separate, larger API change that is explicitly out of scope here.
- Wallet UTxO discovery for test fixtures is allowed to use cardano-node's `LocalStateQuery` because each test acts as its own wallet, on its own connection, infrequently. The forbidden cost surface is only the server-side hot path under production traffic.
- Network calls that the boot path cannot avoid (protocol parameters, transaction submission) are not in scope for atomicity. Protocol parameters change at most once per epoch and are not chain-state-dependent in the same sense as UTxOs.
