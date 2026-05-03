# Feature Specification: Atomic POST /tx/request/{insert,delete,update}

**Feature Branch**: `256-atomic-request-handlers`
**Created**: 2026-05-03
**Status**: Draft
**Input**: User description: "atomic-request-handlers — apply the same atomic-indexer-read pattern from #249 (PR #253) to the three request endpoints. Each handler MUST read its `BundleSnapshot` and the resolved `TxOut` bytes + CSMT inclusion proofs for every wallet UTxO at the requester address in ONE indexer transaction via `runIndexerTx ctx $ do { … }` composing the existing `Cardano.MPFS.Indexer.Reads` primitives. The boot pattern is the reference: `bootTokenCore` is pure, and `runBootBuilder` does the IO orchestration. The TxBuild DSL replaces the imperative builder bodies. Drop `Provider.queryUTxOs` from these three build paths entirely."

## User Scenarios & Testing

### User Story 1 — Honest request submission under realistic chain churn (Priority: P1)

A wallet client sends `POST /tx/request/insert { token, key, value, address }` (or the `delete` / `update` variants) against an MPFS server whose chain follower is actively processing blocks. The server returns an unsigned request transaction together with a `VerificationSnapshot` and a CSMT inclusion proof for every wallet input the transaction spends. The wallet signs, submits, and the on-chain validator accepts the transaction. An offline verifier given the response and an independently-obtained trusted UTxO root checks the response without performing any further query: every proof verifies against the snapshot's `utxo_root`, and the snapshot's `utxo_root` equals the externally-supplied trusted root.

**Why this priority**: This is the same proof-bearing promise the boot slice (#249, PR #253) delivers, extended to the request endpoints. Today the three request handlers carry the same race window the boot slice had before it was fixed: they read the snapshot in the HTTP layer separately from the per-input proof reads inside `requestInsertImpl` / `requestDeleteImpl` / `requestUpdateImpl`. Until that race is closed, the response cannot be reliably verified offline under churn.

**Independent Test**: Run each request endpoint end-to-end against a devnet under load: submit `request/insert`, `request/delete`, `request/update` while the chain follower is processing concurrent blocks, then run the verifier on each response with the snapshot's own root as the trusted root. The verifier MUST accept on every attempt. Repeat with a tampered root and the verifier MUST reject with a snapshot mismatch. The test holds independently of the boot slice and independently of every other handler.

**Acceptance Scenarios**:

1. **Given** an MPFS server actively following the chain with multiple blocks per second, **When** a client sends `POST /tx/request/insert { token, key, value, address }` for an address that has wallet UTxOs in the indexer, **Then** the response carries a `VerificationSnapshot { utxo_root, chainpoint }` and a CSMT inclusion proof for every wallet input in the unsigned transaction; every proof verifies against `utxo_root`; and the response is accepted by the offline verifier.
2. **Given** the same conditions, **When** the chain follower applies a new block while the server is building the request response, **Then** the snapshot, the resolved input bytes, and the proofs all reflect the same single point in chain history (no torn read).
3. **Given** an offline verifier given the same response and a trusted root tampered with one byte, **When** the verifier runs, **Then** it rejects with a snapshot-root mismatch at the snapshot's `utxo_root` field.
4. **Given** the same flow but with `POST /tx/request/delete` (then `POST /tx/request/update`), **When** the verifier runs against each response, **Then** acceptance and tamper-rejection behave identically to the `insert` case.

---

### User Story 2 — Server stops querying cardano-node for UTxO state on the request path (Priority: P1)

The MPFS server's request tx-build path is fully self-sufficient: it does not consult cardano-node's `LocalStateQuery` for UTxO state at any point. All UTxO knowledge needed to build a request transaction comes from the local indexer.

**Why this priority**: Same as the boot slice. cardano-node's `GetUTxOByAddress` is `O(total UTxOs on chain)`, not `O(UTxOs at the address)`. At production scale (millions of UTxOs on mainnet) the call is unusable on a hot path. Each of the three request endpoints today carries an identical `queryUTxOs prov addr` call in its impl module — three distinct copies of the same defect. They must all go.

**Independent Test**: After all three request flows are exercised through the system, `grep` the request tx-builder source. The forbidden call MUST be absent from `requestInsertImpl`, `requestDeleteImpl`, `requestUpdateImpl`, and any helper they share on the build path. Operationally, run the three endpoints while disabling cardano-node's `GetUTxOByAddress` (or running against a node that rejects it) and confirm all three still succeed.

**Acceptance Scenarios**:

1. **Given** the request tx-builder source, **When** a reviewer searches for `queryUTxOs` on the build path, **Then** the search returns zero matches in any of the three request impl modules and zero matches in the request handlers.
2. **Given** an MPFS server whose connection to cardano-node has been configured to reject `GetUTxOByAddress`, **When** a client sends each of `POST /tx/request/insert`, `POST /tx/request/delete`, `POST /tx/request/update`, **Then** all three succeed because the indexer is the single source of UTxO state.

---

### User Story 3 — Builder modules are pure (Priority: P2)

The three request builder modules follow the same shape the boot slice established: a pure `*Core` function returning a `*Core` record (program + inputs + funding + snapshot), with all IO orchestration confined to `Cardano.MPFS.TxBuilder.Real`. The TxBuild DSL replaces every imperative ledger-lens builder body.

**Why this priority**: This is the operational-coupling fix of the boot slice extended to the request endpoints. Without it, each request module would be re-doing imperative tx assembly that the DSL has already absorbed; reviews would have to re-validate the same mechanical pieces three times. P2 because it is observable through code reading rather than through user-visible behavior.

**Independent Test**: A reviewer opens the request builder module(s) and finds: no `IO` in any function signature, no `Provider` import, and the transaction body described as a `TxBuild` program (not as a sequence of lens updates on `mkBasicTxBody`).

**Acceptance Scenarios**:

1. **Given** the request builder module(s), **When** a reviewer searches for `Provider`, `IO`, or `evaluateAndBalance` imports, **Then** the search returns zero matches in those modules.
2. **Given** the same modules, **When** a reviewer searches for `mkBasicTxBody`, `inputsTxBodyL`, or `mintTxBodyL` outside of comments and module-level documentation, **Then** the search returns zero matches.

---

### User Story 4 — Test fixtures keep working without a chain follower (Priority: P2)

Test fixtures that drive the indexer manually (`followerEnabled = False`) and call the request builders directly continue to work without any new infrastructure. They use the existing `walletBootInputs` helper (or an equivalent) to obtain the wallet inputs they need, then feed them to the new pure `*Core` constructors. Tests with `followerEnabled = True` use the production HTTP path and observe the same proof-bearing acceptance.

**Why this priority**: P2 because it preserves an existing test surface rather than introducing new behavior. Failure here would block the existing `CageSpec` / `CageFlowSpec` / `IndexerSpec` harnesses — the same harnesses already validated by the boot slice.

**Independent Test**: Run the existing e2e suite end-to-end. The fixtures that exercise `requestInsert`, `requestDelete`, `requestUpdate` on `followerEnabled = False` build with the wallet helper and submit successfully; the fixtures on `followerEnabled = True` go through HTTP and verify against the response's snapshot.

**Acceptance Scenarios**:

1. **Given** a test fixture with `followerEnabled = False` that calls `requestInsert` directly, **When** the fixture builds the transaction by feeding `walletBootInputs` (or an analogous helper) to the new pure constructor, **Then** the transaction is accepted on-chain and the indexer's manual application of the resulting block produces the expected events.
2. **Given** a test fixture with `followerEnabled = True` that calls `POST /tx/request/insert` over HTTP, **When** the fixture submits the response and verifies it against the response's own snapshot, **Then** verification accepts.

---

### Edge Cases

- The wallet address has zero UTxOs in the indexer (unfunded address): the server MUST fail with a deterministic 4xx error explicitly identifying "no wallet UTxOs at address" as the cause.
- The chain follower has not yet written a checkpoint (server just started): the server MUST fail with a deterministic 503 explicitly identifying "indexer not ready: snapshot unavailable" as the cause.
- A block lands during the request build: the response MUST reflect a single, coherent point in chain history.
- The token referenced by the request does not exist in the indexer: the existing 404 behavior is unchanged.
- The CSMT contains UTxOs at the requester address but their KV column is missing the resolved `TxOut` bytes (corrupted database): the server MUST fail loudly rather than emit a partial response — same loud-error semantics as the boot slice's `readWalletInputsAt`.

## Requirements

### Functional Requirements

- **FR-001**: Each of the three request endpoints (`POST /tx/request/insert`, `POST /tx/request/delete`, `POST /tx/request/update`) MUST emit a response whose snapshot, resolved-input bytes, and CSMT inclusion proofs all reflect a single coherent point in the indexer's chain history. Block application by the chain follower MUST NOT be able to interleave between any two reads contributing to a single response.
- **FR-002**: The three request tx-builders MUST source every UTxO they spend from the local indexer's CSMT, walked at the requester address's prefix. They MUST NOT call cardano-node's `LocalStateQuery` `GetUTxOByAddress` at any point on the build path.
- **FR-003**: The wire contracts for `POST /tx/request/insert`, `POST /tx/request/delete`, `POST /tx/request/update` MUST remain unchanged.
- **FR-004**: When the indexer cannot produce a coherent snapshot, the request endpoints MUST fail with a distinct, deterministic error that names the cause. They MUST NOT silently emit a partial or synthetic response.
- **FR-005**: The request tx-builders' wallet-input lookup time MUST grow with the number of UTxOs at the requester address, not with the total number of UTxOs on the chain.
- **FR-006**: The three request builder modules MUST be pure: their public function signatures MUST NOT mention `IO` or `Provider`; the transaction body MUST be expressed as a `TxBuild` program; the IO step that runs the DSL `build` loop MUST live in `Cardano.MPFS.TxBuilder.Real`.
- **FR-007**: The verifier component, given a request response and a trusted root that matches the response's snapshot, MUST accept the response without making any further chain or indexer call.

### Key Entities

- **RequestCore (×3)**: One per request endpoint (`RequestInsertCore`, `RequestDeleteCore`, `RequestUpdateCore`) — pure record bundling the `TxBuild` program, the ledger-typed inputs, the requester address, the funding witnesses, and the snapshot. Mirrors the boot slice's `BootCore`.
- **WalletInput**: A UTxO at the requester's address that the request transaction can spend. The indexer holds (input reference, transaction-output bytes, inclusion proof) for each one — same shape as the boot slice.
- **BundleSnapshot**: The snapshot the response is anchored to; produced by the existing `readSnapshot` primitive. Unchanged.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Each of the three request endpoints accepts proof-bearing verification by the offline verifier (using the response's own snapshot as trusted root) on 100% of attempts under sustained chain churn (≥ 1 block per second sustained for ≥ 60 seconds before the call). On the previous racy implementation this rate is observably below 100%.
- **SC-002**: A reviewer searching the request tx-builder source for `queryUTxOs` returns zero matches in any of the three request impl modules and zero matches in the request handlers.
- **SC-003**: A reviewer searching the request builder modules for `IO` or `Provider` in function signatures returns zero matches; a search for `mkBasicTxBody` / `inputsTxBodyL` / `mintTxBodyL` outside comments returns zero matches.
- **SC-004**: The full automated test suite — the standard build of all derivations the project ships, formatter and linter, unit tests, and the devnet-backed end-to-end suite — passes locally and in CI on the change.
- **SC-005**: The atomicity claim per endpoint is visible in one place: each handler is a single `runIndexerTx ctx $ do { … }` block followed by a single call to the IO orchestrator that runs the DSL `build` loop. Reviews must not require chasing reads across multiple functions or modules.

## Assumptions

- The `IndexerTx` primitives library introduced in #249 (PR #253) — `readSnapshot` and `readWalletInputsAt` — is sufficient for the three request endpoints. They each need exactly the same indexer reads as boot: the snapshot and the wallet inputs at one address. No new primitives are needed.
- The `walletBootInputs` e2e helper is reusable for request fixtures; if a request-specific helper is more convenient, it is a thin wrapper around the same `Provider.queryUTxOs` call.
- The TxBuild DSL's existing combinators (`spend`, `payTo'`, `attachScript`, `mint`, `collateral`) plus the request-side `Cardano.MPFS.TxBuilder.Real.Internal` helpers are sufficient to express the three request transactions. No DSL extensions are needed for this slice.
- The multi-band snapshot redesign (issue #254) is a separate, larger spec and is explicitly out of scope here. This slice ships the same single-snapshot-at-tip semantics the boot slice ships.
- The remaining handlers (`retract`, `reject`, `update`, `end`) are out of scope for this spec; they need new indexer primitives (state-UTxO read, request-UTxO read, trie-fact read) and will be specced separately as the workstream progresses.
