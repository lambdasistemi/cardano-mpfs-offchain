# Feature Specification: Fact-provider pivot

**Feature Branch**: `259-fact-provider-pivot`
**Created**: 2026-05-04
**Status**: Draft
**Input**: User description: "fact-provider-pivot — replace the MPFS server's `GET /transaction/{address}/{op}` endpoints that return unsigned transactions with `POST /facts/{op}` endpoints that return only the proof-bearing data needed to build that operation's transaction at one indexer snapshot. The wallet (or any client, starting with MOOG) builds the transaction locally using cage-protocol DSL helpers hosted in `cardano-mpfs-client` (the in-repo client library), which compose the generic `Cardano.Node.Client.TxBuild` operational-monad primitives from upstream `cardano-node-clients`. The MPFS-specific cage protocol logic stays in this repo; only the generic DSL primitives stay upstream. Hard cutover; no coexistence period. The verifier reduces to pure proof-validity checks. Protocol parameters returned with explicit \"unverified\" status; wallet hard-cap policy bounds are the documented mitigation."

## User Scenarios & Testing

### User Story 1 — Honest end-to-end MPFS interaction without trusting the server's transaction body (Priority: P1)

A wallet client (MOOG, today; any other client tomorrow) wants to interact with the MPFS protocol — boot a token, submit a request, retract a request, end a token, run a token update, run a token reject. The wallet asks the MPFS server for the proof-bearing data needed for that operation at the indexer's current snapshot. The server returns a "facts" response: the snapshot reference, the relevant indexer-resolved UTxOs (each with a CSMT inclusion proof against the snapshot's UTxO root), the relevant trie facts (for tier-3 operations), and the protocol parameters. The wallet verifies every proof against an independently-obtained trusted UTxO root, builds the unsigned transaction locally, signs it with its own keys, and submits it (either directly to a Cardano node it controls or via the MPFS server's submit endpoint).

**Why this priority**: This is the operational shape after the architectural pivot. Without this story working end-to-end, the pivot has not landed. Today's behavior — server returns an unsigned transaction the wallet must inspect via a strict structural verifier — is the shape we are replacing.

**Independent Test**: From a freshly-seeded devnet, run a full MOOG flow (boot a token, insert a request, run an update, retract a different request, end the token) end-to-end. At every step, the MOOG CLI calls the new `POST /facts/{op}` endpoints, runs the cage-protocol DSL locally to build the unsigned transaction, signs, and submits. On the proof-verification side, the response's CSMT/MPF proofs verify against the response's snapshot root and against an independently-obtained trusted root. The flow MUST complete without invoking any of the old `transaction/...` endpoints.

**Acceptance Scenarios**:

1. **Given** a fresh devnet and a funded wallet, **When** MOOG calls `POST /facts/boot { address }`, **Then** the response carries a snapshot, a list of wallet UTxOs at the address with valid CSMT proofs, and the protocol parameters; the wallet builds the boot transaction from those facts; the transaction is accepted on-chain; the chain follower indexes the resulting cage state.
2. **Given** a booted token, **When** MOOG calls `POST /facts/request/insert { token, key, value, address }`, **Then** the response carries a snapshot, wallet UTxOs at the requester address with proofs, and protocol parameters; the wallet builds the request transaction locally; the transaction is accepted on-chain.
3. **Given** a booted token with pending requests, **When** MOOG calls `POST /facts/update { token, address }`, **Then** the response carries a snapshot, the state UTxO with a CSMT proof, the pending request UTxOs with proofs, the wallet UTxOs at the owner address with proofs, the MPF facts (key/value/proof) for each affected trie key, and the protocol parameters; the wallet runs the MPF fold locally, builds the update transaction, signs, submits.
4. **Given** any of the responses above with a tampered proof (one byte flipped), **When** the wallet's verifier runs, **Then** the verifier rejects with a proof-failure error and the wallet does not proceed to building the transaction.
5. **Given** a wallet with `WalletPolicy { wpMaxFee = 5 ADA, … }` and a server that returns a protocol-parameters block whose `minFeeA × estimated tx size` exceeds 5 ADA, **When** the wallet's tx-builder runs, **Then** the build fails with a policy-violation error before signing.

---

### User Story 2 — Existing `transaction/...` endpoints are removed (Priority: P1)

After the pivot lands, the MPFS server publishes only `POST /facts/*` endpoints (plus reads, plus submit). The legacy `GET /transaction/{address}/{op}` endpoints (`boot-token`, `end-token`, `request-insert`, `request-delete`, `request-update`, `retract-change`, `update-token`) are removed in the same release. swagger.json reflects only the new shape; tests and CLIs that called the legacy endpoints are updated.

**Why this priority**: The pivot is committed to as a hard cutover. A coexistence period would mean two implementations of the same logic running side by side and would invite drift between the "tx-builder" and "fact-provider" paths. The team's standing rule on bridges between two answers is the load-bearing argument here.

**Independent Test**: After the pivot, `grep -rn 'transaction/[a-z-]*-token\|/request-\|/retract-' cardano-mpfs-offchain/lib cardano-mpfs-offchain/exe` returns zero matches. `gh api repos/.../contents/docs/assets/swagger.json` shows no `transaction` paths. The MOOG repo's `MPFS.API` module has been replaced (renamed and rewritten); no callsite of the legacy module remains.

**Acceptance Scenarios**:

1. **Given** the post-pivot HEAD, **When** swagger.json is regenerated, **Then** the diff against the previous swagger shows the legacy endpoints removed and only `/facts/*` + `/submit` + the existing read endpoints present.
2. **Given** the MOOG repo's post-pivot HEAD, **When** the binary is built, **Then** no module imports `transaction-builder`-shaped names; the new `MPFS.Facts` module is imported instead.

---

### User Story 3 — Verifier surface is pure proof validation only (Priority: P2)

The `cardano-mpfs-client` package's verifier surface, after the pivot, exports only pure proof-validity functions: "given a facts bundle and a trusted root, are these CSMT/MPF proofs valid against the bundle's snapshot, and does the snapshot's UTxO root equal the trusted root". No transaction-shape grammar, no "is this output at the right cage script address", no "is this datum the canonical one for the request"; those checks are absent because the wallet built the transaction.

**Why this priority**: This is the architectural gain from the pivot — a verifier that is the pure offline fold the constitution's Principle VIII originally specified. P2 because it is observed through code reading, not through user-visible behavior beyond Story 1.

**Independent Test**: A reviewer reads `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs` (or equivalent post-pivot location). No function takes a `Tx` or imports `Cardano.Ledger.Api.Tx`. The verifier's input types are `BootFacts`, `RequestFacts`, etc. (records of bytes + proofs + snapshot + protocol parameters). The verifier returns `Either VerifyError VerifiedFacts` and the cage-protocol DSL consumes `VerifiedFacts` to build transactions.

**Acceptance Scenarios**:

1. **Given** the post-pivot `cardano-mpfs-client`, **When** a reviewer searches for `Cardano.Ledger.Api.Tx` or `Tx ConwayEra` imports in the verifier modules, **Then** zero matches are found.
2. **Given** the same package, **When** the cross-target QuickCheck (Principle IX) runs, **Then** verifier outputs are byte-identical for native, GHC-WASM, and GHC-JS builds for the same input.

---

### User Story 4 — Protocol parameters are returned with explicit unverified status; wallet caps mitigate (Priority: P2)

Each `POST /facts/{op}` response carries the protocol parameters at the response's snapshot's slot, marked explicitly as "unverified" in the response body and the spec. Wallets are documented to enforce policy bounds (maximum fee, maximum ExUnits price, maximum minUTxO coin-per-byte, maximum validity window). Wallets with their own trusted node connection are documented to fetch protocol parameters directly via cardano-node and ignore the server's value.

**Why this priority**: Cardano has no native protocol-parameter signing. The pivot's "client builds transactions locally" model still depends on protocol parameters, which the wallet cannot independently verify against a trusted root. Documenting the gap and the mitigation strategies is part of shipping the pivot honestly.

**Independent Test**: spec.md (this file) and the public API documentation describe the unverified status of pp and the recommended `WalletPolicy` shape. MOOG ships with default `WalletPolicy` values that are appropriate for mainnet (e.g., `wpMaxFee = 5 ADA`). A regression test demonstrates: when the server returns artificially-inflated pp (test seam), MOOG's tx-builder refuses to sign rather than emitting an over-budget transaction.

**Acceptance Scenarios**:

1. **Given** spec.md and the API documentation, **When** a reader looks up "protocol parameters", **Then** the documentation states they are unverified and explains the wallet-policy mitigation.
2. **Given** MOOG's default `WalletPolicy` and a stubbed server returning `minFeeA × 100`, **When** the wallet builds a request transaction, **Then** the build returns a `PolicyViolation` error before signing.

---

### Edge Cases

- The wallet address has zero UTxOs in the indexer (unfunded address): the server returns the snapshot + an empty UTxOs list + protocol parameters. The wallet's tx-builder rejects with "no funding UTxOs" before attempting to build.
- The chain follower has not yet written a checkpoint: the server returns 503 with "indexer not ready: snapshot unavailable".
- The token referenced (in `update`, `reject`, `retract`, `end`) does not exist in the indexer: the server returns 404.
- The named request UTxO (in `retract`) does not exist in the indexer: the server returns 404.
- A block lands during the facts-read: the response reflects a single coherent indexer snapshot (the IndexerTx primitives from #253 enforce this).
- Indexer corruption (a leaf in the CSMT whose KV column has no resolved bytes, or a leaf whose proof generation fails): the server returns 500 with a clear diagnostic and identifies the offending UTxO ref.
- The wallet's trusted root and the response's snapshot root disagree: the wallet's verifier rejects with a snapshot-mismatch error before the tx-builder runs.

## Requirements

### Functional Requirements

- **FR-001**: The MPFS server MUST publish `POST /facts/{op}` endpoints for each of: boot, request-insert, request-delete, request-update, retract, end, update, reject. Each endpoint accepts the same parameters as the corresponding legacy `transaction/...` endpoint.
- **FR-002**: Each facts response MUST contain a `BundleSnapshot` (UTxO root, slot, block id), the proof-bearing data the named operation needs at that snapshot (per the per-endpoint shape below), and the current protocol parameters.
- **FR-003**: Per-endpoint facts shape:
  - `BootFacts` and the three `RequestFacts` (insert/delete/update share a shape): snapshot, wallet UTxOs at the requester address with CSMT proofs, protocol parameters.
  - `RetractFacts`: snapshot, the named request UTxO with a CSMT proof, funding wallet UTxOs at the requester address with proofs, protocol parameters.
  - `EndFacts`: snapshot, the state UTxO for the named token with a CSMT proof, funding wallet UTxOs at the owner address with proofs, protocol parameters.
  - `UpdateFacts` and `RejectFacts`: snapshot, the state UTxO with a CSMT proof, the pending request UTxOs with proofs, funding wallet UTxOs at the owner address with proofs, MPF inclusion/exclusion facts for every trie key the operations touch, protocol parameters.
- **FR-004**: All proof-bearing UTxO entries in a response MUST verify against the response's snapshot's UTxO root. All MPF facts MUST verify against the trie root recorded in the consumed state UTxO's datum at the same snapshot.
- **FR-005**: Every facts read MUST observe a single coherent indexer snapshot — the IndexerTx primitives from #249 (PR #253) discharge this property; new primitives required by tier-2 and tier-3 endpoints (state-UTxO read, request-UTxO read, trie-fact read) MUST be added inside the same `runIndexerTx ctx $ do { … }` discipline.
- **FR-006**: The legacy `GET /transaction/{address}/{op}` endpoints MUST be removed in the same release that introduces `POST /facts/*`. swagger.json MUST reflect only the new shape.
- **FR-007**: The `cardano-mpfs-client` verifier surface MUST consist only of pure proof-validity functions over facts bundles; it MUST NOT import `Cardano.Ledger.Api.Tx` or any transaction-grammar type.
- **FR-008**: The cage-protocol transaction-building helpers (boot, request × 3, retract, end, update, reject) MUST live in `cardano-mpfs-client` (the in-repo client library, alongside the verifier) as native-Haskell functions consuming a verified facts bundle and a `WalletPolicy`. The helpers compose the generic `Cardano.Node.Client.TxBuild` primitives from upstream `cardano-node-clients`; cage-protocol-specific construction logic (datums, redeemers, asset-name derivation, MPF fold) is MPFS-domain and stays in this repo.
- **FR-009**: Each facts response MUST mark the protocol parameters as "unverified" in the response body and in the public API documentation. Wallet integrations MUST be documented to enforce a `WalletPolicy` with hard caps on fee, ExUnits price, minUTxO coin-per-byte, and validity-interval window.
- **FR-010**: MOOG's `MPFS.API` module MUST be replaced by a new `MPFS.Facts` module (HTTP client for the new endpoints). Every legacy callsite in MOOG MUST be migrated; no module under `lambdasistemi/moog` may import a transaction-builder-shaped legacy name after the cutover.
- **FR-011**: The pivot MUST land as one coordinated change across `cardano-mpfs-offchain` (server + verifier library) and `lambdasistemi/moog` (CLI client). Both repositories' default branches MUST move in lockstep so neither is broken between landings.

### Key Entities

- **Facts bundle**: per-operation record carrying a `BundleSnapshot`, the proof-bearing UTxOs / MPF facts the operation needs, and the protocol parameters at that snapshot. Eight variants (one per operation).
- **`BundleSnapshot`**: existing entity from #249; UTxO root + slot + block id. Unchanged.
- **`WalletPolicy`**: new entity hosted client-side; named hard-cap bounds (max fee, max ExUnits price, max minUTxO coin-per-byte, max validity-interval window) the wallet's tx-builder enforces before signing. Documented; not part of the wire contract.
- **`VerifiedFacts`**: the output of the verifier — the same facts bundle, but with a proof token guaranteeing every CSMT/MPF proof inside has been validated against the bundle's snapshot and that the snapshot matches a caller-supplied trusted root.
- **Cage-protocol DSL helpers**: native-Haskell tx-builder functions, one per operation, consuming `VerifiedFacts` + `WalletPolicy` and producing an unsigned `Tx ConwayEra`.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A MOOG end-to-end test exercises all eight operations (boot, three requests, retract, end, update, reject) end-to-end against the new `POST /facts/*` endpoints; every response's CSMT and MPF proofs verify against the response's snapshot root; every resulting transaction is accepted on-chain.
- **SC-002**: A reviewer searching the post-pivot `cardano-mpfs-offchain` and `cardano-mpfs-client` source returns zero matches for the legacy `transaction/{address}` endpoint shape; swagger.json contains zero `transaction` path entries.
- **SC-003**: A reviewer searching the post-pivot `cardano-mpfs-client` for `Cardano.Ledger.Api.Tx` or `Tx ConwayEra` returns zero matches.
- **SC-004**: The post-pivot MOOG default `WalletPolicy` rejects a stubbed server response with `minFeeA × 100` before signing in the regression test.
- **SC-005**: Both `cardano-mpfs-offchain` main and `lambdasistemi/moog` main move in the same release window; neither default branch is broken at any point during the cutover.

## Assumptions

- The IndexerTx primitives merged in PR #253 are the foundation for all atomic facts reads. New per-endpoint primitives (`readStateUtxoAt`, `readRequestUtxosAt`, `readNamedRequestUtxo`, `readTrieFact`) are added inside `Cardano.MPFS.Indexer.Reads` in the same discipline.
- The upstream `cardano-node-clients` TxBuild DSL is sufficient to express the eight cage-protocol transaction shapes natively as MPFS-side wrappers. The DSL's existing combinators (`spend`, `payTo'`, `attachScript`, `mint`, `collateral`) plus the cage-protocol-specific datum constructors (`bootStateDatum`, request-datum constructors, etc.) port cleanly from the current server-side `Cardano.MPFS.TxBuilder.Real.*` modules to in-repo cage helpers in `cardano-mpfs-client`. The `Real.Boot` module on `main` is already a pure cage builder of this shape (`bootTokenCore` returns `BootCore { bcProgram :: TxBuild ... }`); the relocation is rename + repackage, not a rewrite.
- MOOG (a native Haskell binary) is the immediate downstream consumer and validates the architecture before any browser-wallet integration.
- The WASM artifact for browser wallets (lambdasistemi/cardano-node-clients#123) is deferred. The pivot's correctness is established by MOOG; the WASM artifact is a packaging exercise on top of that, scheduled when browser-wallet demand materialises.
- Multi-band snapshots (#254) are deferred. The pivot ships with single-snapshot-at-tip semantics — the same as the legacy endpoints today. The `/facts/{op}` request body has room for an optional `snapshot` band parameter without a wire-contract change later.
- Cardano has no native protocol-parameter signing today. The "unverified pp" caveat is a known platform-level gap; wallet-policy hard caps are the documented mitigation. Future signed-pp protocols (Mithril or otherwise) can be adopted without a wire-contract change.
- Verifier purity (Principle VIII) and cross-target compilation (Principle IX) are maintained — the post-pivot verifier is strictly smaller than the pre-pivot verifier (no tx-shape grammar), so cross-target compilation is at least as feasible.
- Out of scope: WASM artifact (cardano-node-clients#123), multi-band snapshots (#254), browser-wallet integration, third-party CLIs other than MOOG, any change to on-chain validators in `cardano-mpfs-onchain`.
