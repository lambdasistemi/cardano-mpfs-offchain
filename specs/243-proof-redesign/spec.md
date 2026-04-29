# Feature Specification: Post-Split Proof Redesign

**Feature Branch**: `feat/243-proof-redesign`
**Created**: 2026-04-29
**Status**: Draft
**Input**: lambdasistemi/cardano-mpfs-offchain#243 — redesign the entire MPFS HTTP surface around the post-split on-chain protocol so that proofs ride alongside data answers, the offchain server is no longer authoritative for the CSMT root, and write endpoints are reorganised by signer role.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Oracle reads a cage's current state without trusting the server (Priority: P1)

A trust-minimised oracle wants to inspect the state of one of their cages — the state UTxO datum, the trie root, and the set of pending requests — without taking the offchain service's word for any of it.

**Why this priority**: The oracle's reading flow is the foundation. Until the oracle can trust the read of the cage's state, every subsequent decision they make (whether to update, reject, end) is unsafe. This is the gate to all other oracle-side flows.

**Independent Test**: Drive a single `GET /tokens/:id` call against an instrumented server. Verify offline, given a separately-obtained trusted UTxO-CSMT root for the same chain point: (a) the response's state UTxO has a valid CSMT inclusion proof, (b) the encoded TxOut's address matches the locally-derived global state validator address, (c) the encoded TxOut's value contains exactly one NFT under the trusted state policy id with the requested asset name, (d) the per-cage requests list comes with a valid CSMT prefix-completeness proof at the locally-derived per-cage request address.

**Acceptance Scenarios**:

1. **Given** a booted cage, **When** the oracle calls `GET /tokens/:id`, **Then** the response includes the state UTxO with its CSMT inclusion proof, plus the full enumeration of pending request UTxOs at the per-cage request address with a single completeness proof outside the list, all anchored to one snapshot.
2. **Given** a booted cage with no pending requests, **When** the oracle calls `GET /tokens/:id`, **Then** the requests list is empty and the completeness proof attests that the per-cage request address holds no UTxOs at the snapshot.
3. **Given** the indexer has never seen the requested token id, **When** the client calls `GET /tokens/:id`, **Then** the server returns an unverified 404 with no body, and the client falls back to `GET /tokens` for a verifiable answer.

---

### User Story 2 - Requester submits an unsigned-tx request with verifiable inputs (Priority: P1)

A requester acting against a known cage wants to submit an `Insert`, `Delete`, or `UpdateValue` request to the oracle. The unsigned transaction the offchain service builds must be verifiable end-to-end: every input the tx will consume is one the requester recognises and approves before signing.

**Why this priority**: Writes are the second foundation. Trust-minimised reads are useless if the requester is then asked to sign a tx whose inputs were silently substituted by a malicious server. The verifiable-write flow protects the requester's funds and intent.

**Independent Test**: Call `POST /tx/requester/insert`, `POST /tx/requester/delete`, or `POST /tx/requester/update`. Verify offline that the response's `unsigned_tx_cbor` decodes to a transaction whose every consumed input appears in the response's `inputs` list with a valid CSMT inclusion proof against the externally-trusted UTxO-CSMT root, and that the produced request UTxO is paid to the locally-derived per-cage request address with the expected operation datum.

**Acceptance Scenarios**:

1. **Given** a known cage and a wallet UTxO at the requester's address, **When** the requester calls any `POST /tx/requester/...` endpoint, **Then** the response carries the unsigned tx and a flat `inputs` list containing every spent and reference input with CSMT inclusion proofs, anchored to one snapshot.
2. **Given** the response from (1), **When** the client decodes the unsigned tx and cross-checks each consumed input against `inputs[*].ref`, **Then** every consumed input is found in the list with a valid proof and the corresponding `txout_cbor` matches the resolved input the validator will see.

---

### User Story 3 - Oracle signs an update batch with cryptographic fairness (Priority: P1)

An oracle has been asked to process a batch of pending requests against one of their cages. They must verify that the unsigned tx the server proposes consumes a subset of the *full* set of pending requests at the per-cage request address, not a server-curated subset that hides some pending requests from view.

**Why this priority**: This is the core fairness property the on-chain split was designed to enable, and the central new trust property of this redesign. Without per-cage requests completeness, a malicious offchain server can trick the oracle into processing a partial batch while pretending other requests don't exist — leaving requesters unprocessed without recourse.

**Independent Test**: Call `POST /tx/oracle/update`. Decode the unsigned tx; collect every consumed request UTxO. Verify offline that the response's `requests_completeness_proof` is a valid CSMT prefix-completeness proof attesting the full pending set at the per-cage request address against the externally-trusted UTxO-CSMT root, and that every consumed request UTxO from the tx appears in that attested set.

**Acceptance Scenarios**:

1. **Given** a cage with N pending requests, **When** the oracle calls `POST /tx/oracle/update`, **Then** the response includes a `requests_completeness_proof` attesting all N pending request UTxOs at the per-cage request address, anchored to one snapshot.
2. **Given** the response from (1), **When** the client decodes the unsigned tx and finds K ≤ N request UTxOs being consumed, **Then** every one of those K UTxOs is verifiable as a member of the attested N-element set, and the choice of which K to consume is recognised as a policy decision the oracle accepts.
3. **Given** a malicious server hides one pending request from the response, **When** the client verifies completeness against the externally-trusted UTxO-CSMT root, **Then** verification fails because the attested set's CSMT root does not match the trusted root.

---

### User Story 4 - Oracle ends a cage with verifiable empty-pending state (Priority: P2)

When an oracle wants to destroy a cage, they must guarantee no pending requests are silently abandoned. The end transaction must come with a witness that the per-cage request address is genuinely empty before the oracle signs.

**Why this priority**: Important but lower frequency than the daily oracle/requester flows. Wrong end behaviour leaks pending requesters' funds or strands their requests; right end behaviour protects them.

**Independent Test**: Call `POST /tx/oracle/end`. Verify offline that the response's `requests_completeness_proof` attests an *empty* leaf set at the per-cage request address against the externally-trusted UTxO-CSMT root.

**Acceptance Scenarios**:

1. **Given** a cage with no pending requests, **When** the oracle calls `POST /tx/oracle/end`, **Then** the response carries a completeness proof attesting the empty per-cage request address.
2. **Given** a cage with one or more pending requests, **When** the oracle calls `POST /tx/oracle/end`, **Then** the server either returns a build error (preferred) or returns a completeness proof that does not attest emptiness; in the second case the client refuses to sign.

---

### User Story 5 - Cage discovery via verifiable global state listing (Priority: P2)

A consumer (oracle, requester, or indexer) wants to discover the full set of currently-existing cages without trusting the offchain server's enumeration. The response must include every UTxO sitting at the global state validator address with a single completeness proof, so the client can independently classify each entry as a legitimate cage or sweepable garbage.

**Why this priority**: Discovery is needed for any client that does not already know the token id of the cage they want to interact with, and for the public sweep flow at the global state address. Less critical than per-cage flows but still core to the trust-minimised model.

**Independent Test**: Call `GET /tokens`. Verify offline that the response's completeness proof attests every UTxO at the locally-derived global state validator address against the externally-trusted UTxO-CSMT root, and that decoding each entry's TxOut CBOR yields either a legitimate state UTxO (single NFT under the trusted state policy id, well-formed State datum) or sweepable garbage.

**Acceptance Scenarios**:

1. **Given** a server that has indexed M state UTxOs and K garbage UTxOs at the global state address, **When** the client calls `GET /tokens`, **Then** the response lists all M+K entries with a single completeness proof outside the list, anchored to one snapshot.
2. **Given** the response from (1), **When** the client decodes each entry, **Then** legitimate cages are classified by NFT presence + datum shape, and garbage entries are flagged as candidates for `POST /tx/sweep`.

---

### User Story 6 - Public sweep of non-legitimate UTxOs at the global state address (Priority: P2)

Any client may spend a non-legitimate UTxO sitting at the global state validator address (the validator does not require any specific signer for such UTxOs). The offchain service must be able to build the unsigned sweep tx without requiring an oracle-role signature.

**Why this priority**: Symmetric to per-cage sweep, important for keeping the global state address tidy, but lower urgency than the in-cage flows.

**Independent Test**: Call `POST /tx/sweep` (top-level public path) with a target UTxO ref at the global state address. Verify offline that the unsigned tx spends the targeted UTxO, the response's `inputs` list provides CSMT inclusion proofs for every spent input, and no oracle signature is required by the validator.

**Acceptance Scenarios**:

1. **Given** a non-legitimate UTxO at the global state address, **When** any client calls `POST /tx/sweep`, **Then** the response builds an unsigned tx that consumes the targeted UTxO without requiring an oracle's verification key in the witness set.

---

### User Story 7 - Verifiable confirmation of a submitted transaction (Priority: P3)

After submitting a signed tx, a client wants to wait for confirmation and receive a proof that the transaction's first output is now in the indexed UTxO set, anchored to a verifiable snapshot.

**Why this priority**: Confirmation is convenience; the chain is the source of truth. Useful for downstream tooling but not load-bearing.

**Independent Test**: Call `GET /tx/:txId?timeout=N`. On success, verify offline that the response's CSMT inclusion proof attests `(txId, 0)` against the externally-trusted UTxO-CSMT root.

**Acceptance Scenarios**:

1. **Given** a submitted tx whose first output has appeared in the indexed UTxO set, **When** the client calls `GET /tx/:txId`, **Then** the response carries `(ref, txout_cbor, inclusion_proof)` plus a snapshot anchoring the proof.
2. **Given** the same tx before the timeout elapses, **When** the indexed UTxO set does not yet contain the output, **Then** the server holds the connection open until the output arrives or the timeout fires, returning HTTP 408 with no body on timeout.

---

### Edge Cases

- **Empty per-cage requests list with completeness proof for `update`/`end`.** The CSMT primitive must be able to attest that no leaves exist under a script-hash prefix. Confirmed feasible by haskell-mts contributors but flagged in the plan's research section.
- **Token id known to indexer but state UTxO consumed in a recent block not yet indexed.** Treat as 404 unverified. Client retries against a fresher snapshot or falls back to `/tokens`.
- **Multiple non-legitimate UTxOs at the global state address with the same NFT.** Cannot occur on a healthy chain (NFT minting policy enforces uniqueness), but if observed must be surfaced — the discovery response carries every UTxO at the address, so the anomaly is visible to the client.
- **Malicious server returns a snapshot whose `utxo_root` differs from the externally-trusted root for the same `chainpoint`.** Client refuses every proof in the response since none can be checked against the trusted root.
- **Server returns a snapshot whose `chainpoint` is older than the client's expected freshness.** Client may reject the snapshot for staleness; the snapshot's chainpoint makes this client-side decision possible.
- **Request UTxO at the per-cage address with malformed datum.** Classified as garbage by the client; included in the completeness witness regardless; consumable via `POST /tx/oracle/sweep`.
- **State UTxO at the global state address whose value contains zero or more than one of the trusted state policy's NFTs.** Classified as non-legitimate by the client; included in the global completeness witness regardless; consumable via the public `POST /tx/sweep`.
- **Pending request set exceeds what fits in a single Cardano transaction.** Out of scope for this redesign; the deferred multi-tx bundle design (separate follow-up) addresses this. For now `POST /tx/oracle/update` operates against bounded request sets.

## Requirements *(mandatory)*

### Functional Requirements — Read endpoints

- **FR-001**: `GET /status` MUST return only the indexer's current chain tip (slot and block id). It MUST NOT return any UTxO-CSMT root, indexed checkpoint, or other authoritative-server-state field.
- **FR-002**: `GET /tokens` MUST return every UTxO at the global state validator address with a single CSMT prefix-completeness proof outside the list, anchored to one verification snapshot. The response MUST NOT include the global state address itself, since the client derives it locally from the trusted blueprint.
- **FR-003**: `GET /tokens/:id` MUST return both the state UTxO for `:id` (with a CSMT inclusion proof) and the full set of UTxOs at the per-cage request address derived from `(state_policy_id, :id)` (with a single CSMT prefix-completeness proof), all anchored to one shared verification snapshot.
- **FR-004**: `GET /tokens/:id` MUST respond with HTTP 404 and no body when the indexer has no state UTxO for `:id`. This response is unverified by design; verifiable absence is obtained by calling `GET /tokens`.
- **FR-005**: `GET /tokens/:id/facts/:key` MUST split into two response shapes by HTTP status — HTTP 200 with `FactPresentResponse` carrying the value plus an MPF inclusion proof when the key exists, and HTTP 404 with `FactAbsentResponse` carrying an MPF exclusion proof when the key does not exist in a known token's trie. HTTP 404 with no body MUST be returned when the token itself is unknown to the indexer.
- **FR-006**: `GET /tx/:txId?timeout=N` MUST return HTTP 200 with a CSMT inclusion proof for `(txId, 0)` when the output is observed within the timeout, and HTTP 408 with no body on timeout.
- **FR-007**: The following endpoints MUST be removed: `GET /tokens/:id/root`, `GET /tokens/:id/proofs/:key`, `GET /tokens/:id/requests`, `GET /utxo/:txId/:txIx`, `GET /utxo/:txId/:txIx/proof`, `GET /utxo/root`. No alias or compatibility shim is provided.

### Functional Requirements — Write endpoints (uniform shape)

- **FR-010**: Every transaction-building endpoint MUST return a uniform response carrying the unsigned tx CBOR, a verification snapshot, and a flat list of `inputs` where each entry includes the input ref, its CBOR-encoded TxOut, and a CSMT inclusion proof against the snapshot's `utxo_root`. The `inputs` list MUST cover both spent and reference inputs of the unsigned tx; role discrimination is the responsibility of the client decoding the CBOR.
- **FR-011**: `POST /tx/boot` MUST be reachable at the top level of the `/tx/` namespace (not under any signer-role subpath), reflecting that the booter has no oracle role yet at the moment they call this endpoint.
- **FR-012**: `POST /tx/requester/{insert,delete,update,retract}` MUST be reachable at the per-role `requester` subpath, replacing the previous `/tx/request/...` and `/tx/retract` paths.
- **FR-013**: `POST /tx/oracle/{reject,update,sweep,end}` MUST be reachable at the per-role `oracle` subpath, replacing the previous `/tx/reject`, `/tx/update`, `/tx/sweep`, `/tx/end` paths.
- **FR-014**: `POST /tx/sweep` (top-level) MUST build an unsigned tx that spends a non-legitimate UTxO at the global state validator address without requiring any specific signer, distinct from the oracle-only `POST /tx/oracle/sweep` which operates against the per-cage request address.
- **FR-015**: `POST /tx/oracle/update` MUST additionally include a `requests_completeness_proof` field — a CSMT prefix-completeness proof attesting the full set of pending request UTxOs at the per-cage request address against the snapshot's `utxo_root`.
- **FR-016**: `POST /tx/oracle/end` MUST additionally include a `requests_completeness_proof` field that attests an empty leaf set at the per-cage request address against the snapshot's `utxo_root`.
- **FR-017**: `POST /tx/submit` MUST remain at the top level with its existing request and response shapes unchanged.

### Functional Requirements — Client verification surface

- **FR-020**: `cardano-mpfs-client` MUST ship a verifier for every new response shape introduced by this feature. Each verifier MUST take a client-supplied trusted `utxo_root` plus the response payload and return `Either VerifyError ()`.
- **FR-021**: Verifiers MUST be pure offline functions with no `IO`, networking, filesystem, time, or non-determinism, in compliance with constitution principles VIII and IX.
- **FR-022**: Verifiers MUST cross-compile to GHC-WASM and GHC-JS in addition to native GHC. Any new dependency that fails any of the three targets MUST be replaced before the verifier ships.
- **FR-023**: Read-side verifiers MUST validate (a) the snapshot's `chainpoint` is consistent across all proofs in the response, (b) every embedded `txout_cbor`'s address matches the locally-derived script address from the trusted blueprint, (c) every CSMT inclusion proof verifies against the supplied trusted root, (d) every CSMT prefix-completeness proof verifies against the supplied trusted root and attests the expected address prefix, (e) every MPF inclusion or exclusion proof verifies against the trie root recovered from the state UTxO datum.
- **FR-024**: Write-side verifiers MUST decode the response's `unsigned_tx_cbor`, derive the set of consumed and reference inputs, and verify that every one is covered by an entry in the response's `inputs` list with a valid CSMT inclusion proof against the supplied trusted root. For `POST /tx/oracle/update` and `POST /tx/oracle/end` they MUST additionally verify the `requests_completeness_proof` against the supplied trusted root and the locally-derived per-cage request address.

### Functional Requirements — Documentation and migration

- **FR-030**: The Swagger description (`docs/assets/swagger.json`) MUST be regenerated and reflect every endpoint shape change in this feature. The repo's `just update-swagger` recipe MUST produce a clean diff after the implementation lands.
- **FR-031**: Existing downstream consumers (MOOG, harvest, internal tooling, devnet smoke tests, e2e harnesses) MUST be migrated to the new endpoint paths and response shapes in the same release. No dual-path compatibility shim is shipped.

### Key Entities

- **Verification Snapshot**: the pair `(utxo_root, chainpoint)` that anchors every CSMT proof in a response. The `chainpoint` identifies the exact block at which the proofs are valid; the `utxo_root` is the CSMT root at that point. The same snapshot MUST be carried by every proof in a single response.
- **CSMT Inclusion Proof**: cryptographic witness that a given `(ref, txout_cbor)` pair is a leaf in the CSMT under a given `utxo_root`.
- **CSMT Prefix-Completeness Proof**: cryptographic witness that a given enumerated set of `(ref, txout_cbor)` pairs is *exactly* the set of UTxOs sitting under a given script-hash prefix in the CSMT under a given `utxo_root`. Supports the empty-set case.
- **MPF Inclusion Proof**: cryptographic witness that a given `(key, value)` is in a Merkle Patricia Forestry trie under a given trie root.
- **MPF Exclusion Proof**: cryptographic witness that a given `key` is not in a Merkle Patricia Forestry trie under a given trie root.
- **Trusted UTxO-CSMT Root**: a `utxo_root` value the client obtains from a source it trusts (typically a separate CSMT service), used as the anchor against which every CSMT proof in a response is verified. The MPFS offchain server is *not* a source of truth for this value under this redesign.
- **Per-cage Request Address**: the script address derived from the trusted blueprint and `(state_policy_id, cage_token_name)`, where pending request UTxOs for that cage live under the post-split protocol.
- **Global State Validator Address**: the script address derived from the trusted blueprint, where every cage's state UTxO lives. Single shared address across all cages.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A trust-minimised oracle can verify a cage's complete state (state UTxO + every pending request) using a single API call and a single externally-obtained trusted UTxO-CSMT root, with no further server round-trips.
- **SC-002**: A trust-minimised requester can verify every input the offchain service proposes to consume in their request transaction before signing, using one API call and one trusted UTxO-CSMT root.
- **SC-003**: A trust-minimised oracle attempting to sign an `update` transaction can detect any case where the server has hidden one or more pending requests from the batch, with cryptographic certainty (= probability of evading detection equal to the probability of forging a CSMT proof, which under standard hash assumptions is negligible).
- **SC-004**: A trust-minimised oracle attempting to sign an `end` transaction can detect any case where the per-cage request address still holds a pending request, with the same cryptographic certainty as SC-003.
- **SC-005**: The HTTP API surface shrinks by at least six removed endpoints (`/tokens/:id/root`, `/tokens/:id/proofs/:key`, `/tokens/:id/requests`, `/utxo/:txId/:txIx`, `/utxo/:txId/:txIx/proof`, `/utxo/root`) without any loss of client-observable functionality.
- **SC-006**: Every read response carries its proof inline; no client flow requires a separate "fetch the proof" call.
- **SC-007**: Every write response embeds CSMT inclusion proofs for every input the unsigned transaction touches; no client flow requires a separate "resolve and prove these inputs" call.
- **SC-008**: The `cardano-mpfs-client` verifier package builds successfully on native GHC, GHC-WASM, and GHC-JS targets in CI for every new response shape introduced by this feature.
- **SC-009**: Honest-and-forgery e2e coverage exists for every new response shape: an honest fixture passes verification; a corpus of forgeries (wrong root, wrong address, wrong NFT, missing element of completeness, fabricated input) each produce a distinct named `VerifyError`.
- **SC-010**: The Swagger document published from `docs/assets/swagger.json` after the implementation lands describes the new shapes and contains no references to removed endpoints.

## Assumptions

- The on-chain validator split (cardano-mpfs-onchain PR #50, mainline since `e3214cf`) is in place and the offchain repo's main branch has adopted it (cardano-mpfs-offchain PR #241, merge `e4e7cbb`).
- Clients have access to a trusted CSMT service that publishes the UTxO-CSMT root for chain points the offchain server will return in its responses. Out of scope for this feature: how that trusted CSMT service is operated, or how clients pin its identity.
- The trusted Aiken blueprint exposing the global state validator and the per-cage request validator is available client-side, and the client knows the trusted state policy id corresponding to that blueprint. Out of scope for this feature: how the blueprint is distributed.
- The CSMT primitive in `haskell-mts` exposes both inclusion and prefix-completeness proofs, including support for the empty-leaf-set case under a script-hash prefix. (To be confirmed in the plan's research phase.)
- Pending request sets for any cage in scope of this feature fit in a single Cardano transaction. The unbounded-batch case is explicitly deferred to a follow-up bundle design.
- All downstream consumers (MOOG, harvest, internal tooling) can be migrated to the new shapes in the same release; no dual-path compatibility shim is needed.
