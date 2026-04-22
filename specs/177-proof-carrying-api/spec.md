# Feature Specification: Proof-Carrying API Responses

**Feature Branch**: `177-proof-carrying-api`
**Created**: 2026-04-17
**Status**: Draft
**Input**: lambdasistemi/cardano-mpfs-offchain#208

## User Scenarios & Testing

### User Story 1 - Client verifies token state and facts (Priority: P1)

A wallet, oracle, or downstream indexer reads token state, facts, and
pending requests from an untrusted offchain service. Each response must
carry the UTxO witness, the UTxO root, the indexed chain point, and MPF
proof material needed to verify the returned data against an
independently checked UTxO-CSMT root.

**Why this priority**: Trust-minimized reads are the foundation for the
rest of the API. If the server can lie about state or facts, every
downstream decision built on that data is unsafe.

**Independent Test**: Call `GET /tokens/:id`,
`GET /tokens/:id/facts/:key`, `GET /tokens/:id/proofs/:key`, and
`GET /tokens/:id/requests`. Verify offline that each response carries
its own `utxo_root` and `chainpoint`, every returned UTxO exists in the
indexed set, and every fact proof matches the reported token root.

**Acceptance Scenarios**:

1. **Given** an existing token, **When** the client calls
   `GET /tokens/:id`, **Then** the response includes the returned token
   state plus the state UTxO witness, its UTxO-CSMT inclusion proof, the
   `utxo_root`, and the indexed `chainpoint`.
2. **Given** an existing key/value in the trie, **When** the client
   calls `GET /tokens/:id/facts/:key`, **Then** the response includes
   the fact value, the state UTxO witness/proof, the `utxo_root`, the
   indexed `chainpoint`, and an MPF inclusion proof tying that key/value
   to the reported root.
3. **Given** an existing key/value in the trie, **When** the client
   calls `GET /tokens/:id/proofs/:key`, **Then** the response includes
   the MPF proof, the state UTxO witness/proof, the `utxo_root`, and
   the indexed `chainpoint` needed to trust the root the proof is
   checked against.
4. **Given** pending requests for a token, **When** the client calls
   `GET /tokens/:id/requests`, **Then** each returned request includes
   the request UTxO reference, resolved TxOut, UTxO-CSMT inclusion
   proof, `utxo_root`, and indexed `chainpoint`, with no ambiguity about
   which proof belongs to which request.

---

### User Story 2 - Client verifies unsigned transactions before signing (Priority: P2)

An external signer asks the service to build an unsigned transaction.
Because signing happens client-side and the offchain service is
untrusted, the response must explain every input the transaction spends
and every trie fact the transaction logic depends on, and it must carry
the `utxo_root` plus indexed `chainpoint` for the exact snapshot the
proofs target.

**Why this priority**: External signing is a project constitution
principle. Returning bare CBOR is not enough if the client cannot verify
what it is being asked to sign.

**Independent Test**: Call each affected transaction-building endpoint
and confirm the response contains the unsigned transaction plus a
verification bundle with `utxo_root`, indexed `chainpoint`, every spent
input, and every trie-dependent datum used by the builder.

**Acceptance Scenarios**:

1. **Given** `POST /tx/update`, **When** the client builds an update
   transaction, **Then** the response includes the unsigned transaction,
   every spent input resolved as a TxOut with a UTxO-CSMT inclusion
   proof, the `utxo_root`, the indexed `chainpoint`, and MPF proofs for
   the state root and request keys used to justify the update.
2. **Given** `POST /tx/request/insert`, `POST /tx/request/delete`,
   `POST /tx/request/update`, `POST /tx/retract`, `POST /tx/reject`, or
   `POST /tx/end`, **When** the client builds a transaction, **Then**
   the response includes the unsigned transaction plus a per-endpoint
   proof shape that names each witnessed input's role (state, pending
   request, state reference, or funding), the `utxo_root`, and the
   indexed `chainpoint` sufficient to verify every consumed UTxO
   before signing.
3. **Given** `POST /tx/boot`, **When** the client builds the initial
   boot transaction, **Then** the response includes the unsigned
   transaction and proof-bearing witnesses for all consumed wallet
   inputs, the `utxo_root`, the indexed `chainpoint`, and no MPF proof
   section because no pre-existing trie data is read.
4. **Given** a transaction builder consumes multiple requests in one
   batch, **When** the response is returned, **Then** proofs remain
   associated with the exact inputs and logical request keys they
   justify.
5. **Given** any proof-bearing transaction response, **When** the
   client verifies the proof, **Then** the verification runs entirely in
   pure Haskell inside `cardano-mpfs-client`, without any dependency on
   `cardano-ledger-*` libraries, so the same verifier compiles to
   GHC-WASM and GHC-JS per Constitution IX.
6. **Given** the shallow `TxOut` decoder shipped inside
   `cardano-mpfs-client`, **When** the cross-check test suite in the
   server repo runs, **Then** the shallow decoder produces byte-
   identical `(address, ada)` output to the authoritative
   `cardano-ledger-conway` decoder across every generated TxOut and the
   pinned regression corpus, and rejects every truncated prefix of a
   valid CBOR blob.

---

### User Story 3 - Client discovers verification contracts and compares roots (Priority: P3)

An integrator needs clear JSON contracts for all proof-bearing responses
and a way to compare each response's baked-in root and chain point
against trusted external providers.

**Why this priority**: Clients cannot validate proofs reliably if the
root and chain point are not embedded in the proof-bearing JSON itself,
or if the contract leaves ambiguity about how to compare them with an
external trusted source.

**Independent Test**: Inspect the Swagger/OpenAPI docs and call one
proof-bearing query endpoint plus one proof-bearing transaction
endpoint. Confirm both responses embed `utxo_root` and `chainpoint`, and
that those values can be compared with `GET /status` and `GET /utxo/root`.

**Acceptance Scenarios**:

1. **Given** a proof-bearing response, **When** the client reads its
   JSON, **Then** the response itself includes `utxo_root` and
   `chainpoint` for the exact snapshot the bundled proofs target.
2. **Given** updated Swagger/OpenAPI docs, **When** an integrator
   inspects the API contract, **Then** every proof-bearing endpoint
   documents the response object and the meaning of each proof field,
   including `utxo_root` and `chainpoint`.
3. **Given** a trusted external provider keyed by chain point, **When**
   the client compares a proof-bearing response's `utxo_root` and
   `chainpoint` against that provider, **Then** it can confirm the root
   match without inferring snapshot metadata from a separate call.
4. **Given** the direct UTxO verification endpoints already exist,
   **When** the client cross-checks inline proofs against `GET
   /utxo/root` and `GET /utxo/:txId/:txIx/proof`, **Then** the witness
   bytes and baked-in root agree for the same indexed snapshot.
5. **Given** a debugging or isolated deployment scenario, **When** the
   client uses `GET /utxo/root` instead of `GET /status`, **Then** it
   can obtain the same UTxO-CSMT root from the same indexed source of
   truth without depending on a separate CSMT service.

### Edge Cases

- The indexer may advance between reading state/request data and
  generating proofs. A response must be self-consistent for one indexed
  snapshot; mixed-root or mixed-chainpoint bundles are invalid.
- `GET /tokens/:id/facts/:key` and `GET /tokens/:id/proofs/:key` keep
  their existing not-found behavior for absent keys. Non-membership
  proofs are out of scope.
- Batch endpoints may touch many request UTxOs. Response size can grow,
  but proof-to-item association must remain deterministic.
- Existing clients that expect scalar `Hex` payloads on affected
  endpoints must migrate to structured JSON response bodies. Compatibility
  shims or versioned endpoints are out of scope for this feature.

## Requirements

### Functional Requirements

- **FR-001**: `GET /tokens/:id` MUST return token state together with the
  state UTxO witness and its UTxO-CSMT inclusion proof.
- **FR-002**: `GET /tokens/:id/facts/:key` MUST return the fact value
  together with the state UTxO witness/proof and the MPF inclusion proof
  for the requested key/value.
- **FR-003**: `GET /tokens/:id/proofs/:key` MUST return the MPF
  inclusion proof together with the state UTxO witness/proof needed to
  trust the root that the proof targets.
- **FR-004**: `GET /tokens/:id/requests` MUST return each pending
  request together with the request UTxO witness and its UTxO-CSMT
  inclusion proof.
- **FR-005**: Every proof-bearing query response MUST identify the
  exact `utxo_root` and indexed `chainpoint` needed to verify all
  bundled proofs against one indexed state.
- **FR-006**: `POST /tx/boot`, `POST /tx/request/insert`,
  `POST /tx/request/delete`, `POST /tx/request/update`,
  `POST /tx/update`, `POST /tx/retract`, `POST /tx/reject`, and
  `POST /tx/end` MUST return structured JSON objects that include the
  unsigned transaction plus proof-bearing verification metadata.
- **FR-007**: Every transaction response MUST include every consumed
  `TxIn` resolved to its `TxOut` plus a UTxO-CSMT inclusion proof for
  that input.
- **FR-008**: Every transaction response MUST include the exact
  `utxo_root` and indexed `chainpoint` for the snapshot used to build
  its proof bundle.
- **FR-009**: Transaction responses MUST include MPF inclusion proofs
  for every token state root or request key/value that the client must
  trust before signing.
- **FR-010**: Endpoints that do not read trie data, such as boot, MUST
  omit MPF proof material rather than invent placeholder proofs.
- **FR-011**: `GET /status` MUST expose the current UTxO-CSMT root hash
  alongside chain/checkpoint fields that let clients compare it with the
  baked-in values carried by proof-bearing responses.
- **FR-012**: Swagger/OpenAPI MUST describe all new proof-bearing
  response objects and document which fields are verified against the
  UTxO-CSMT root versus the MPF root, including the semantics of the
  baked-in `chainpoint`.
- **FR-013**: A client MUST be able to verify all proof-bearing query
  and transaction responses offline using the `utxo_root` and
  `chainpoint` carried inside the response, and compare them with an
  independently trusted provider if desired.
- **FR-014**: Responses that contain multiple requests, inputs, or
  proofs MUST preserve deterministic association between each business
  object and its proof bundle.
- **FR-015**: Existing direct UTxO proof endpoints (`GET /utxo/:txId/:txIx`,
  `GET /utxo/:txId/:txIx/proof`, `GET /utxo/root`) MUST remain usable
  for cross-checking and debugging.
- **FR-016**: `GET /utxo/root` MUST remain a first-class root-discovery
  endpoint that returns the same indexed UTxO-CSMT root as `GET /status`,
  so debugging and isolated deployments can use this service as the root
  source of truth even without a separate CSMT endpoint.
- **FR-017**: Transaction-building endpoints MUST return per-endpoint
  proof shapes whose named fields encode the role of every witnessed
  input (state, pending request, state reference, funding). Flat
  undifferentiated input lists are not acceptable. The endpoint URL is
  the discriminator; no tagged-union wrapper is added to the JSON.
- **FR-018**: `cardano-mpfs-client` MUST ship a pure-Haskell shallow
  `TxOut` decoder that extracts at minimum the address bytes and the
  ada (lovelace) amount from Conway-era `TxOut` CBOR. The decoder MUST
  NOT depend on `cardano-ledger-*` or any C FFI, so the verifier stays
  buildable with the GHC-WASM and GHC-JS backends per Constitution IX.
- **FR-019**: A cross-check test suite in `cardano-mpfs-offchain` MUST
  prove the shallow decoder agrees byte-for-byte with the authoritative
  `cardano-ledger-conway` decoder. Generator coverage MUST include all
  Shelley address shapes (payment × stake: KeyHash / ScriptHash / Pointer
  / absent), all value shapes (ada-only, ada + single asset, ada + many
  assets across many policies), all datum variants (no datum, datum
  hash, short inline datum, long inline datum), and both presence and
  absence of a reference script. The suite MUST also prove the shallow
  decoder returns `Left` (never diverges or returns `Right`) for every
  truncated prefix of a valid CBOR blob and for random bytes.
- **FR-020**: A regression corpus of pinned real `TxOut` hex blobs
  (preprod / mainnet observations) MUST live beside the cross-check
  suite and be re-checked on every ledger dependency bump, so accidental
  CBOR-layout drift across ledger versions fails CI rather than silently
  diverging verifier behaviour.
- **FR-021**: For every transaction-building endpoint in scope,
  `cardano-mpfs-client` MUST expose a pure verifier whose shape is
  `verify :: TrustedRoot -> ProofEnvelope p -> Either VerifyError ()`,
  and the verifier MUST execute the documented per-endpoint check list
  (snapshot well-formedness, snapshot freshness hook, inputs-match-
  witnesses set equality, per-witness ownership + CSMT-path check, and
  endpoint-specific output / mint / datum checks). The check list for
  each endpoint MUST be recorded alongside the verifier source.

### Key Entities

- **VerificationSnapshot**: The exact `utxo_root` plus indexed
  `chainpoint` that identifies the snapshot against which the bundled
  proofs are valid.
- **WitnessedUtxo**: A consumed or returned UTxO represented by its
  `TxIn`, resolved `TxOut`, and UTxO-CSMT inclusion proof. Used on
  read-side responses.
- **WitnessedInput**: The tx-side analogue of `WitnessedUtxo`: the
  consumed `TxIn`, the resolved `TxOut` CBOR, and the UTxO-CSMT
  inclusion proof that binds the pair to the snapshot's `utxo_root`.
- **FactWitness**: A token state witness plus an MPF inclusion proof
  tying a key/value to the token's reported root.
- **ProofEnvelope p**: A wrapper around the unsigned transaction, the
  `VerificationSnapshot`, and an endpoint-specific proof payload `p`.
  The shape of `p` mirrors the dataflow of the endpoint's tx, so
  verification is a type-directed walk over named `WitnessedInput`
  fields rather than a flat list check.
- **BootProof**: Proof payload for `POST /tx/boot`. Carries only the
  funding inputs because boot mints a fresh state UTxO and reads no
  trie data.
- **RequestProof**: Proof payload for the three `POST /tx/request/*`
  endpoints. Carries only the funding inputs because the request tx
  does not consume or reference the state UTxO.
- **RetractProof**: Proof payload for `POST /tx/retract`. Carries the
  retracted pending-request witness, the state UTxO reference witness
  (read-only), and the funding inputs.
- **RejectProof**: Proof payload for `POST /tx/reject`. Carries the
  state input witness, the witnesses for every rejected pending-request
  input, and the funding inputs.
- **EndProof**: Proof payload for `POST /tx/end`. Carries the state
  input witness and the funding inputs.
- **UpdateProof**: Proof payload for `POST /tx/update`. Carries the
  state input witness, the witnesses for every batched pending-request
  input, the funding inputs, the trie root from the consumed state
  datum, and an MPF inclusion proof for every trie key the batch
  touched.
- **WitnessedRequest**: A pending request together with the request UTxO
  witness that proves the request existed in the indexed UTxO set.
- **ShallowTxOut**: The pair `(address bytes, ada lovelace)` extracted
  from a Conway `TxOut` CBOR blob by the WASM-safe decoder inside
  `cardano-mpfs-client`. Every tx-proof verifier cross-checks that the
  extracted address equals the caller's address and that the ada is
  consistent with the expected fees and locked amounts.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A client can verify `GET /tokens/:id`,
  `GET /tokens/:id/facts/:key`, `GET /tokens/:id/proofs/:key`, and
  `GET /tokens/:id/requests` entirely offline using the `utxo_root` and
  `chainpoint` embedded in each response.
- **SC-002**: A client can verify every input and trie-dependent datum
  returned by the affected transaction-building endpoints before signing
  any CBOR.
- **SC-003**: A client can compare any proof-bearing response's
  `utxo_root` and `chainpoint` with an external trusted provider without
  making a separate metadata-discovery call first.
- **SC-004**: `GET /status` exposes the same UTxO-CSMT root that direct
  `GET /utxo/root` returns for the same indexed snapshot.
- **SC-005**: Swagger/OpenAPI and HTTP tests cover the new response
  contracts for every affected endpoint.
- **SC-006**: `cardano-mpfs-client` builds and runs the verifier
  successfully under GHC-native, GHC-WASM, and GHC-JS, with
  byte-identical `Either VerifyError a` output across the three
  targets for the same inputs.
- **SC-007**: The shallow `TxOut` decoder cross-check suite passes
  with ≥ 1000 shrinks per property in CI, and the pinned regression
  corpus decodes identically to the ledger reference.

## Assumptions

- `cardano-utxo-csmt` can already produce inclusion proofs, or exposing
  them is available as part of the dependency work referenced in issue
  `#208`.
- Non-membership proofs are out of scope. This feature only covers data
  that currently exists and is returned successfully.
- `GET /tokens` and `GET /tokens/:id/root` are unchanged in this
  feature. Clients that require trust-minimized reads use the
  proof-bearing endpoints listed above.
- Keeping both `GET /status` and `GET /utxo/root` as root-discovery
  surfaces is acceptable duplication because they serve different client
  workflows while remaining backed by the same indexed state.
- Changing the JSON shape of the affected endpoints is acceptable for
  this feature, and downstream clients will be updated to consume the new
  object responses.
