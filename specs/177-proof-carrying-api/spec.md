# Feature Specification: Proof-Carrying API Responses

**Feature Branch**: `177-proof-carrying-api`
**Created**: 2026-04-17
**Status**: Draft
**Input**: lambdasistemi/cardano-mpfs-offchain#208

## User Scenarios & Testing

### User Story 1 - Client verifies token state and facts (Priority: P1)

A wallet, oracle, or downstream indexer reads token state, facts, and
pending requests from an untrusted offchain service. Each response must
carry the UTxO witness and MPF proof material needed to verify the
returned data against an independently obtained UTxO-CSMT root.

**Why this priority**: Trust-minimized reads are the foundation for the
rest of the API. If the server can lie about state or facts, every
downstream decision built on that data is unsafe.

**Independent Test**: Call `GET /status` to obtain the current indexed
root, then call `GET /tokens/:id`, `GET /tokens/:id/facts/:key`,
`GET /tokens/:id/proofs/:key`, and `GET /tokens/:id/requests`. Verify
offline that every returned UTxO exists in the indexed set and every
fact proof matches the reported token root.

**Acceptance Scenarios**:

1. **Given** an existing token, **When** the client calls
   `GET /tokens/:id`, **Then** the response includes the returned token
   state plus the state UTxO witness and its UTxO-CSMT inclusion proof.
2. **Given** an existing key/value in the trie, **When** the client
   calls `GET /tokens/:id/facts/:key`, **Then** the response includes
   the fact value, the state UTxO witness/proof, and an MPF inclusion
   proof tying that key/value to the reported root.
3. **Given** an existing key/value in the trie, **When** the client
   calls `GET /tokens/:id/proofs/:key`, **Then** the response includes
   the MPF proof and the state UTxO witness/proof needed to trust the
   root the proof is checked against.
4. **Given** pending requests for a token, **When** the client calls
   `GET /tokens/:id/requests`, **Then** each returned request includes
   the request UTxO reference, resolved TxOut, and UTxO-CSMT inclusion
   proof, with no ambiguity about which proof belongs to which request.

---

### User Story 2 - Client verifies unsigned transactions before signing (Priority: P2)

An external signer asks the service to build an unsigned transaction.
Because signing happens client-side and the offchain service is
untrusted, the response must explain every input the transaction spends
and every trie fact the transaction logic depends on.

**Why this priority**: External signing is a project constitution
principle. Returning bare CBOR is not enough if the client cannot verify
what it is being asked to sign.

**Independent Test**: Call each affected transaction-building endpoint
and confirm the response contains the unsigned transaction plus a
verification bundle for every spent input and every trie-dependent datum
used by the builder.

**Acceptance Scenarios**:

1. **Given** `POST /tx/update`, **When** the client builds an update
   transaction, **Then** the response includes the unsigned transaction,
   every spent input resolved as a TxOut with a UTxO-CSMT inclusion
   proof, and MPF proofs for the state root and request keys used to
   justify the update.
2. **Given** `POST /tx/request/insert`, `POST /tx/request/delete`,
   `POST /tx/request/update`, `POST /tx/retract`, `POST /tx/reject`, or
   `POST /tx/end`, **When** the client builds a transaction, **Then**
   the response includes the unsigned transaction plus proof-bearing
   input witnesses sufficient to verify every consumed UTxO before
   signing.
3. **Given** `POST /tx/boot`, **When** the client builds the initial
   boot transaction, **Then** the response includes the unsigned
   transaction and proof-bearing witnesses for all consumed wallet
   inputs, and no MPF proof section is required because no pre-existing
   trie data is read.
4. **Given** a transaction builder consumes multiple requests in one
   batch, **When** the response is returned, **Then** proofs remain
   associated with the exact inputs and logical request keys they
   justify.

---

### User Story 3 - Client discovers the verification root and contracts (Priority: P3)

An integrator needs a single place to discover the current UTxO-CSMT
root and the JSON contracts for all proof-bearing responses.

**Why this priority**: Clients cannot validate proofs reliably if they
cannot identify the root they should check against or the exact meaning
of each proof field.

**Independent Test**: Call `GET /status` and inspect the Swagger/OpenAPI
docs. Confirm the root is present and every proof-bearing endpoint is
documented with a structured response schema.

**Acceptance Scenarios**:

1. **Given** a healthy service, **When** the client calls `GET /status`,
   **Then** the response includes the current UTxO-CSMT root together
   with chain/checkpoint metadata that identifies the indexed snapshot.
2. **Given** updated Swagger/OpenAPI docs, **When** an integrator
   inspects the API contract, **Then** every proof-bearing endpoint
   documents the response object and the meaning of each proof field.
3. **Given** the direct UTxO verification endpoints already exist,
   **When** the client cross-checks inline proofs against `GET
   /utxo/root` and `GET /utxo/:txId/:txIx/proof`, **Then** the witness
   bytes agree for the same indexed snapshot.
4. **Given** a debugging or isolated deployment scenario, **When** the
   client uses `GET /utxo/root` instead of `GET /status`, **Then** it
   can obtain the same UTxO-CSMT root from the same indexed source of
   truth without depending on a separate CSMT service.

### Edge Cases

- The indexer may advance between reading state/request data and
  generating proofs. A response must be self-consistent for one indexed
  snapshot; mixed-root bundles are invalid.
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
  UTxO-CSMT root, checkpoint, or equivalent snapshot metadata needed to
  verify all bundled proofs against one indexed state.
- **FR-006**: `POST /tx/boot`, `POST /tx/request/insert`,
  `POST /tx/request/delete`, `POST /tx/request/update`,
  `POST /tx/update`, `POST /tx/retract`, `POST /tx/reject`, and
  `POST /tx/end` MUST return structured JSON objects that include the
  unsigned transaction plus proof-bearing verification metadata.
- **FR-007**: Every transaction response MUST include every consumed
  `TxIn` resolved to its `TxOut` plus a UTxO-CSMT inclusion proof for
  that input.
- **FR-008**: Transaction responses MUST include MPF inclusion proofs
  for every token state root or request key/value that the client must
  trust before signing.
- **FR-009**: Endpoints that do not read trie data, such as boot, MUST
  omit MPF proof material rather than invent placeholder proofs.
- **FR-010**: `GET /status` MUST expose the current UTxO-CSMT root hash
  alongside the existing tip and checkpoint fields.
- **FR-011**: Swagger/OpenAPI MUST describe all new proof-bearing
  response objects and document which fields are verified against the
  UTxO-CSMT root versus the MPF root.
- **FR-012**: A client MUST be able to verify all proof-bearing query
  and transaction responses offline against an independently obtained
  UTxO-CSMT root without trusting the server's narrative.
- **FR-013**: Responses that contain multiple requests, inputs, or
  proofs MUST preserve deterministic association between each business
  object and its proof bundle.
- **FR-014**: Existing direct UTxO proof endpoints (`GET /utxo/:txId/:txIx`,
  `GET /utxo/:txId/:txIx/proof`, `GET /utxo/root`) MUST remain usable
  for cross-checking and debugging.
- **FR-015**: `GET /utxo/root` MUST remain a first-class root-discovery
  endpoint that returns the same indexed UTxO-CSMT root as `GET /status`,
  so debugging and isolated deployments can use this service as the root
  source of truth even without a separate CSMT endpoint.

### Key Entities

- **VerificationSnapshot**: The UTxO-CSMT root plus checkpoint metadata
  that identifies the indexed state against which proofs are valid.
- **WitnessedUtxo**: A consumed or returned UTxO represented by its
  `TxIn`, resolved `TxOut`, and UTxO-CSMT inclusion proof.
- **FactWitness**: A token state witness plus an MPF inclusion proof
  tying a key/value to the token's reported root.
- **UnsignedTxWitnessBundle**: An unsigned transaction plus the full set
  of witnessed inputs and any MPF proofs required before signing.
- **WitnessedRequest**: A pending request together with the request UTxO
  witness that proves the request existed in the indexed UTxO set.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A client can verify `GET /tokens/:id`,
  `GET /tokens/:id/facts/:key`, `GET /tokens/:id/proofs/:key`, and
  `GET /tokens/:id/requests` entirely offline against an independently
  sourced UTxO-CSMT root.
- **SC-002**: A client can verify every input and trie-dependent datum
  returned by the affected transaction-building endpoints before signing
  any CBOR.
- **SC-003**: `GET /status` exposes the same UTxO-CSMT root that direct
  `GET /utxo/root` returns for the same indexed snapshot.
- **SC-004**: Swagger/OpenAPI and HTTP tests cover the new response
  contracts for every affected endpoint.

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
