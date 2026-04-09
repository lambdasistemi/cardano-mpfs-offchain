# Feature Specification: API Completeness — Reject and Request Update

**Feature Branch**: `175-api-completeness`
**Created**: 2026-04-09
**Status**: Draft
**Input**: Close API gaps vs on-chain validator: add reject endpoint to clean up Phase 3 requests, add request-update endpoint for value changes (parity with TS implementation).

## User Scenarios & Testing

### User Story 1 — Oracle rejects expired requests (Priority: P1)

A token oracle discovers stale requests stuck in Phase 3 (past both the processing window and the retract window). The oracle submits a reject transaction that consumes the expired request UTxOs, keeps the fee, and refunds remaining ADA to the original requesters.

**Why this priority**: Without this, any request that misses its processing window locks ADA permanently at the script address. We already have 6 stuck requests on preprod with ~2-3 ADA each.

**Independent Test**: Submit a request, wait for Phase 3, call the reject endpoint, verify the oracle receives the fee and the requester receives the refund.

**Acceptance Scenarios**:

1. **Given** a request in Phase 3 (past submitted_at + process_time + retract_time), **When** the oracle calls reject, **Then** the request UTxO is consumed, the oracle keeps the fee, and the requester receives (locked ADA - fee).
2. **Given** a request still in Phase 1 or Phase 2, **When** the oracle calls reject, **Then** the transaction fails (on-chain is_rejectable check fails).
3. **Given** multiple expired requests for the same token, **When** the oracle calls reject, **Then** all expired requests are consumed in a single transaction with correct refunds for each.
4. **Given** a reject transaction, **Then** the token's MPF root does NOT change (reject does not modify the trie).

---

### User Story 2 — Requester updates an existing key's value (Priority: P2)

A requester wants to change the value associated with an existing key in a token's MPF trie. Instead of deleting and re-inserting (two separate requests, two fees), the requester submits a single update request specifying the key, old value, and new value.

**Why this priority**: The on-chain Operation::Update(old, new) exists and the TS implementation already exposes this. The Haskell offchain is missing it — users must work around it with delete + insert, paying double fees.

**Independent Test**: Insert a key, process it, then submit a request-update with old and new values, process the update, verify the trie root changes correctly.

**Acceptance Scenarios**:

1. **Given** a key "k" with value "v1" in the trie, **When** the requester submits an update request with key="k", oldValue="v1", newValue="v2", and the oracle processes it, **Then** the trie root reflects key "k" mapped to "v2".
2. **Given** a key "k" with value "v1", **When** the requester submits an update request with wrong oldValue="v3", **Then** the on-chain proof verification fails (including(key, wrong_old, proof) != root).
3. **Given** no key "k" in the trie, **When** the requester submits an update request for key="k", **Then** the proof verification fails.

---

### Edge Cases

- What happens when reject is called with a mix of Phase 3 and Phase 1/2 requests pending? Only Phase 3 requests should be consumed; Phase 1/2 requests must remain.
- What happens when the oracle calls reject but is not the token owner? The on-chain Reject redeemer requires the owner's signature.
- What happens when a request has a dishonest (future) submitted_at timestamp? The on-chain is_rejectable function should still accept it for rejection.

## Requirements

### Functional Requirements

- **FR-001**: System MUST expose a `POST /tx/reject` endpoint that builds a transaction consuming Phase 3 request UTxOs for a given token.
- **FR-002**: The reject transaction MUST use the on-chain `Reject` redeemer on the State UTxO and `Contribute` redeemer on each request UTxO being rejected.
- **FR-003**: The reject transaction MUST NOT change the MPF root (newRoot == oldRoot).
- **FR-004**: The reject transaction MUST produce refund outputs: for each rejected request, (locked ADA - fee) to the request owner.
- **FR-005**: The reject transaction MUST require the token owner's signature.
- **FR-006**: System MUST expose a `POST /tx/request/update` endpoint that builds a request transaction with Operation::Update(oldValue, newValue).
- **FR-007**: The request-update endpoint MUST accept token, key, old_value, new_value, and address parameters.
- **FR-008**: The TxBuilder MUST include `requestReject` and `requestUpdate` functions.
- **FR-009**: The update transaction builder (processRequest) already handles OpUpdate — no changes needed there.

### Key Entities

- **RejectRequest**: token ID + oracle address. The endpoint discovers which requests are rejectable by querying pending requests and checking their phase.
- **UpdateRequest (value change)**: token ID + key + old value + new value + requester address.

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 6 stuck preprod requests can be cleaned up via the reject endpoint.
- **SC-002**: The reject E2E test passes: submit request, wait for Phase 3, reject, verify refund.
- **SC-003**: The request-update E2E test passes: insert key, update value, verify trie root.
- **SC-004**: The Haskell API has feature parity with the TS API for all request operations (insert, delete, update).

## Assumptions

- The on-chain Reject validator logic is correct and tested (it exists in the onchain repo with test coverage).
- Phase timing on devnet is short enough to test Phase 3 transitions within a reasonable test duration.
- The reject endpoint processes ALL rejectable requests for a token in a single transaction (matching the on-chain fold pattern).
