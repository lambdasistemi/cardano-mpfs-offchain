# Feature Specification: Bind proof bundles to unsigned transactions

**Feature Branch**: `feat/client-bind-proof-bundle-content-to-the-unsigned-t`
**Created**: 2026-04-25
**Status**: Draft
**Input**: Issue [#227](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/227) - "client: bind proof-bundle content to the unsigned tx (proof usefulness)"

## User Scenarios & Testing

### User Story 1 - Reject a response whose proof inputs do not cover the tx inputs (Priority: P1)

A wallet receives a proof-bearing response and verifies it before
signing. The response's CSMT proofs may be cryptographically valid, but
they are useful only if they cover the unsigned transaction's actual
inputs and reference inputs. The client verifier must reject any response
where the tx body consumes an input that is not represented by the proof
roles for that endpoint.

**Why this priority**: This closes the immediate trust gap from issue
#227. Without this, the server can ship a valid proof bundle for one set
of UTxOs beside an unrelated unsigned transaction.

**Independent Test**: Build honest fixtures whose transaction body input
sets match their proof roles, then replace only the `tx` field with a
different valid Conway-shaped transaction CBOR. The verifier must reject
with a structured binding error.

**Acceptance Scenarios**:

1. **Given** an honest `BootTxResponse`, **When** its transaction inputs
   equal the `boot.funding[*].tx_in` witnesses, **Then**
   `verifyBootTxResponse` accepts it.
2. **Given** an otherwise honest `BootTxResponse`, **When** its
   transaction consumes an extra input not present in `boot.funding`,
   **Then** `verifyBootTxResponse` rejects it before the caller signs.
3. **Given** an honest `RetractTxResponse`, **When** its transaction
   consumes `request_in` and funding while referencing `state_ref`,
   **Then** `verifyRetractTxResponse` accepts it.
4. **Given** an otherwise honest `RetractTxResponse`, **When** its
   transaction omits `state_ref` from reference inputs, **Then**
   `verifyRetractTxResponse` rejects it.

### User Story 2 - Keep the verifier pure and portable (Priority: P1)

The binding check must run anywhere the existing verifier runs. It must
not import `cardano-ledger-*`, native crypto packages, RocksDB, network
clients, or any other server-side dependency.

**Why this priority**: Principle IX requires one verifier across native,
WASM, and JS targets. The fix cannot move trust back to the server.

**Independent Test**: `cardano-mpfs-client` builds and its unit tests run
with only pure Haskell dependencies.

**Acceptance Scenarios**:

1. **Given** a response verifier, **When** it performs tx binding,
   **Then** it uses only the response's `tx`, `snapshot`, and `proof`
   fields.
2. **Given** a malformed or unsupported tx CBOR shape, **When** the
   verifier reaches the binding pass, **Then** it returns a structured
   binding failure rather than throwing an exception.

### User Story 3 - Record the scope of remaining deeper binding work (Priority: P2)

Input/reference-input coverage is the first independently checkable
binding layer. Deeper assertions about mint policies, redeemer payloads,
state-carrying outputs, and MPF facts must be documented as follow-up
work unless implemented in this slice.

**Why this priority**: The issue calls out more than input coverage. The
first slice must not pretend to prove properties it does not yet check.

**Independent Test**: The plan and task list identify the residual work
explicitly.

### User Story 4 - Reject mint and continuing-state-output mismatches (Priority: P1)

A wallet receives a proof-bearing response whose consumed inputs match
the proof roles, but whose mint field or continuing state output does
not match the endpoint role. The verifier must reject boot, reject, end,
and update responses where the transaction mints/burns an unexpected
asset or fails to carry the state token forward in exactly one inline
datum output.

**Why this priority**: Input binding alone proves which UTxOs the tx
uses. Mint and state-output binding proves that token lifecycle
endpoints act on the same state token represented by the proof bundle.

**Independent Test**: Build honest fixtures with mint/output fields,
then replace only the `tx` field so the tx omits the continuing state
output or burns the wrong state token quantity. Verification must reject
with `TxBindingFailed`.

## Edge Cases

- Tx bodies may encode Cardano sets either as plain arrays or as tag
  258-wrapped arrays. The decoder must accept both.
- Tx body map fields may appear in any order.
- Reference inputs are optional; missing reference inputs are an empty
  set.
- Collateral inputs are not covered by this first slice because they are
  not regular spending inputs or reference inputs.
- Fixtures must use valid Conway-shaped transaction CBOR rather than
  placeholder bytes once binding is enabled.

## Requirements

### Functional Requirements

- **FR-001**: The client MUST decode enough Conway transaction CBOR to
  read tx body inputs and reference inputs.
- **FR-002**: The decoder MUST be pure and MUST NOT depend on
  `cardano-ledger-*`.
- **FR-003**: Each write response verifier MUST compare decoded tx inputs
  and reference inputs against the endpoint proof roles.
- **FR-004**: The binding pass MUST reject extra, missing, or misplaced
  inputs with a structured error.
- **FR-005**: Existing honest client fixtures MUST use tx CBOR whose
  input/reference-input sets match their proof roles.
- **FR-006**: Forged-binding unit tests MUST replace only the response
  `tx` field and prove the verifier rejects the mismatch.
- **FR-007**: The implementation MUST document residual binding work for
  mint, redeemers, state-carrying outputs, and MPF fact binding.
- **FR-008**: Boot responses MUST mint exactly one asset and include
  exactly one inline-datum state output carrying that same asset.
- **FR-009**: End responses MUST burn the asset carried by the witnessed
  state input and MUST NOT leave a continuing state output carrying it.
- **FR-010**: Reject and update responses MUST preserve the asset carried
  by the witnessed state input into exactly one inline-datum state output
  and MUST NOT mint or burn assets.

## Success Criteria

- **SC-001**: Honest unit fixtures for boot, request, retract, reject,
  end, and update continue to pass.
- **SC-002**: At least one forged tx-binding unit test fails verification
  for each endpoint family: funding-only, reference-input, and
  state/request-consuming endpoints.
- **SC-003**: `cardano-mpfs-client:unit-tests` passes.
- **SC-004**: The PR description states which binding layers are covered
  and lists remaining deeper binding work.

## Assumptions

- Conway transaction body field `0` is the input set and field `18` is
  the reference-input set, matching the local Conway CDDL.
- The client can parse transaction CBOR as generic CBOR terms using
  `cborg`, which is already a client dependency.
- Existing CSMT replay remains the source of truth for whether each
  witnessed UTxO actually belongs to the advertised snapshot root.
