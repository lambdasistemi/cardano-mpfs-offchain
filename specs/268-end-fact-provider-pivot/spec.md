# Feature Specification: End fact-provider pivot

**Feature Branch**: `268-end-fact-provider-pivot`
**Created**: 2026-05-18
**Status**: Draft
**Input**: Issue #268: replace the legacy server-built end transaction endpoint with `POST /facts/end`, `verifyEndFacts`, and `endCageTx`.

## User Scenarios & Testing

### User Story 1 - End through verified facts and local transaction construction (Priority: P1)

An MPFS operator or client retires a token without trusting the MPFS server to choose a transaction body. The client requests end facts for a token and funding address, verifies the proof-bearing facts against a trusted root, proves the per-cage request set is empty, builds the unsigned end transaction locally with `endCageTx`, signs it, submits it, and observes the burn indexed.

**Independent Test**: Against deterministic fixtures and the HTTP handler test surface, `POST /facts/end { token, address }` returns state, funding, empty-request-set completeness, and unverified protocol parameters. The client verifies those facts and builds an end transaction with the same ledger shape as the legacy builder.

**Acceptance Scenarios**:

1. **Given** an indexed token state UTxO, an empty per-cage request set, funding at the owner address, and a trusted UTxO root, **When** a client calls `POST /facts/end`, **Then** the response contains one coherent snapshot, token id, state UTxO inclusion proof, wallet UTxO inclusion proofs, empty request-set completeness proof, and unverified protocol parameters.
2. **Given** valid end facts, **When** `verifyEndFacts` runs with the client-owned cage config and trusted root, **Then** it succeeds and returns an opaque `VerifiedEndFacts` value.
3. **Given** `VerifiedEndFacts` and an acceptable wallet policy, **When** `endCageTx` runs, **Then** it returns an unsigned transaction that consumes the state UTxO, burns one token, carries spending and minting redeemers, and requires the owner signer.
4. **Given** a tampered trusted root, state proof, funding proof, or request-set completeness proof, **When** `verifyEndFacts` runs, **Then** it rejects before transaction building or signing.
5. **Given** non-empty request-set entries under the per-cage request address, **When** `verifyEndFacts` runs for the end operation, **Then** it rejects because end requires an empty request set.

### User Story 2 - Legacy end transaction endpoint is gone (Priority: P1)

After this slice lands, the offchain server exposes `POST /facts/end` as the only end write path. The previous `POST /tx/end` endpoint and server-side end transaction handler are removed in the same PR, and Swagger reflects only the new facts shape.

**Independent Test**: Source and Swagger searches find `POST /facts/end` and find no live `POST /tx/end` route.

### User Story 3 - End verifier is proof-only (Priority: P2)

The end verifier checks snapshot/root equality, state and funding CSMT inclusion, and the per-cage request-set completeness proof. It does not inspect an unsigned transaction because the client builds that transaction after verification.

**Independent Test**: Focused tests cover happy path, root mismatch, state proof tamper, funding proof tamper, completeness proof tamper, and non-empty request-set rejection. A source search confirms the new end facts verifier surface does not import transaction grammar modules.

### User Story 4 - MOOG boundary status is recorded (Priority: P2)

The boot slice established that paired MOOG work is not a normal legacy caller migration. MOOG PR #95 is boundary-spike evidence, and cardano-foundation/moog#96 owns the staged MPFS-v2 canary or replacement decision. This issue records the boundary status instead of requiring a legacy MOOG caller migration.

**Independent Test**: Issue #268 and the PR body state the MOOG boundary status and do not claim a production old-MOOG requester/oracle/agent migration.

## Requirements

- **FR-001**: The server MUST expose `POST /facts/end` accepting `token` and `address`.
- **FR-002**: `EndFacts` MUST contain `snapshot`, `token`, `state_utxo`, `wallet_utxos`, `request_set`, and `protocol_parameters`.
- **FR-003**: The end facts handler MUST read snapshot, state UTxO, wallet UTxOs, and request-set completeness inside one `runIndexerTx ctx` block.
- **FR-004**: `request_set` MUST be a `UtxoSetWitness` whose completeness proof targets the locally-derived per-cage request address prefix. For end, `entries` MUST be empty.
- **FR-005**: The facts endpoint MUST NOT return unsigned transaction CBOR.
- **FR-006**: The legacy `POST /tx/end` route and server handler MUST be removed in the same PR.
- **FR-007**: The client library MUST expose `EndFacts`, `VerifiedEndFacts`, `verifyEndFacts`, and `endCageTx`.
- **FR-008**: `VerifiedEndFacts` MUST be opaque to downstream clients.
- **FR-009**: `verifyEndFacts` MUST reject trusted-root mismatch, malformed roots, state/funding CSMT proof failures, request-set completeness failures, and non-empty request sets.
- **FR-010**: `endCageTx` MUST enforce `WalletPolicy` before returning a transaction for signing.
- **FR-011**: `docs/assets/swagger.json` MUST document `POST /facts/end` and MUST NOT document `POST /tx/end`.
- **FR-012**: The PR metadata MUST record the MOOG boundary status and MUST NOT require a legacy MOOG caller migration.

## Key Entities

- **EndFacts**: Facts bundle for one token end operation: snapshot, token id, state UTxO, wallet UTxOs, empty request-set witness, and unverified protocol parameters.
- **VerifiedEndFacts**: Opaque verifier output consumed by `endCageTx`.
- **UtxoSetWitness**: Completeness proof plus entries under a locally-derived address prefix. End requires an empty witness.
- **TrustedRoot**: Client-supplied UTxO-CSMT root.
- **WalletPolicy**: Client-side caps for protocol parameters and built transaction bounds.
- **endCageTx**: Local transaction builder for the end operation.

## Success Criteria

- **SC-001**: End facts verifier tests pass for happy path and all tamper cases.
- **SC-002**: `endCageTx` focused tests pass for burn shape, owner signer, wallet policy rejection, and non-placeholder script budgets.
- **SC-003**: HTTP tests prove `POST /facts/end` exists, returns no tx CBOR, assembles facts atomically, and removes `POST /tx/end`.
- **SC-004**: Swagger and gate searches enforce the hard swap.
- **SC-005**: Issue and PR metadata record the MOOG-v2 boundary status without a legacy caller migration claim.

## Assumptions

- Boot issue #261 is merged and establishes the local builder/verifier pattern.
- This slice handles only the end operation. Request, retract, update, and reject facts remain in their own child issues.
- Protocol parameters remain explicitly unverified; wallet policy caps are the mitigation.
- The verifier derives the request-set prefix from client-owned cage configuration and token id, not from server-provided text.
