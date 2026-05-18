# Feature Specification: Boot fact-provider pivot

**Feature Branch**: `261-boot-fact-provider-pivot`
**Created**: 2026-05-17
**Status**: Draft - specs phase review
**Input**: User description: "Issue #261: Ship the fact-provider pivot end-to-end for the boot transaction type. MOOG or any client calls `POST /facts/boot { address }`, verifies the facts client-side, builds the unsigned boot transaction locally with `bootCageTx`, signs it, submits it, and observes the boot transaction accepted on-chain and indexed. The legacy boot transaction endpoint is removed."

## User Scenarios & Testing

### User Story 1 - Boot through verified facts and local transaction construction (Priority: P1)

A MOOG operator boots a token without trusting the MPFS server to choose a transaction body. The operator's client requests boot facts for a wallet address, verifies the proof-bearing facts against a trusted root, builds the unsigned boot transaction locally, signs it with its own keys, submits it, and observes the boot transaction accepted on-chain and indexed.

**Why this priority**: This is the complete user-visible slice for the first fact-provider pivot child. If this story is not independently true, the boot slice has not shipped.

**Independent Test**: Against a freshly seeded devnet, MOOG calls `POST /facts/boot { address }`, verifies the returned facts, builds with `bootCageTx`, signs and submits the transaction, then confirms the chain follower indexes the resulting cage boot event.

**Acceptance Scenarios**:

1. **Given** an indexed funded wallet address and a trusted UTxO root, **When** MOOG requests `POST /facts/boot { address }`, **Then** the response contains one coherent snapshot, wallet UTxOs at the address with CSMT proofs, and protocol parameters marked unverified.
2. **Given** a valid boot facts response, **When** the client verifies the facts against the trusted root, **Then** verification succeeds and returns a verified boot facts value that can be passed to the local transaction builder.
3. **Given** verified boot facts and an acceptable wallet policy, **When** the client builds, signs, and submits the boot transaction, **Then** the transaction is accepted on-chain and the indexer records the boot event.
4. **Given** a facts response whose snapshot root, trusted root, wallet UTxO bytes, or CSMT proof has been tampered with, **When** verification runs, **Then** it rejects before transaction building or signing.
5. **Given** protocol parameters that exceed the wallet policy caps, **When** the local builder evaluates them, **Then** it rejects before signing.

---

### User Story 2 - Legacy boot transaction endpoint is gone (Priority: P1)

After the boot slice lands, the offchain server exposes `POST /facts/boot` as the only boot write path. The previous server-built unsigned boot transaction endpoint and server-side boot transaction builder are removed in the same PR, and public API documentation reflects only the new boot shape.

**Why this priority**: The parent pivot requires a hard swap for each operation. Keeping both boot paths would preserve two authorities for the same transaction shape and would violate the per-slice cutover invariant.

**Independent Test**: Search the post-slice offchain source and generated Swagger for the old boot transaction path and server-side boot transaction builder references; the search returns zero live boot write paths except `POST /facts/boot`.

**Acceptance Scenarios**:

1. **Given** the post-slice server, **When** a client calls the legacy boot transaction route, **Then** the route is not present.
2. **Given** regenerated `docs/assets/swagger.json`, **When** a reviewer inspects the boot write surface, **Then** only the facts boot endpoint is documented.
3. **Given** the post-slice source, **When** a reviewer searches the offchain server boot code path, **Then** transaction construction is no longer performed in the HTTP handler.

---

### User Story 3 - Boot verifier is proof-only (Priority: P2)

A client verifies boot facts by checking only snapshot/root consistency and CSMT inclusion for wallet UTxOs. The verifier does not inspect or validate a transaction body because the client builds that transaction locally after verification.

**Why this priority**: This is the architectural gain of the pivot. It is P2 because it supports the P1 flow and is verified mostly by tests and source inspection.

**Independent Test**: Boot verifier tests cover happy path, snapshot tamper, trusted-root mismatch, and proof tamper. A source search confirms the boot verifier surface does not import transaction grammar types.

**Acceptance Scenarios**:

1. **Given** honest boot facts and a matching trusted root, **When** `verifyBootFacts` runs, **Then** it succeeds.
2. **Given** boot facts with a mismatched trusted root, **When** `verifyBootFacts` runs, **Then** it returns a snapshot mismatch error.
3. **Given** boot facts with a tampered CSMT proof, **When** `verifyBootFacts` runs, **Then** it rejects the affected wallet UTxO proof.
4. **Given** the boot verifier modules, **When** a reviewer searches for transaction grammar imports, **Then** no such imports exist in the boot verifier surface.

---

### User Story 4 - Paired MOOG cutover is controlled (Priority: P2)

The offchain boot slice and the matching MOOG migration are prepared as a paired release-window change. The offchain PR remains draft until its implementation, documentation, proof, and paired MOOG path are ready to land without a production deploy using a cutover-window commit.

**Why this priority**: The parent epic requires offchain and MOOG to move together per operation. A server-only merge would break the current MOOG boot call path.

**Independent Test**: The offchain PR body names the paired MOOG requirement, and the child completion record lists the offchain and MOOG merge SHAs plus the cutover-window timestamps.

**Boundary update, 2026-05-18**: The paired MOOG PR is now treated as a
boundary spike, not production readiness evidence. The new MPFS on-chain
validators change enough of the state-machine surface that the remaining
cross-repo proof is tracked by
https://github.com/cardano-foundation/moog/issues/96. This PR stays
draft until that track produces a canary-backed boot proof or records an
explicit MOOG-v2 replacement decision.

**Acceptance Scenarios**:

1. **Given** the offchain PR is ready for review, **When** reviewers inspect its metadata, **Then** it names issue #261, parent #257, and the paired MOOG migration requirement.
2. **Given** the boot slice has merged in both repositories, **When** the parent issue is updated, **Then** it records both merge SHAs and confirms no production deploy used the offchain-only cutover window.

### Edge Cases

- The wallet address has no indexed UTxOs: the facts endpoint returns a deterministic client error, and no transaction is built.
- The indexer has not produced a snapshot yet: the facts endpoint returns service unavailable rather than serving unanchored facts.
- A facts read races a new block: the response still reflects one coherent indexer snapshot.
- A CSMT leaf or proof cannot be loaded from the indexer: the endpoint fails loudly as indexer corruption instead of returning unverifiable facts.
- The client receives a valid facts response but has a different trusted root: verification rejects before building.
- The protocol parameters are untrusted and maliciously expensive: wallet policy enforcement rejects before signing.

## Requirements

### Functional Requirements

- **FR-001**: The server MUST expose `POST /facts/boot` accepting a wallet address and returning boot facts for that address.
- **FR-002**: Boot facts MUST contain a snapshot, wallet UTxOs at the requested address with CSMT proofs, and protocol parameters explicitly marked unverified.
- **FR-003**: The boot facts handler MUST read the snapshot and wallet UTxOs in one atomic indexer transaction.
- **FR-004**: The server MUST NOT return an unsigned boot transaction from the facts endpoint.
- **FR-005**: The legacy server-built boot transaction route MUST be removed in the same offchain PR that introduces `POST /facts/boot`.
- **FR-006**: Public API documentation MUST show the new boot facts shape and MUST NOT show the legacy boot transaction shape.
- **FR-007**: The client library MUST expose a `BootFacts` representation and JSON instances for the boot facts wire shape.
- **FR-008**: The client library MUST expose `verifyBootFacts`, which returns a verified boot facts value only after snapshot and CSMT proof validation pass.
- **FR-009**: The verified boot facts constructor MUST NOT be exported as a bypass around verification.
- **FR-010**: The boot verifier surface MUST NOT import transaction grammar types or inspect a transaction body.
- **FR-011**: The client library MUST expose `bootCageTx` that consumes cage configuration, wallet policy, and verified boot facts to produce an unsigned boot transaction or a build error.
- **FR-012**: `bootCageTx` MUST enforce wallet policy caps before signing can occur.
- **FR-013**: `bootCageTx` MUST produce byte-identical transaction CBOR to the captured legacy boot transaction vector for equivalent inputs.
- **FR-014**: The implementation MUST capture the legacy boot transaction CBOR vector before deleting the legacy boot builder path.
- **FR-015**: Tests MUST cover boot facts verification happy path, snapshot tamper, trusted-root mismatch, and proof tamper.
- **FR-016**: End-to-end proof MUST exercise the real boot flow through `POST /facts/boot`, client verification, local build, signing, submission, on-chain acceptance, and indexing.
- **FR-017**: The offchain PR MUST stay draft until the paired MOOG-v2 boundary track produces a canary-backed boot proof or records an explicit replacement decision.

### Key Entities

- **BootFacts**: The boot facts bundle for one address, carrying the snapshot, proof-bearing wallet UTxOs, and unverified protocol parameters.
- **VerifiedBootFacts**: The verifier output proving the boot facts have passed trusted-root and CSMT proof checks.
- **TrustedRoot**: The client-supplied root used to decide whether a facts response is anchored to a trusted indexer state.
- **WalletPolicy**: Client-side caps for fee, execution prices, min-UTxO parameters, and validity-window bounds.
- **bootCageTx**: The local boot transaction builder that consumes verified facts and wallet policy.
- **Legacy boot CBOR vector**: A captured reference transaction used to prove byte equivalence between the old and new boot builders.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A boot token flow using only `POST /facts/boot` succeeds on a devnet: facts verify, the locally built transaction is accepted on-chain, and the boot event is indexed.
- **SC-002**: The boot helper byte-equivalence test passes against the captured legacy boot CBOR vector for equivalent inputs.
- **SC-003**: Boot facts verifier tests pass for happy path, snapshot tamper, trusted-root mismatch, and proof tamper.
- **SC-004**: Source search of the boot verifier surface finds zero transaction grammar imports.
- **SC-005**: Source and Swagger searches find no live legacy boot transaction endpoint after the slice.
- **SC-006**: The paired MOOG-v2 boundary track succeeds against the offchain slice before the slice is marked complete, or records an explicit decision to replace rather than port the old MOOG domain model.

## Assumptions

- This is only the boot child of parent issue #257. Non-boot endpoints remain unchanged until their own child issues.
- The merged pivot artifacts in `specs/259-fact-provider-pivot/` are the architectural source for shared facts, verifier, cage-helper, and release-window contracts.
- Existing indexer snapshot and wallet UTxO read primitives are sufficient for boot; no new state-UTxO or MPF fact read is required in this slice.
- Protocol parameters are unverified by the server response; wallet policy caps are the mitigation for malicious or extreme values.
- The paired MOOG-v2 boundary track is required for slice completion,
  but this repository's PR owns the offchain server, client library,
  docs, and verification assets.
- The boot slice does not close parent issue #257.
