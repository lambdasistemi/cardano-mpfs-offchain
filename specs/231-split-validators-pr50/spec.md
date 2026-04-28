# Feature Specification: Adopt split state + request validators (upstream PR #50)

**Feature Branch**: `231-split-validators-pr50`
**Created**: 2026-04-28
**Status**: Draft
**Input**: Adopt upstream
[cardano-mpfs-onchain PR #50](https://github.com/cardano-foundation/cardano-mpfs-onchain/pull/50)
(tip
[`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f))
in this offchain repo. The on-chain redesign splits the previous
single-validator cage into two validators — a global state validator
(parametrised only by an output reference) and a per-cage request
validator (parametrised by `(statePolicyId, cageTokenName)`) — which
changes user-visible behaviour for the Requester role, the Oracle role,
and the indexer/server topology.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Requester submits and retracts requests at a per-cage address (Priority: P1)

A requester acting against a known cage submits an `insert`, `delete`, or
`update` request. The transaction the offchain service builds for that
requester pays the request UTxO to a per-cage request address derived
deterministically from the global state policy id and the cage's token
name — not to the global state address. When the same requester later
retracts a still-pending request, the spend happens at that same per-cage
address, while the cage's state UTxO is referenced (not consumed) at the
global state address.

**Why this priority**: This is the customer-facing surface of MPFS. If
the address routing is wrong, no third party can submit a request
correctly; nothing else in the system matters until this works.

**Independent Test**: Drive the offchain transaction builder through its
existing E2E harness on the local devnet. For a freshly booted cage,
build a `Request{Insert,Delete,Update}` tx and observe that the request
UTxO is paid to `requestAddrFromCfg cfg tokenName network`, not to the
global state address. Build a `Retract` tx for that same request and
observe that it spends at the per-cage request address and references
the state UTxO at the global state address.

**Acceptance Scenarios**:

1. **Given** a cage that was booted on the devnet, **When** a requester
   builds an insert/delete/update request transaction through the
   offchain service, **Then** the request UTxO is paid to the per-cage
   request address derived from `(statePolicyId, cageTokenName)` and
   never to the global state address.
2. **Given** a pending request submitted as in (1), **When** the same
   requester builds a retract transaction, **Then** the transaction
   spends the request UTxO at the per-cage request address and
   references the state UTxO at the global state address as a read-only
   reference input.
3. **Given** an offchain client that lists pending requests for a known
   cage by querying the offchain service, **When** the response is
   returned, **Then** every entry is anchored at the per-cage request
   address and none at the global state address.

---

### User Story 2 - Oracle processes pending requests across two validators in one transaction (Priority: P1)

The cage owner (oracle) periodically processes pending requests against
its cage. The offchain service must build a transaction that, in one
step, spends the cage's state UTxO at the global state address with the
`Modify` redeemer and spends one or more request UTxOs at the per-cage
request address with the `Contribute(stateRef)` redeemer. The same shape
is used both for the "accept and apply" (Update) flow and for the
"reject and refund" (Reject) flow. Both validators must be attached as
witnesses to the same transaction.

**Why this priority**: Oracle progress is the second customer-facing
surface — without it, requests pile up at the per-cage address and the
trie never advances. This is what makes the system usable end-to-end.

**Independent Test**: Drive the offchain transaction builder through the
existing E2E harness on the devnet: boot a cage, submit one or more
requests via Story 1, then drive the oracle through Update (or Reject)
and observe that the resulting transaction validates on the devnet and
consumes the request UTxOs while advancing the state UTxO's datum.

**Acceptance Scenarios**:

1. **Given** at least one pending request at the per-cage request
   address and a state UTxO at the global state address, **When** the
   oracle builds an Update transaction, **Then** the transaction spends
   the state UTxO with redeemer `Modify`, spends each request UTxO with
   redeemer `Contribute(stateRef)`, attaches both validator scripts as
   witnesses, and submits successfully on the devnet.
2. **Given** at least one pending request that should be rejected,
   **When** the oracle builds a Reject transaction, **Then** the
   transaction follows the same two-validator shape as Update,
   refunding the requester and producing a state-validating tx on the
   devnet.
3. **Given** an end-of-life cage with no pending requests, **When** the
   owner builds an End/Burn transaction, **Then** the burn redeemer
   carries the `OnChainTokenId` being burned (matching the upstream
   redeemer shape).

---

### User Story 3 - Owner sweeps non-legitimate UTxOs at the cage's request address (Priority: P2)

Because the per-cage request address is a public address derived from
on-chain parameters, anyone may pay arbitrary UTxOs to it. The cage
owner needs a way to clean up such non-legitimate UTxOs without
disturbing the cage's state or its legitimate pending requests. The
offchain service must expose an owner-only "sweep" flow that spends the
targeted UTxO at the per-cage request address with redeemer
`Sweep(stateRef)`, references the cage's state UTxO at the global state
address (so the validator can read the owner key hash from the state
datum), and does not consume the state UTxO.

**Why this priority**: This is a new user story introduced by the
on-chain split. It is P2 rather than P1 because legitimate flows do not
depend on it, but the system is operationally incomplete without it —
owners would otherwise have no way to clear noise from their cage's
request address.

**Independent Test**: On the devnet, manually pay a UTxO to a known
cage's per-cage request address from a wallet other than the owner's
and confirm that no oracle/requester flow can clear it. Then drive the
new owner sweep flow and observe that the offending UTxO is consumed
while the state UTxO and any legitimate pending requests are untouched.

**Acceptance Scenarios**:

1. **Given** a UTxO at a cage's per-cage request address that does not
   match the legitimate request datum shape, **When** the owner calls
   the sweep flow, **Then** the offending UTxO is spent with redeemer
   `Sweep(stateRef)`, the state UTxO is referenced but not consumed,
   and the transaction validates on the devnet.
2. **Given** the same scenario, **When** a non-owner attempts the sweep
   flow, **Then** the transaction fails to validate (the on-chain check
   reads the owner key hash from the referenced state datum).
3. **Given** legitimate pending requests at the same per-cage address,
   **When** an owner sweep transaction is built and submitted, **Then**
   those legitimate request UTxOs are not consumed by the sweep.

---

### User Story 4 - Indexer and server follow N+1 addresses and resolve per-token requests correctly (Priority: P1)

The offchain indexer and HTTP server must follow the new on-chain
topology: one global state policy id (and its associated state address)
for boot mints and state UTxO updates, plus one per-cage request
address for every cage the indexer knows about. HTTP endpoints that
list "requests for token T" must derive the per-cage request address
from `(statePolicyId, T)` and query that address, rather than filtering
a single global address. New cages discovered via the global state
policy must become routable through their per-cage request address
without operator intervention or process restart.

**Why this priority**: The HTTP surface is how every other actor —
requester, oracle, sweeper — reads the world. If "list requests for T"
silently returns the empty set after the validator split, every
downstream flow breaks even though the underlying chain is correct.

**Independent Test**: Run the offchain server against the devnet through
the existing `HTTPLifecycleSpec` and `IndexerSpec` E2E suites, with a
cage booted before server start and a second cage booted while the
server is running. Observe that listing requests for either cage's
token returns the same set of pending requests the chain shows at the
per-cage address, with no manual indexer restart between boot and first
listing.

**Acceptance Scenarios**:

1. **Given** a cage that was booted before the offchain server started
   and at least one pending request, **When** an HTTP client calls
   "list requests for token T", **Then** the response contains exactly
   the pending requests at that cage's per-cage request address.
2. **Given** a cage that is booted while the offchain server is
   running, **When** a request is submitted against the new cage,
   **Then** "list requests for token T" returns the new request
   without any manual indexer restart.
3. **Given** the indexer is following the global state policy and N
   known cages, **When** the operator inspects which addresses the
   indexer is subscribed to, **Then** there are exactly N+1 addresses
   (1 global state address + N per-cage request addresses).

---

### Edge Cases

- A non-owner pays a well-formed-looking UTxO to a cage's per-cage
  request address but with a datum that is invalid for any flow: it
  must be sweepable by the owner (Story 3) and must not be treated as a
  pending request by the indexer (Story 4).
- A pending request UTxO is created at one cage's per-cage request
  address; the operator subsequently boots a different cage with a
  different token name. The new cage's per-cage address must resolve
  independently of the first, with no cross-contamination of the
  request listings.
- The state UTxO is reorged out at the same time the oracle is building
  an Update tx: the tx must fail to validate (because the referenced
  `stateRef` is no longer current) rather than partially apply requests
  against a stale state datum.
- An End/Burn transaction is built for a cage that still has pending
  requests at its per-cage request address: this spec does not require
  an offchain pre-check for this — the offchain layer inherits the
  on-chain validator's behaviour from the upstream cage test vectors.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The offchain service MUST derive the per-cage request
  address for every request-bearing transaction (insert, delete,
  update, retract, oracle update, oracle reject, owner sweep) from the
  global state policy id and the cage's token name, matching the
  derivation used by the upstream on-chain validator.
- **FR-002**: The offchain transaction builder MUST pay request UTxOs
  (insert, delete, update) to the per-cage request address and never
  to the global state address.
- **FR-003**: The offchain transaction builder MUST attach both the
  global state validator script and the per-cage request validator
  script to oracle Update and Reject transactions, with redeemers
  `Modify` for the state UTxO and `Contribute(stateRef)` for each
  request UTxO consumed.
- **FR-004**: The offchain transaction builder for Retract MUST attach
  the per-cage request validator script and MUST reference (not
  consume) the state UTxO at the global state address.
- **FR-005**: The offchain service MUST expose an owner-only Sweep flow
  that spends a UTxO at the per-cage request address with redeemer
  `Sweep(stateRef)` while referencing the state UTxO.
- **FR-006**: The offchain transaction builder for End/Burn MUST carry
  the `OnChainTokenId` being burned in the burn redeemer payload,
  matching the upstream redeemer shape.
- **FR-007**: The offchain indexer MUST follow exactly N+1 chain
  addresses at any moment: one global state address plus one per-cage
  request address for each known cage token.
- **FR-008**: The offchain indexer MUST detect new cage boots from the
  global state policy and automatically begin following the
  newly-derived per-cage request address without requiring a process
  restart or operator action.
- **FR-009**: HTTP endpoints that list pending requests for a token
  MUST derive the per-cage request address from
  `(statePolicyId, tokenName)` and query that address, returning the
  same set of pending requests that the chain holds there.
- **FR-010**: The offchain configuration value carried by the
  transaction builder MUST contain the unapplied request validator
  bytecode supplied by the upstream library, so that per-cage
  addresses can be derived deterministically at build time.
- **FR-011**: All transaction-shape decisions affected by this feature
  (redeemer payloads, address routing, attached scripts) MUST match
  the upstream cage test vectors byte-for-byte; any divergence is a
  critical bug per Constitution Principle V.

### Key Entities

- **Global state validator**: One on-chain validator parametrised only
  by an `OutputRef`. There is exactly one global state address per
  deployment. Holds every cage's state UTxO. Spent with redeemer
  `Modify` during oracle progress; referenced (not spent) by Retract
  and Sweep.
- **Per-cage request validator**: An on-chain validator parametrised by
  `(statePolicyId, cageTokenName)`. There is one request address per
  cage. Holds each cage's pending request UTxOs. Spent with redeemer
  `Contribute(stateRef)` during oracle progress, with the retract
  redeemer by the originating requester, and with `Sweep(stateRef)` by
  the owner for non-legitimate UTxOs.
- **Cage token**: A native asset whose policy id is the global state
  policy and whose token name uniquely identifies one cage. Used both
  as the cage's identity and as the parameter that determines the
  cage's per-cage request address.
- **Pending request UTxO**: A UTxO at a cage's per-cage request address
  carrying a request datum (insert/delete/update). Created by Story 1,
  consumed by Story 2 or by retract.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of E2E specs (`CageSpec`, `CageFlowSpec`,
  `ChainSyncSpec`, `HTTPLifecycleSpec`, `IndexerSpec`, `ProofsSpec`)
  pass on the local devnet against the pinned upstream commit, with no
  test marked pending or skipped.
- **SC-002**: After this feature lands, every documented user flow
  (boot, request insert/delete/update, retract, oracle update, oracle
  reject, owner sweep, end/burn) has at least one passing E2E scenario
  that exercises the split-validator transaction shape.
- **SC-003**: For any cage booted on the devnet — whether before the
  offchain server starts or while it is running — an HTTP client can
  list that cage's pending requests within one block of the request
  landing on chain, without operator intervention.
- **SC-004**: A non-owner attempt at the Sweep flow fails to validate
  on the devnet in 100% of trials; an owner-driven Sweep succeeds on a
  non-legitimate UTxO without consuming the state UTxO or any
  legitimate request UTxO in 100% of trials.
- **SC-005**: The set of redeemer payloads produced by the offchain
  transaction builders for boot, request, retract, update, reject,
  sweep, and end/burn matches the upstream cage test vectors exactly
  (byte-for-byte) at the pinned commit.

## Assumptions

- The upstream pin is fixed at
  [`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f);
  this spec does not cover any later upstream change to PR #50.
- Cosmetic renaming from `cage*` identifiers to `state*` identifiers in
  offchain Haskell modules is **out of scope**; the on-chain split is
  captured here, the rename is a separate concern.
- Migration of any cage previously booted under PR #48
  (single-validator) on any live network is **out of scope**; this
  branch targets only the upstream PR #50 tip and the local devnet
  used by E2E tests.
- The offchain library, unlike the upstream cage tests, picks the cage
  seed at runtime from the wallet rather than threading a fixed seed
  through configuration; the per-cage request address derivation does
  not depend on the seed.
- Constitution Principle V (Aiken Compatibility) governs this work;
  the Constitution Check on `plan.md` MUST explicitly cite the
  upstream cage test vectors at the pinned commit as the byte-for-byte
  arbiter for every new redeemer shape and address derivation
  introduced here.
