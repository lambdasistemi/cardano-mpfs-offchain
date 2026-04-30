# Feature Specification: Value persistence for the fact lookup endpoint

**Feature Branch**: `248-value-persistence`
**Created**: 2026-04-30
**Status**: Draft
**Input**: User description: "248 — `GET /tokens/:id/facts/:key` returns the wrong byte string in its `value` field. Today the endpoint returns `mkMPFHash key` (a hash of the request's key) in the `value` field of `FactResponse`, instead of the bytes the requester originally inserted. Root cause: the per-token MPF trie is content-addressed and stores only the value-hash; the raw value is discarded at insert time, and the lookup helper was wired to return any 32 bytes that satisfied the type signature. The endpoint's e2e test only asserts \"non-empty hex\" on `value` and so missed it for the endpoint's entire life (postmortem: lambdasistemi/cardano-mpfs-offchain#248). Scope: make the endpoint return the actual inserted bytes. Persist raw value bytes inside the offchain so lookup can recover them. The chain-follower's \"one block = one DB transaction\" invariant must still hold; rollbacks must restore prior raw-value state via the existing inverse-op machinery (`InvTrieInsert`/`InvTrieDelete` in `Cardano.MPFS.Indexer.Event`). Migration story for existing devnet/preprod databases included. Acceptance: the e2e for the endpoint inserts a (key, value) pair and asserts the GET response's `value` field equals the inserted bytes byte-for-byte."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fact lookup returns the inserted value (Priority: P1)

A requester submits an Insert request that pairs a key with a value. After the request is processed by the oracle, an oracle or external client calls `GET /tokens/:id/facts/:key`. The response's `value` field carries the same bytes the requester originally inserted, byte-for-byte.

**Why this priority**: This is the entire feature. The endpoint exists to serve values; today it does not. Every consumer that takes its `value` output at face value gets garbage. Without this, the endpoint is misleading by construction.

**Independent Test**: insert a (key, value) pair through the live request → process flow on a devnet, then call `GET /tokens/:id/facts/:key`, decode the response's `value` field, and assert it equals the original value bytes. No other endpoint or feature is required for this slice to be demonstrable.

**Acceptance Scenarios**:

1. **Given** a token whose trie contains the pair `(k, v)` produced by a successful Insert, **When** an HTTP client calls `GET /tokens/:id/facts/:key` with `:key = k`, **Then** the response's `value` field decodes to `v` byte-for-byte.
2. **Given** the same token where an Update has overwritten `v` with `v'`, **When** the same lookup runs, **Then** the response's `value` field decodes to `v'` (not `v`, not `mkMPFHash k`, not any other byte string).
3. **Given** a token whose trie does not contain `:key`, **When** the lookup runs, **Then** the endpoint preserves its existing absent-key response semantics (HTTP 404 today). No `value` field is fabricated.

---

### User Story 2 - Rollback restores prior values (Priority: P1)

The chain-follower processes blocks atomically: every mutation a block produces is part of one all-or-nothing storage write. When a rollback unwinds a block whose application inserted/updated/deleted entries in a token's trie, the raw value bytes a subsequent fact lookup observes must reflect the state at the new chain tip — never a value that only existed in the rolled-back block.

**Why this priority**: Without this, the endpoint becomes a window onto inconsistent state during rollbacks. Any client that polls during a re-org could read a value that no longer "exists" on chain. This is the same correctness bar the rest of the indexer already meets for trie roots, requests, and cage state; raw value persistence cannot be the one drawer that escapes it.

**Independent Test**: drive a block-by-block reorg fixture in the existing follower test suite — apply a block that inserts `(k, v)`, then roll back to before that block. After the rollback, calling `GET /tokens/:id/facts/:key` returns the same response shape as it would have at the rolled-back tip (404 if `(k, v)` was the first-ever insert; the prior value if `(k, v)` overwrote one).

**Acceptance Scenarios**:

1. **Given** a fresh token with no prior trie state, **When** block N applies an Insert of `(k, v)` and the follower then rolls back to block N-1, **Then** the lookup at `:key = k` returns the same response it would return for an absent key.
2. **Given** a token whose trie already contained `(k, v_old)` at block N-1, **When** block N applies an Update that overwrites it with `(k, v_new)` and the follower rolls back to N-1, **Then** the lookup at `:key = k` returns `v_old`, not `v_new`.
3. **Given** any block sequence ending in a rollback, **When** the follower has finished applying the rollback, **Then** the storage state of raw values is identical to what it would have been if the rolled-back blocks had never been observed.

---

### User Story 3 - Existing operators have a migration path (Priority: P2)

Operators running this offchain on devnets, preprod, or any other deployment have an existing database on disk built before this change. The release notes for the version that ships value persistence document, in plain language, what those operators need to do when upgrading.

**Why this priority**: Real deployments exist (devnet stack, preprod indexer reachable at `umpfs.plutimus.com`). Shipping a storage-shape change without operator-facing instructions creates an outage even if the code is correct. Lower priority than the fix and the rollback story because the technical risk is contained — failed startup is loud and recoverable — but it's still required for the feature to be operationally correct.

**Independent Test**: take a database snapshot from a deployment running the pre-change version, attempt to start the new version against it, and verify that the documented migration procedure (whatever it is) is sufficient to bring the deployment up to a state where User Story 1 passes.

**Acceptance Scenarios**:

1. **Given** a database directory produced by a pre-change version of the offchain, **When** an operator follows the documented upgrade procedure, **Then** the new version starts successfully and `GET /tokens/:id/facts/:key` returns correct values for every fact inserted after the upgrade completes.
2. **Given** a database directory produced by a pre-change version, **When** an operator runs the new version without following the documented procedure, **Then** the offchain refuses to start with an error message that points the operator at the migration documentation. (Failure is loud; never silent corruption.)

---

### Edge Cases

- **Empty value insert.** A requester inserts `(k, "")` (zero-byte value). The lookup returns a present response with `value = ""`. Empty is a legitimate value, distinct from "key absent."
- **Insert then delete in the same block.** The follower applies Insert `(k, v)` and then Delete `k` inside one block. After the block commits, the lookup at `k` returns the absent response, the same as if neither operation had been observed. Rollback of this block also returns to the prior state.
- **Concurrent reads during chain sync.** An HTTP client calls `GET /tokens/:id/facts/:key` while the follower is mid-block. The client sees only states that correspond to a committed block — never a partial view where the trie root has moved but the raw value bytes haven't, or vice versa.
- **Large value.** The size of an inserted value is bounded only by what the on-chain protocol accepts in a request datum. The endpoint serves any value the trie accepts an insert for; performance for very large values does not need to match small-value performance, but correctness does.
- **Database upgraded once, downgraded.** Operator upgrades, then for any reason re-runs the prior version against the upgraded database. Behaviour is undefined for now and not specified here; the only requirement is that the forward upgrade path is supported.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: When a request that inserts or updates a key-value pair is observed and processed by the chain-follower, the offchain MUST persist the requester's raw value bytes such that a subsequent fact lookup for the same key returns those exact bytes.
- **FR-002**: When a request that deletes a key is observed and processed, the offchain MUST remove the previously persisted raw value for that key from storage. A subsequent fact lookup MUST report the key as absent.
- **FR-003**: Every storage mutation introduced by FR-001 and FR-002 MUST happen within the same atomic write boundary as the existing trie-node and KV-hash mutations the follower already performs for the same block. There is no observable storage state in which the trie reflects block N while raw values reflect block N-1, or vice versa.
- **FR-004**: When the chain-follower rolls back a block that previously applied an Insert, Update, or Delete to a token's trie, the offchain MUST restore the raw-value storage to the state it had immediately before that block was applied. After the rollback completes, fact lookups MUST return the values that were correct at the new chain tip.
- **FR-005**: When the offchain starts against a database directory whose schema does not include the storage required by FR-001, the offchain MUST refuse to start and emit an error pointing the operator at the migration documentation. The offchain MUST NOT silently corrupt, partially upgrade, or otherwise mutate such a database.
- **FR-006**: The end-to-end test for `GET /tokens/:id/facts/:key` MUST exercise FR-001 by inserting a known `(key, value)` pair through the request → process flow and asserting the response's `value` field equals the inserted bytes byte-for-byte. The pre-existing `assertFactEnvelope` structural-only check (whose only test on `value` was `not . T.null`) MUST be removed.
- **FR-007**: The migration documentation produced by FR-005 MUST cover, at minimum: which deployments need migration (devnet, preprod, mainnet); what the operator must do (e.g., wipe-and-resync vs explicit migration step); and the expected downtime for each affected deployment class.

### Key Entities *(include if feature involves data)*

- **Raw fact value**: the bytes a requester originally supplied as the `value` of an Insert or Update request, scoped to a single `(token, key)` pair. Distinct from the value-hash that the merkle tree retains for proof construction. Lifecycle: created when the corresponding request is processed; replaced when an Update for the same `(token, key)` is processed; removed when a Delete is processed; restored to its prior state when any of those operations is rolled back.
- **Storage upgrade boundary**: the schema version distinction between "database produced by the pre-change offchain" and "database produced by the post-change offchain." Detection of this boundary by the offchain at startup is what gates FR-005.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of `GET /tokens/:id/facts/:key` calls following a successful Insert/Update return the requester's exact value bytes, verified by an end-to-end test that compares response bytes to inserted bytes byte-for-byte. (Today: 0%.)
- **SC-002**: Rollback fixtures exercising Insert, Update, and Delete operations across single-block and multi-block reorgs leave fact-lookup responses byte-identical to the lookup responses that would be observed if the rolled-back blocks had never been seen. Measured by automated test, no manual inspection.
- **SC-003**: Operator-facing release notes for the version that ships this change include a migration section. Reviewed by a person who has run the offchain in deployment but did not write the change.
- **SC-004**: The pre-existing `assertFactEnvelope` structural-only check (`val \`shouldSatisfy\` (not . T.null)`) no longer appears in the test suite as the sole verification of the fact endpoint's `value` field. Replaced by the byte-equality test from SC-001.

## Assumptions

- The offchain's chain-follower already implements atomic block writes (`InvTrieInsert`/`InvTrieDelete` and the rest of the inverse-op machinery in `Cardano.MPFS.Indexer.Event`). FR-003 and FR-004 reuse that mechanism rather than introducing a parallel one.
- Wipe-and-resync is acceptable for devnet and preprod deployments. Resync time on preprod is roughly 70 minutes based on the most recent measurement during the dependency-bump work; that's the order of magnitude expected, not a guarantee. Mainnet has not yet shipped this offchain, so no mainnet migration is in scope.
- The current wire shape of `GET /tokens/:id/facts/:key` (the `FactResponse` envelope, including the `value` field's existence and JSON encoding) is unchanged by this feature. A separate concurrent change (#243) replaces the wire shape; that work is out of scope here.
- "Raw value" means the byte string the requester originally placed in the request datum's value field, exactly as it arrived in the indexed transaction's CBOR. No re-encoding, no normalisation.
- Operators reading the migration documentation are technically competent with the offchain's deployment story (Docker, RocksDB on disk). The documentation does not need to teach them what RocksDB is.
