# Feature Specification: Fair Fee Model + Upstream Helpers

**Feature Branch**: `176-fair-fee-model`
**Created**: 2026-04-10
**Status**: Draft
**Input**: lambdasistemi/cardano-mpfs-offchain#188

## User Scenarios & Testing

### User Story 1 — Requester pays fair fees (Priority: P1)

A requester submits a request locking ADA. Under the old model, the locked ADA covers a worst-case `max_fee` that bundles the oracle's margin AND the transaction fee. The requester overpays because the actual tx fee is much lower than max_fee.

Under the new model, the requester locks ADA covering only the oracle's `tip` (margin). The actual transaction fee (`tx.fee`) is split across requests at settlement time. The refund is: `locked_ada - tip - tx.fee/N` where N is the number of requests in the batch.

**Why this priority**: This is the core behavioral change. The on-chain validator enforces the new conservation equation. Without this, the offchain can't build transactions that pass the updated validator.

**Independent Test**: Boot token → submit request → update → verify refund equals `locked_ada - tip - share_of_tx_fee` (not `locked_ada - max_fee`).

**Acceptance Scenarios**:

1. **Given** a request with 10 ADA locked and tip=1 ADA, **When** the oracle processes it in a batch of 2 with tx fee 0.5 ADA, **Then** the refund is 10 - 1 - 0.25 = 8.75 ADA.
2. **Given** the old validator, **When** building an update tx with the old code, **Then** it still works (backward compatibility during migration).
3. **Given** the new validator, **When** building an update tx with the new code, **Then** the on-chain conservation equation passes.

---

### User Story 2 — Adopt upstream tx helpers (Priority: P2)

The offchain has local copies of `computeScriptIntegrity`, `spendingIndex`, `placeholderExUnits`, and `evaluateAndBalance` in `Internal.hs`. These are now available upstream in `cardano-node-clients`. Replace local copies with upstream imports.

**Why this priority**: Reduces maintenance burden and ensures we benefit from upstream fixes. The upstream `evaluateAndBalance` is parameterized by `Language`, supporting future multi-language validators.

**Independent Test**: All existing unit and E2E tests pass after replacing local helpers with upstream.

**Acceptance Scenarios**:

1. **Given** the upstream helpers, **When** building any tx (boot, request, update, retract, reject, end), **Then** the tx is identical to what the local helpers produced.
2. **Given** `Internal.hs`, **Then** the removed functions no longer exist locally.

---

### User Story 3 — Field renames: max_fee → tip (Priority: P3)

Rename `stateMaxFee` → `stateTip` and `requestFee` → `requestTip` in all on-chain types, tx builders, indexer, codecs, HTTP types, and tests.

**Why this priority**: Pure rename — no behavioral change. Can be done after the fee model works, or concurrently.

**Independent Test**: All tests pass. Swagger reflects new field names.

**Acceptance Scenarios**:

1. **Given** the API, **When** booting a token, **Then** the state shows `tip` not `max_fee`.
2. **Given** the codebase, **Then** no reference to `maxFee`, `max_fee`, `requestFee` remains.

---

### Edge Cases

- What if `balanceFeeLoop` doesn't converge? The upstream implementation has a max iteration count and errors on divergence.
- What if `tx.fee / N` doesn't divide evenly? The on-chain validator uses integer division; any remainder goes to the oracle (rounding in oracle's favor).
- What about the reject tx? Reject also has a conservation equation — same `tip` model applies.
- What about existing preprod tokens with `max_fee`? They use the old validator — can't interact with the new one. New tokens must be booted with the updated blueprint.

## Requirements

### Functional Requirements

- **FR-001**: The update tx builder MUST use `balanceFeeLoop` to converge fee and refunds.
- **FR-002**: Refund outputs MUST follow the conservation equation: `sum(refunds) = sum(inputs) - tx.fee - N * tip`.
- **FR-003**: The reject tx builder MUST follow the same conservation equation (root unchanged, refunds adjusted for tx.fee).
- **FR-004**: Local helper functions (`computeScriptIntegrity`, `spendingIndex`, `placeholderExUnits`, `evaluateAndBalance`) MUST be replaced with upstream imports from `cardano-node-clients`.
- **FR-005**: On-chain type fields MUST be renamed: `stateMaxFee` → `stateTip`, `requestFee` → `requestTip`.
- **FR-006**: The HTTP API MUST reflect new field names in request/response bodies.
- **FR-007**: Swagger MUST be regenerated and pass the freshness check.
- **FR-008**: The `cardano-node-clients` dependency MUST be bumped to `a37cbd6`.
- **FR-009**: The `cardano-mpfs-onchain` dependency MUST be bumped to the commit with the fair fee model.
- **FR-010**: Every commit MUST compile. Old code is only removed after new code works.

### Key Entities

- **State.tip**: Oracle's margin per request (was `max_fee`). Set at boot time.
- **Request.tip**: Requester agrees to oracle's margin (was `fee`). Must match `state.tip` at update time.
- **Conservation equation**: `sum(refunds) = sum(request_inputs_lovelace) - tx.fee - N * tip`

## Success Criteria

### Measurable Outcomes

- **SC-001**: All 353+ unit tests pass with new fee model.
- **SC-002**: All 21+ E2E tests pass on devnet with new validator blueprint.
- **SC-003**: Preprod: boot new token with `tip`, insert, update, verify refund matches `locked - tip - fee_share`.
- **SC-004**: No local copies of upstream helpers remain in `Internal.hs`.
- **SC-005**: No references to `maxFee` or `max_fee` remain in the codebase.

## Known Gap: Minimum Locked ADA

The conservation equation `refund = locked - tip - fee/N` requires sufficient locked ADA for `refund >= minUTxO`. Currently there is no on-chain enforcement — requests can lock too little, making them unprocessable. The oracle must filter these off-chain.

Tracked in cardano-foundation/cardano-mpfs-onchain#38: add `minLocked` field to State datum and enforce in `Contribute` validator.

For now, the offchain `requestLockedAda` uses `tip + refundMinUTxO` as the minimum. The E2E tests use a low `tip` (100_000) to leave room for the fee share. Production deployments should set `tip` conservatively.

## Assumptions

- The `cardano-mpfs-onchain` PR (#37) is merged before we start implementation.
- The TxBuild DSL (cardano-node-clients fix/eval-retry branch) handles conservation-aware fee convergence.
- Existing preprod tokens (old validator) are abandoned — new tokens use the updated blueprint.
- The `tip` value is set by the oracle at boot time and is immutable (same as `max_fee` was).
- Requests that can't cover `tip + fee_share + minUTxO` are skipped by the oracle during update (not enforced on-chain yet, see #38).

## Vertical Slice Strategy

To ensure every commit compiles and git bisect works:

1. **Bump deps first** — bump `cardano-node-clients` and `cardano-mpfs-onchain` pins. Add new imports alongside old code. Everything compiles, tests pass (old validator still used).
2. **Add new helpers alongside old** — import upstream `evaluateAndBalance`, `balanceFeeLoop` etc. Don't remove old ones yet.
3. **Build new tx builders** — write new `updateTokenImpl'` using `balanceFeeLoop` + new conservation equation. Old `updateTokenImpl` still exists. Wire the new one.
4. **Rename fields** — `maxFee` → `tip` everywhere. Old code that references `maxFee` is already gone because step 3 replaced it.
5. **Delete old code** — remove local helpers from `Internal.hs`, remove old tx builder if any.
6. **Update tests + swagger** — fix all assertions, regenerate swagger.
