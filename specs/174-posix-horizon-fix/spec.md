# Feature Specification: Fix PastHorizon error in tx validity range

**Feature Branch**: `fix/posix-horizon`
**Created**: 2026-04-07
**Status**: Draft
**Input**: GitHub issue #174

## User Scenarios & Testing

### User Story 1 - Update/retract transactions succeed on preprod (Priority: P1)

A token owner posts an update or retract transaction on preprod. The tx
builder computes the validity slot range using the node's hard-fork
interpreter instead of naive arithmetic, so the slot numbers are correct
across era boundaries (Byron→Shelley transition).

**Why this priority**: This is the bug — update and retract txs fail with
PastHorizon on preprod because naive `posixMs / slotLengthMs` overestimates
slot numbers by ~1.6M slots past the last known era boundary.

**Independent Test**: Submit an update tx on preprod and verify it succeeds.

**Acceptance Scenarios**:

1. **Given** a booted token on preprod, **When** the oracle submits an
   update tx, **Then** the validity range is within the known era and the
   tx is accepted.
2. **Given** a booted token on preprod, **When** the requester submits a
   retract tx, **Then** the validity range is correct and the tx is accepted.

---

### Edge Cases

- Devnet: slot conversion still works (devnet has no Byron era, but the
  hard-fork interpreter handles single-era chains)
- Mainnet: Byron era is longer (4.5M slots at 20s each), so the bug would
  be even worse there

## Requirements

### Functional Requirements

- **FR-001**: `posixMsToSlot` and `posixMsCeilSlot` MUST use the node's
  hard-fork interpreter (via Provider) instead of naive arithmetic
- **FR-002**: `systemStartPosixMs` and `slotLengthMs` MUST be removed
  from `CageConfig` (no more hardcoded era parameters)
- **FR-003**: The mpfs-offchain Provider MUST expose `posixMsToSlot` and
  `posixMsCeilSlot` fields, delegating to cardano-node-clients
- **FR-004**: Mock Provider MUST implement the time conversion fields
  for unit/E2E tests

## Success Criteria

- **SC-001**: Update and retract txs succeed on preprod without
  PastHorizon errors
- **SC-002**: Devnet E2E tests still pass

## Assumptions

- cardano-node-clients PR #33 is merged (provides upstream Provider fields)
- The pin bump is acceptable as a single change
