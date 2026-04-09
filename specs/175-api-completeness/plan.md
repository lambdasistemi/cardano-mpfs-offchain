# Implementation Plan: API Completeness — Reject and Request Update

**Branch**: `175-api-completeness` | **Date**: 2026-04-09 | **Spec**: [spec.md](spec.md)

## Summary

Add two missing API operations: (1) Reject — oracle cleans up Phase 3 requests recovering locked ADA, and (2) Request Update — submit Operation::Update(old, new) requests for value changes. Both follow established patterns in the codebase.

## Technical Context

**Language/Version**: Haskell (GHC 9.8.4)
**Primary Dependencies**: cardano-ledger, cardano-mpfs-onchain (Aiken validators), servant
**Storage**: RocksDB (persistent trie + indexer state)
**Testing**: hspec (unit + E2E with devnet)
**Target Platform**: Linux server (Docker)
**Project Type**: web-service (HTTP API for unsigned tx building)

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | Pass | Uses existing Addr, TxIn, Coin from cardano-ledger |
| II. Records of Functions | Pass | New functions added to existing TxBuilder record |
| III. Atomic Block Processing | N/A | No indexer changes |
| IV. External Signing | Pass | Returns unsigned CBOR, no keys |
| V. Aiken Compatibility | Pass | Reject redeemer encoding matches OnChain.hs Constr 4 [] |
| VI. Test Locally First | Pass | E2E on devnet, unit tests with mocks |
| VII. Nix Reproducibility | Pass | No new dependencies |

## Project Structure

### Source Code Changes

```
cardano-mpfs-offchain/lib/Cardano/MPFS/
├── TxBuilder.hs              # Add rejectRequests, requestUpdate to record
├── TxBuilder/Real.hs         # Wire new impl functions
├── TxBuilder/Real/Reject.hs  # NEW: rejectRequestsImpl
├── TxBuilder/Real/Request.hs # Add requestUpdateImpl
├── HTTP/Server.hs            # Add txRejectHandler, txUpdateRequestHandler
├── HTTP/Types.hs             # Add RejectRequest, UpdateValueRequest types
├── HTTP/API.hs               # Add TxRejectAPI, TxRequestUpdateAPI types
├── Mock/TxBuilder.hs         # Add mock stubs

cardano-mpfs-offchain/test/
├── TxBuilderSpec.hs          # Unit tests for reject + request-update

cardano-mpfs-offchain/e2e-test/
├── CageFlowSpec.hs           # E2E: request-update flow
                               # E2E: reject flow (Phase 3 timing)

scripts/
├── e2e-reject.sh             # Preprod reject script
├── e2e-request-update.sh     # Preprod update-value script
```

## Implementation Phases

### Phase 1: Request Update (P2 — simpler, follows existing pattern exactly)

**Why first**: Trivial — copy requestDeleteImpl, change OpDelete to OpUpdate, add one more ByteString parameter. Low risk, fast win.

**Files**: Request.hs, TxBuilder.hs, TxBuilder/Real.hs, Server.hs, Types.hs, API.hs, Mock/TxBuilder.hs

**Pattern**: Same as requestDeleteImpl but with `OpUpdate oldVal newVal`.

### Phase 2: Reject (P1 — more complex, new tx structure)

**Why second**: Needs a new module (Reject.hs) with a different tx structure than anything existing. The reject tx spends both the state UTxO and request UTxOs, uses two different redeemers, and has a specific validity interval constraint.

**Files**: NEW Reject.hs, TxBuilder.hs, TxBuilder/Real.hs, Server.hs, Types.hs, API.hs, Mock/TxBuilder.hs

**Key design decisions**:

1. **Which requests to reject**: The endpoint takes a token ID. The implementation queries all pending requests, filters to those in Phase 3 (using current time vs submitted_at + process_time + retract_time), and rejects all of them.

2. **Validity interval**: Lower bound must be after the latest `submitted_at + process_time + retract_time` among rejected requests (to satisfy `is_rejectable`). Upper bound unconstrained (or use a reasonable future slot).

3. **Redeemers**: State UTxO gets `Reject` (Constr 4 []). Each request UTxO gets `Contribute stateRef` (same as in update tx).

4. **Outputs**: First output is state UTxO with unchanged root. Remaining outputs are refunds to request owners.

### Phase 3: Tests

**Unit tests**: TxBuilderSpec — reject tx structure (redeemers, outputs, root unchanged), request-update tx structure.

**E2E tests**: CageFlowSpec — full flow with follower:
- Request-update: insert key → update value → verify root
- Reject: insert request → wait for Phase 3 → reject → verify refund

**Preprod scripts**: e2e-reject.sh (clean up stuck UTxOs), e2e-request-update.sh.

## Risks

- **Phase 3 timing in E2E**: The devnet uses short epochs (process_time=15s, retract_time=15s). The reject test needs to wait ~30s for Phase 3, plus time for follower to catch up. Total ~45s per reject test. May hit PastHorizon — reuse the fallback from Update.hs.
- **Reject redeemer encoding**: Must verify Constr 4 [] matches the on-chain expectation. The OnChain.hs already has `Reject` in the UpdateRedeemer type.
