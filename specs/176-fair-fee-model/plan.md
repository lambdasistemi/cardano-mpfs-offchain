# Implementation Plan: Fair Fee Model

**Branch**: `176-fair-fee-model` | **Date**: 2026-04-10 | **Spec**: [spec.md](spec.md)

## Summary

Adopt the fair fee model from cardano-mpfs-onchain, replace local helpers with upstream from cardano-node-clients, rename max_fee→tip. Vertical slices: every commit compiles, old code lives alongside new until fully replaced.

## Technical Context

**Language**: Haskell (GHC 9.8.4)
**Key deps**: cardano-node-clients (bump to a37cbd6), cardano-mpfs-onchain (bump to 001-fair-fee-model branch tip)
**Testing**: hspec unit + E2E on devnet
**Constraint**: Every commit must compile. Old code only deleted after new code passes all tests.

## Constitution Check

All principles pass. No new dependencies outside the flake. Records of functions pattern preserved. Atomic block processing unchanged.

## Implementation Phases (Vertical Slices)

### Slice 1: Bump dependencies

Bump `cardano-node-clients` and `cardano-mpfs-onchain` pins in `cabal.project` and `flake.nix`. Import new modules alongside existing code. Everything compiles, all tests pass (still using old code paths).

**Files**: `cabal.project`, `flake.nix`, `flake.lock`

### Slice 2: Import upstream helpers alongside local

Add imports from `Cardano.Node.Client.Balance` and `Cardano.Node.Client.Evaluate` in `Internal.hs`. Alias them to avoid name clashes. Don't change any call sites yet.

**Files**: `TxBuilder/Real/Internal.hs`

### Slice 3: Import canonical types from onchain library

Add `cardano-mpfs-onchain` Haskell library as a dependency. Import `Cardano.MPFS.OnChain.Types` alongside the local `Core.OnChain`. Map between old and new field names where needed.

**Files**: `cardano-mpfs-offchain.cabal`, `Core/OnChain.hs`

### Slice 4: New update tx builder with balanceFeeLoop

Write `updateTokenFair` alongside the existing `updateTokenImpl`. Uses:
- Upstream `evaluateAndBalance` (with Language param)
- `balanceFeeLoop` for fee/refund convergence
- Conservation equation: `refund_i = reqValue_i - tip - tx_fee/N`
- New field names (`tip` not `maxFee`)

Wire `updateTokenFair` as the active implementation. Old `updateTokenImpl` still exists.

**Files**: `TxBuilder/Real/Update.hs`, `TxBuilder/Real.hs`

### Slice 5: New reject tx builder with balanceFeeLoop

Same pattern as slice 4 but for reject. Conservation equation is identical.

**Files**: `TxBuilder/Real/Reject.hs`, `TxBuilder/Real.hs`

### Slice 6: Rename fields

`stateMaxFee` → `stateTip`, `requestFee` → `requestTip` in:
- `Core/OnChain.hs` (types + serialization)
- `TxBuilder/Real/Request.hs` (datum construction)
- `TxBuilder/Real/Boot.hs` (state datum)
- `TxBuilder/Config.hs` (`defaultMaxFee` → `defaultTip`)
- `Indexer/Event.hs`, `Indexer/Follower.hs` (event processing)
- `Indexer/Codecs.hs` (CBOR codecs)
- `HTTP/Types.hs` (API request/response types)
- All tests

### Slice 7: Delete old code

Remove `updateTokenImpl` (replaced by `updateTokenFair`), local copies of upstream helpers from `Internal.hs`.

**Files**: `TxBuilder/Real/Internal.hs`, `TxBuilder/Real/Update.hs`

### Slice 8: Tests + swagger + preprod

- Fix all unit test assertions for new field names and fee model
- Verify E2E tests pass with new validator blueprint
- Regenerate swagger
- Deploy to preprod, test full flow

## CI Evidence (Slice 1)

After bumping deps (cardano-node-clients to a37cbd6, cardano-mpfs-onchain to 001-fair-fee-model):

- **Build**: compiles. `posixMsToSlot`/`posixMsCeilSlot` moved to local `NodeClient.hs` since the upstream refactored them out of `Provider`.
- **Unit tests**: 2 failures — `script hash matches` and `mints exactly one token`. Expected: the new blueprint produces a different script hash (`e462b38f...` raw, `fd8ad19e...` after parameter application). Test fixtures hardcode the old hash.
- **E2E tests**: all cage/update/reject tests fail with `evaluateAndBalance: script eval failed`. The offchain builds datums with old field layout (`max_fee`, `fee`), but the new validator expects `tip` fields. The blueprint IS loaded correctly (verified: `$MPFS_BLUEPRINT` points to new blueprint, hash confirmed).
- **Key insight**: the failures are NOT from loading the wrong blueprint. They're from the offchain constructing datums with the old `OnChainTokenState`/`OnChainRequest` field layout. The new validator's `expect StateDatum(State{...})` pattern-match fails because the datum has 6 fields (with `max_fee`) but the new State type expects 5 fields (with `tip`).

This confirms slices 4-6 (new tx builders + field renames) are required to make tests pass. Slices 2-3 (import upstream helpers/types) are safe intermediate steps.

## Risks

- **balanceFeeLoop convergence**: The refund calculation involves integer division. If the function `fee → outputs` isn't monotonic, the loop may not converge. Upstream has a max-iteration guard (returns error after N rounds).
- **Blueprint mismatch**: The new validator must match the blueprint bundled in the docker image. If the onchain PR isn't merged, we can't deploy.
- **Existing preprod tokens**: They use the old validator. They'll stop working with the new offchain. We need to boot new tokens.
