# Tasks: Fair Fee Model

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md)

## Slice 1: Bump dependencies

- [ ] T001 Bump `cardano-node-clients` pin in `cabal.project` to `a37cbd6`
- [ ] T002 Bump `cardano-mpfs-onchain` pin in `flake.nix` to the 001-fair-fee-model branch tip
- [ ] T003 Update `flake.lock`
- [ ] T004 Verify build compiles with new deps (`nix develop -c cabal build all -O0`)

**Checkpoint**: Everything compiles, all tests pass with old code.

---

## Slice 2: Import upstream helpers alongside local

- [ ] T005 Import `Cardano.Node.Client.Balance` functions in `Internal.hs` with qualified aliases
- [ ] T006 Import `Cardano.Node.Client.Evaluate.evaluateAndBalance` with alias
- [ ] T007 Verify build compiles (no call site changes yet)

**Checkpoint**: New imports available, old code untouched, tests pass.

---

## Slice 3: Import canonical types

- [ ] T008 Add `cardano-mpfs-onchain` Haskell library to `cardano-mpfs-offchain.cabal` build-depends
- [ ] T009 Import `Cardano.MPFS.OnChain.Types` in `Core/OnChain.hs` with aliases
- [ ] T010 Verify build compiles

**Checkpoint**: Canonical types available alongside local copies.

---

## Slice 4: New update tx builder (US1 — conservation equation)

- [ ] T011 Write `updateTokenFair` in `TxBuilder/Real/Update.hs`:
  - Uses upstream `evaluateAndBalance` (Language param)
  - Uses `balanceFeeLoop` for fee/refund convergence
  - Conservation: `refund_i = reqValue_i - tip - tx_fee/N`
  - Remainder of `tx_fee % N` goes to oracle
- [ ] T012 Wire `updateTokenFair` as the active `updateToken` in `TxBuilder/Real.hs`
- [ ] T013 Keep old `updateTokenImpl` as `updateTokenLegacy` (not called, but compiles)
- [ ] T014 Verify build compiles and unit tests pass

**Checkpoint**: Update tx uses fair fee model. Old code still present.

---

## Slice 5: New reject tx builder

- [ ] T015 Update `rejectRequestsImpl` in `TxBuilder/Real/Reject.hs`:
  - Same conservation equation as update
  - Uses `balanceFeeLoop`
- [ ] T016 Verify build compiles

**Checkpoint**: Reject tx uses fair fee model.

---

## Slice 6: Rename fields (US3)

- [ ] T017 `Core/OnChain.hs`: `stateMaxFee` → `stateTip`, `requestFee` → `requestTip`, update ToData/FromData
- [ ] T018 `TxBuilder/Config.hs`: `defaultMaxFee` → `defaultTip`
- [ ] T019 `TxBuilder/Real/Request.hs`: datum construction uses `requestTip`
- [ ] T020 `TxBuilder/Real/Boot.hs`: state datum uses `stateTip`
- [ ] T021 `Indexer/Event.hs`, `Indexer/Follower.hs`: event field refs
- [ ] T022 `Indexer/Codecs.hs`: CBOR codec field names
- [ ] T023 `HTTP/Types.hs`: API types (`max_fee` → `tip` in JSON)
- [ ] T024 All test files: update field references
- [ ] T025 Verify build compiles and all tests pass

**Checkpoint**: No `maxFee` or `max_fee` references remain.

---

## Slice 7: Delete old code (US2)

- [ ] T026 Remove `updateTokenLegacy` from `Update.hs`
- [ ] T027 Remove local `computeScriptIntegrity`, `spendingIndex`, `placeholderExUnits` from `Internal.hs`
- [ ] T028 Remove local `evaluateAndBalance` from `Internal.hs`
- [ ] T029 Switch all remaining call sites to upstream imports
- [ ] T030 Verify build compiles and all tests pass

**Checkpoint**: No local copies of upstream helpers.

---

## Slice 8: Tests + swagger + deploy

- [ ] T031 Update unit tests for new fee model assertions
- [ ] T032 Update E2E tests: verify refund = locked - tip - fee_share
- [ ] T033 Run `just update-swagger` and verify freshness check passes
- [ ] T034 Run `just ci` locally
- [ ] T035 Deploy to preprod with new blueprint
- [ ] T036 Preprod: boot new token, insert, update, verify fair refund
- [ ] T037 Create PR, update description, push, wait for CI, merge

---

## Dependencies

```
T001-T003 → T004 (bump → verify)
T005-T006 → T007 (import → verify)
T008-T009 → T010 (types → verify)
T004, T007, T010 → T011 (all imports ready → new builder)
T011-T012 → T014 (new builder → verify)
T014 → T015 (update done → reject)
T014 → T017-T025 (can rename in parallel with reject)
T025, T016 → T026-T030 (rename + reject done → delete old)
T030 → T031-T037 (all code clean → tests + deploy)
```

## Notes

- Every slice is a commit (or small group of commits). Each compiles.
- Old code is renamed with `Legacy` suffix, not deleted, until slice 7.
- The `cardano-mpfs-onchain` PR must be merged before slice 1 (we need the commit hash for the pin).
- `balanceFeeLoop` closure: `\fee -> let refunds = ... in Right (StrictSeq.fromList (stateOut : refunds))` — refunds depend on fee.
