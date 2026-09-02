# 387 — collateral input must be disjoint from spent inputs

## Problem

Every cage transaction builder that sets a Conway collateral input reused the
wallet's largest UTxO as **both** a spent input **and** the collateral input.
The ledger tolerates this; CIP-30 wallets (Eternl and other CSL-based wallets)
do not — they present a second, greyed-out signing step and dead-end, so a
funded wallet cannot complete a write from the browser.

PR #387 fixed `boot`, `update`, `reject`, and `end` in July 2026. `retract`
was left with the defect and is consumed by the accepted `moog-v2` baseline
(`src/MPFS/Retract.hs`). Milestone contract C-BUILDER admits no exception, so
this ticket widens the same fix to `retract` and proves the guardrail the
original PR claimed but never tested.

## Requirements

- **R-1** Every cage builder that sets a collateral input builds a body whose
  collateral inputs are disjoint from its spent inputs. Applies to `boot`,
  `update`, `reject`, `end`, and `retract`. `requestInsert/Update/Delete`
  attach no scripts and set no collateral, so they satisfy R-1 vacuously.
- **R-2** A wallet that can supply only one UTxO is rejected with
  `InsufficientCollateralUtxos`, never served an unsignable transaction. An
  empty wallet keeps returning `EmptyFunding`; the two rejections stay
  distinguishable.
- **R-3** The evaluator rejects any draft whose collateral inputs are absent
  from the collateral UTxO set supplied to it, and that rejection is proved by
  a test that fails when the guard is removed.
- **R-4** Every affected builder still evaluates fully: one script, one
  redeemer, strictly positive ex-units, and a script-integrity hash matching
  the pinned protocol parameters.
- **R-5** Reserving a row for collateral does not shrink the funding available
  to a builder. Every wallet row that is not the reserved collateral row stays
  available to fund the transaction.

## Rejection behaviour

| Wallet rows | Result |
|---|---|
| 0 | `Left EmptyFunding` |
| 1 | `Left (InsufficientCollateralUtxos …)` |
| ≥ 2 | built transaction, collateral disjoint from inputs |

## Observable success

`nix run .#client-unit-tests` covers, for each of the five collateral-setting
builders, a one-row rejection and a disjointness assertion; the evaluator
guard has a named example that drives it to its rejecting branch; and full
local CI (build, unit, format, lint, dev-shell cabal build, version sync, e2e)
is green.

## Out of scope

- How the browser app supplies wallet UTxOs to the builder (it currently
  queries a single address). A wallet still needs ≥2 reachable UTxOs.
- The `cardano-mpfs-onchain` pin skew between offchain and `moog-v2`, which
  blocks the composed-build boundary proof independently of this ticket and is
  owned by M2-T101 under contract C-DEP-PINS.
