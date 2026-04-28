# Contract: Transaction shapes (per affected flow)

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

This document is the **byte-for-byte arbiter** required by Constitution
Principle V. Every row below MUST match the upstream cage test vectors
at
[`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f).
A divergence is a critical bug.

The "Inputs" and "Outputs" columns track the cage-relevant inputs and
outputs only — wallet UTxOs supplying ADA and the change output are
omitted for clarity. "Referenced" means a reference input (read-only,
not consumed).

| Flow | Cage-relevant inputs | Cage-relevant outputs | Redeemers (per consumed input) | Attached scripts |
|---|---|---|---|---|
| Boot | seed UTxO from wallet | state UTxO at **global state address**; cage token paid forward | mint policy: `Minting onChainRef` | mint policy + global state validator |
| Request{Insert,Delete,Update} | (none on cage side) | request UTxO at **per-cage request address** with the request datum | (none on cage side; only wallet inputs) | (none — paying to script address only) |
| Retract | request UTxO at **per-cage request address**; state UTxO at **global state address** **referenced** | requester refund | per-cage request validator: retract redeemer | per-cage request validator |
| Update | state UTxO at **global state address**; one or more request UTxOs at **per-cage request address** | new state UTxO at **global state address** with advanced datum; per-request payouts as required | global state validator: `Modify`; per request UTxO: `Contribute(stateRef)` | global state validator + per-cage request validator |
| Reject | as Update | refunds to the originating requesters | as Update (`Modify` + `Contribute(stateRef)`) | as Update |
| Sweep (owner-only, NEW) | one non-legitimate UTxO at **per-cage request address**; state UTxO at **global state address** **referenced** | sweep payout to the owner | per-cage request validator: `Sweep(stateRef)` | per-cage request validator |
| End/Burn | state UTxO at **global state address**; cage token UTxO | (none — cage retired) | mint policy: `Burning (onChainTokenId tid)` | mint policy + global state validator |

## Cross-flow invariants

- The **global state address** is one address per deployment (derived
  from the global state validator parametrised only by an
  `OutputRef`). It hosts every cage's state UTxO.
- The **per-cage request address** is one address per cage (derived
  from the per-cage request validator parametrised by
  `(statePolicyId, cageTokenName)` per Phase 0 R-001). It hosts that
  cage's pending request UTxOs.
- Update, Reject, Retract, and Sweep all reference the cage's state
  UTxO (consumed only by Update / Reject / End-Burn; referenced by
  Retract and Sweep).
- Sweep's validator path reads the owner key hash from the
  **referenced** state datum; this is what makes Sweep owner-only
  even though the per-cage request address is publicly known.

## Acceptance hook

Every row above is exercised by at least one E2E scenario in the
existing `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/*` suite, on
the local devnet, after the implementation lands (SC-001, SC-002).
SC-005 — byte-for-byte parity with the upstream cage test vectors —
is enforced by the unit tests in
`cardano-mpfs-offchain/test/Cardano/MPFS/{OnChainSpec,TxBuilderSpec}.hs`,
which carry the canonical hash literals.
