# Contract: HTTP endpoints

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

This feature changes the server's internal address resolution for
the existing per-token request listing and adds **one new tx-builder
endpoint** for the owner-only Sweep flow (Story 3 / FR-005). All
other public shapes are unchanged.

| Endpoint | Public shape | Change |
|---|---|---|
| `GET /tokens/:id/requests` (existing route — the canonical "list pending requests for token T" handler in `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`) | Unchanged: same URL, same JSON envelope, same set of pending requests — exactly the requests the chain holds at the cage's per-cage request address. | Server derives the per-cage request address from `(statePolicyId, tokenName)` per Phase 0 R-001 and reads from the indexer's per-address index, instead of filtering a single global address. |
| `POST /tx/sweep` (NEW) | Request: `SweepRequest` (token id, target output reference at the per-cage request address, owner address); response: `SweepTxResponse` carrying the unsigned CBOR (same envelope shape as the existing tx-builder responses). | New route added to `TxWriteAPI` to expose the owner-only Sweep tx-builder entry point introduced in `Cardano.MPFS.TxBuilder.Real.Sweep`. The handler builds a tx that spends one UTxO at the per-cage request address with redeemer `Sweep(stateRef)` and references the state UTxO. Constitution Principle IV stands — the response is unsigned CBOR; signing remains client-side. |

## What does **not** change

- No removed public endpoints.
- No JSON envelope change for any existing route.
- The route table in `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` grows
  by exactly one entry (`TxSweepAPI`) and gets re-exported through
  `TxWriteAPI` so native Servant clients pick it up automatically.

## Acceptance hook

Story 4 / SC-003 are exercised by `HTTPLifecycleSpec` and
`IndexerSpec` against a devnet that boots one cage **before** the
server starts and a second cage **while** the server is running. Both
must be reachable through the per-token endpoint without operator
intervention or process restart.

Story 3 / SC-004 are exercised against `POST /tx/sweep` end-to-end on
the devnet: an owner-driven sweep succeeds and a non-owner-driven
sweep fails to validate, neither path consuming the state UTxO or any
legitimate request UTxO.
