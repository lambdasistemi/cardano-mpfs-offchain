# Contract: HTTP endpoints

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

This feature does **not** change any public HTTP shape. Only the
server's internal address resolution shifts.

| Endpoint | Public shape | Internal resolution change |
|---|---|---|
| Per-token request listing (existing route — `GET /requests/{token}` or whichever path the current `Cardano.MPFS.HTTP.Server` already exposes for "list pending requests for token T") | Unchanged: same URL, same JSON envelope, same set of pending requests — exactly the requests the chain holds at the cage's per-cage request address. | Server derives the per-cage request address from `(statePolicyId, token)` per Phase 0 R-001 and reads from the indexer's per-address index, instead of filtering a single global address. |

## What does **not** change

- No new public endpoints.
- No removed public endpoints.
- No JSON envelope or proof-bundle change (Sweep is a TxBuilder entry
  point only and does not require its own public route in this
  feature; if the existing server already exposes a generic
  "build tx for redeemer" route, Sweep can ride along on that
  surface — that decision is captured at task time, not as a public
  contract change here).
- Constitution Principle IV stands: every endpoint that returns a
  transaction returns unsigned CBOR.

## Acceptance hook

Story 4 / SC-003 are exercised by `HTTPLifecycleSpec` and
`IndexerSpec` against a devnet that boots one cage **before** the
server starts and a second cage **while** the server is running. Both
must be reachable through the per-token endpoint without operator
intervention or process restart.
