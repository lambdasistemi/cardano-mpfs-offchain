# Phase 1 Data Model: Adopt split state + request validators (PR #50)

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

The on-chain entity model is described in the spec
(`spec.md` → "Key Entities"). This document captures the **offchain
type-level** changes that mirror them.

## Modified types

### `CageConfig` (`Cardano.MPFS.TxBuilder.Config`)

| Field | Change | Notes |
|---|---|---|
| `requestScriptBytes :: ShortByteString` | **add** | Unapplied request validator UPLC, supplied by the upstream library at the pinned commit. Used by `requestAddrFromCfg` to derive each cage's per-cage request address. |
| (new) `cageSeed` | **NOT added** | The offchain library picks the seed at runtime from the wallet, unlike the upstream cage tests. The per-cage request address derivation does not depend on the seed. |

### Mint shape (`Cardano.MPFS.Core.OnChain`)

| Item | Change | Notes |
|---|---|---|
| `Mint(..)` | **drop** from exports + import | Upstream PR #50 removes the `Mint` redeemer wrapper. Boot redeemer becomes `Minting onChainRef`; End/Burn redeemer becomes `Burning (onChainTokenId tid)`. |
| Hardcoded `cageScriptHash` literal | **update** | From the per-token PR #48 value to the current global state validator hash `ad0a8eeeec8b0a5ee9930be5d6ea2e80b285fc2f3e9675a13a392dd5`. |

## New helpers

Added to `Cardano.MPFS.TxBuilder.Real.Internal` (mirroring upstream
`Internal.hs` at `cf3a8bdc`):

| Helper | Signature (intent) |
|---|---|
| `mkRequestScript` | `CageConfig -> TokenName -> Script` — applies `requestScriptBytes` to `(statePolicyId, tokenName)` via the blueprint helpers. |
| `requestAddrFromCfg` | `CageConfig -> TokenName -> NetworkId -> Address` — script address of `mkRequestScript` for the given network. |
| `onChainTokenId` | `TokenName -> OnChainTokenId` — produces the value upstream uses for burn redeemers. |
| `requestScriptBytesFromCfg` | `CageConfig -> ShortByteString` — accessor used by the script-attachment paths in Update / Reject / Retract / Sweep. |

## New module

`Cardano.MPFS.TxBuilder.Real.Sweep` — owner-only entry point that
spends one UTxO at the per-cage request address with redeemer
`Sweep(stateRef)` while referencing (not consuming) the state UTxO
at the global state address. Re-exported by
`Cardano.MPFS.TxBuilder.Real`. Listed in
`cardano-mpfs-offchain.cabal`'s `exposed-modules`.

## Per-flow type-level changes

| Module | Change |
|---|---|
| `TxBuilder/Real/Boot.hs` | Drop `Mint` import; redeemer `Minting onChainRef` (no wrapper). |
| `TxBuilder/Real/End.hs` | Redeemer `Burning (onChainTokenId tid)`. |
| `TxBuilder/Real/Request.hs` | Pay request UTxO output to `requestAddrFromCfg cfg tid (network cfg)`. |
| `TxBuilder/Real/Update.hs` | `queryContext` splits into a state-UTxO query at the global state address and a per-cage request-UTxOs query at `requestAddrFromCfg cfg tid`. Attach **two** scripts as witnesses. |
| `TxBuilder/Real/Reject.hs` | Same routing and script attachment as Update. |
| `TxBuilder/Real/Retract.hs` | Request UTxO at `requestAddrFromCfg cfg tid`; state UTxO referenced (not consumed) at the global state address. Attach the request script. |
| `TxBuilder/Real/Sweep.hs` (new) | Owner-only spend at `requestAddrFromCfg cfg tid` with redeemer `Sweep(stateRef)`; state UTxO referenced. Attach the request script. |

## Indexer / HTTP touch points

| Module | Change |
|---|---|
| `Cardano.MPFS.Indexer.Backend` (and friends) | Follower set is the global state address plus N per-cage request addresses; on boot mint, derive the new cage's per-cage request address and add it to the follower set in the same atomic block batch. |
| `Cardano.MPFS.HTTP.Server` | "List requests for token T" derives the per-cage request address from `(statePolicyId, T)` and reads from the per-address index; public endpoint shape unchanged. |

## Validation rules

- All redeemer payloads, applied script hashes, and address
  derivations MUST be byte-identical to the upstream cage test
  vectors at
  [`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f)
  (Constitution Principle V).
- All TxBuilder additions return unsigned CBOR (Constitution
  Principle IV).
- All new modules cross the public API of the offchain library
  (`exposed-modules`) so call sites in `exe/` and `e2e-test/` can
  import them.
