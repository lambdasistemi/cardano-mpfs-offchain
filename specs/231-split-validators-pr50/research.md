# Phase 0 Research: Adopt split state + request validators (PR #50)

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

The plan's Phase 0 lists three research questions. Each is resolved
below; no `[NEEDS CLARIFICATION]` markers remain.

## R-001 — Per-cage request address derivation

- **Decision**: The offchain `requestAddrFromCfg cfg tokenName network`
  applies the unapplied request validator UPLC
  (`requestScriptBytes :: ShortByteString` in `CageConfig`) to two
  parameters in upstream order — first the global `statePolicyId`,
  then the `cageTokenName` — using the `applyBytesParam` blueprint
  helper, hashes the resulting applied script to a script hash, and
  turns that hash into a Cardano address with the configured
  `network`. `onChainTokenId :: TokenName -> OnChainTokenId` produces
  the value upstream uses for burn redeemers.
- **Rationale**: Identical to upstream PR #50's `mkRequestScript` /
  `requestAddrFromCfg` shape at
  [`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f).
  Constitution Principle V (Aiken Compatibility) requires
  byte-for-byte parity with the upstream cage test vectors; reusing
  the same parameterisation order and the same blueprint helpers is
  the only way to achieve it.
- **Alternatives considered**:
  - Derive the address from the pre-applied bytes shipped in the
    existing config — rejected: pre-application freezes a single cage
    and breaks the per-cage parameterisation.
  - Hash the unapplied bytes directly — rejected: a Cardano script
    address is the **applied** script's hash, not the unapplied
    template's.

## R-002 — Indexer subscription topology (N+1)

- **Decision**: `Cardano.MPFS.Indexer.Backend`'s follower set is
  parameterised by:
  - exactly one global state address (one for the deployment, derived
    from the global state validator's `OutputRef` parameter), and
  - one per-cage request address per known cage token, derived as in
    R-001 from `(statePolicyId, tokenName)`.

  When the global state policy emits a boot mint, the indexer derives
  the new cage's per-cage request address and adds it to the follower
  set in **the same atomic block batch** in which the boot is
  recorded.
- **Rationale**: Matches FR-007 / FR-008 and Story 4. Atomic addition
  preserves Constitution Principle III (Atomic Block Processing) —
  either the boot and the new follower entry both land or neither
  does. Avoids a window where a freshly booted cage is on chain but
  unreachable through HTTP.
- **Alternatives considered**:
  - One global request address with a chain-side filter — impossible:
    different `(statePolicyId, tokenName)` parameterisations produce
    different applied script hashes and so different addresses.
  - Periodic resync of the follower set — rejected: violates SC-003
    (a cage booted while the server is running must be reachable
    immediately, not after the next sync tick).

## R-003 — HTTP per-token request listing

- **Decision**: `Cardano.MPFS.HTTP.Server` resolves the per-token
  endpoint by deriving the per-cage request address (R-001) and
  reading from the indexer's per-address index. The endpoint shape
  (URL, JSON envelope, response set) is unchanged for clients;
  only the server's internal address resolution changes.
- **Rationale**: Preserves the public HTTP contract while moving the
  internal lookup to the new on-chain topology. SC-003 / Story 4
  acceptance is "the same set of pending requests that the chain
  shows at the per-cage address" — semantically the same envelope,
  resolved against a different address.
- **Alternatives considered**:
  - Expose an explicit `address` query parameter — rejected: bleeds
    on-chain layout into the public API and forces every client to
    re-derive the per-cage address.

## Open questions

None. All three research items are resolved in line with Constitution
Principle V and the upstream cage test fixtures at the pinned commit.
