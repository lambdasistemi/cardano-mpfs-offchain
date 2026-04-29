# Quickstart: Adopt split state + request validators (PR #50)

**Feature**: 231-split-validators-pr50
**Date**: 2026-04-28

End-to-end walkthrough an operator runs on the local devnet after the
implementation lands. Tracks the four user stories in `spec.md` and
the success criteria.

## Prerequisites

- Worktree at `/code/cardano-mpfs-offchain-onchain-bump-50` on branch
  `231-split-validators-pr50`.
- Upstream pin already in place at `cf3a8bdc` (`cabal.project`,
  `flake.nix`, `flake.lock`, `Core/Blueprint.hs`).
- Nix flakes enabled. The test/style commands below run through flake
  apps, so they do not require entering `nix develop`.

## Build and full E2E

```bash
just build          # full build of the cabal project under the pinned upstream
just unit           # nix run .#unit-tests
just unit-offchain  # same unit-test flake app
nix run .#e2e-tests # direct flake app for the E2E suite
just e2e            # same app; CageSpec, CageFlowSpec, ChainSyncSpec,
                    #   HTTPLifecycleSpec, IndexerSpec, ProofsSpec
just ci             # nix build package/check → unit → unit-offchain → format-check → hlint
                    #   (does NOT include e2e — see GATE below)

# The full GATE for this stack — required before every push:
just ci && just e2e
```

Expected: every suite passes (SC-001).

## Story 1 — Requester routing

1. Boot a cage on the devnet using the existing `exe/` boot tool.
2. Submit an `Insert` request through the offchain server.
3. Inspect the resulting transaction's outputs: the request UTxO
   must be paid to `requestAddrFromCfg cfg tid (network cfg)`, never
   to the global state address (FR-002).
4. Drive `Retract` for the same request: the spend is at the
   per-cage request address; the state UTxO is referenced (not
   consumed) at the global state address (FR-004).

## Story 2 — Oracle two-validator transaction

1. With at least one pending request from Story 1, drive `Update` as
   the cage owner.
2. Inspect the transaction's witnesses: both the global state
   validator and the per-cage request validator scripts are attached
   (FR-003).
3. Inspect the redeemers: `Modify` for the state UTxO,
   `Contribute(stateRef)` for each consumed request UTxO.
4. Repeat with `Reject` and confirm the same two-validator shape.

## Story 3 — Owner sweep

1. From a wallet **other than the cage owner**, pay a UTxO with a
   junk datum to the cage's per-cage request address.
2. Confirm the indexer does not list it as a pending request.
3. As the cage owner, drive the new `Sweep` entry point.
4. Inspect the transaction: the offending UTxO is consumed with
   redeemer `Sweep(stateRef)`; the state UTxO is referenced (not
   consumed); legitimate pending requests at the same address are
   not consumed (Story 3 / SC-004).
5. Repeat the sweep attempt from a non-owner wallet — it must fail
   to validate.

## Story 4 — Indexer N+1 + dynamic boot

1. Stop the offchain server.
2. Boot one cage `T1` on the devnet.
3. Start the offchain server.
4. `GET /tokens/T1/requests` returns the pending requests at `T1`'s per-cage
   request address (FR-009).
5. With the server still running, boot a second cage `T2`.
6. Submit a request against `T2`.
7. `GET /tokens/T2/requests` returns the new request without restarting the
   server (FR-008, SC-003).

## Verifying byte-for-byte parity (SC-005)

The unit suites `OnChainSpec` and `TxBuilderSpec` carry hash literals
and redeemer round-trips that match the upstream cage test vectors at
`cf3a8bdc`. A passing `just unit-offchain` is the local proof of
SC-005; a divergence shows up as a failing hash literal.
