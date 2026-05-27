# Implementation Plan: Update Fact Provider Pivot

## Architecture

Update is a Tier-3 facts endpoint. The server performs one coherent indexer
read for:

- the current verification snapshot,
- the cage state UTxO,
- all pending request UTxOs for the token's request address,
- the operator wallet UTxOs that fund fees and collateral, and
- MPF trie facts needed to fold the pending requests from the state datum's
  current root to the expected new root.

The server returns those facts plus unverified protocol parameters. It does
not build a transaction. The client verifies the facts against an
independently trusted UTxO root, applies the MPF fold locally, enforces wallet
policy, builds the unsigned transaction with `updateCageTx`, signs locally,
and submits.

The update wire shape is expected to contain:

- `snapshot`
- `token`
- `state_utxo`
- `request_utxos`
- `wallet_utxos`
- `trie_root`
- `trie_facts`
- `protocol_parameters`

The `trie_root` must match the root embedded in the state UTxO datum. Each
`TrieFact` carries `key`, optional `value`, and `mpf_proof`. Insert requests use
an exclusion fact before insertion; delete and update requests use inclusion
facts over the old value before mutation. The shared fold helper consumes the
request datum operations plus `TrieFact`s and returns the new root. The helper
is general enough for #270 reject to reuse, but this PR only exposes update
behavior.

## Shared Surfaces

- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs`
- `cardano-mpfs-client/cardano-mpfs-client.cabal`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Update.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/UpdateFactsSpec.hs`
- `cardano-mpfs-client/test/Main.hs`
- `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types/Facts.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Reads.hs`
- `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/UpdateFactsSpec.hs`
- `cardano-mpfs-offchain/test/main.hs`
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
- `docs/assets/swagger.json`
- `specs/269-facts-update-pivot/test-vectors/legacy-update.cbor`
- `gate.sh`

Shared API files are included because prior facts slices define public
Servant types and wire DTOs in `cardano-mpfs-api`; the ticket brief's owned
file list was initial, not exhaustive.

## Orchestrator / Worker Split

The ticket owner owns this plan, task list, gate lifecycle, PR metadata, MOOG
boundary note, review, and final verification. Behavior-changing Haskell
source, tests, fixtures, generated Swagger, and E2E matrix changes are owned by
visible Codex driver+navigator pairs. Each implementation slice produces one
bisect-safe commit with a `Tasks:` trailer. The ticket owner reviews the diff,
reruns focused proof or `./gate.sh`, stamps matching `tasks.md` checkboxes by
amending the worker commit, and pushes.

## Vertical Slices

1. **Wire Types And Indexer Reads**: add `TrieFact` and `UpdateFacts` DTOs,
   JSON/schema tests, server conversion helpers, and atomic read helpers for
   update's state/request/trie facts. No HTTP route is added yet.
2. **Update Facts Verifier**: add `VerifiedUpdateFacts` and
   `verifyUpdateFacts` with focused tests covering happy path, snapshot
   tamper, trusted-root mismatch, CSMT proof tamper, MPF proof tamper, and
   trie-fact value tamper. Verifier code must not import
   `Cardano.Ledger.Api.Tx`.
3. **Cage Helper And Golden Vector**: capture `legacy-update.cbor`, add
   `Cardano.MPFS.Client.Cage.Update.updateCageTx`, implement/reuse the MPF
   fold helper, and prove byte equality plus same-new-root behavior against
   the legacy server-side fold.
4. **HTTP Hard Swap And Swagger**: add `POST /facts/update`, remove
   `/tx/update` from shared API, server wiring, client wrappers, active tests,
   and Swagger, then regenerate `docs/assets/swagger.json`. Reject and sweep
   legacy routes remain untouched.
5. **Local-Cluster Matrix And Boundary Status**: add the update row to the
   facts matrix, prove live legacy-route absence, record MOOG boundary status
   in the PR body, run the focused commands and final gate, then leave the PR
   ready for the final gate drop.

## Verification

- Wire/indexer focused tests:
  `nix develop --quiet -c just unit-offchain "/update facts wire|readTrieFact|readRequestUtxosAt/"`
- Verifier focused tests:
  `nix develop --quiet -c just unit-client "/verifyUpdateFacts/"`
- Cage/golden focused tests:
  `nix develop --quiet -c just unit-client "/updateCageTx/"`
- HTTP route focused tests:
  `nix develop --quiet -c just unit-offchain "/POST /facts/update/"`
- Swagger refresh:
  `nix develop --quiet -c just update-swagger`
- Matrix proof:
  `nix develop --quiet -c just e2e-facts-matrix`
- Final PR gate:
  `./gate.sh`
- Static verifier-surface check before finalization:
  `! rg -n 'Cardano\\.Ledger\\.Api\\.Tx' cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`
- Static legacy-route check before finalization:
  `! rg -n '"/tx/update"|TxUpdateAPI|txUpdateHandler|"tx" :> "update"|updateTx' docs/assets/swagger.json cardano-mpfs-api/lib/Cardano/MPFS/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`

## Live-Boundary Smoke Decision

Unit tests prove wire shape, proof replay, MPF folding, byte equality, and
route wiring. They do not prove the live server boundary builds the same state
transition from facts and accepts it on-chain. The #278 facts API matrix is
therefore required before this PR leaves draft. Because it is slow, `gate.sh`
keeps the standard `just ci` spine plus the final legacy-route sentinel, while
the matrix command is run and recorded explicitly.

## MOOG Boundary

Update has no operation-specific MOOG canary at planning time. Boundary status
is recorded as deferred to cardano-foundation/moog#96 unless an update
canary/staged-port proof is produced before finalization. This is not a legacy
MOOG caller migration and must not be worded as production readiness.
