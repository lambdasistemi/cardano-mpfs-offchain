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
- `validity_upper_slot`
- `protocol_parameters`

The `trie_root` must match the root embedded in the state UTxO datum. Each
`TrieFact` carries `key`, optional `value`, and `mpf_proof`. Insert requests use
an exclusion fact before insertion; delete and update requests use inclusion
facts over the old value before mutation. The shared fold helper consumes the
request datum operations plus `TrieFact`s and returns the new root. The helper
is general enough for #270 reject to reuse, but this PR only exposes update
behavior.

Q-002 classifies update validity slot conversion as an era-schedule fact, not
an evaluator result: the server computes the upper slot from the request
deadline using its provider, the verifier checks the field's basic
consistency, and `updateCageTx` consumes the verified slot. Per-redeemer
ExUnits remain client-local evaluator output.

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
- `specs/269-facts-update-pivot/research.md`
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
3. **Cage Helper And Structural Parity**: add
   `Cardano.MPFS.Client.Cage.Update.updateCageTx`, implement/reuse the MPF
   fold helper, and prove Q-001 structural parity for fact-derived fields plus
   same-new-root behavior against the legacy server-side fold. Validity upper
   slot and per-redeemer ExUnits are provider-runtime fields and are excluded
   from structural parity.
4. **HTTP Hard Swap And Swagger**: add `POST /facts/update`, remove
   `/tx/update` from shared API, server wiring, client wrappers, active tests,
   and Swagger, then regenerate `docs/assets/swagger.json`. Reject and sweep
   legacy routes remain untouched.
4b. **Validity Slot Fact**: extend `UpdateFacts` with
   `validity_upper_slot`, have the server compute it via provider slot
   conversion, verify it, consume it in `updateCageTx`, update Swagger, and
   document the Q-001 to Q-002 boundary revision.
5. **Local-Cluster Matrix And Boundary Status**: add the update row to the
   facts matrix, prove live legacy-route absence, record MOOG boundary status
   in the PR body, run the focused commands and final gate, then leave the PR
   ready for the final gate drop.
7. **Proof-Envelope Regression Repair**: migrate the pre-existing proof-bearing
   envelope e2e coverage off the removed `/tx/update` route. The test must
   prove the facts-only `/facts/update` response is still verifiable with the
   client verifier and can build a wallet-side update transaction, while
   retaining legacy `/tx/reject` proof-envelope coverage and `/facts/end`
   facts coverage. Do not restore `/tx/update`.
8. **Request-Funding Wart Removal**: remove the e2e-only request overfunding
   scaffolding. Diagnostic evidence without the helper shows
   `reqValue=2831830`, `tipAmount=1000000`, initial `refundCoin=1831830`,
   `refMin=849070`, then update fee convergence attempts a negative refund
   coin. A-S8 selects the bounded request-side fix: derive the request
   `feeBuffer` from the live Conway protocol parameters using a conservative
   worst-case update transaction envelope, keep both request builders aligned,
   keep update refund output shape unchanged, then drop the test-only ADA-shift
   helpers from `ProofsSpec` and `FactsMatrixSpec`, with request-builder unit
   expectations updated to assert the bounded funding behavior directly.

## Verification

- Wire/indexer focused tests:
  `nix develop --quiet -c just unit-offchain "/update facts wire|readTrieFact|readRequestUtxosAt/"`
- Verifier focused tests:
  `nix develop --quiet -c just unit-client "/verifyUpdateFacts/"`
- Cage/structural-parity focused tests:
  `nix develop --quiet -c just unit-client "/updateCageTx/"`
- HTTP route focused tests:
  `nix develop --quiet -c just unit-offchain "/POST /facts/update/"`
- Validity slot focused tests:
  `nix develop --quiet -c just unit-client "/verifyUpdateFacts|updateCageTx/"`
  and
  `nix develop --quiet -c just unit-offchain "/POST /facts/update|update facts wire/"`
- Swagger refresh:
  `nix develop --quiet -c just update-swagger`
- Matrix proof:
  `nix develop --quiet -c just e2e-facts-matrix`
- Proof-envelope regression proof:
  `nix develop --quiet -c just e2e "read and write envelopes"`
- Request-funding wart proof:
  `nix develop --quiet -c just unit-client`, then focused e2e runs
  `nix develop --quiet -c just e2e "read and write envelopes"` and
  `nix develop --quiet -c just e2e "facts API coverage matrix"`
- Final PR gate:
  `./gate.sh`
- Static verifier-surface check before finalization:
  `! rg -n 'Cardano\\.Ledger\\.Api\\.Tx' cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`
- Static legacy-route check before finalization:
  `! rg -n '"/tx/update"|TxUpdateAPI|txUpdateHandler|"tx" :> "update"|updateTx' docs/assets/swagger.json cardano-mpfs-api/lib/Cardano/MPFS/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`

## Live-Boundary Smoke Decision

Unit tests prove wire shape, proof replay, MPF folding, structural parity, and
route wiring. They do not prove the live server boundary builds the same state
transition from facts and accepts it on-chain. The #278 facts API matrix is
therefore required before this PR leaves draft. Because it is slow, `gate.sh`
keeps the standard `just ci` spine plus the final legacy-route sentinel, while
the matrix command is run and recorded explicitly.

Q-002 came from this live-boundary smoke: with unmodified update facts,
`updateCageTx` treated POSIX milliseconds as `SlotNo`, so the tx could not
submit. The fix is a new slot fact, not a matrix workaround.

## MOOG Boundary

Update has no operation-specific MOOG canary at planning time. Boundary status
is recorded as deferred to cardano-foundation/moog#96 unless an update
canary/staged-port proof is produced before finalization. This is not a legacy
MOOG caller migration and must not be worded as production readiness.
