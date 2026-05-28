# Implementation Plan: Reject Fact Provider Pivot

## Architecture

Reject is a Tier-3 facts endpoint. The server performs one coherent
indexer read for:

- the current verification snapshot,
- the cage state UTxO,
- the named request UTxO selected for rejection,
- the operator wallet UTxOs that fund fees and collateral,
- MPF trie facts needed to validate the reject step against the state
  datum's current root, and
- the provider-derived validity upper slot for the reject deadline.

The server returns those facts plus unverified protocol parameters. It
does not build a transaction. The client verifies the facts against an
independently trusted UTxO root, replays the CSMT and MPF proofs,
enforces wallet policy, builds the unsigned transaction with
`rejectCageTx`, signs locally, and submits.

The reject wire shape is expected to contain:

- `snapshot`
- `token`
- `state_utxo`
- `request_utxo`
- `wallet_utxos`
- `trie_root`
- `trie_facts`
- `validity_upper_slot`
- `protocol_parameters`

The `trie_root` must match the root embedded in the state UTxO datum.
Each `TrieFact` reuses the API type introduced for update (#269); the
field grammar is identical (`key`, optional `value`, `mpf_proof`).

Reject is a single-request operation, so the wire shape names
`request_utxo` (one entry) rather than the update shape's
`request_utxos` (list). Trie content for reject mirrors the
on-chain reject validator: the per-request fold step is exercised
without committing a new root; the structural-parity proof in S4 is
"same wallet-policy, same `invalidHereafter`, root unchanged" rather
than the update "same new root".

Per #269 Q-002, reject validity slot conversion is an era-schedule
fact, not an evaluator result: the server computes the upper slot
from the request deadline using its provider, the verifier checks
the field's basic consistency, and `rejectCageTx` consumes the
verified slot. Per-redeemer ExUnits remain client-local evaluator
output.

## Shared Surfaces

- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs`
- `cardano-mpfs-client/cardano-mpfs-client.cabal`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Reject.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/DSL.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/UpdateFactsSpec.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/RejectFactsSpec.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/RejectSpec.hs`
- `cardano-mpfs-client/test/Main.hs`
- `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types/Facts.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Reads.hs`
- `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/RejectFactsSpec.hs`
- `cardano-mpfs-offchain/test/main.hs`
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
- `docs/assets/swagger.json`
- `specs/270-reject-facts-pivot/research.md`
- `gate.sh`

## Orchestrator / Worker Split

The ticket owner owns this plan, task list, gate lifecycle, PR
metadata, MOOG boundary note, review, and final verification.
Behavior-changing Haskell source, tests, fixtures, generated Swagger,
and E2E matrix changes are owned by visible Claude or Codex
driver+navigator pairs. Each implementation slice produces one
bisect-safe commit with a `Tasks:` trailer. The ticket owner reviews
the diff, reruns focused proof or `./gate.sh`, stamps matching
`tasks.md` checkboxes by amending the worker commit, and pushes.

## Vertical Slices

S1 is non-negotiable per operator directive (post-#285 merge debrief,
2026-05-28). It ships before any reject wire type because it pays
back a documented debt (#269 S7 dropped a negative e2e and never
restored a DSL runner for the new `UpdateFacts` shape), and because
S3's `runForgeRejectFacts` reuses the exact pattern S1 establishes.

1. **Forgery DSL Port For UpdateFacts**: add
   `runForgeUpdateFacts :: CsmtForge () -> UpdateFacts -> UpdateFacts`
   and `runForgeUpdateFactsTrie :: TrieForge () -> UpdateFacts ->
   UpdateFacts` to `Cardano.MPFS.Client.Verify.DSL`, with the same
   shape as the existing six `*TxResponse` runners. Add
   `forgeEntryProof` / `forgeEntryTxOut` helpers for the
   `UtxoEntry`-shaped facts. Migrate the inline tamperers in
   `UpdateFactsSpec.hs` to the DSL primitives. Reinstate the dropped
   #269 S7 negative e2e in
   `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
   using `runForgeUpdateFacts` plus the verifier's reported field
   path. No reject types touched in this slice.
2. **Reject Facts Wire Type**: add `RejectFacts` DTO, JSON
   instances, ToSchema instance, server conversion helpers, and
   indexer reads for reject's named request UTxO. Reuse the `TrieFact`
   already shipped by #269. No HTTP route added yet.
3. **Reject Facts Verifier (with DSL completion)**: add
   `runForgeRejectFacts :: CsmtForge () -> RejectFacts -> RejectFacts`
   and `runForgeRejectFactsTrie :: TrieForge () -> RejectFacts ->
   RejectFacts` to the DSL, then `VerifiedRejectFacts` and
   `verifyRejectFacts` with focused tests covering happy path,
   snapshot tamper, trusted-root mismatch, CSMT proof tamper, MPF
   proof tamper, trie-fact value tamper, and validity-slot tamper.
   Verifier code must not import `Cardano.Ledger.Api.Tx`.
4. **Cage Helper And Structural Parity**: add
   `Cardano.MPFS.Client.Cage.Reject.rejectCageTx`. Prove structural
   parity for fact-derived fields plus same-new-state (root
   unchanged) behaviour against the legacy server-side reject builder.
   Validity upper slot and per-redeemer ExUnits are provider-runtime
   fields and are excluded from structural parity.
4b. **Validity Slot Fact** (only if S2/S3/S4 reveal the same gap as
   #269 S4b): extend `RejectFacts` with `validity_upper_slot`, have
   the server compute it via provider slot conversion, verify it,
   consume it in `rejectCageTx`, update Swagger, and document the
   #269 Q-002 boundary applies here too. If S2 already includes the
   field (likely, since the brief calls for it from the start), S4b
   collapses into S2 and this entry is dropped from `tasks.md` after
   S3 review.
5. **HTTP Hard Swap, Swagger, Matrix, And MOOG Boundary**: add
   `POST /facts/reject`, remove `/tx/reject` from shared API, server
   wiring, client wrappers, active tests, and Swagger, then regenerate
   `docs/assets/swagger.json`. Extend the local-cluster facts matrix
   with a reject row that proves `POST /facts/reject ->
   verifyRejectFacts -> rejectCageTx -> submit -> reject indexed`.
   Add a ProofsSpec negative e2e for reject using
   `runForgeRejectFacts`. Record MOOG boundary status in the PR body.
6. **Live-Boundary Fix-Ups**: only used if S5 surfaces a
   live-boundary issue (analogous to the request feeBuffer fix in
   #269 c42e7bb). Otherwise dropped from `tasks.md` before
   finalization.
7. **Finalize**: drop `gate.sh` in a `chore: drop gate.sh (ready for
   review)` commit, mark PR ready, record MOOG boundary status final
   text in PR body, leave for external merge.

## Verification

- DSL focused tests (S1):
  `nix develop --quiet -c just unit-client "/verifyUpdateFacts/"`
  (the existing UpdateFactsSpec suite, now driven by DSL primitives).
- Reject wire/indexer focused tests (S2):
  `nix develop --quiet -c just unit-offchain "/reject facts wire|readRejectRequest/"`
- Reject verifier focused tests (S3):
  `nix develop --quiet -c just unit-client "/verifyRejectFacts/"`
- Reject cage/structural-parity focused tests (S4):
  `nix develop --quiet -c just unit-client "/rejectCageTx/"`
- HTTP route focused tests (S5):
  `nix develop --quiet -c just unit-offchain "/POST /facts/reject/"`
- Swagger refresh (S5):
  `nix develop --quiet -c just update-swagger`
- Matrix proof (S5):
  `nix develop --quiet -c just e2e-facts-matrix`
- Proof-envelope reject regression proof (S5):
  `nix develop --quiet -c just e2e "read and write envelopes"`
- Final PR gate:
  `./gate.sh`
- Static verifier-surface check before finalization:
  `! rg -n 'Cardano\\.Ledger\\.Api\\.Tx' cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`
- Static legacy-route check before finalization:
  `! rg -n '"/tx/reject"|TxRejectAPI|txRejectHandler|"tx" :> "reject"|rejectTx' docs/assets/swagger.json cardano-mpfs-api/lib/Cardano/MPFS/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`

## Live-Boundary Smoke Decision

Unit tests prove wire shape, proof replay, MPF folding, structural
parity, and route wiring. They do not prove the live server boundary
builds the same state transition from facts and accepts it on-chain.
The #278 facts API matrix is therefore required before this PR leaves
draft. The matrix command is run and recorded explicitly; `gate.sh`
keeps the standard `just ci` spine plus the final legacy-route
sentinel.

If the matrix surfaces a slot or validity issue analogous to #269
Q-002, S4b ships the corresponding fact. If the matrix surfaces a
funding issue analogous to #269 S8, scope is contained to reject's
request side; the update fix has already shipped on main
(c42e7bb).

## MOOG Boundary

Reject has no operation-specific MOOG canary at planning time.
Boundary status is recorded as deferred to
cardano-foundation/moog#96 unless a reject canary/staged-port proof is
produced before finalization. This is not a legacy MOOG caller
migration and must not be worded as production readiness.
