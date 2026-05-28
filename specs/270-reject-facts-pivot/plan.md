# Implementation Plan: Reject Fact Provider Pivot

## Architecture

Reject is a Tier-3 facts endpoint. The server performs one coherent
indexer read for:

- the current verification snapshot,
- the cage state UTxO,
- the batch of rejectable request UTxOs (filter:
  `submitted_at + process_time + retract_time < now`, mirroring the
  legacy `queryRejectContext`),
- the operator wallet UTxOs that fund fees and collateral, and
- provider-derived validity **lower** and **upper** slots for the
  reject deadline (lower = latest Phase-3 deadline among the rejected
  requests, upper = explicit TTL).

The server returns those facts plus unverified protocol parameters. It
does not build a transaction. The client verifies the facts against an
independently trusted UTxO root, replays the CSMT proofs, enforces
wallet policy, builds the unsigned transaction with `rejectCageTx`,
signs locally, and submits.

The reject wire shape is:

- `snapshot`
- `token`
- `state_utxo`
- `request_utxos` (list — batch)
- `wallet_utxos`
- `validity_lower_slot`
- `validity_upper_slot`
- `protocol_parameters`

Per Q-S2-001 (operator decision 2026-05-28), reject does **not** carry
MPF trie facts or a trie root. The legacy `RejectProof` already
contains only CSMT-layer state/request/funding witnesses; the
on-chain reject validator does not fold the trie; `prepareRejectState`
in `Real/Reject.hs` leaves `stateRoot` unchanged. Trie content would
be ceremonial — no fold output to bind, no validator constraint to
witness — so the wire shape omits it. The issue acceptance
criterion "trie-fact-tamper" is documented as vacuous; the S3
verifier matrix replaces it with validity-slot-tamper coverage on
both bounds.

Per Q-S2-001, the request set is the **batch** that mirrors the
legacy `rejectRequestsImpl` filter, not a singular "named" request
UTxO. Single-target reject would require matching changes in the
on-chain validator (out of #270 scope).

Per #269 Q-002, reject validity slot conversion is an era-schedule
fact, not an evaluator result: the server computes both bounds using
its provider, the verifier checks the field envelopes, and
`rejectCageTx` consumes the verified slots
(`lower → Tx.validFrom`, `upper → Tx.validTo`). Per-redeemer
ExUnits remain client-local evaluator output.

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
2. **Reject Facts Wire Type**: add `RejectFacts` DTO (batch
   `rfRequestUtxos :: [UtxoEntry]`, both `rfValidityLowerSlot` and
   `rfValidityUpperSlot` from the start, NO trie root, NO trie
   facts), JSON instances, ToSchema instance, server conversion
   helpers, and indexer reads for the rejectable-request batch
   filter. No HTTP route added yet.
3. **Reject Facts Verifier (with DSL completion)**: add
   `runForgeRejectFacts :: CsmtForge () -> RejectFacts -> RejectFacts`
   to the DSL (no trie-level runner — reject has no trie facts), then
   `VerifiedRejectFacts` and `verifyRejectFacts` with focused tests
   covering happy path, snapshot tamper, trusted-root mismatch, CSMT
   proof tamper (state, request_ins[i], funding[i]), and
   validity-slot tamper (lower and upper). Trie-fact tamper is
   documented as vacuous (operator decision Q-S2-001). Verifier code
   must not import `Cardano.Ledger.Api.Tx`.
4. **Cage Helper And Structural Parity**: add
   `Cardano.MPFS.Client.Cage.Reject.rejectCageTx`. Consume the
   verified lower slot via `Tx.validFrom` and upper slot via
   `Tx.validTo`. Prove structural parity for fact-derived fields plus
   same-state (root unchanged) behaviour against the legacy
   server-side reject builder. Per-redeemer ExUnits are
   provider-runtime and excluded from structural parity.
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

Q-S2-001 pre-empted the validity-slot lesson: S2 ships both bounds
from the start, so an S4b-style fact-extension slice is not
expected. If the matrix surfaces a funding issue analogous to #269
S8, scope is contained to reject's request side; the update fix has
already shipped on main (c42e7bb).

## MOOG Boundary

Reject has no operation-specific MOOG canary at planning time.
Boundary status is recorded as deferred to
cardano-foundation/moog#96 unless a reject canary/staged-port proof is
produced before finalization. This is not a legacy MOOG caller
migration and must not be worded as production readiness.
