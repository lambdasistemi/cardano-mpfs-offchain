# Tasks: Update Fact Provider Pivot

## Slice S1 - Wire Types And Indexer Reads

- [X] T001-S1 [US1] Add update facts wire/indexer foundation: RED tests for
      `UpdateFacts`/`TrieFact` JSON/schema and update read helpers, then GREEN
      `UpdateFacts`, shared `TrieFact`, server conversion helpers,
      `readRequestUtxosAt`, and `readTrieFact` without adding `/facts/update`
      yet.

## Slice S2 - Update Facts Verifier

- [X] T002-S2 [US1] Add `VerifiedUpdateFacts` and `verifyUpdateFacts`: RED
      tests for happy path, snapshot tamper, trusted-root mismatch, CSMT proof
      tamper, MPF proof tamper, and trie-fact value tamper, then GREEN the
      pure verifier and exports with zero `Cardano.Ledger.Api.Tx` imports in
      the verifier surface.

## Slice S3 - Cage Helper And Structural Parity

- [X] T003-S3 [US1] Add
      `Cardano.MPFS.Client.Cage.Update.updateCageTx`, implement/reuse the MPF
      fold helper, and prove Q-001 structural parity for fact-derived update
      tx fields plus same-new-root behavior against the legacy server-side
      update fold.

## Slice S4 - HTTP Hard Swap And Swagger

- [ ] T004-S4 [US1] Add `POST /facts/update`, remove the legacy
      `POST /tx/update` path from shared API/server/client wrappers/tests,
      regenerate `docs/assets/swagger.json`, and prove Swagger/API expose only
      the new update facts shape while reject and sweep remain untouched.

## Slice S5 - Matrix, MOOG Boundary, And Final Proof

- [ ] T005-S5 [US1] Add the update row to the facts API local-cluster matrix,
      prove live `/tx/update` absence, record update MOOG boundary status in
      the PR body, run focused verifier/cage/HTTP/matrix commands plus
      `./gate.sh`, and leave the branch ready for final gate removal.

## Worker Slice Briefs

### Slice S1: Wire Types And Indexer Reads

Worker owns T001-S1. Write RED tests first for `UpdateFacts`/`TrieFact`
JSON/schema and the missing update indexer read helpers. Then add the wire
types to `cardano-mpfs-api`, server conversion helpers to
`Cardano.MPFS.HTTP.Types.Facts`, and the `Indexer.Reads` primitives needed by
the later HTTP route. Do not add `/facts/update`, remove `/tx/update`, or edit
client cage builders in this slice.

### Slice S2: Update Facts Verifier

Worker owns T002-S2. Write RED client verifier tests first in
`cardano-mpfs-client/test/Cardano/MPFS/Client/UpdateFactsSpec.hs`, including
happy path, snapshot tamper, trusted-root mismatch, CSMT proof tamper, MPF
proof tamper, and trie-fact value tamper. Then add the opaque verified witness,
verifier, and exports. The verifier surface must not import
`Cardano.Ledger.Api.Tx`. Do not build transactions or add HTTP routing in this
slice.

### Slice S3: Cage Helper And Structural Parity

Worker owns T003-S3. Write RED cage tests first for `updateCageTx`, including
empty funding/policy behavior, Q-001 structural parity against the legacy
update transaction shape for fact-derived fields, and a proof that the local
MPF fold produces the same new state root as the legacy server-side fold for
equivalent inputs. Exclude only provider-runtime validity upper slot and
per-redeemer ExUnits from structural parity. Do not edit server route wiring in
this slice.

### Slice S4: HTTP Hard Swap And Swagger

Worker owns T004-S4. Write RED HTTP/Swagger tests proving `POST /facts/update`
exists, returns facts without transaction CBOR, and `/tx/update` is absent.
Then add the facts route, remove `TxUpdateAPI`/`txUpdateHandler`/typed
`updateTx`, regenerate `docs/assets/swagger.json`, and keep reject and sweep
legacy routes intact.

### Slice S5: Matrix, MOOG Boundary, And Final Proof

Worker owns T005-S5. Extend the local-cluster facts matrix with update:
`POST /facts/update -> verifyUpdateFacts -> updateCageTx -> submit -> expected
state root indexed`, and assert `/tx/update` absence at the live WAI boundary.
Record the update MOOG boundary status in the PR body as deferred to
cardano-foundation/moog#96 unless a real update canary/staged-port proof exists.
Run the focused commands and `./gate.sh`; do not drop `gate.sh` here.
