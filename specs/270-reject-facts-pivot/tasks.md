# Tasks: Reject Fact Provider Pivot

## Slice S1 - Forgery DSL Port For UpdateFacts

- [ ] T001-S1 [US1] Port the forgery DSL to the facts-shape envelope:
      RED a UpdateFactsSpec test that uses
      `runForgeUpdateFacts (flipProof "state_utxo")` and expects
      `csmtReplayFailedAt "update.state_utxo.inclusion_proof"`,
      reinstate the dropped #269 S7 negative e2e assertion in
      `ProofsSpec.hs`, then GREEN by adding `runForgeUpdateFacts ::
      CsmtForge () -> UpdateFacts -> UpdateFacts` and
      `runForgeUpdateFactsTrie :: TrieForge () -> UpdateFacts ->
      UpdateFacts` to `Cardano.MPFS.Client.Verify.DSL`, plus
      `forgeEntryProof`/`forgeEntryTxOut` helpers for `UtxoEntry`
      and a `forgeFactsTrieValue` helper for the API-types `TrieFact`.
      Migrate the inline `tamperEntryProof`/`tamperTrieProof`/
      `tamperTrieValue`/`tamperListAt` tamperers in `UpdateFactsSpec.hs`
      to the DSL primitives. Do not touch any reject types in this
      slice.

## Slice S2 - Reject Facts Wire Type

- [ ] T002-S2 [US1] Add reject facts wire/indexer foundation: RED
      tests for `RejectFacts` JSON/schema and reject's named
      request-UTxO read helper, then GREEN `RejectFacts` (with
      `validity_upper_slot` from the start, per #269 Q-002), server
      conversion helpers, and `Indexer.Reads` primitives needed by
      the later HTTP route. Reuse the existing `TrieFact` type. Do
      not add `/facts/reject`, remove `/tx/reject`, or edit client
      cage builders in this slice.

## Slice S3 - Reject Facts Verifier (with DSL completion)

- [ ] T003-S3 [US1] Add the reject runners to the forgery DSL
      (`runForgeRejectFacts` for CSMT, `runForgeRejectFactsTrie` for
      MPF), then RED client verifier tests in
      `cardano-mpfs-client/test/Cardano/MPFS/Client/RejectFactsSpec.hs`
      including happy path, snapshot tamper, trusted-root mismatch,
      CSMT proof tamper, MPF proof tamper, trie-fact value tamper,
      and validity-slot tamper. GREEN the opaque verified witness,
      `verifyRejectFacts`, and exports. The verifier surface must
      not import `Cardano.Ledger.Api.Tx`. Do not build transactions
      or add HTTP routing in this slice.

## Slice S4 - Cage Helper And Structural Parity

- [ ] T004-S4 [US1] Add `Cardano.MPFS.Client.Cage.Reject.rejectCageTx`.
      RED cage tests in
      `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/RejectSpec.hs`
      for empty funding/policy behaviour, structural parity against
      the legacy reject transaction shape for fact-derived fields, and
      a proof that the local cage helper produces the same new state
      (root unchanged) as the legacy server-side reject for equivalent
      inputs. Exclude only provider-runtime validity upper slot and
      per-redeemer ExUnits from structural parity. Do not edit server
      route wiring in this slice.

## Slice S5 - HTTP Hard Swap, Swagger, Matrix, MOOG Boundary

- [ ] T005-S5 [US1] RED HTTP/Swagger tests proving `POST
      /facts/reject` exists, returns facts without transaction CBOR,
      and `/tx/reject` is absent. GREEN by adding the facts route,
      removing `TxRejectAPI`/`txRejectHandler`/typed `rejectTx`,
      regenerating `docs/assets/swagger.json`, extending the
      local-cluster facts matrix with a reject row that proves
      `POST /facts/reject -> verifyRejectFacts -> rejectCageTx ->
      submit -> reject indexed`, adding a ProofsSpec negative e2e for
      reject using `runForgeRejectFacts`, and recording the MOOG
      boundary status in the PR body. Keep update legacy-route
      assertions intact. Run the focused commands and `./gate.sh`;
      do not drop `gate.sh` here.

## Slice S7 - Finalize

- [ ] T007-S7 Drop `gate.sh` in a `chore: drop gate.sh (ready for
      review)` commit, mark the PR ready, and leave for external
      merge. Add only if a live-boundary issue surfaces.

## Worker Slice Briefs

### Slice S1: Forgery DSL Port For UpdateFacts

Worker owns T001-S1. The slice is the forgery DSL port for
facts-shape envelopes. It does not touch any reject types — its job
is to pay back the #269 S7 debt (the dropped negative e2e assertion
on the update path, and the inline tamperers sitting in
`UpdateFactsSpec.hs` because no DSL runner existed for `UpdateFacts`).

RED:

1. In `cardano-mpfs-client/test/Cardano/MPFS/Client/UpdateFactsSpec.hs`,
   add a unit test that uses
   `runForgeUpdateFacts (flipProof "state_utxo") facts` and asserts
   `shouldRejectWith verifyUpdateUnit $
     csmtReplayFailedAt "update.state_utxo.inclusion_proof"`. This
   fails because `runForgeUpdateFacts` doesn't exist.
2. In
   `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`,
   reinstate the dropped #269 S7 negative assertion against the
   honest update facts response using
   `runForgeUpdateFacts (flipProof "state_utxo")` and the verifier's
   actual reported field path (read the verifier source to get the
   exact path string).

GREEN:

3. Add to `Cardano.MPFS.Client.Verify.DSL`:
   - `forgeEntryProof :: UtxoEntry -> UtxoEntry` (flip
     `ueInclusionProof` via `flipApiHexMidByte`).
   - `forgeEntryTxOut :: UtxoEntry -> UtxoEntry` (flip
     `ueTxOutCbor` via `flipApiHexMidByte`).
   - `forgeFactsTrieValue :: TrieFact -> TrieFact` (flip
     `tfValue` via `flipApiHexMidByte`; no-op for exclusion
     facts). Use the API-types `TrieFact`, not the Bundle
     `TrieFact`.
   - `forgeFactsTrieProof :: TrieFact -> TrieFact` (flip
     `tfMpfProof` via `flipApiHexMidByte`).
   - `runForgeUpdateFacts :: CsmtForge () -> UpdateFacts ->
     UpdateFacts`. Path grammar:
     `"state_utxo"` → `ufStateUtxo`,
     `"request_utxos[i]"` → `ufRequestUtxos[i]`,
     `"wallet_utxos[i]"` → `ufWalletUtxos[i]`,
     `FlipSnapshotRoot` → `ufSnapshot.utxoRoot` via the existing
     `swapSnapRoot` helper, swapped over to take a snapshot-shape
     that matches the API-types `VerificationSnapshot`. (The
     existing `swapSnapRoot` is keyed on the Client.Snapshot
     `VerificationSnapshot`; add an API-shape sibling
     `swapApiSnapRoot` or generalise the helper. Pick whichever
     keeps the shared DSL helpers thin.)
   - `runForgeUpdateFactsTrie :: TrieForge () -> UpdateFacts ->
     UpdateFacts`. Same shape as `runForgeUpdateTrie` but operating
     on `ufTrieFacts` / `ufTrieRoot`. Use
     `forgeFactsTrieValue`/`forgeFactsTrieProof` and a hex-flipping
     helper for `ufTrieRoot`. The `Hex` here is the API-types
     `Hex` (a `ByteString`), so use `flipApiHexMidByte` directly.
4. Migrate the inline tamperers in `UpdateFactsSpec.hs`
   (`tamperEntryProof`, `tamperTrieProof`, `tamperTrieValue`,
   `tamperListAt`) to call the new DSL primitives directly via
   `runForgeUpdateFacts` / `runForgeUpdateFactsTrie` programs.
   Existing test names and assertions stay; only the construction
   of `forged` changes. Make sure the path strings match what the
   verifier actually reports (read
   `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Replay.hs`
   or the relevant verifier module to confirm).

Gate (focused, then full):

- `nix develop --quiet -c just unit-client "/verifyUpdateFacts/"`
- `nix develop --quiet -c just unit-client`
- `nix develop --quiet -c just e2e "read and write envelopes"`
- `./gate.sh`

Commit shape:

```
feat(verify-dsl): port forgery DSL to UpdateFacts shape

Tasks: T001-S1
```

Forbidden scope:

- No `RejectFacts`, no `rejectCageTx`, no `/facts/reject`, no
  `/tx/reject` removal.
- Do not weaken or rename any existing DSL primitive used by the
  `*TxResponse` runners — additive only.
- Do not change the existing UpdateFactsSpec test names or
  assertions; only their `forged` constructions migrate to the DSL.
- Do not edit `gate.sh`, `spec.md`, `plan.md`, or `tasks.md`.

When done:

- Stop after committing. Do not push. The orchestrator amends
  `tasks.md` and pushes.

### Slice S2: Reject Facts Wire Type

Worker owns T002-S2. Write RED tests first for `RejectFacts`
JSON/schema and the missing reject indexer read helpers. Then add the
wire type to `cardano-mpfs-api`, server conversion helpers to
`Cardano.MPFS.HTTP.Types.Facts`, and the `Indexer.Reads` primitives
needed by the later HTTP route. Include `validity_upper_slot` from the
start, computed via the same provider conversion the legacy reject
path uses. Reuse the existing `TrieFact` type. Do not add
`/facts/reject`, remove `/tx/reject`, or edit client cage builders in
this slice.

### Slice S3: Reject Facts Verifier (with DSL completion)

Worker owns T003-S3. First, extend
`Cardano.MPFS.Client.Verify.DSL` with `runForgeRejectFacts ::
CsmtForge () -> RejectFacts -> RejectFacts` and
`runForgeRejectFactsTrie :: TrieForge () -> RejectFacts ->
RejectFacts`. Path grammar:
`"state_utxo"` → `rfStateUtxo`,
`"request_utxo"` → `rfRequestUtxo` (single entry, no index),
`"wallet_utxos[i]"` → `rfWalletUtxos[i]`,
`FlipSnapshotRoot` → `rfSnapshot.utxoRoot`. Then RED client verifier
tests in
`cardano-mpfs-client/test/Cardano/MPFS/Client/RejectFactsSpec.hs`
covering happy path, snapshot tamper, trusted-root mismatch, CSMT
proof tamper (state/request/wallet), MPF proof tamper, trie-fact
value tamper, and validity-slot tamper. Then GREEN the opaque
verified witness, `verifyRejectFacts`, and exports. The verifier
surface must not import `Cardano.Ledger.Api.Tx`.

### Slice S4: Cage Helper And Structural Parity

Worker owns T004-S4. Write RED cage tests first for `rejectCageTx`,
including empty funding/policy behaviour, structural parity against
the legacy reject transaction shape for fact-derived fields, and a
proof that the local cage helper produces the same new state (root
unchanged) as the legacy server-side reject for equivalent inputs.
Exclude only provider-runtime validity upper slot and per-redeemer
ExUnits from structural parity. Do not edit server route wiring in
this slice.

### Slice S5: HTTP Hard Swap, Swagger, Matrix, MOOG Boundary

Worker owns T005-S5. Write RED HTTP/Swagger tests proving `POST
/facts/reject` exists, returns facts without transaction CBOR, and
`/tx/reject` is absent. Then add the facts route, remove
`TxRejectAPI`/`txRejectHandler`/typed `rejectTx`, regenerate
`docs/assets/swagger.json`, extend the local-cluster facts matrix
with a reject row that proves `POST /facts/reject ->
verifyRejectFacts -> rejectCageTx -> submit -> reject indexed`, add
a ProofsSpec negative e2e for reject using `runForgeRejectFacts`,
and record the MOOG boundary status in the PR body as deferred to
cardano-foundation/moog#96 unless a real reject canary/staged-port
proof exists. Run the focused commands and `./gate.sh`; do not drop
`gate.sh` here.

### Slice S7: Finalize

Owner-only slice. Drop `gate.sh`, mark the PR ready, leave for
external merge.
