# Tasks — #247: Trie.lookup returns value-bearing bytes

Decomposition of `plan.md` into bisect-safe slices. One commit per
slice. Each behavior-changing commit carries
`Tasks: T###-S<n>[, T###-S<n>]` in its trailer and stamps the
corresponding `[X]` here in the same amended commit.

Slice ordering, RED → GREEN contract, and atomicity invariants
(INV-1 / INV-2 / INV-3) live in `plan.md` and `spec.md`. This file
is the orchestrator's check-off ledger.

## Slice 1 — Plumbing + fail-loud startup check

Owns: column-family addition, codec entry, RocksDB CF list,
INV-3 startup pre-flight.

- [X] T001-S1 — Add `TrieRawValues :: AllColumns (KV HexKey ByteString)`
      to `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Columns.hs`
      (constructor + `GEq` + `GCompare` + module-haddock count
      update). Add the `TrieRawValues :=> Codecs {...}` entry to
      `allCodecs` in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Codecs.hs`
      with `keyCodec = hexKeyPrism` and identity value codec. Add
      `("trie-raw-values", dbConfig)` to `cageColumnFamilies` in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs` and
      update the `withApplication` haddock counts (cage 6 → 7,
      unified 13 → 14).
- [X] T002-S1 — RED + GREEN for `opens_with_trie_raw_values_column`
      in `cardano-mpfs-offchain/test/Cardano/MPFS/Trie/PersistentSpec.hs`:
      a fresh `withApplication` exposes `TrieRawValues` as an
      empty queryable CF.
- [X] T003-S1 — RED + GREEN for `refuses_to_start_on_stale_schema`
      in the same `PersistentSpec`: opening a DB with rows in
      `TrieKV` and no rows in `TrieRawValues` raises a structured
      `SchemaMigrationRequired` exception whose message names
      "drop the RocksDB directory and resync from genesis". The
      startup check is wired inside `withApplication` after
      `withDBCF` returns and before runtime control is handed off.

## Slice 2 — Persistent trie: write + delete + lookup via TrieRawValues

Owns: INV-1 (write atomicity) wiring + INV-2 (rollback atomicity)
verification + lookup contract change in
`cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`.

- [X] T004-S2 — Rewire `unifiedInsert` and `unifiedDelete` to also
      `KV.put TrieRawValues (byteStringToHexKey (hashBS k)) v` /
      `KV.delete TrieRawValues ...` inside the existing
      `Transaction m cf AllColumns ops` body (INV-1 enforced by
      reuse). Rewire `unifiedLookup` to return
      `KV.query TrieRawValues hexKey` when the inclusion proof
      witnesses presence; return `Nothing` if either the proof
      lookup or the raw-value read is absent. **The
      `persistent*`/`speculative*` functions at ~lines 930–1089
      are OUT OF SLICE 2 SCOPE** — they operate on the
      `MPFStandalone*Col` schema which has no `TrieRawValues`
      analogue; their asymmetry is captured by `T013-S5`.
- [X] T005-S2 — RED + GREEN for `insert_then_lookup_returns_raw_value`
      and `delete_then_lookup_returns_nothing` in
      `cardano-mpfs-offchain/test/Cardano/MPFS/Trie/PersistentSpec.hs`.
      Delete test also asserts via direct `KV.query TrieRawValues`
      that the row is gone.
- [X] T006-S2 — RED + GREEN for `rollback_undoes_both_columns`
      (INV-2 verification): insert `(k, v)`, checkpoint, insert
      `(k, v')`, roll the chain-follower back to the checkpoint,
      assert `Trie.lookup k == Just v` AND
      `KV.query TrieRawValues hashOfKey == Just v`.

## Slice 3 — Pure trie: in-memory raw-value mirror

Owns: parallel write/lookup contract for the pure backend in
`cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs` (and
`PureManager.hs` if it exposes the underlying ref).

- [X] T007-S3 — Extend the pure backend with a sibling
      `IORef (Map HexKey ByteString)` (or fold it into the
      existing `IORef` cell as one record under one ref to dodge
      the race-window concern in R2). Rewire `pureInsert` to write
      both stores, `pureDelete` to clear both, `pureLookup` to
      return from the raw mirror. The persistent rollback test
      does not need a pure analogue (pure backend has no rollback
      machinery).
- [X] T008-S3 — RED + GREEN for `pure_insert_then_lookup_returns_raw_value`
      and `pure_delete_then_lookup_returns_nothing` in
      `cardano-mpfs-offchain/test/Cardano/MPFS/TrieSpec.hs`.

## Slice 4 — E2E: replace `assertFactEnvelope` with verifier round-trip

Owns: the proof-replay acceptance criterion in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
and any matrix entries in `FactsMatrixSpec.hs` that asserted on
the structural-only shape.

- [X] T009-S4 — Add `verifyFactPresentFacts` /
      `verifyFactAbsentFacts` to
      `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`
      following the existing `verifyXFacts` pattern: take a
      `TrustedRoot`, run structural + replay checks, return
      `Either VerifyError VerifiedFactPresentFacts` (and
      symmetric for absent). May add `FactPresentFacts` /
      `FactAbsentFacts` typed shapes in
      `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs` or
      reuse `FactResponse` (driver's call). Unit tests cover
      happy path + tampered-value / tampered-proof / mismatched-
      root cases.
- [X] T009b-S4 — Replace (or supplement) `assertFactEnvelope` in
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
      with a call to the new T009-S4 verifier, asserting
      `Right ()`. Trusted root from the same response's snapshot
      (mirror `endFactsTrustedRoot` ~line 495). Update the
      now-wrong haddock "MPFS stores values as 32-byte content
      hashes" (~lines 451-454).
- [X] T010-S4 — Sweep
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
      around lines 920–950 (the
      `/tokens/:id/facts/:key after process` and
      `/tokens/:id/facts/:key after delete+process` matrix
      entries) for any other structural-only shape assertions
      that need to flip to verifier round-trips.

## Slice 5 — Research note + gate sentinel

Owns: the acceptance-required research write-up + a `gate.sh`
sentinel that the legacy bug cannot return.

- [X] T011-S5 — Append a `## #247 — value-bearing lookup` section
      to `specs/243-proof-redesign/research.md` covering: the
      three options that were weighed, the chosen one (A), the
      two atomicity invariants (INV-1, INV-2), the fail-loud
      startup check (INV-3), the migration sub-option (A-002 /
      sub-option (1)), and a pointer to this PR.
- [X] T012-S5 — Extend `./gate.sh` with a `check_absent` sentinel
      for the legacy `Just (hashBS k)` /
      `Just (renderMPFHash (mkMPFHash k))` fallback ONLY at the
      unified-path call sites in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
      (`unifiedLookup`) and the entirety of
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs`.
      Scope-exclude the speculative-path call site at
      `Trie/Persistent.hs:~1089` (which remains legitimately
      broken pending `T013-S5`).
- [X] T013-S5 — Document the deferred speculative-path
      asymmetry in
      `specs/243-proof-redesign/research.md`:
      `speculativeLookup` / `persistentLookup` continue to return
      `Just (hashBS k)` after #247 because they operate on the
      `MPFStandalone*Col` schema which has no `TrieRawValues`
      analogue. Captures the asymmetry; no code change.

## Slice 7 — E2E verifier regression fix (post-mark-ready CI failure)

Owns: diagnosis and fix for the `MpfReplayFailed "fact_present.fact.mpf_proof" "root mismatch"` failure
introduced by Slice 4's e2e wiring (run
https://github.com/lambdasistemi/cardano-mpfs-offchain/actions/runs/26400824147
at HEAD `814446a`).

- [X] T015-S7 — Reproduce the e2e failure locally
      (`just e2e` with the failing scenario isolated).
      Capture the exact trie root, proof bytes, key, and
      value used by the verifier; pinpoint where the
      server-side root diverges from the proof's root.
      Document the diagnosis in
      `/tmp/epic-257/247/s7-driver/STATUS.md` as a
      `NOTE diagnosis: <one-line summary>` line.
- [X] T016-S7 — Land the fix at whichever layer the
      diagnosis identifies (likely
      `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
      or `Trie/Persistent.hs`). Do NOT relax the verifier;
      fix the root/proof/value disagreement at its source.
      `./gate.sh` and `just e2e` both pass at HEAD.
- [X] T017-S7 — Add a unit-level regression test that
      reproduces the disagreement at the indexer layer
      (no cardano-node), so future drivers can catch this
      class of bug without running the full e2e.

## Slice 8 — Serve exclusion proofs at `/tokens/:id/proofs/:key`

Owns: extend `Trie.getProof` to construct exclusion proofs
when the inclusion variant is absent, so
`/tokens/:id/proofs/:key after delete+process` returns a
`ProofResponse` whose embedded MPF proof verifies via
`verifyFactAbsentFacts`. Second post-mark-ready CI failure
at HEAD `73fbdc9`:
https://github.com/lambdasistemi/cardano-mpfs-offchain/actions/runs/26405777252

- [x] T018-S8 — Confirm the pre-stated diagnosis with a
      local repro (`just e2e --match
      "/Facts API coverage matrix/.../"`). The diagnosis is
      pre-stated in `plan.md` Slice 8: server's
      `Trie.getProof` only calls `mkMPFInclusionProof`,
      never `mkMPFExclusionProof`; absent keys yield 404
      instead of an exclusion-proof response. Confirm with
      one targeted STATUS line and proceed; do not chase
      alternative hypotheses unless the local repro
      contradicts the brief.
- [x] T019-S8 — Add a serializer for exclusion proofs in
      `cardano-mpfs-offchain/lib/Cardano/MPFS/Core/Proof.hs`
      (preferred: reuse the existing CBOR step encoder via
      a thin wrapper around `mpfExclusionProofSteps`).
      Extend `unifiedGetProof` (in
      `lib/Cardano/MPFS/Trie/Persistent.hs`),
      `pureGetProof` (in `lib/Cardano/MPFS/Trie/Pure.hs`),
      `persistentGetProof` / `speculativeGetProof` (also in
      `Persistent.hs`) to fall back to
      `mkMPFExclusionProof` when `mkMPFInclusionProof`
      returns `Nothing`.
- [x] T020-S8 — Add a unit regression test (no
      cardano-node) exercising `verifyFactAbsentFacts`
      against the persistent proofs endpoint for an absent
      key: insert k, delete k, GET
      `/tokens/:id/proofs/:k`, assert
      `verifyFactAbsentFacts` returns `Right _`. The test
      MUST use the typed client verifier — do NOT bypass
      it by hand-rolling `verifyAikenExclusionProof` raw.

## Slice 6 — Drop gate.sh (finalization)

Owns: the standard resolve-ticket finalization sentinel.

Re-run after Slices 7 + 8 land — the prior drop-gate
commit (`814446a`) has been reverted (`b10b7f3`); a fresh
`chore: drop gate.sh (ready for review)` commit lands
after Slice 8 is green and CI is green on HEAD.

- [X] T014-S6 — Drop `./gate.sh` in a
      `chore: drop gate.sh (ready for review)` commit. No
      `Tasks:` trailer required (chore allowed by the commit
      gate). `gh pr ready 284` to flip out of draft.

## Out-of-slice scope

Documented in `plan.md` "Out of slice scope". A-002 chose
sub-option (1), so no backfill slice is needed; the PR body
clarity follow-through (A-002 #1) is handled at finalization, not
as a slice.
