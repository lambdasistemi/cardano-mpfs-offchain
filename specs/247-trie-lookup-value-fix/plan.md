# Plan — #247: Add `TrieRawValues` column and route lookups through it

Implements Option A from `spec.md` per A-001. Locks atomicity
invariants INV-1 / INV-2 from the spec into the column wiring.

Migration shape gates on Q-002 / A-002. Plan content below is
written for any of the three migration sub-options; the slice that
records migration semantics is parameterised on the answer.

## Status

- **Done.** spec.md, A-001 (Option A), atomicity invariants.
- **Current.** plan.md + Q-002 (migration strategy).
- **Blockers.** Q-002 must answer before tasks.md / slice
  dispatch.

## Constitution check

Re-checked against `.specify/memory/constitution.md` and
`CLAUDE.md`:

- Ledger-native types: unchanged. `TrieRawValues` stores raw
  `ByteString` payloads, no shadow types.
- Service-boundary records of functions: the `Trie m` interface in
  `Cardano.MPFS.Trie` already abstracts insert/delete/lookup;
  callers see no change. Compliant.
- Block-processing atomicity across column families: enforced by
  INV-1; rollback symmetry by INV-2. Both invariants ride on the
  existing `Transaction m cf UnifiedColumns ops` runner —
  compliant by reuse.
- Fact-provider invariant: server still returns proof-bearing
  material; `frValue` now actually carries the value. No new
  transaction-building responsibility on the server. Compliant.
- Proof / hashing compatibility with Aiken validators: unchanged —
  the trie still stores `(hashOfKey, mkMPFHash v)`; only the
  out-of-band raw-value lookup is added. Compliant.
- Client verifier purity: unchanged — verifier still consumes
  `valueBs` from the wire shape and re-hashes it internally.
  Compliant.

## Approach

Make `unifiedInsert` / `pureInsert` write both `TrieKV` (the
existing `(hashOfKey, mkMPFHash v)` entry) **and** `TrieRawValues`
(the new `hashOfKey → v` entry) in the same transaction. Make
`unifiedDelete` / `pureDelete` symmetrically drop both rows.
Rewire `unifiedLookup` / `pureLookup` to read from `TrieRawValues`
instead of returning `hashBS k`. The rollback machinery
(`applyCageInverses`) already routes through the `Trie m`
interface, so it picks up the new behaviour for free.

For the pure backend, add an `IORef (Map HexKey ByteString)`
companion alongside the existing `MPFInMemoryDB`. Both refs sit in
the same `IORef` if a single mutable cell is more ergonomic, or
share a wrapper record.

The persistent backend adds a 7th `AllColumns` constructor and a
codec entry. `UnifiedColumns` grows from 13 → 14 column families
once `InCage` is summed with `InUtxo` and `InRollbacks`. The
RocksDB column-family list in `Cardano.MPFS.Application`
(`cageColumnFamilies`) gets the new `"trie-raw-values"` entry.

The e2e in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
loses its `assertFactEnvelope` shape check and gains a call to
`verifyFactPresentResponse` against a trusted root pulled from the
same response's snapshot. The absent-fact path keeps its existing
exclusion-proof check.

## Slice breakdown

One bisect-safe slice per concern. Each slice is a single commit
with a `Tasks: T###-Sn` trailer.

### Slice 1 — Plumbing: add `TrieRawValues` column + codec + RocksDB wiring

**Goal.** Add the new column-family type, codec, and on-disk
config without touching the trie operations yet. After this slice,
the indexer opens with 14 column families and ignores
`TrieRawValues` everywhere. The build is green, all existing tests
pass.

**Files.**

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Columns.hs`
  - Add `TrieRawValues :: AllColumns (KV HexKey ByteString)`
  - Extend `GEq` / `GCompare` instances.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Codecs.hs`
  - Add the `TrieRawValues :=> Codecs { keyCodec = hexKeyPrism, valueCodec = identityPrism }`
    entry to `allCodecs`. The value codec is identity (raw
    passthrough; the MPF library's hashing already treats the
    column as opaque).
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs`
  - Add `("trie-raw-values", dbConfig)` to `cageColumnFamilies`.
  - Update the haddock comments mentioning the count (12 → 13
    cage CFs, 13 → 14 unified CFs).
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Columns.hs`
  (module haddock) — update the column-count narrative.

**RED.** A `PersistentSpec` test
`opens_with_trie_raw_values_column` opens a fresh `withApplication`
and confirms `TrieRawValues` is queryable as an empty CF (no
rows). The test currently fails because the constructor does not
exist.

**GREEN.** Add the constructor + codec + CF config. The test
passes.

**Tasks.** `T001-S1`, `T002-S1`.

### Slice 2 — Persistent trie: write + delete + lookup via `TrieRawValues`

**Goal.** Make `unifiedInsert` / `unifiedDelete` write both
columns inside the same transaction. Make `unifiedLookup` read
from `TrieRawValues`. Both atomicity invariants land here.

**Files.**

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
  - `unifiedInsert`: after `inserting … TrieKV TrieNodes …`, call
    `KV.put TrieRawValues (byteStringToHexKey (hashBS k)) v`
    inside the same transaction.
  - `unifiedDelete`: after `deleting …`, call
    `KV.delete TrieRawValues (byteStringToHexKey (hashBS k))`.
  - `unifiedLookup`: replace
    `Just _ -> Just (hashBS k)` with
    `Just _ -> KV.query TrieRawValues hexKey` (or equivalent).
    The proof guard stays — `unifiedLookup` still returns
    `Nothing` if the MPF inclusion proof can't be built, even if
    the raw column happened to have a row (defence in depth).
  - The `unifiedManager` variants (lines 942, 963, 1039, 1059 in
    the existing source) get the same treatment.

**RED.** `cardano-mpfs-offchain/test/Cardano/MPFS/Trie/PersistentSpec.hs`
gains:

- `insert_then_lookup_returns_raw_value`: insert `(k, v)`, expect
  `Trie.lookup k` to return `Just v` (not `Just (hashBS k)`).
- `delete_then_lookup_returns_nothing`: insert `(k, v)`, delete
  `k`, expect `Trie.lookup k == Nothing`. Also confirm
  `TrieRawValues` no longer has the row by direct KV inspection.
- `rollback_undoes_both_columns` (covers INV-2): insert `(k, v)`,
  checkpoint, insert `(k, v')`, roll back to checkpoint, expect
  `Trie.lookup k == Just v` AND direct
  `KV.query TrieRawValues hashOfKey == Just v` (not `v'`, not
  absent).

**GREEN.** Wire the two `KV.put` / `KV.delete` / `KV.query` calls
inside `unifiedInsert` / `unifiedDelete` / `unifiedLookup`. The
tests pass.

**Tasks.** `T003-S2`, `T004-S2`, `T005-S2` (rollback test).

### Slice 3 — Pure trie: in-memory raw-value mirror

**Goal.** Same contract as slice 2 for `Cardano.MPFS.Trie.Pure`,
so unit tests and the pure manager honour the value-bearing
lookup. Pure backend doesn't have RocksDB transactions, but it
must still observe the symmetric write semantics (one logical
update touches both stores at once from the caller's perspective).

**Files.**

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs`
  - Extend the `IORef` cell or pair it with a sibling
    `IORef (Map HexKey ByteString)`.
  - `pureInsert` writes both.
  - `pureDelete` clears both.
  - `pureLookup` returns from the raw mirror instead of
    `Just (renderMPFHash (mkMPFHash k))`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/PureManager.hs`
  (if it exists and exposes the underlying ref) — extend
  symmetrically.

**RED.** `cardano-mpfs-offchain/test/Cardano/MPFS/TrieSpec.hs`
covers the pure trie. Add:

- `pure_insert_then_lookup_returns_raw_value`
- `pure_delete_then_lookup_returns_nothing`

The Persistent rollback test is unnecessary here (no rollback
machinery in the pure backend).

**GREEN.** Wire the mirror map.

**Tasks.** `T006-S3`, `T007-S3`.

### Slice 4 — E2E: replace `assertFactEnvelope` with verifier round-trip

**Goal.** Per the acceptance criteria, the e2e for the proof
redesign no longer asserts structural fields only — it decodes
the response into the typed wire shape and runs the offline
verifier.

**Files.**

- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
  - Replace (or supplement) `assertFactEnvelope` with a call to
    `verifyFactPresentResponse trustedRoot blueprint resp`. The
    trusted root comes from the same response's snapshot (mirrors
    the existing `endFactsTrustedRoot` helper around line 495).
  - Update the haddock pointing at "MPFS stores values as 32-byte
    content hashes" — that comment is now wrong (cf. line 451–454
    in the existing source).
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
  — touch any matrix entry that asserted on the structural-only
  shape (look around lines 920–950).

**RED.** Run the e2e with the structural assertion replaced by
the verifier call — pre-slice-2/3 it fails with
`MpfReplayFailed "root mismatch"`. The Slice 2/3 fixes flip it
green.

**GREEN.** Slices 2 + 3 already shipped the trie-side fix. This
slice just lands the e2e update; the test now passes.

**Tasks.** `T008-S4`, `T009-S4`.

### Slice 5 — Research note + extend `gate.sh`

**Goal.** Record the chosen option in
`specs/243-proof-redesign/research.md` (per the issue acceptance)
and harden `gate.sh` with a sentinel that the bad behaviour is
gone.

**Files.**

- `specs/243-proof-redesign/research.md` — append a section
  `## #247 — value-bearing lookup` summarising the three options,
  the chosen one (A), the atomicity invariants, and a pointer to
  this PR.
- `gate.sh` — extend with `check_absent "legacy hashBS-as-value
  fallback" 'Just \(hashBS k\)|Just \(renderMPFHash \(mkMPFHash k\)\)' …`
  pointing at `Trie/Persistent.hs` and `Trie/Pure.hs` so any
  regression that re-introduces the bug fails the gate.

**RED.** Not applicable — this is documentation + a sentinel.
`gate.sh` runs and the sentinel passes.

**Tasks.** `T010-S5`, `T011-S5`.

### Slice 6 — Drop `gate.sh` (finalization)

**Goal.** Standard resolve-ticket finalization. After the
finalization audit passes, drop the gate and mark the PR ready.

**Files.**

- `gate.sh` — removed.

**Commit.** `chore: drop gate.sh (ready for review)`. No
`Tasks:` trailer (chore allowed by the commit gate).

**Tasks.** `T012-S6`.

## Out of slice scope

- Migration tooling for any of Q-002's three sub-options is
  considered after `A-002` lands. The plan above is written so
  that sub-option (1) "re-index from genesis required" needs no
  additional slice — operators drop the RocksDB directory before
  upgrading. Sub-option (2) "degrade gracefully" needs no
  additional slice either — pre-migration keys simply return
  `Nothing` from the `TrieRawValues` column. Sub-option (3)
  "backfill from journal" requires a new Slice 1.5 that walks
  `CageRequests` and back-fills `TrieRawValues`; this is **not**
  in the current breakdown and would be added if the operator
  picks it.

## Risks

- **R1 — Migration story.** Covered by Q-002. The plan above is
  written to accommodate any of the three sub-options.
- **R2 — `IORef` ergonomics in the pure backend.** A naive second
  `IORef` introduces a tiny race window in multi-threaded test
  setups. Mitigation: house both pieces of state in a single
  record under one `IORef`. Captured as a Slice-3 review item.
- **R3 — Storage growth.** The new column doubles disk footprint
  per inserted fact in the worst case (large raw values). The
  cage layer's expected fact size is small (kilobytes at most),
  so practical impact is negligible on devnet. Noted; no action
  needed.
- **R4 — Existing journaled `InvTrieInsert` ops carry
  `(hashBS k)` as the "value".** This is only a problem if we
  preserve pre-migration RocksDB state across the bump. The plan
  recommends re-index from genesis in Q-002 specifically to
  sidestep this.

## Notes

- The slice order is bisect-safe by construction: slice 1 is a
  pure plumbing add (build still green, tests untouched); slice 2
  introduces the persistent-side write/read change with its tests;
  slice 3 introduces the pure-side parallel; slice 4 replaces the
  e2e shape check; slice 5 records the decision and tightens the
  gate; slice 6 finalizes.
- Slices 2 and 3 are independent and could be parallelised, but
  sequencing them keeps the per-pane workload focused and avoids
  cross-cutting reviews.
- The driver+navigator pair is fresh per slice (`/clear`'d between
  slices per `resolve-ticket`).
