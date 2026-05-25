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

### Slice 1 — Plumbing: add `TrieRawValues` column + codec + RocksDB wiring + fail-loud startup check

**Goal.** Add the new column-family type, codec, and on-disk
config. Wire the INV-3 startup check (A-002 follow-through #2): if
the trie columns carry pre-migration data and the new
`TrieRawValues` column is empty, refuse to start with a structured
error naming the resync step. After this slice, the indexer opens
fresh DBs with 14 column families, refuses stale DBs loudly, and
otherwise ignores `TrieRawValues` (the trie operations still
return `hashBS k` until Slice 2).

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

**RED.** Two `PersistentSpec` tests:

- `opens_with_trie_raw_values_column` opens a fresh
  `withApplication` and confirms `TrieRawValues` is queryable as
  an empty CF (no rows). Currently fails because the constructor
  does not exist.
- `refuses_to_start_on_stale_schema` opens an existing DB whose
  `TrieKV` has rows and whose `TrieRawValues` is empty, and
  expects `withApplication` to throw a structured
  `SchemaMigrationRequired` exception (or equivalent) whose
  message names "drop the RocksDB directory and resync from
  genesis". Test fabricates the pre-migration state by inserting
  directly into `TrieKV` via `KV.put` and skipping the
  corresponding `TrieRawValues` write. Currently fails because no
  such check exists.

**GREEN.** Add the constructor + codec + CF config + startup
pre-flight. The startup check runs inside `withApplication` after
RocksDB opens and before the runtime returns control. The tests
pass.

**Tasks.** `T001-S1` (CF + codec + RocksDB wiring), `T002-S1`
(opens-with-fresh-DB RED/GREEN), `T003-S1` (fail-loud
startup-check RED/GREEN).

> Note: tasks below this slice renumber by 1 — Slice 2 starts at
> `T004-S2` and so on.

### Slice 2 — Persistent trie: write + delete + lookup via `TrieRawValues`

**Goal.** Make `unifiedInsert` / `unifiedDelete` write both
columns inside the same transaction. Make `unifiedLookup` read
from `TrieRawValues`. Both atomicity invariants land here.

**Scope correction (2026-05-25, navigator-surfaced).** Earlier
drafts of this plan named "the `unifiedManager` variants at lines
942, 963, 1039, 1059" as in-scope. **They are not.** Those
functions are `persistentInsert` / `persistentDelete` /
`speculativeInsert` / `speculativeDelete` / `speculativeLookup`
operating on the `MPFStandalone HexKey MPFHash MPFHash` schema
(`MPFStandaloneKVCol` / `MPFStandaloneMPFCol`), used by the
TxBuilder speculative dry-run path. `TrieRawValues` lives in
`AllColumns`, which the speculative path does not address —
"same treatment" would not typecheck and is out of scope. The
broken `/tokens/:id/facts/:key` endpoint reads via the
unified path, not the speculative path. The speculative-path
asymmetry (`speculativeLookup` at ~line 1089 still returns
`Just (hashBS k)`) is documented as a follow-up in Slice 5's
research note + a new task `T013-S5`.

**Files.**

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
  - `unifiedInsert` (~line 180): after the existing
    `inserting … TrieKV TrieNodes …`, call
    `KV.put TrieRawValues (byteStringToHexKey (hashBS k)) v`
    inside the same `Transaction m cf AllColumns ops` body.
  - `unifiedDelete` (~line 197): after `deleting …`, call
    `KV.delete TrieRawValues (byteStringToHexKey (hashBS k))`.
  - `unifiedLookup` (~line 212): replace
    `Just _ -> Just (hashBS k)` with
    `Just _ -> KV.query TrieRawValues hexKey` (or equivalent).
    The proof guard stays — `unifiedLookup` still returns
    `Nothing` if the MPF inclusion proof can't be built, even if
    the raw column happened to have a row (defence in depth).
  - The `persistent*` / `speculative*` functions ~lines
    930–1089 are **out of Slice 2 scope** — they operate on a
    different column-family schema; do not touch them.

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

**Tasks.** `T004-S2`, `T005-S2`, `T006-S2` (rollback test).

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

**Tasks.** `T007-S3`, `T008-S3`.

### Slice 4 — E2E: replace `assertFactEnvelope` with verifier round-trip

**Goal.** Per the acceptance criteria, the e2e for the proof
redesign no longer asserts structural fields only — it decodes
the response into the typed wire shape and runs the offline
verifier.

**Scope expansion (2026-05-25).** `verifyFactPresentResponse`
does not yet exist in `cardano-mpfs-client`. Slice 4 therefore
covers BOTH (a) adding the new verifier to
`cardano-mpfs-client/Facts.hs` following the existing
`verifyXFacts` pattern (taking a `TrustedRoot` and returning
`Either VerifyError VerifiedFactPresentFacts`), and (b) wiring
the e2e to use it. May need a new typed shape `FactPresentFacts`
in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs` —
driver's call whether to reuse `FactResponse` directly or extract
a leaner shape.

**Files.**

- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs` — add
  `FactPresentFacts` / `FactAbsentFacts` if a leaner shape is
  preferred over reusing the existing `FactResponse`.
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs` — add
  `verifyFactPresentFacts` / `verifyFactAbsentFacts` mirroring
  the existing per-endpoint verifier shape; export
  `VerifiedFactPresentFacts` / `VerifiedFactAbsentFacts`
  newtypes.
- `cardano-mpfs-client/test/Cardano/MPFS/Client/...` — unit
  tests for the new verifiers (happy path + tampered cases).
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
  - Replace (or supplement) `assertFactEnvelope` with a call to
    the new verifier. Trusted root from the same response's
    snapshot (mirrors `endFactsTrustedRoot` ~line 495).
  - Update the haddock about "MPFS stores values as 32-byte
    content hashes" (~lines 451–454) — now wrong.
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
  — touch any matrix entry that asserted on the structural-only
  shape (look around lines 920–950).

**RED.** Run the e2e with the structural assertion replaced by
the verifier call — pre-slice-2/3 it fails with
`MpfReplayFailed "root mismatch"`. The Slice 2/3 fixes flip it
green.

**GREEN.** Slices 2 + 3 already shipped the trie-side fix. This
slice just lands the e2e update; the test now passes.

**Tasks.** `T009-S4` (new verifier API), `T009b-S4` (e2e
wiring), `T010-S4` (FactsMatrixSpec sweep).

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
  pointing **only** at the unified-path call sites in
  `Trie/Persistent.hs` (unifiedLookup) and the entire
  `Trie/Pure.hs`. The speculative-path call site at
  `Trie/Persistent.hs` ~line 1089 still legitimately uses
  `Just (hashBS k)` after this slice (see Slice 2's scope
  correction and `T013-S5` below); the sentinel must not
  false-positive on it.

**RED.** Not applicable — this is documentation + a sentinel.
`gate.sh` runs and the sentinel passes.

**Tasks.** `T011-S5`, `T012-S5`, `T013-S5`.

- `T013-S5` is a documentation-only task: add a section to
  `specs/243-proof-redesign/research.md` (or a comment block at
  the head of `Trie/Persistent.hs`'s speculative section) noting
  the deferred speculative-path asymmetry: `speculativeLookup` /
  `persistentLookup` still return `Just (hashBS k)` because they
  operate on the `MPFStandalone*Col` schema which has no
  `TrieRawValues` analogue. Captures the asymmetry for future
  readers; no code change.

### Slice 6 — Drop `gate.sh` (finalization)

**Goal.** Standard resolve-ticket finalization. After the
finalization audit passes, drop the gate and mark the PR ready.

**Files.**

- `gate.sh` — removed.

**Commit.** `chore: drop gate.sh (ready for review)`. No
`Tasks:` trailer (chore allowed by the commit gate).

**Tasks.** `T014-S6`.

### Slice 7 — E2E verifier regression: diagnose and fix (post-mark-ready CI failure)

**Goal.** After the initial Slice 6 mark-ready, CI run
https://github.com/lambdasistemi/cardano-mpfs-offchain/actions/runs/26400824147
turned the e2e job RED at HEAD `814446a` with a deterministic
failure in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
`Proof-bearing envelopes E2E "read and write envelopes carry
verifiable proofs"` and the
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
matrix entries:

```
verifyFactPresentFacts failed:
  MpfReplayFailed "fact_present.fact.mpf_proof" "root mismatch"
```

The verifier itself is internally consistent (uses
`factWitnessTrieRoot frFact` = `wtsState.root` for the trie
proof, `frSnapshot.utxoRoot` for the snapshot/state checks). The
unit tests in `cardano-mpfs-client/test/...` passed because
they built fixtures where root/proof/value were guaranteed
coherent. The live indexer-driven flow exposes a gap between
the proof bytes the server generates and the trie root the
server embeds in `tokenState.root` (the on-chain state datum).

**Diagnosis hypotheses (driver to confirm by inspection +
local reproduction):**

1. The server-side `requireMpfProof` extracts the proof from
   the indexer's current trie, but the on-chain
   `LocatedTokenState.tokenState.root` field carries the root
   from a different point in time (e.g. the state UTxO carries
   the **pre-update** root because the current state UTxO is
   the boot UTxO that hasn't been replaced by an update yet —
   but the trie HAS been mutated by request processing).
2. A subtle mismatch in how the proof is generated vs how
   `tokenState.root` is computed (e.g. different hashing
   convention, wrong `tokenHexPrefix`, off-by-one in the
   trie node addressing).
3. A Slice 1-2 regression in how `unifiedInsert` updates
   `TrieKV` — silently writes a slightly different
   `mkMPFHash v` for some inputs.

**Verification strategy:**

The driver MUST reproduce the failure locally (the e2e
harness uses a cardano-node subprocess; the existing
`just e2e` recipe runs it). Once reproduced, capture the
exact proof bytes, root bytes, key bytes, value bytes, and
the trie state at the lookup point. Pinpoint which of the
three hypotheses applies; if none, surface a `Q-NNN` to the
ticket-orchestrator.

**Files (provisional — depend on the diagnosis).**

- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
  (`tokenFactHandler` + `requireMpfProof`) — likely the
  proof-generation or root-embedding bug.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
  (`unifiedGetRoot` / `unifiedGetProof`) — possibly the trie
  side.
- `cardano-mpfs-offchain/test/...` — add a unit-level
  regression test that reproduces the bug at the indexer
  layer (does not require cardano-node), so we don't regress
  again.
- Possibly the `cardano-mpfs-client/test/...` unit tests if
  the verifier itself has a bug that the unit fixtures
  masked.

**Strict constraint.** Do NOT relax the verifier (e.g.
swap the trie-root source) without first proving where the
disagreement comes from. The verifier asserts a real
cryptographic invariant; relaxing it would re-introduce the
class of bug postmortem #248 surfaced.

**RED.** A unit-level test reproducing the proof/root
mismatch at the indexer layer (no cardano-node). Plus the
e2e itself (already failing on CI; the diagnosis confirms
the local repro).

**GREEN.** The fix lands at whichever layer the diagnosis
identifies. The unit-level regression test passes. `./gate.sh`
passes. The e2e (`just e2e`) passes.

**Tasks.** `T015-S7` (diagnose + repro), `T016-S7` (fix),
`T017-S7` (regression unit test).

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
