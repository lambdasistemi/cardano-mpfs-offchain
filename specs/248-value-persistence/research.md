# Phase 0 Research: Value persistence for the fact lookup endpoint

**Branch**: `248-value-persistence` | **Plan**: [plan.md](./plan.md) | **Spec**: [spec.md](./spec.md)

This document resolves the technical unknowns flagged in the Technical
Context section of the plan. Each section below pairs a question with
the chosen path, the reasoning, and the alternatives considered.

## §1 — Inverse-op carrier extension

**Question.** Do the `InvTrieInsert` / `InvTrieDelete` carriers in
`Cardano.MPFS.Indexer.Event` need to grow a field for the prior raw
value, so rollback can restore it?

**Decision.** **No carrier changes.** The carriers already pass raw
value bytes (`InvTrieInsert !TokenId !ByteString !ByteString`); their
on-disk CBOR shape stays identical. The fix is purely in `Trie.lookup`
semantics and in `Trie.insert` / `Trie.delete` mirroring writes to the
new column.

**Rationale.** Tracing the existing flow in
`Cardano.MPFS.Indexer.Follower.applyRequestOp` (Follower.hs:391):

```haskell
Insert v -> do
    oldVal <- trieLookup requestKey
    void $ trieInsert requestKey v
    pure $ case oldVal of
        Nothing  -> [InvTrieDelete tid requestKey]
        Just old -> [InvTrieInsert tid requestKey old]
```

The carrier already holds whatever `trieLookup` returned. Today that
return is `Just (hashBS k)` (Trie/Persistent.hs:234), so the rollback
is byte-compatible-but-meaningless. Once `trieLookup` returns the real
prior value bytes from the new `TrieRawValues` column, `old` becomes
the actual prior value, and `applyCageInverses` (Follower.hs:493) —
which already replays via `withTrie tm tid $ \trie -> insert trie key
val` — restores both the merkle layer and the raw-value mirror in one
call, because the underlying `insert` writes to both columns.

This is the smallest intervention that satisfies FR-004: it leaves the
existing rollback carrier wire format and decoder untouched, which is
important because rollback points are persisted on disk.

**Alternatives considered.**

- **Carrier extension (`InvTrieInsert tid k oldHash oldRawValue`).**
  Rejected. Forces a CBOR tag bump in `encodeInvOp` /
  `decodeInvOp` (Codecs.hs:562), breaks any in-flight rollback points
  in existing devnet databases (we wipe-and-resync per spec
  Clarification 2026-04-30, but compounds the disruption),
  and duplicates information already implied by the trie state.

- **Two-step rollback (replay old root, then re-derive raw value).**
  Rejected. There's no inverse map from value-hash to raw bytes; the
  whole point of this work is that the raw value was discarded.

## §2 — Read-path bridge wiring

**Question.** How does the HTTP fact-lookup handler reach the new
raw-value storage, given that the existing `Trie.lookup` is the only
read interface and `mkPersistentTrieManager` (used by the HTTP path)
is wired to its own RocksDB column-family handles?

**Decision.** **Thread one extra `ColumnFamily` argument through
`mkPersistentTrieManager` / `withPersistentTrieManager` and through
`mkPrefixedTrieDB`.** `Trie.lookup` (and its IO/speculative variants)
queries the new column; the HTTP handler reads through the same
`Trie.lookup` it already uses. No new sibling primitive on `Context`,
no parallel read path. (Confirmed by spec Clarification 2026-05-01.)

**Rationale.** The HTTP handler's call site is
`withTrie trieMgr tid $ \trie -> lookup trie key`. Every variant of
`Trie` (transactional `mkUnifiedTrie`, IO `mkPersistentTrie`,
speculative `mkSpeculativeTrie`, pure `mkPureTrieFromRef`) already has
a `lookup` field. Changing the field's implementation in each variant
to query the raw-value store is a focused, type-checked change. The
chain-follower path uses `mkUnifiedTrieManager` over `AllColumns`, so
the `TrieRawValues` selector reaches it for free once added to the
GADT.

`withPersistentTrieManager` currently opens 3 CFs (`nodes`, `kv`,
`meta`) and pattern-matches `case columnFamilies of [nodesCF, kvCF,
metaCF] -> ...` (Trie/Persistent.hs:456). It grows by one entry to
`[nodesCF, kvCF, metaCF, rawValuesCF]`. `mkPersistentTrieManager`'s
arity grows by one `ColumnFamily` parameter and threads it through
each persistent helper.

**Alternatives considered.**

- **Sibling primitive on `Context`** (e.g.
  `lookupRawValue :: TokenId -> ByteString -> IO (Maybe ByteString)`).
  Rejected by spec clarification — splits the read interface, requires
  every consumer (HTTP handler, future verifier wiring, CLI inspect
  tool) to pick the right primitive, and decouples the raw-value
  storage from the trie's visibility/hidden checks.

- **Encode the raw value into the existing `TrieKV` value column**
  (e.g. CBOR-pack `(MPFHash, ByteString)`).
  Rejected. The MPF library reads `TrieKV` directly via its own codec
  (`mpfHashCodecs`); changing the value shape breaks the merkle layer
  or forces an MPF library change. Keeping a separate column also
  isolates the raw-value bytes from MPF iteration logic.

## §3 — Key derivation in `TrieRawValues`

**Question.** What's the on-disk key shape for the new column? The
existing trie columns use `HexKey` (nibble-split MPF paths); does
`TrieRawValues` need to match?

**Decision.** **Composite raw-byte key
`tokenPrefix tid <> requestKey`**, where `tokenPrefix` is the existing
length-prefixed token serialization
(`BS.singleton (length raw) <> raw`, Trie/Persistent.hs:151). Value:
the raw value bytes as the requester supplied them. Codecs are
identity prisms (no CBOR wrapping).

**Rationale.** The new column is a flat KV store, not a trie. There's
no merkle structure to maintain over it, no proofs derived from it,
no iteration order requirement. The token prefix gives us the same
namespace isolation the MPF columns already get; reusing `tokenPrefix`
(not `tokenHexPrefix`) keeps the key space dense and avoids the 2×
expansion of nibble encoding.

The request key is used directly. The MPF layer hashes keys for path
construction (`byteStringToHexKey (hashBS k)`) so that the merkle tree
is balanced; the raw-value column has no such constraint, so no
hashing is applied — the request key bytes go in verbatim. This means
a future operator inspecting RocksDB can grep raw keys directly.

**Alternatives considered.**

- **Hash the key to match the MPF column's key derivation** (i.e.
  `tokenPrefix tid <> hashBS requestKey`).
  Rejected. Adds work without benefit — the raw-value column is never
  joined to the MPF nodes column at the storage layer, so the key
  shapes don't need to align. Keeping raw keys also makes
  `mpfs-inspect-db` output human-readable.

- **Per-token sub-database / separate CF per token.**
  Rejected. Existing trie columns are single shared CFs with prefix
  isolation; this work follows the established pattern. Per-token CFs
  would also force schema migration on every mint.

## §4 — Pure implementation parity

**Question.** Where does `Cardano.MPFS.Trie.Pure` keep the raw value
bytes, given it's an in-memory `IORef` over `MPFInMemoryDB`?

**Decision.** **Add a sibling `IORef (Map ByteString ByteString)` for
raw values**, threaded into `mkPureTrieFromRef` alongside the existing
MPF database ref. `pureLookup` reads from this map; `pureInsert` /
`pureDelete` mutate it.

**Rationale.** `MPFInMemoryDB` is owned by the `mts:mpf` library and
its shape is fixed. Wedging raw values inside it would require an
upstream library change — out of scope. A sibling IORef is the
simplest, locally-owned mirror that keeps `Trie IO` honest in unit
tests (`TrieSpec`, `TrieManagerSpec`) and in any test that uses
`mkPureTrie` as a stand-in for the persistent backend.

The pair (`MPF database ref`, `raw-value map ref`) gets passed
together by the trie manager (`PureManager`) so the two stay in
lockstep within a single `Trie`.

**Alternatives considered.**

- **Skip the pure backend update; only fix the persistent backend.**
  Rejected. Unit tests rely on the pure backend matching persistent
  semantics; diverging them re-introduces the same class of bug
  this work fixes (test theatre that only checks structural shape).

## §5 — Test surface

**Question.** Where do the new tests live and what do they cover?

**Decision.** Three test surfaces:

1. **E2E (`e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`).** The existing
   fixture inserts known `(key, value)` pairs through the live
   request → process flow. Replace `assertFactEnvelope`'s
   `not . T.null` check with a byte-equality assertion against the
   inserted value (FR-005 / SC-001 / SC-003).

2. **Unit rollback fixtures (`test/Cardano/MPFS/Indexer/RollbackSpec.hs`).**
   Cover Insert / Update / Delete under single-block and multi-block
   reorgs. Each scenario asserts that `Trie.lookup` after rollback
   returns byte-identical results to the lookup that would have run
   at the rolled-back tip (SC-002).

3. **Trie unit suite (`test/Cardano/MPFS/Trie/PersistentSpec.hs`,
   `Cardano.MPFS.TrieSpec`, `Cardano.MPFS.TrieManagerSpec`).** Add
   round-trip cases: insert raw value → lookup returns the same bytes
   (including empty `BS.empty` and a moderately large blob to
   exercise the size axis from spec Edge Cases).

The pre-existing `assertFactEnvelope` helper is removed entirely
(SC-003); its only callers are in `ProofsSpec.hs`.

**Rationale.** Three layers, three failure modes:

- E2E catches "the bug we shipped" (postmortem #248) by exercising
  the wire-level contract.
- Rollback unit tests catch "we broke the atomicity invariant" by
  driving the inverse-op machinery directly without the chain-sync
  setup.
- Trie unit tests catch "we broke the storage layer" without the
  follower or HTTP layers in the loop.

**Alternatives considered.**

- **E2E only.** Rejected. E2E doesn't exercise rollback paths
  end-to-end (no devnet reorg fixture); SC-002 needs unit-level
  reorg drivers.

## §6 — `cageColumnFamilies` ordering

**Question.** Where in the existing 6-CF cage list does the new
column go, and does ordering matter?

**Decision.** Append `("trie-raw-values", dbConfig)` as the 7th
entry, after `trie-meta`. The corresponding GADT constructor
`TrieRawValues` goes last in `AllColumns`, after `TrieMeta`.

**Rationale.** RocksDB CF ordering must match between
`allColumnFamilies` (Application.hs:256) and the destructuring sites
(Application.hs and the test fixtures). Appending preserves the
existing positional unpacking for the 6 prior columns; the new
position consumes the new last slot. The `GEq` / `GCompare` instances
extend with the standard lexicographic-by-constructor pattern.

`UnifiedColumns` doesn't need changes — it's parametric over
`AllColumns x`, so the new constructor reaches it for free via
`InCage`. `allUnifiedCodecs` similarly inherits the new entry from
`allCodecs` via `DMap.mapKeysMonotonic InCage allCodecs`.

**Alternatives considered.**

- **Insert between `TrieKV` and `TrieMeta`** (so all "trie data"
  columns are contiguous before "trie metadata").
  Rejected. Forces re-pinning of every positional unpack site and
  the on-disk CF order. Append is the boring choice with the
  smallest blast radius.
