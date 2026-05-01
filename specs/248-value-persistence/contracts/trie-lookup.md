# Contract: `Cardano.MPFS.Trie.Trie.lookup` semantic change

**Branch**: `248-value-persistence` | **Type**: internal record-of-functions boundary

This contract describes the only externally-observable interface
change introduced by this feature: the meaning of the `lookup` field
on the `Trie m` record-of-functions.

## Scope of this contract

`Trie m` is an internal seam, not an HTTP/JSON contract. It is
consumed by:

- `Cardano.MPFS.HTTP.Server` (the fact-lookup handler)
- `Cardano.MPFS.Indexer.Follower` (`applyRequestOp`,
  `applyCageInverses`)
- `Cardano.MPFS.TxBuilder.*` (speculative trie sessions for fee
  estimation)
- Unit tests in `test/Cardano/MPFS/`

The HTTP wire shape (`FactResponse`'s JSON encoding, status codes,
URL paths) is **not** changed by this work. See "HTTP wire shape" at
the bottom of this file.

## Interface

```haskell
data Trie m = Trie
    { insert        :: ByteString -> ByteString -> m Root
    , delete        :: ByteString -> m Root
    , lookup        :: ByteString -> m (Maybe ByteString)
    , getRoot       :: m Root
    , getProof      :: ByteString -> m (Maybe Proof)
    , getProofSteps :: ByteString -> m (Maybe [ProofStep])
    }
```

The type signatures are unchanged. The change is in the meaning of
`lookup`.

## Semantic change

### Before this feature

```haskell
lookup :: ByteString -> m (Maybe ByteString)
-- For any key k:
--   present in trie ⇒ Just (renderMPFHash (mkMPFHash k))
--                     i.e. the hash of the *key*, not the value
--   absent in trie  ⇒ Nothing
```

The pre-existing `lookup` returns 32 bytes of hash-of-key sentinel
when the key exists. This is the bug postmortem #248 documents: a
type-correct lie that satisfies `Maybe ByteString` and exists only
because the MPF trie discards raw values at insert time.

### After this feature

```haskell
lookup :: ByteString -> m (Maybe ByteString)
-- For any key k:
--   present in trie ⇒ Just v
--                     where v is the exact bytes the requester
--                     supplied as the `value` field of the most
--                     recent Insert/Update request for k
--   absent in trie  ⇒ Nothing
```

The new `lookup` returns the raw value bytes from the new
`TrieRawValues` column family. Empty value (`BS.empty`) is a
legitimate present result and is distinct from `Nothing`.

## Behavioural requirements

For all implementations of `Trie m` (transactional, persistent IO,
speculative IO, pure in-memory):

### LR-1 — Round-trip on insert

```haskell
trie.insert k v >> trie.lookup k = pure (Just v)
```

For all `k :: ByteString`, `v :: ByteString` (including
`v = BS.empty`).

### LR-2 — Round-trip on delete

```haskell
trie.insert k v >> trie.delete k >> trie.lookup k = pure Nothing
```

For all `k`, `v`.

### LR-3 — Last-write-wins on update

```haskell
trie.insert k v1 >> trie.insert k v2 >> trie.lookup k = pure (Just v2)
```

Equivalently for an `Update` request operation (which `applyRequestOp`
implements as `delete` then `insert`).

### LR-4 — Independence across keys

```haskell
trie.insert k1 v1 >> trie.insert k2 v2 >> trie.lookup k1 = pure (Just v1)
```

For all distinct `k1`, `k2`. The token prefix in the storage layout
guarantees this across tokens too.

### LR-5 — Independence across tokens

For tries `t_a` and `t_b` belonging to distinct `TokenId`s:

```haskell
t_a.insert k v1 >> t_b.insert k v2 >> t_a.lookup k = pure (Just v1)
```

The trie's prefix scoping (via `tokenHexPrefix` and `tokenPrefix`)
guarantees this.

### LR-6 — Atomicity with merkle root (transactional layer only)

When `Trie m` is the transactional variant
(`mkUnifiedTrie pfx`, used by the chain-follower's per-block
transaction):

```text
After commit of a transaction containing trie.insert k v:
  - getRoot returns the new merkle root incorporating k → hash(v)
  - lookup k returns Just v
  - both observations are simultaneous (no in-between window)
```

This is the storage-layer expression of FR-003 / INV-1. Mechanically
guaranteed by both operations writing into the same RocksDB
`Transaction`.

## HTTP wire shape (unchanged)

For reference, the fact-lookup endpoint that consumes this interface:

- **Path:** `GET /tokens/:id/facts/:key`
- **Response shape:** `FactResponse` envelope as defined in slice 3
  (PR #246 / branch `243-proof-redesign`). This work changes which
  bytes go into the `value` field, **not** the field's existence,
  type, name, or JSON encoding.
- **Status codes:** unchanged. 200 for present, 404 for absent.

The wire-level acceptance test is in `e2e-test/Cardano/MPFS/E2E/
ProofsSpec.hs` and asserts byte-equality between the inserted bytes
and the bytes returned in the `value` field.

## Implementation surface

Each `Trie m` constructor must satisfy LR-1 through LR-6:

| Constructor | Module | Backing store for raw values |
|---|---|---|
| `mkUnifiedTrie` | `Trie/Persistent.hs` | `TrieRawValues` CF inside the unified `Transaction` |
| `mkPersistentTrie` | `Trie/Persistent.hs` | `TrieRawValues` CF via `mkPrefixedTrieDB` (4th CF arg) |
| `mkSpeculativeTrie` | `Trie/Persistent.hs` | Same `TrieRawValues` CF, wrapped in `runSpeculation` |
| `mkPureTrie` / `mkPureTrieFromRef` | `Trie/Pure.hs` | New sibling `IORef (Map ByteString ByteString)` |
