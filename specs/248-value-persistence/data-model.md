# Phase 1 Data Model: Value persistence for the fact lookup endpoint

**Branch**: `248-value-persistence` | **Plan**: [plan.md](./plan.md)
| **Spec**: [spec.md](./spec.md) | **Research**: [research.md](./research.md)

## Entities

### `RawFactValue` (storage-level)

The bytes a requester originally supplied as the `value` of an
Insert or Update request, scoped to a single `(token, key)` pair.

| Field | Type | Source | Notes |
|---|---|---|---|
| token | `TokenId` | request datum's `requestToken` | Namespace key |
| key | `ByteString` | request datum's `requestKey` (raw) | The exact bytes the requester sent — no normalisation |
| value | `ByteString` | request datum's `requestValue` payload | The exact bytes the requester sent — no normalisation, no re-encoding |

This entity is not a domain object exposed via Haskell types. It is
the abstract content of the new RocksDB column family `TrieRawValues`,
described below.

## Storage layout

### New RocksDB column family: `TrieRawValues`

| Property | Value |
|---|---|
| Name (in `cageColumnFamilies`) | `"trie-raw-values"` |
| Position in `allColumnFamilies` | 7th cage CF (after `trie-meta`), 13th overall (before `composed-rollbacks`) |
| GADT constructor | `TrieRawValues :: AllColumns (KV ByteString ByteString)` |
| Key codec | `identityPrism :: Prism' ByteString ByteString` (passthrough) |
| Value codec | `identityPrism :: Prism' ByteString ByteString` (passthrough) |
| Key shape | `tokenPrefix tid <> requestKey` (concatenation, no separator) |
| Value shape | The raw value bytes the requester supplied |

`tokenPrefix tid` is the existing length-prefixed encoding from
`Cardano.MPFS.Trie.Persistent`:

```haskell
tokenPrefix :: TokenId -> ByteString
tokenPrefix (TokenId (AssetName sbs)) =
    let raw = SBS.fromShort sbs
        len = BS.length raw
    in  BS.singleton (fromIntegral len) <> raw
```

The 1-byte length prefix gives a parseable boundary between the token
namespace and the user-supplied key suffix, so prefix iteration over
a token's raw values is deterministic.

### Existing column families (unchanged)

| CF name | GADT | Purpose | This feature's relationship |
|---|---|---|---|
| `trie-nodes` | `TrieNodes` | MPF node store (`KV HexKey (HexIndirect MPFHash)`) | Untouched. Merkle layer continues to retain only the value-hash. |
| `trie-kv` | `TrieKV` | MPF leaf hashes (`KV HexKey MPFHash`) | Untouched. Mirror of value-hashes the merkle layer queries. |
| `trie-meta` | `TrieMeta` | Per-token visibility (`KV TokenId TrieStatus`) | Untouched. Visibility check still gates `withTrie`. |

### CF ordering invariant

```text
allColumnFamilies =
    utxoColumnFamilies                  -- 6 CFs (UTxO/CSMT, including journal & runner rollbacks)
        <> cageColumnFamilies            -- 7 CFs (was 6, +1 for TrieRawValues)
        <> [("composed-rollbacks", _)]   -- 1 CF (chain-follower)
                                         -- = 14 total (was 13)
```

The destructuring sites in `Cardano.MPFS.Application` and any test
that opens a cage-only DB pattern-match positionally on this list;
they all extend by one slot.

## Lifecycle / state transitions

### Per `(token, key)` pair

```text
                                  ┌───────────────────────────────┐
                                  │     [absent in storage]       │
                                  └───────────┬───────────────────┘
                                              │
              ┌───────────────────────────────┤
              │ Insert (k, v) processed       │
              ▼                               │
   ┌────────────────────┐                     │
   │  raw value = v     │◀───────────────┐    │
   │  in TrieRawValues  │                │    │
   └─────┬──────────────┘                │    │
         │                               │    │
         │ Update (k, v')                │    │
         ▼                               │    │
   ┌────────────────────┐                │    │
   │  raw value = v'    │                │    │
   │  in TrieRawValues  │                │    │
   └─────┬──────────────┘                │    │
         │                               │    │
         │ Delete k                      │    │
         ▼                               │    │
              ┌───────────────────────────────┤
              │ Rollback of any of the above  │
              │  → restored to immediately    │
              │    prior state                │
              └───────────────────────────────┘
```

Transitions are produced by the chain-follower's `applyRequestOp`
inside the same `Transaction m cf AllColumns ops` that mutates
`TrieNodes` / `TrieKV`. Rollback is produced by the existing
`applyCageInverses` replaying through the same `Trie.insert` /
`Trie.delete` interface.

### Sequencing rule

For a single block containing operations `op1, op2, ...` against the
same `(token, key)`, the final state of `TrieRawValues` after the
block commits is the state after the last operation:

| Within a block | Final raw value state |
|---|---|
| Insert v | present, v |
| Insert v then Delete | absent |
| Insert v then Update v' | present, v' |
| Insert v then Delete then Insert v'' | present, v'' |

This matches the spec Edge Case "Insert then delete in the same
block": after the block commits, the lookup returns absent.

## Invariants

### INV-1 — Atomicity with merkle state

For every committed block N and every `(token, key)`:

```text
(present in TrieRawValues at block N) ⇔ (present in TrieKV at block N)
```

Mechanically enforced by routing every raw-value mutation through
the same `Transaction` as the corresponding `TrieKV` / `TrieNodes`
mutation. Violation would be visible to clients as one of:

- Lookup returns absent for a key whose merkle proof is present.
- Lookup returns present bytes for a key whose merkle proof is absent.

INV-1 is the storage-layer expression of FR-003.

### INV-2 — Rollback equivalence

For any block sequence ending in a rollback to chain tip T:

```text
∀ (token, key). lookup_after_rollback(token, key)
            ≡ lookup_at_tip_T_in_alternate_world(token, key)
```

where the "alternate world" is the storage state in which the
rolled-back blocks had never been observed. INV-2 is the storage-layer
expression of FR-004.

INV-2 holds inductively: each `applyCageInverses` call replays the
inverse op via `Trie.insert` / `Trie.delete`, which write to both the
merkle columns and `TrieRawValues` atomically; therefore the state
after rollback matches the state at tip T.

### INV-3 — Codec passthrough

The key and value codecs for `TrieRawValues` are
`prism' id Just`. No CBOR wrapping, no length prefix on the value, no
escape encoding. The bytes the requester supplied land in RocksDB
verbatim, and `Trie.lookup` returns them verbatim.

INV-3 is the storage-layer expression of the spec's "no re-encoding,
no normalisation" assumption.

## Non-entities (out of scope here)

- **Migration from pre-#248 databases.** Per spec Clarification
  2026-04-30, existing devnet/preprod databases are wiped and
  re-synced. No on-disk schema-version field, no detection logic, no
  guided migration step.

- **`Request` / `Operation` / `LocatedTokenState`.** These are
  unchanged. The on-chain wire format is unchanged. The `requestKey`
  / `requestValue` fields are already raw `ByteString` in
  `Cardano.MPFS.Core.Types`; the codec in `requestPrism` already
  carries them through verbatim (Codecs.hs:269).

- **`CageInverseOp`.** The `InvTrieInsert` / `InvTrieDelete`
  constructors and their CBOR encoding are unchanged. See research.md
  §1.

- **`FactResponse` / HTTP wire shape.** Unchanged. See contracts/.
