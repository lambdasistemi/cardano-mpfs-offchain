# Quickstart: Value persistence for the fact lookup endpoint

**Branch**: `248-value-persistence` | **Plan**: [plan.md](./plan.md)
| **Spec**: [spec.md](./spec.md) | **Data Model**: [data-model.md](./data-model.md)

This document is the developer/operator on-ramp for the value
persistence work. It does **not** repeat content from spec, plan, or
research — those are linked above. It tells you what to do once the
work has landed.

## What changed for users of the offchain

`GET /tokens/:id/facts/:key` now returns the bytes the requester
originally inserted in the response's `value` field. Previously it
returned `mkMPFHash key` (a hash of the *key*) — see postmortem #248.

The HTTP wire shape (`FactResponse` envelope, JSON encoding, status
codes) is **unchanged**. Existing clients that did not depend on the
broken `value` field semantics keep working. Clients that took the
broken value at face value now get correct bytes — which may be a
breaking change in the sense that "garbage you trusted" becomes
"truth that doesn't match the garbage you persisted."

## What changed for operators

### Database

The RocksDB schema gains one column family: `trie-raw-values`. It is
appended to the cage CF list, so it is the 7th cage CF and the 13th
overall (before `composed-rollbacks`).

**No migration is provided.** Existing devnet/preprod databases must
be wiped and resynced as part of normal upgrade hygiene (per spec
Clarification 2026-04-30). Concretely:

```bash
# Stop the offchain
just stop                    # or systemctl stop, etc.

# Wipe the database directory
rm -rf path/to/rocksdb/dir

# Restart — the offchain will sync from genesis (or your configured
# starting point) and populate trie-raw-values as it processes blocks.
just run                     # or systemctl start, etc.
```

A pre-#248 database opened by post-#248 code will fail at CF-open
time because the new CF doesn't exist on disk; the error will name
`trie-raw-values`. That is the intended detection — not a guided
migration step, just a clear failure mode.

### Disk usage

Storage growth is bounded by the size of values the on-chain
protocol accepts in request datums. There is no new growth model
beyond what already exists for processed requests.

## What changed for developers

### Reading raw values from code

Use the existing `Trie.lookup` interface — its semantics are now
correct. Example:

```haskell
withTrie trieMgr tokenId $ \trie -> do
    mValue <- lookup trie key
    case mValue of
        Just v  -> -- v is the exact bytes the requester supplied
                   handlePresent v
        Nothing -> -- key was never inserted, or was deleted
                   handleAbsent
```

There is no sibling primitive on `Context` for raw value lookup. If
you find yourself wanting one, re-read the §2 of research.md — the
single read seam is intentional.

### Reading raw values from a checked-out RocksDB

The `mpfs-inspect-db` executable should be able to dump the new
column. Composite key shape: `tokenPrefix tid <> requestKey`, where
`tokenPrefix` is `BS.singleton (length asset_name) <> asset_name`.
Strip the first byte and the next `length` bytes from any key in
`trie-raw-values` to recover the requester-supplied key bytes.

### Adding a new `Trie m` implementation

Any new `Trie m` constructor must satisfy LR-1 through LR-6 in
`contracts/trie-lookup.md`. The simplest way to verify is to plug
the new constructor into `Cardano.MPFS.TrieSpec` and re-run the unit
suite — the round-trip cases there exercise all six requirements.

## How to verify the fix locally

### E2E — the byte-equality acceptance test

The fact-lookup E2E in `e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
inserts a known `(key, value)` pair through the live request →
process flow and asserts byte-equality on the response's `value`
field.

```bash
just e2e
```

A passing run is the primary acceptance signal for SC-001.

### Unit — rollback fixtures

The rollback unit suite covers Insert / Update / Delete under
single-block and multi-block reorgs (SC-002):

```bash
just unit
# or, scoped:
cabal test unit-tests --test-options="--match \"RollbackSpec\""
```

### Inspecting state after a sync

After running the offchain against a known fixture, you can confirm
the new column is populated using `mpfs-inspect-db` (already shipped):

```bash
cabal run mpfs-inspect-db -- /path/to/db
```

Expect to see entries under `trie-raw-values` for every `(token, key)`
present in the corresponding token's MPF trie.

## What does NOT need verification

- The HTTP wire shape — there are no schema changes, so swagger /
  contract tests pass without intervention.
- The merkle proof construction — the `TrieNodes` / `TrieKV` columns
  are byte-identical to before. Existing proof-replay tests
  (`Cardano.MPFS.ProofSpec`, `Cardano.MPFS.E2E.ProofsSpec`'s
  proof-cryptography assertions) keep passing.
- Verifier code paths in `cardano-mpfs-client` — untouched. No
  GHC-WASM / GHC-JS rebuild needed for this work.

## Where this fix unblocks downstream work

The slice 3 work in PR #246 (branch `243-proof-redesign`) was
blocked because `verifyFactPresentResponse` rejected honest oracle
responses — the `value` field carried `mkMPFHash key` which did not
match the value-hash on the merkle proof path. Once #248 merges and
slice 3 rebases on top, the verifier sees correct bytes and passes
the cryptographic round-trip end-to-end. Issue #247 tracks closing
that loop.
