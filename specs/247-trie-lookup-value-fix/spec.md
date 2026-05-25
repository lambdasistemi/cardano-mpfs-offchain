# Spec — #247: Trie.lookup returns value-bearing bytes

Tracks `lambdasistemi/cardano-mpfs-offchain#247`. Downstream
remediation of postmortem `#248`. Gates epic `#257` tier-3
children `#269` (update) and `#270` (reject).

## Problem (user-visible)

`GET /tokens/:id/facts/:key` returns a JSON `value` field that
clients are documented to treat as the trie value associated with
the requested key. In production, the server returns
`mkMPFHash key` instead — the hash of the key, not the value, not
even the value's hash.

When a client runs the canonical offline trust check
`verifyFactPresentResponse` (which wraps
`verifyAikenInclusionProof rootBs keyBs valueBs proofBs`), the
verifier hashes `valueBs` internally to compare against the leaf's
stored value-hash. Because the server hands back `mkMPFHash key` as
`valueBs`, the verifier ends up checking `mkMPFHash (mkMPFHash key)`
against the stored `mkMPFHash actualValue` and rejects with
`MpfInclusionInvalid`.

Concretely, the bug lives in two functions:

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
  `unifiedLookup` (line 234): returns `Just (hashBS k)`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs`
  `pureLookup` (line 125): returns
  `Just (renderMPFHash (mkMPFHash k))`.

Both backends discard the raw value at insert time (only
`mkMPFHash value` survives in the `TrieKV` column), so the lookup
implementations had to return *something* — and that something got
the `value` label without earning it.

This was undetected because the e2e in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
`assertFactEnvelope` only asserts that the `value` field is
non-empty hex. `mkMPFHash key` is a 32-byte content hash and passes
that bar trivially. No live cryptographic verifier was run against
a live indexer response.

## User stories

### US-1 (P1) — Offline trust check passes for present facts

As an MPFS client (in-browser, on a phone, in a CI bot), I run
`verifyFactPresentResponse trustedRoot blueprint resp` and expect
`Right verified` for any honest server response that asserts the
key is present.

**Acceptance.** Given a token `tid` whose trie contains a fact
`(k, v)` whose root is `r`:

1. The client calls `GET /tokens/:tid/facts/:k`.
2. The client decodes the response into the typed wire shape
   defined in `cardano-mpfs-client`.
3. The client runs `verifyFactPresentResponse (TrustedRoot r) blueprint resp`.
4. The result is `Right _` (the verifier accepts the proof
   cryptographically and binds it to the advertised key and value).

This holds across a real devnet round-trip (server inserts `(k, v)`
via the request/update facts-API pivot, advances the chain, replies
to a fact lookup, the client verifies).

### US-2 (P1) — Offline trust check still passes for absent facts

As the same MPFS client, I run `verifyFactAbsentResponse` for a key
the server reports as absent and expect `Right ()`.

**Acceptance.** Exclusion proofs do not consume `value`, so the
existing path already works. The fix for US-1 must not regress this:
absent-fact responses continue to verify against
`verifyAikenExclusionProof`.

### US-3 (P1) — The e2e replays the verifier, not a shape check

As a reviewer of any future change that touches the fact endpoint,
I want the e2e to fail loudly the moment the server drifts from the
verifier contract.

**Acceptance.** `assertFactEnvelope` in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs` is
replaced (or supplemented) by a call equivalent to:

```haskell
verifyFactPresentResponse trustedRoot blueprint resp
  `shouldBe` Right ()
```

The "non-empty hex" structural check is no longer the sole gate.

### US-4 (P1) — Schema decision is documented

As a future maintainer of the proof-redesign series, I can read
`specs/243-proof-redesign/research.md` and understand why the wire
shape settled where it did and which of the three options (A/B/C
from issue #247) was chosen.

**Acceptance.** A `## #247 — value-bearing lookup` section appears
in `specs/243-proof-redesign/research.md` that names the chosen
option, summarizes the trade-offs that were weighed, and points to
this PR.

## Out-of-scope

- Epic `#257` siblings `#269` (request-update facts pivot), `#270`
  (reject pivot), and the broader `#248` postmortem cleanup
  (general "every proof-bearing endpoint must have a verifier
  round-trip test" sweep).
- Replacing the existing per-token MPF trie with an externalized
  data-availability layer (out of scope for this PR; absorbed into
  the cage-extraction track called out in `#248`).
- Re-architecting `cardano-mpfs-client`'s verifier dispatch beyond
  what option B specifically requires.
- Touching `cardano-utxo-csmt` — the generic UTxO-CSMT indexer
  didn't promise the broken contract and is unaffected.

## Success criteria

1. Both backends (`Persistent.hs` `unifiedLookup` and `Pure.hs`
   `pureLookup`) honour the value-bearing contract under Option A
   (operator decision recorded in `Q-001` / `A-001`):
   `Trie.lookup` returns the raw value bytes pulled from the new
   `TrieRawValues` column (or the pure-backend in-memory mirror).
2. `verifyFactPresentResponse` round-trip passes against a live
   devnet response.
3. `verifyFactAbsentResponse` round-trip passes unchanged.
4. `assertFactEnvelope` (or its renamed successor) calls
   `verifyFactPresentResponse` and asserts `Right ()` — the
   structural-only check is replaced.
5. `specs/243-proof-redesign/research.md` records the chosen
   option (A) and the reasoning.
6. `./gate.sh` (the resolve-ticket bootstrap gate) is green; the
   final mark-ready commit drops it.

## Hard invariants (MUST)

These are non-negotiable operator-mandated invariants for the
`TrieRawValues` schema. Any implementation slice that violates them
is rejected at review.

### INV-1 (MUST) — Write atomicity

Every write to the `TrieRawValues` column lands in the **same**
RocksDB write batch as the corresponding `(key_hash, value_hash)`
write into the existing `TrieKV` / `TrieNodes` columns. There is no
acceptable interleaving where one column is updated but the other
is not, even momentarily, even on a crash, even on a process
restart between writes.

Implementation guidance: the existing `unifiedInsert` /
`unifiedDelete` functions in
`cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
already run inside a single `Transaction m cf AllColumns ops`
monadic action via `MPF.Insertion.inserting` /
`MPF.Deletion.deleting`. The fix is to add the
`KV.put TrieRawValues …` / `KV.delete TrieRawValues …` call inside
that same transaction. The block-processing transaction in
`Cardano.MPFS.Application` already commits the unified column
families in one batch per block; nothing has to change at the
batch-boundary layer.

### INV-2 (MUST) — Rollback atomicity

The `TrieRawValues` column participates in chain-follower's
existing rollback machinery in lockstep with the trie. When the
indexer rolls back from block N to block M, every `TrieRawValues`
row written between M+1 and N must be undone in the **same atomic
step** as the trie rollback. Partial states — where `TrieKV` /
`TrieNodes` have been rolled back but `TrieRawValues` still
carries entries for keys that no longer exist in the trie, or vice
versa — are correctness bugs, not degraded modes.

Implementation guidance: rollback is driven by replaying
`CageInverseOp` entries (see
`cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Follower.hs`
`applyCageInverses`). The two trie-touching inverse ops route
through the `Trie m` interface:

```haskell
InvTrieInsert tid key val ->
    withTrie tm tid $ \trie -> void $ insert trie key val
InvTrieDelete tid key ->
    withTrie tm tid $ \trie -> void $ delete trie key
```

Because the rollback path reuses the same `insert` and `delete`
operations as the forward path, making `unifiedInsert` /
`unifiedDelete` write both columns inside one transaction
**automatically** makes the rollback replay write both columns
inside one transaction. The existing single-write-batch rollback
boundary (composed-rollback column family + the
`Transaction m cf UnifiedColumns ops` runner) handles atomicity at
the block boundary. No new rollback machinery is required.

Verification: the implementation slice must include a
`PersistentSpec` test that (a) inserts a key, (b) writes a
post-insert checkpoint, (c) executes a rollback that crosses the
insert, and (d) confirms both `TrieKV[hashOfKey]` is absent AND
`TrieRawValues[hashOfKey]` is absent after rollback. The symmetric
delete-then-rollback test confirms both rows are restored.

## Open questions / pending clarifications

- `Q-001` (resolved by `A-001`): **Option A** chosen — add a
  `TrieRawValues` column family. Decision is recorded in the
  `## Clarifications` section below.
- `Q-002` (open): **Migration strategy.** Operator requires this
  not to be picked silently. Surfaced as
  `/tmp/epic-257/247/questions/Q-002-migration-strategy.md`.
  Plan and tasks land only after `A-002` is answered.

## Clarifications

### Session 2026-05-25 — Schema option (Q-001)

The issue lays out three options for how the `value` field can be
made honest. The choice has a large effect on storage layout, the
cross-repo coupling to `haskell-mts`, and the wire contract — so it
is the central architectural decision for this ticket.

- **Option A — Add a `TrieRawValues` RocksDB column.** Persist raw
  bytes keyed by `tokenHexPrefix + hashOfKey`. `Trie.lookup`
  returns the raw bytes; `verifyAikenInclusionProof` re-hashes them
  the way it already does. The on-disk schema migrates from 13
  column families (`AllColumns` 6 + UTxO 6 + `InRollbacks` 1) to
  14. Existing devnet/prod data has no raw values to back-fill;
  affects re-indexing or migration semantics.
- **Option B — Return the value-hash; add a hashed verifier
  upstream.** The wire `value` becomes the 32-byte
  `mkMPFHash actualValue` already in the trie. `mpf-write` in
  `haskell-mts` gains `verifyAikenInclusionProofHashed` that skips
  the internal `mkMPFHash`. No schema change in this repo; one
  upstream change in another repo. Client code must know which
  verifier to call. Cross-repo PR coordination required.
- **Option C — Drop the `value` field.** The MPF inclusion proof at
  the trie root already witnesses "there exists a value with this
  hash"; the application layer carries the raw value out of band.
  Wire shape change downstream of `#243`'s spec. Smallest code
  footprint here, biggest contract change for clients of
  `FactPresentResponse`.

This decision is logged as `Q-001-schema-option.md` in the
ticket's runtime root. **Resolved (A-001, 2026-05-25): Option A.**
The operator confirmed Option A and added two MUST-level atomicity
invariants (INV-1 write atomicity, INV-2 rollback atomicity) — see
the `## Hard invariants (MUST)` section above.

### Session 2026-05-25 — Migration strategy (Q-002)

Adding a 14th column family on an existing RocksDB indexer DB
requires a story for how pre-migration data is handled. The
sub-options are:

- **(1) Re-index from genesis required.** Operators drop the
  RocksDB directory and resync the indexer from genesis. Simplest
  story; aligned with cage being in the proof-redesign series.
  Document the requirement in the PR body and the release notes.
- **(2) Forward-compatible / degrade gracefully.** New column is
  created on first open; pre-migration keys return `Nothing` from
  `Trie.lookup` (and therefore `404` from the facts endpoint)
  until they are re-inserted. Schema is forward-compatible. Risk:
  silent loss of fact lookups for old keys until operators notice.
- **(3) Backfill from `CageRequests` journal.** Replay forward
  from genesis using the existing block journal, re-inserting raw
  values into the new column. Most operational complexity; also
  the most defensive.

This is logged as `Q-002-migration-strategy.md`. The plan and
tasks gate on `A-002`.

## References

- Issue: https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/247
- Postmortem: https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248
- Slice 3 of #243 (where the verifier first ran cryptographically):
  https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/246
- Verifier: `haskell-mts/lib/mpf-write/MPF/Verify.hs`
  (`verifyAikenInclusionProof`)
- Touch points:
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs`
  (`unifiedLookup`),
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs`
  (`pureLookup`),
  `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
  (`assertFactEnvelope`).
- Server wiring: `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
  (`tokenFactHandler` at line 404).
