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
   `pureLookup`) honour the value-bearing contract agreed in the
   chosen option (A, B, or C — picked via `Q-001`).
2. `verifyFactPresentResponse` round-trip passes against a live
   devnet response.
3. `verifyFactAbsentResponse` round-trip passes unchanged.
4. `assertFactEnvelope` (or its renamed successor) calls
   `verifyFactPresentResponse` and asserts `Right ()` — the
   structural-only check is replaced.
5. `specs/243-proof-redesign/research.md` records the chosen
   option.
6. `./gate.sh` (the resolve-ticket bootstrap gate) is green; the
   final mark-ready commit drops it.

## Open questions / pending clarifications

The architectural choice between options A, B, and C is the parent
decision. It is enumerated in `## Clarifications` below and
escalated to the operator via `Q-001-schema-option.md` before any
implementation slice is dispatched.

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
ticket's runtime root and is the gating dependency for `plan.md`
and `tasks.md`.

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
