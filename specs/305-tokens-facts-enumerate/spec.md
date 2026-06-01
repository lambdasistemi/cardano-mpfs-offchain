# Spec — #305 GET /tokens/:id/facts (enumerate all facts)

## Context

The token read surface exposes per-key MPF fact lookup
(`GET /tokens/:id/facts/:key` → inclusion proof) and `/proofs/:key`,
plus `/tokens/:id` (witnessed state) and `/tokens/:id/requests`. There
is **no endpoint to enumerate all facts** of a token. A client cannot
enumerate fact keys from the MPF root alone — the indexer holds the
trie. Clients that reconstruct a domain view from the full fact set
(moog v2: registered users, roles, pending test-runs, whitelist) have
no way to do so. This blocks the moog-v2 cutover
(`cardano-foundation/moog#146`, foundation `#147`).

## P1 user story

As a `cardano-mpfs-client` front-end, I GET `/tokens/:id/facts` and
receive the complete `(key, value)` fact set for the token plus the
witnessed token state, so I can **rebuild the MPF and prove the set is
complete** against the on-chain root — without trusting the server.

## Existing state (discovered)

- **Both trie backends discard the original key.** `Trie.insert` hashes
  the key and stores `hash(key) → value`:
  - `Trie/Pure.hs`: `rawValueKey = byteStringToHexKey . renderMPFHash .
    mkMPFHash`; `ptsRawValues :: Map HexKey ByteString`.
  - `Trie/Persistent.hs`: `hexKey = byteStringToHexKey (hashBS k)`;
    `KV.insert TrieRawValues hexKey v`.
  The single-fact endpoints never needed the preimage (caller supplies
  the key, server hashes and looks up), so the gap was invisible until
  enumeration — the first operation that must go key → out, not in.
- **`Trie` has no enumeration method.** Interface is
  `insert/delete/lookup/getRoot/getProof/getProofSteps` only.
- **The witnessed-state assembly already exists and is reusable.**
  `tokenHandler` (`HTTP/Server.hs`) builds
  `TokenResponse { snapshot, state = WitnessedTokenState{utxo, state} }`
  via `requireToken` → `requireSnapshot` → `requireUtxoWitness`. The
  decoded `wtsState` carries the on-chain MPF root.
- **DB is empty** (preprod/devnet only) — no historical rows to backfill
  when the storage shape changes; a fresh sync repopulates with original
  keys captured.
- The insert site has the original key in hand: `Follower.applyRequestOp`
  calls `trieInsert requestKey v` with `Request{requestKey, requestValue}`.

## Decision

Add `GET /tokens/:id/facts` returning the full fact set plus the
witnessed token state. The endpoint mirrors `tokenHandler`'s
verification envelope and adds the enumerated facts.

To return real fact keys (not hashes), **persist the original key**
alongside the value: storage becomes `hash(key) → (key, value)`. The
insert already holds both halves. No bespoke migration — the empty DB
repopulates on the next sync.

## Wire contract (locked)

```
GET /tokens/:id/facts
 -> { "snapshot" : VerificationSnapshot,
      "state"    : WitnessedTokenState,        // utxo inclusion + content
      "facts"    : [ { "key": <Hex>, "value": <Hex> }, ... ] }
```

- `state` is the existing `WitnessedTokenState` (UTxO ref + TxOut CBOR +
  CSMT inclusion proof against `snapshot.utxo_root`, plus decoded state
  carrying the MPF root). Reused verbatim from the `/tokens/:id` surface.
- `facts` is the complete `(key, value)` set; `key` is the **original**
  fact key bytes (hex), `value` the raw value bytes (hex).

### No top-level `root` field

The issue draft proposed a sibling `root`. It is omitted on purpose: a
server-asserted root is unverifiable. The trusted root the client
compares against MUST come from the verified UTxO (`state`), not the
envelope. Returning it separately would invite skipping the UTxO check.

### No per-fact `slot`

Dropped. The MPF root binds only `(hash(key), hash(value))`; slot is not
in the trie and nothing on-chain commits to it. A slot field would be an
unverifiable trust-me value inside a response whose purpose is to need
zero trust. If moog needs slot for display/ordering, that is a separate,
explicitly-untrusted channel and a separate issue.

## Completeness verification (client-side, trustless)

1. Trust `snapshot.utxo_root` out-of-band.
2. Verify `state.utxo` inclusion proof binds `txout_cbor` into
   `utxo_root` at `ref`.
3. Decode `txout_cbor`; extract the MPF root from the token state.
4. Rebuild the MPF from `facts` (`buildComposeFromList` over the hashed
   key/value) → recomputed root.
5. Assert recomputed == extracted root. Match ⇒ the set is exactly
   complete: the MPF root binds the whole map, so any omission, addition,
   or alteration changes the root.

## Success criteria

- `GET /tokens/:id/facts` returns every `(key, value)` fact for the
  token plus the witnessed state, against a single snapshot.
- The returned `key`s are the original fact keys, not hashes.
- A client rebuilding the MPF from `facts` gets a root byte-equal to the
  token's on-chain MPF root (carried in `state`).
- e2e: a booted token with N inserted facts returns all N and
  reconstructs to the on-chain root.
- Documented in the README HTTP API table + Swagger (`swagger-up-to-date`
  green).

## Non-goals

- Read-side verifier in `cardano-mpfs-verify/.../Verify/Read.hs` (stub,
  specs/243). The reconstruction check lives in the e2e test for this
  issue; wiring the shipped verifier is #243 read-side work.
- Per-fact slot (see above).
- MPFS-on-mainnet migration (empty DB; resync only).
