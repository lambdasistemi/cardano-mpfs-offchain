# Plan — #305 GET /tokens/:id/facts (enumerate all facts)

## Tech stack

Haskell GHC 9.10.1, Servant HTTP API, Aeson wire types, `mts:mpf`
(MPF backend, `buildComposeFromList`), RocksDB KV
(`Database.KV.Cursor`) for the persistent trie. Hspec for unit + e2e
(`withDevnet`). Nix/cabal via `just`.

## Constitution check

- Ledger-native / existing types: reuses `WitnessedTokenState`,
  `VerificationSnapshot`, `Root`; no shadow types. ✓
- Service boundary via record-of-functions: extends the `Trie`
  record with one method; both backends implement it. ✓
- Fact-provider rule: read-only endpoint, no tx building/signing. ✓
- No new infra: reuses the wired `TrieManager`, snapshot, and CSMT
  proof seams; storage widening only. ✓
- Verifiability: every response field is provable (UTxO inclusion vs
  snapshot; facts vs MPF root in state). No server-asserted root/slot. ✓

## Modules touched

- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie.hs` — add
  `enumerate :: m [(ByteString, ByteString)]` to the `Trie` record
  (original key, value).
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs` — store
  `(key, value)`: `ptsRawValues :: Map HexKey (ByteString, ByteString)`;
  implement `pureEnumerate = Map.elems` (the stored pairs).
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/PureManager.hs` — wire
  `enumerate` into the manager-produced `Trie`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs` — widen
  `TrieRawValues` value to `(ByteString, ByteString)` (store original
  key with the value); implement `enumerate` as a token-prefixed cursor
  scan of `TrieRawValues`.
- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` — add `FactEntry
  {feKey, feValue}` and `FactsResponse {frsSnapshot, frsState, frsFacts}`
  with `ToJSON`/`FromJSON`/`ToSchema`.
- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` — add `TokensFactsAPI`
  route `"tokens" :> Capture "id" :> "facts" :> Get '[JSON]
  FactsResponse`; export the new types.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` — re-export.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` —
  `tokenFactsHandler`: `requireToken` → `requireSnapshot` →
  `requireUtxoWitness` → `Trie.withTrie … enumerate`; assemble
  `FactsResponse`. Add to the handler tree (`:<|>`).
- `cardano-mpfs-client/lib/...` — expose the enumerate call in the
  client subset (mirrors the existing token/fact client functions).
- `docs/assets/swagger.json` — regenerated.
- `README.md` — HTTP API table row.
- e2e + unit specs (below).

## Routing note

`/tokens/:id/facts` (no trailing capture) and `/tokens/:id/facts/:key`
coexist — Servant distinguishes by path depth. Place the new route so it
does not shadow the per-key route.

## Storage-widening note

`hash(key) → value` becomes `hash(key) → (key, value)`. Lookup
(`Trie.lookup`) returns the value half, so existing callers are
unaffected after they project out the value. The empty DB means no
backfill; the codec/column change just needs a clean (already empty)
store. Confirm `Trie.lookup` and proof paths still pass their existing
unit suites at HEAD.

## Slices (each one bisect-safe commit)

### Slice S1 — trie stores original key + `enumerate`

Add `enumerate` to the `Trie` record; widen both backends to persist
`(key, value)`; implement enumerate (pure `Map.elems`, persistent
token-prefixed cursor scan). Keep `lookup`/proof behaviour identical.
RED: a `TrieSpec` case inserting N pairs into both the pure and
persistent managers and asserting `enumerate` returns exactly those
`(key, value)` pairs (order-insensitive). Proof: `just unit` +
`just unit` for the trie suites + `just ci` (build, hlint, format).

### Slice S2 — API type + route + handler + client + swagger

Add `FactEntry` + `FactsResponse` (+ `ToSchema`); add `TokensFactsAPI`
route; add `tokenFactsHandler` mirroring `tokenHandler` plus
`enumerate`; wire into the handler tree; expose in the client subset;
`just update-swagger`. RED: an HTTP-level handler spec (sibling to
`TokenSpec`/`TrieSpec` under `test/.../HTTP/`) booting a token with a
few facts and asserting the response carries the snapshot, the witnessed
state, and all inserted `(key, value)` pairs. Proof: `just unit` +
`just ci` (incl. `swagger-up-to-date`).

### Slice S3 — e2e completeness proof + README

e2e spec under `withDevnet`: boot a token, insert N facts via the
request/update lifecycle, GET `/tokens/:id/facts`, assert all N returned
**and** reconstruct the MPF root from `facts` byte-equal to the on-chain
MPF root in `state`. Add the README HTTP API table row. RED: the e2e
spec (live-boundary proof). Proof: `just e2e` green for the new row.
