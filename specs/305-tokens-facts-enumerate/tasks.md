# Tasks — #305 GET /tokens/:id/facts (enumerate all facts)

One commit per slice. Each commit body carries `Tasks: T305-S<n>`.

## Slice S1 — trie stores original key + `enumerate`

- [X] T305-S1 Add `enumerate :: m [(ByteString, ByteString)]` to the
      `Trie` record in `Trie.hs` (haddock: original key, raw value).
- [X] T305-S1 Pure backend: change `ptsRawValues` to
      `Map HexKey (ByteString, ByteString)` storing `(origKey, value)`;
      implement `pureEnumerate` (the stored pairs); keep `lookup`/proof
      behaviour identical (project out the value half).
- [X] T305-S1 Persistent backend: widen `TrieRawValues` value to
      `(ByteString, ByteString)`; implement `enumerate` as a
      token-prefixed cursor scan; keep `lookup`/proof identical.
- [X] T305-S1 Wire `enumerate` into `PureManager` and the persistent
      manager's produced `Trie`.
- [X] T305-S1 RED: `TrieSpec` case — insert N pairs into the pure AND
      persistent managers, assert `enumerate` returns exactly those
      `(key, value)` pairs (order-insensitive).
- [X] T305-S1 Proof: `just unit` + `just ci` green.

## Slice S2 — API type + route + handler + client + swagger

- [X] T305-S2 Add `FactEntry {feKey, feValue}` and `FactsResponse
      {frsSnapshot, frsState, frsFacts}` with `ToJSON`/`FromJSON`/
      `ToSchema` in `API/Types.hs`; export from `API.hs` and re-export
      from offchain `HTTP/Types.hs`.
- [X] T305-S2 Add `TokensFactsAPI` route `"tokens" :> Capture "id" :>
      "facts" :> Get '[JSON] FactsResponse` (coexists with the per-key
      route); add to the API product.
- [X] T305-S2 Add `tokenFactsHandler` (mirror `tokenHandler`:
      `requireToken` → `requireSnapshot` → `requireUtxoWitness`, then
      `Trie.withTrie … enumerate`); assemble `FactsResponse`; add to the
      handler tree.
- [X] T305-S2 Expose the enumerate call in the `cardano-mpfs-client`
      subset (mirror existing token/fact client functions).
- [X] T305-S2 RED: HTTP-level handler spec (sibling to `TokenSpec`) —
      boot a token with a few facts, assert response carries snapshot,
      witnessed state, and all inserted `(key, value)`.
- [X] T305-S2 `just update-swagger`; confirm `swagger-up-to-date` green.
- [X] T305-S2 Proof: `just unit` + `just ci` green.

## Slice S3 — e2e completeness proof + README

- [X] T305-S3 e2e spec under `withDevnet`: boot a token, insert N facts
      via the request/update lifecycle, GET `/tokens/:id/facts`.
- [X] T305-S3 Assert all N `(key, value)` returned.
- [X] T305-S3 Reconstruct the MPF root from `facts` and assert it is
      byte-equal to the on-chain MPF root carried in `state`
      (completeness proof).
- [X] T305-S3 Add the README HTTP API table row; wire the spec into the
      e2e suite (`main.hs`).
- [X] T305-S3 Proof: `just e2e` green for the new row.
