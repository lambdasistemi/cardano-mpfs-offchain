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

## Companion: proof-bearing GET /tokens (folded into this PR)

Reshape the bare `GET /tokens` (`[TokenIdJSON]`) into a complete,
proof-bearing token-set response. Tokens are UTxOs at the cage script
address; the UTxO-CSMT has compact prefix-completeness, so a single
proof attests the set is exactly the leaves under that prefix — no
reconstruction. Reuses `readRequestSetAt :: Addr -> IndexerTx
ResolvedUtxoSet` (call it at `cageAddr`) and `requestSetToJSON ::
ResolvedUtxoSet -> UtxoSetWitness`. Shape:

```
GET /tokens -> { "snapshot": VerificationSnapshot,
                 "tokens":   UtxoSetWitness }   // {entries, completeness_proof}
```

No derived token-id list (pure witness; client derives ids from each
`entries[].txout_cbor`). No unverifiable fields.

### Slice S4 — TokensResponse type + reshape route + handler + client + swagger

- [X] T305-S4 Add `TokensResponse {trsSnapshot :: VerificationSnapshot,
      trsTokens :: UtxoSetWitness}` with `ToJSON`/`FromJSON`/`ToSchema`
      in `API/Types.hs`; export from `API.hs`; re-export from offchain
      `HTTP/Types.hs`.
- [X] T305-S4 Reshape `TokensAPI` from `"tokens" :> Get '[JSON]
      [TokenIdJSON]` to `... Get '[JSON] TokensResponse`.
- [X] T305-S4 Reshape `tokensHandler`: `requireSnapshot` + run the
      UTxO-set read at `cageAddr` (reuse `readRequestSetAt`; if a rename
      to a generic `readUtxoSetAt` is cleaner, do it and update the
      requests use-site) → `requestSetToJSON` into `trsTokens`.
- [X] T305-S4 Update the `cardano-mpfs-client` `/tokens` call + any
      client-internal decode to the new shape.
- [X] T305-S4 RED: HTTP-level handler spec — boot >=2 tokens, assert the
      response carries the snapshot and a `UtxoSetWitness` whose
      `entries` match the booted token UTxOs.
- [X] T305-S4 `just update-swagger`; `swagger-up-to-date` green.
- [X] T305-S4 Proof: `just unit` + `just ci` green. NOTE in WIP if any
      in-repo consumer (CLI/SPA) still decodes the old bare list —
      surface as a follow-up, do not fix here.

### Slice S5 — e2e completeness proof for GET /tokens + README

- [X] T305-S5 e2e spec under `withDevnet`: boot >=2 tokens, GET
      `/tokens`, assert `entries` is exactly the booted token set and the
      `completeness_proof` verifies against `snapshot.utxo_root` at the
      cage script-hash prefix.
- [X] T305-S5 Update the README HTTP table row for `GET /tokens`.
- [X] T305-S5 Proof: `just e2e` green for the new row + `just ci` green.

### Slice S6 — fix e2e /tokens consumers broken by the reshape

CI caught two e2e helpers (`tokenCount` in `HTTPLifecycleSpec` and
`BootFactsSpec`) still decoding `/tokens` as a bare array. (Pre-existing
`main` WorkflowsIntegration `readWalletInputsAt` failures, #250, are NOT
in scope.)

- [X] T305-S6 `HTTPLifecycleSpec.tokenCount`: decode `TokensResponse`,
      count `trsTokens`'s `uswEntries`.
- [X] T305-S6 `BootFactsSpec.tokenCount`: same.
- [X] T305-S6 Proof: `just e2e "HTTP lifecycle"` + `just e2e "Boot
      facts"` pass; `just ci` green; no NEW e2e failures vs main.
