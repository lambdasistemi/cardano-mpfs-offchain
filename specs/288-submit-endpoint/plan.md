# Plan — #288 POST /submit endpoint

## Tech stack

Haskell GHC 9.10.1, Servant HTTP API, Aeson wire types,
`cardano-ledger-conway` (`Tx ConwayEra`, `decodeFull'`), existing
node-to-client `Submitter` (`mkN2CSubmitter`). Hspec for e2e (`withDevnet`
harness). Nix/cabal via `just`.

## Constitution check

- Ledger-native types: uses `Tx ConwayEra`, `TxId`; no shadow types. ✓
- Service boundary via record-of-functions: reuses `Submitter`. ✓
- Fact-provider rule: `/submit` forwards a *client-built, client-signed*
  tx — the server does not build or sign. Returns only the txId / error.
  Consistent with "server MUST NOT return unsigned transactions". ✓
- No new infra: reuses the wired N2C connection. ✓

## Modules touched

- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` — reshape
  `SubmitRequest` (field `signedTxCbor`); add `SubmitResponse {txId}` and
  `SubmitError {error, detail}`; their `ToJSON`/`FromJSON`/`ToSchema`.
- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` — `TxSubmitAPI` route
  `"submit"` (drop `"tx"` prefix); response `Post '[JSON] SubmitResponse`;
  export `SubmitResponse`, `SubmitError`.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` — re-export the
  new types.
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` —
  `txSubmitHandler` returns `SubmitResponse`; 400/502 carry
  `SubmitError` JSON bodies (content-type application/json).
- `docs/assets/swagger.json` — regenerated.
- `cardano-mpfs-offchain/e2e-test/...` — new HTTP-level submit spec +
  wire it into the e2e suite.

## Structured JSON errors

Current handlers throw `errXXX { errBody = plain text }`. For `/submit`,
build the body as `encode (SubmitError code detail)` and set
`errHeaders = [("Content-Type","application/json")]`. A small local
helper `submitError :: ServerError -> Text -> Text -> ServerError`
keeps 400 and 502 consistent.

- 400: `error = "decode failed"`, `detail = show DecoderError`.
- 502: `error = "submission rejected"`, `detail = decodeUtf8 reason`.

## Slices (each one bisect-safe commit)

### Slice S1 — reshape endpoint to the locked contract
API route + wire types + handler + ToSchema + swagger regen, all in one
commit so the build and unit suite stay green. Removes `/tx/submit`.
Proof: `just unit-offchain` + `just ci` (incl. `swagger-up-to-date`).

### Slice S2 — HTTP-level e2e submit test
A spec that starts the app against `withDevnet`, balances+signs a genesis
ADA-transfer tx (reusing `balanceTx` / `genesisSignKey` / `addKeyWitness`
from `Cardano.Node.Client.E2E.Setup`, exactly as `SubmitterSpec` does),
POSTs `{"signedTxCbor": …}` to `/submit`, asserts 200 + `txId`, then
awaits the txId against the node (live-boundary proof). Tx construction
does all fee/ttl work (hard rule #2) — no `/submit`-side mutation.

## Risks

- Structured-JSON-error pattern is new to this codebase — confirm the
  content-type header makes clients parse it as JSON.
- E2E must reuse the existing devnet harness; do not add new scaffolding
  that fakes user actions (hard rule #2).
```
