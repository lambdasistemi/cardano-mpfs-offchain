# Spec — #288 POST /submit endpoint

## Context

Child 1 of epic #287 (client front-ends for `cardano-mpfs-client`). The
CLI (#290) and the PureScript SPA (#291) both need a single canonical
endpoint to push a locally-built, locally-signed Conway transaction to
the chain. This endpoint is a **pure forwarder**: it decodes the signed
tx CBOR and hands it to the node-to-client `LocalTxSubmission` half of
the connection the indexer already holds.

## P1 user story

As a `cardano-mpfs-client` front-end, after I build and sign a Conway tx
locally, I POST its CBOR to `/submit` and receive the resulting txId on
acceptance, or a structured error I can show the user on rejection.

## Existing state (discovered)

- The N2C submit seam already exists and is wired: `Submitter`
  record-of-functions (`Cardano.MPFS.Submitter`), real implementation
  `mkN2CSubmitter` (`Cardano.MPFS.Submitter.N2C`), installed into
  `Context.submitter` at `Application.hs:653`. **No new submission
  infrastructure is needed** (hard rule #1 satisfied).
- A legacy endpoint `POST /tx/submit` (`txSubmitHandler`, `Server.hs`)
  already forwards to `Submitter`, but with a non-conforming contract:
  request `{"tx": hex}`, response a bare hex string, plain-text errors.
  It has **zero consumers** (not part of the `TxWriteAPI` client subset,
  no e2e test, no docs reference).

## Decision

Replace `POST /tx/submit` with `POST /submit` reshaped to the locked
contract below. This mirrors `b54334e` (add `/facts/reject`, remove
`/tx/reject` in one commit). The handler's submit logic is preserved;
only the route, request field, success shape, and error shape change.

## Wire contract (locked, per issue body)

```
POST /submit
{ "signedTxCbor": "<hex>" }
→ 200 { "txId": "<hex>" }
→ 400 { "error": "...", "detail": "..." }   # malformed/undecodable CBOR
→ 502 { "error": "...", "detail": "..." }   # node-side rejection (ledger reason in detail)
```

Field names are taken verbatim from the issue body (camelCase
`signedTxCbor`, `txId`), overriding the repo's usual snake_case wire
convention because the contract is locked.

## Functional requirements

- **FR1** — `POST /submit` accepts `{"signedTxCbor": "<hex>"}` and
  returns `200 {"txId": "<hex>"}` when the node accepts the tx.
- **FR2** — Malformed hex or CBOR that fails to decode to a
  `Tx ConwayEra` returns `400 {"error","detail"}` with the decoder
  error in `detail`.
- **FR3** — A node-side rejection returns `502 {"error","detail"}` with
  the ledger rejection reason passed through in `detail`.
- **FR4** — The handler delegates to the already-wired `Context.submitter`
  (`mkN2CSubmitter`); no parallel N2C connection is opened.
- **FR5** — `docs/assets/swagger.json` reflects the new route and shapes
  (`just update-swagger`; `swagger-up-to-date` check stays green).
- **FR6** — `/tx/submit` is removed; the API type list and re-exports no
  longer reference the legacy route shape.

## Success criteria

- `just unit-offchain` green; `just ci` green (build, format, hlint,
  swagger-up-to-date).
- E2E: a genesis-signed ADA-transfer tx POSTed to `/submit` returns 200
  with a txId, and that txId is awaited successfully against the node
  (proves the tx really reached the mempool — live-boundary smoke).
- No auth / replay-protection / queueing introduced (hard rule #3).
```
