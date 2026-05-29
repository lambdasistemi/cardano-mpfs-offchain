# Spec — #289 `cardano-mpfs-workflows`

- Issue: https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/289
- Epic: https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/287
  (Child 2 of 4)
- Branch: `289-workflows` (from `main` `b54334e`)

## Problem

The CLI (#290) and the PureScript SPA (#291) both need to drive the
MPFS end-to-end workflows (register a token, request fact
insert/update/delete, oracle apply, retract, reject, end). Without a
shared layer each front-end would re-encode the protocol — which HTTP
endpoint to call, how to verify the proof-bearing response, and how to
build the unsigned transaction. That duplication is the failure mode
this ticket prevents.

`cardano-mpfs-client` already exposes the two halves of every
operation as **pure** functions:

- a verifier `verify{Op}Facts :: TrustedRoot -> {Op}Facts -> Either
  VerifyError Verified{Op}Facts` (the `end` verifier additionally takes
  a `CageConfig`), and
- a builder `{op}CageTx :: CageConfig -> WalletPolicy ->
  Verified{Op}Facts -> Either BuildError (Tx ConwayEra)`.

What is missing is the glue: fetch facts over HTTP, decode them,
verify, build, and emit a submission-ready transaction. Today that glue
only exists inside `Cardano.MPFS.Client.Http`, which hard-wires
`http-client`/`servant-client` — unusable from the SPA's WASM/JS build.

## P1 user story

> As a front-end author (CLI or SPA), I call **one** function per
> workflow — `registerToken`, `insertFact`, `updateFact`,
> `deleteFact`, `applyRequests`, `retractRequest`, `rejectExpired`,
> `endCage` — pass my config and a transport handle, and receive a
> verified, unsigned transaction (or a typed error). I never touch a
> verifier, an endpoint path, or a ledger type.

## User stories

- As the **token owner**, `registerToken` boots a new token,
  `applyRequests` applies pending requests, `rejectExpired` rejects
  expired requests, and `endCage` burns the token.
- As a **requester**, `insertFact` / `updateFact` / `deleteFact` submit
  a fact-change request and `retractRequest` retracts my own pending
  request.
- As the **SPA (WASM/JS) author**, I swap the transport by providing my
  own `HttpClient` value backed by the browser `fetch` API; the
  workflow functions are unchanged.

## Functional requirements

- **FR1** — Expose module `Cardano.MPFS.Workflows` with exactly:
  `registerToken`, `insertFact`, `updateFact`, `deleteFact`,
  `applyRequests`, `retractRequest`, `rejectExpired`, `endCage`,
  plus `WorkflowError(..)`, `UnsignedTx(..)`, `WorkflowsConfig(..)`,
  `HttpClient(..)`. (The brief lists 7; per operator decision the
  oracle `applyRequests` is the 8th.)
- **FR2** — Each workflow: build the API wire request → POST it through
  the caller's `HttpClient` to the correct `/facts/*` path → decode the
  `{Op}Facts` response → `verify{Op}Facts` → `{op}CageTx` → serialize →
  return `UnsignedTx`. Any stage failure returns the matching
  `WorkflowError` constructor.
- **FR3** — No protocol reimplementation. All verification and
  transaction construction route through `cardano-mpfs-client`'s
  exported pure functions. Serialization of the built `Tx ConwayEra`
  uses a new `cardano-mpfs-client` export (`serializeCageTx`), so the
  ledger stays out of the workflows package entirely.
- **FR4** — Transport is abstracted behind a `HttpClient`
  record-of-functions. The workflows package and the workflow function
  signatures depend on **no** `http-client`, `servant-client`, or
  `Cardano.Ledger.*` modules.
- **FR5** — Signing and submission are the caller's responsibility.
  `UnsignedTx` carries submission-ready CBOR bytes; the caller signs
  and POSTs to `/submit` (#288).
- **FR6** — No CLI parsing, no UI, no server changes.

## Out of scope

- Signing, key management, `/submit` wiring (caller / #288).
- WASM/JS build files and the `fetch` shim (#258).
- A live integration test exercising the happy path against a running
  server — **gated on #288 `/submit`**; tracked as a follow-on task,
  not faked here.

## Success criteria

- `Cardano.MPFS.Workflows` exports the 8 workflows + 4 named types and
  compiles with `-Wall -Werror`.
- `cardano-mpfs-workflows` build-depends only on: `base`, `aeson`,
  `bytestring`, `text`, `cardano-mpfs-api`, `cardano-mpfs-client`
  (+ test-only deps). No `http-client`, `servant-*`, or
  `cardano-ledger-*`.
- Unit tests prove, per workflow: the correct path + JSON body is
  posted (captured via a stub `HttpClient`), and each `WorkflowError`
  stage (HTTP, decode, verify, build) propagates. Green under
  `just unit-workflows`.
- `just ci` (extended with the workflows test) passes.
- A `tasks.md` entry records that the live happy-path integration test
  follows the #288 merge.
