# Research: Typed HTTP wrappers for MOOG

## Current client surface

`cardano-mpfs-client` currently provides:

- JSON DTOs for proof-bearing write responses in
  `Cardano.MPFS.Client.Bundle`.
- Snapshot DTOs in `Cardano.MPFS.Client.Snapshot`.
- Pure offline verifiers in `Cardano.MPFS.Client.Verify`.
- A top-level re-export module `Cardano.MPFS.Client`.

It does not provide:

- HTTP request parameter types.
- Base URL or manager configuration.
- Any function that talks to the MPFS offchain service.

## Current server wire contract

The write-side server endpoints are declared in
`Cardano.MPFS.HTTP.API`:

- `POST /tx/boot`
- `POST /tx/request/insert`
- `POST /tx/request/delete`
- `POST /tx/request/update`
- `POST /tx/retract`
- `POST /tx/reject`
- `POST /tx/update`
- `POST /tx/end`

The request JSON shapes are implemented in
`Cardano.MPFS.HTTP.Types`. They use simple wire fields:

- `address`
- `token`
- `key`
- `value`
- `old_value`
- `new_value`
- `utxo`

The server request types depend on ledger-domain types and currently
only implement `FromJSON`, because the server only receives them. The
client package should define its own small request parameter types with
`ToJSON` instances instead of importing `cardano-mpfs-offchain`.

## Transport choice

Decision: implement the MOOG-ready HTTP wrapper with `http-client` over a
caller-supplied `Manager` and a small client-owned `BaseUrl` type.

Rationale:

- MOOG is native Haskell CLI code for this milestone.
- A caller-supplied `Manager` lets MOOG own TLS, proxy, timeout, and
  retry policy.
- `http-client` keeps the wrapper independent of the server package and
  avoids tying the client library to the full Servant API type.
- A small `BaseUrl` type is enough for stable endpoint construction and
  avoids requiring `servant-client` just to reuse its `BaseUrl`.

Rejected alternatives:

- Import server `Cardano.MPFS.HTTP.API` and generate Servant client
  functions: rejected because that couples the client package to the
  server package and its ledger-heavy type graph.
- Put retry policy in `Cardano.MPFS.Client.Http`: rejected because MOOG
  should own operational retry decisions.
- Use #221 WASM/JS cross targets as a merge gate: rejected for this
  milestone because MOOG executes native Haskell CLI code. Browser and
  WASI clients are tracked by milestone #3.

## Verification placement

Decision: HTTP wrappers decode JSON first, then conditionally run the
existing pure verifier based on `VerifierMode`.

Rationale:

- Keeps network and JSON handling outside pure verifier modules.
- Preserves the existing verifier signatures and tests.
- Makes "verify before sign" the default ergonomic path while still
  allowing tests and inspection tooling to choose `SkipVerifier`.

## Error vocabulary

Decision: expose a client-side `ClientError` with distinct constructors
for:

- transport exceptions
- non-success HTTP statuses
- JSON decode failures
- verifier failures

Rationale: MOOG needs to treat these differently. A transport failure may
be retryable; a verifier failure is security-significant and should not
be retried blindly.
