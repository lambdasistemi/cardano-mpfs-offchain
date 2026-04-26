# Feature Specification: Typed HTTP wrappers for MOOG

**Feature Branch**: `feat/client-typed-http-wrappers-cardanompfsclienthttp`
**Created**: 2026-04-26
**Status**: Draft
**Input**: Issue [#230](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/230) - "client: typed HTTP wrappers (Cardano.MPFS.Client.Http)"

## User Scenarios & Testing

### User Story 1 - Build write transactions through one client API (Priority: P1)

The MOOG CLI needs to ask an MPFS offchain service to build unsigned
transactions for boot, request insert/delete/update, retract, reject,
update, and end flows. It should import `cardano-mpfs-client`, configure
one HTTP client value, and call one typed function per endpoint instead
of duplicating URL construction, JSON encoding, response decoding, and
verification wiring.

**Why this priority**: This is the first blocker for rewriting MOOG
against MPFS. Without it, MOOG still owns transport and decoding glue for
each endpoint.

**Independent Test**: Use a local mock HTTP handler for each write
endpoint. The client function must send the documented JSON request body,
decode the proof-bearing response envelope, and return the typed value.

**Acceptance Scenarios**:

1. **Given** a configured MPFS HTTP client and a boot request, **When**
   MOOG calls `bootTx`, **Then** the client posts to `/tx/boot` and
   returns a decoded `BootTxResponse`.
2. **Given** a configured MPFS HTTP client and request-insert params,
   **When** MOOG calls `requestInsertTx`, **Then** the client posts to
   `/tx/request/insert` and returns a decoded `RequestTxResponse`.
3. **Given** the same configured client, **When** each write endpoint is
   called, **Then** no caller has to manually build paths or decode JSON.

### User Story 2 - Verify responses before returning them to MOOG (Priority: P1)

MOOG must not accidentally sign an unverified proof-bearing transaction
response. The HTTP wrapper should run the existing offline verifier by
default when configured to do so, and surface verification failures as a
client error.

**Why this priority**: The API exists to preserve "verify before sign".
An ergonomic transport layer that skips verification by accident would
undo the security property established by the client verifier.

**Independent Test**: Serve an honest JSON response and a forged JSON
response from the test handler. With `RunVerifier`, the honest response
is returned and the forged response becomes `VerifyFailed`; with
`SkipVerifier`, decoding succeeds without running the verifier.

**Acceptance Scenarios**:

1. **Given** `RunVerifier`, **When** a response verifier accepts the
   decoded envelope, **Then** the HTTP call returns `Right response`.
2. **Given** `RunVerifier`, **When** a response verifier rejects the
   decoded envelope, **Then** the HTTP call returns
   `Left (VerifyFailed err)`.
3. **Given** `SkipVerifier`, **When** the response JSON decodes, **Then**
   the HTTP call returns the decoded response without running offline
   replay.

### User Story 3 - Surface transport and decoding failures explicitly (Priority: P2)

MOOG needs stable error constructors for networking failures,
non-success HTTP statuses, and malformed JSON so it can decide whether
to retry, report, or abort.

**Why this priority**: Transport errors are operational failures, while
verification failures are security failures. The client surface must not
flatten them into strings.

**Independent Test**: Exercise connection failure, non-2xx response, and
malformed JSON paths through the wrapper and assert distinct
`ClientError` constructors.

**Acceptance Scenarios**:

1. **Given** the service is unreachable, **When** a wrapper is called,
   **Then** the result is `TransportError`.
2. **Given** the service returns a non-2xx status, **When** a wrapper is
   called, **Then** the result is an HTTP-status client error.
3. **Given** the service returns invalid JSON, **When** a wrapper is
   called, **Then** the result is `DecodeError`.

## Edge Cases

- A configured base URL may or may not include a trailing slash.
- Endpoint paths must not double-slash when the base URL has a path
  prefix.
- Non-2xx HTTP responses may contain JSON or plain text; the client only
  needs to preserve status and a bounded response body for diagnostics.
- Response decoding must fail closed: a JSON shape mismatch must not
  bypass verification.
- Verification is endpoint-specific: a `RequestTxResponse` returned from
  an update endpoint is valid for request endpoints only, not for
  update-tx verification.
- Browser, WASM, and WASI transport packaging is out of scope for this
  milestone; it belongs to milestone #3.

## Requirements

### Functional Requirements

- **FR-001**: The client MUST expose a `Cardano.MPFS.Client.Http` module.
- **FR-002**: The module MUST expose a reusable `MpfsHttp` configuration
  containing an HTTP manager, a base URL, and verifier mode.
- **FR-003**: The module MUST expose `RunVerifier` and `SkipVerifier`
  modes.
- **FR-004**: The module MUST expose one function for each write
  endpoint: boot, request insert, request delete, request update,
  retract, reject, update, and end.
- **FR-005**: Each function MUST encode a typed request body matching the
  server's documented JSON contract.
- **FR-006**: Each function MUST decode the server response into the
  existing `cardano-mpfs-client` response envelope type.
- **FR-007**: When configured with `RunVerifier`, each function MUST run
  the corresponding existing offline verifier before returning success.
- **FR-008**: The module MUST distinguish transport errors, non-success
  HTTP statuses, decode errors, and verifier failures.
- **FR-009**: The HTTP wrapper MUST NOT move network, time, disk, or
  retry behavior into the pure verifier modules.
- **FR-010**: The top-level `Cardano.MPFS.Client` module MUST re-export
  the MOOG-facing HTTP surface.

### Key Entities

- **MpfsHttp**: Shared client configuration for endpoint wrappers.
- **VerifierMode**: Chooses whether a wrapper runs offline verification
  before returning a decoded response.
- **ClientError**: Operational and verification failures from an HTTP
  call.
- **Request parameter types**: Client-side JSON request bodies for
  write endpoints. They mirror the server wire format without importing
  server-internal ledger types.

## Success Criteria

- **SC-001**: MOOG can depend on `cardano-mpfs-client` for all write-side
  HTTP request encoding, response decoding, and response verification.
- **SC-002**: Unit tests cover every write endpoint wrapper with at least
  one successful mocked response.
- **SC-003**: Unit tests cover transport, status, decode, and
  verification failure paths.
- **SC-004**: `cardano-mpfs-client:unit-tests` passes locally.
- **SC-005**: No existing pure verifier test regresses.

## Assumptions

- MOOG executes as native Haskell CLI code in this milestone.
- Browser, WASM, WASI, and npm packaging are handled by the separate
  `WASM/WASI MPFS API Client` milestone.
- The HTTP wrapper may use a native Haskell HTTP transport dependency,
  but the existing offline verifier remains pure and reusable.
- The server JSON request contracts in
  `Cardano.MPFS.HTTP.Types` are the current wire-format source of truth.
