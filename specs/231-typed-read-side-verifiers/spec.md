# Feature Specification: Typed read-side endpoints + verifiers

**Feature Branch**: `feat/231-typed-read-side-verifiers`
**Created**: 2026-04-26
**Status**: Draft
**Input**: Issue [#231](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/231) — "client: typed read-side endpoints + verifiers"

## User Scenarios & Testing

### User Story 1 — Read MPFS state through one client API (Priority: P1)

MOOG (and any other downstream wallet integrator) reads MPFS state on
every poll: it asks for token state, single facts, single MPF
inclusion/absence proofs, and the list of pending requests. Today,
each consumer owns its own URL construction, JSON decoding, and replay
plumbing. This feature adds typed Haskell mirrors of the four
proof-bearing read responses and pure offline verifiers for each, so a
consumer imports `cardano-mpfs-client`, decodes the response, runs one
verifier, and either gets `Right ()` or a structured `VerifyError`.

**Why this priority**: This is the second of the two MOOG-facing slices
that close the read/write asymmetry. After #230 (write-side typed
HTTP wrappers) the client could build transactions; without this,
MOOG still has to roll its own decoder and verifier for every read.

**Independent Test**: For each read endpoint, build an honest fixture
with the pure CSMT + MPF backends, decode it through the new types,
run the matching verifier, and assert `shouldAccept`. Build a forged
variant with the existing forgery DSL and assert `shouldRejectWith`
the expected `VerifyError` constructor and field path.

**Acceptance Scenarios**:

1. **Given** a `TokenResponse` whose state-output witness replays
   against the advertised `utxo_root`, **When** `verifyTokenResponse`
   is called, **Then** it returns `Right ()`.
2. **Given** a `FactResponse` whose state witness replays against
   `utxo_root` and whose MPF inclusion proof replays against the
   carried trie root, **When** `verifyFactResponse` is called,
   **Then** it returns `Right ()`.
3. **Given** a `ProofResponse` whose state witness and MPF
   inclusion/absence proof both replay, **When**
   `verifyProofResponse` is called, **Then** it returns `Right ()`.
4. **Given** a `RequestsResponse` whose every request witness replays
   against `utxo_root`, **When** `verifyRequestsResponse` is called,
   **Then** it returns `Right ()`.

### User Story 2 — Surface read forgeries with the same vocabulary (Priority: P1)

Whatever shape a read response takes, a forged proof must be rejected
with one of the existing `VerifyError` constructors at a dotted field
path that points to the role and leaf that failed. No new error
constructors, no per-endpoint wording — MOOG and any other consumer
already knows the vocabulary from the write-side verifiers.

**Independent Test**: Use the existing `flipProof`, `flipTxOut`,
`flipSnapshotRoot`, `flipTrieValue`, `dropToExclusion`, and
`flipTrieRoot` combinators to forge each read response. Assert each
forgery becomes `Left (CsmtReplayFailed …)` /
`Left (MpfReplayFailed …)` / `Left (MalformedHex …)` at a path of
shape `<endpoint>.<role>[<index>]?.<leaf>`.

**Acceptance Scenarios**:

1. **Given** a `TokenResponse` whose state UTxO proof was bit-flipped,
   **When** `verifyTokenResponse` is called, **Then** the result is
   `Left (CsmtReplayFailed "token.state.utxo_proof" "root mismatch")`.
2. **Given** a `FactResponse` whose MPF proof was bit-flipped,
   **When** `verifyFactResponse` is called, **Then** the result is
   `Left (MpfReplayFailed "fact.mpf_proof" "root mismatch")`.
3. **Given** a `RequestsResponse` whose first request witness has a
   forged `tx_out`, **When** `verifyRequestsResponse` is called,
   **Then** the result is
   `Left (CsmtReplayFailed "requests.requests[0].utxo_proof" "value binding mismatch")`.

### User Story 3 — Top-level re-exports (Priority: P2)

Downstream consumers should keep the single-import experience:
everything they need to verify a read response — types, verifiers, and
DSL combinators — must be reachable from `Cardano.MPFS.Client`.

**Independent Test**: A test module imports only
`Cardano.MPFS.Client` and constructs all four read responses, calls
all four verifiers, and uses the existing DSL combinators
(`shouldAccept`, `shouldRejectWith`, the forgery operations).

**Acceptance Scenarios**:

1. **Given** a consumer that imports only `Cardano.MPFS.Client`,
   **When** it constructs and verifies any read response, **Then** no
   additional client modules need to be imported.

## Edge Cases

- A `FactResponse.value` MUST match the value the MPF inclusion proof
  binds to. The verifier delegates this binding check to
  `verifyAikenInclusionProof`, which folds the advertised `(key, value)`
  into the recomputed root; a mismatch surfaces as
  `MpfReplayFailed _ "root mismatch"`.
- A `ProofResponse` is the only read response whose proof can legally
  carry an absence claim (`value == Nothing`). Verifiers MUST accept
  both inclusion and exclusion shapes against the carried trie root.
- A `RequestsResponse` with an empty requests list is valid — the
  verifier returns `Right ()` after the snapshot pass.
- The trie root the response carries (for `/facts` and `/proofs`) is
  reported by the server inside the state witness. The verifier MUST
  trust the state witness CBOR (already replayed against `utxo_root`)
  but only as far as the bytes go: the trie root is a separate hex
  field on the typed mirror so the client never re-derives it from
  ledger types.
- All field paths follow the existing dotted convention rooted at the
  endpoint name: `token.<role>[<index>]?.<leaf>`,
  `fact.<role>.<leaf>`, `proof.<role>.<leaf>`,
  `requests.<role>[<index>]?.<leaf>`.

## Requirements

### Functional Requirements

- **FR-001**: The client MUST expose typed mirrors `TokenResponse`,
  `FactResponse`, `ProofResponse`, and `RequestsResponse` for the four
  proof-bearing read endpoints.
- **FR-002**: The client MUST expose supporting witness types
  `WitnessedTokenState`, `WitnessedRequest`, and `FactWitness` plus
  decoded payload mirrors `TokenState` and `Request` covering exactly
  the fields the server emits over the wire.
- **FR-003**: The mirrors MUST round-trip the server's JSON wire format
  (snake_case field names, identical object shape).
- **FR-004**: The client MUST expose `verifyTokenResponse`,
  `verifyFactResponse`, `verifyProofResponse`, and
  `verifyRequestsResponse`, each pure, each
  `Response -> Either VerifyError ()`.
- **FR-005**: Each verifier MUST run the existing structural pass
  (hex decode, 32-byte hash check, non-empty payload) before any
  cryptographic replay.
- **FR-006**: Each verifier MUST replay every `WitnessedUtxo` against
  `snapshot.utxo_root` using the existing `replayWitnessedUtxo`
  primitive.
- **FR-007**: `verifyFactResponse` and `verifyProofResponse` MUST
  replay their MPF proof against the trie root carried in the response
  using the existing `replayTrieFact` primitive.
- **FR-008**: Verifiers MUST NOT introduce new `VerifyError`
  constructors; the existing fixed vocabulary suffices.
- **FR-009**: All field paths MUST follow the existing
  `<endpoint>.<role>[<index>]?.<leaf>` shape.
- **FR-010**: `Cardano.MPFS.Client` MUST re-export the new types and
  verifiers.
- **FR-011**: All new code MUST compile under the same dependency set
  as the existing client package — no `cardano-ledger-*`, no C FFI
  beyond pure hashing, no `IO` in verifier paths.

### Key Entities

- **TokenResponse**: Snapshot-bearing envelope for `GET /tokens/<id>`.
- **FactResponse**: Snapshot-bearing envelope for
  `GET /tokens/<id>/facts/<key>`.
- **ProofResponse**: Snapshot-bearing envelope for
  `GET /tokens/<id>/proofs/<key>`.
- **RequestsResponse**: Snapshot-bearing envelope for
  `GET /tokens/<id>/requests`.
- **WitnessedTokenState**: UTxO witness for the state output paired
  with the decoded state datum (owner, root, tip, process/retract
  windows).
- **FactWitness**: State witness plus an MPF proof targeting the trie
  root carried in the state.
- **WitnessedRequest**: UTxO witness for a pending request output
  paired with the decoded request payload.

## Success Criteria

- **SC-001**: Every read endpoint MOOG hits is decoded into a typed
  value and runs the existing replay primitives.
- **SC-002**: Honest fixtures + forgery-DSL tests for each new
  verifier exist in `cardano-mpfs-client:unit-tests`, mirroring the
  structure of `VerifySpec.hs`.
- **SC-003**: `cardano-mpfs-client:unit-tests` passes locally.
- **SC-004**: No write-side verifier test regresses.
- **SC-005**: The top-level `Cardano.MPFS.Client` import surface
  remains the only module a downstream consumer needs to import.

## Assumptions

- Server-side `TokenResponse`, `FactResponse`, `ProofResponse`, and
  `RequestsResponse` already exist in `cardano-mpfs-api:Cardano.MPFS.API.Types`
  and their wire format is the source of truth.
- The decoded state/request payload mirrors only need the fields a
  client integrator (MOOG) consumes; future server-side fields can be
  added later without breaking forwards-compat.
- HTTP transport for read endpoints is out of scope for this slice and
  belongs to a follow-up if needed; this slice is types and verifiers
  only, matching the issue's stated scope.

## Out of Scope

- HTTP transport (typed wrappers around `GET` endpoints) — separate
  follow-up.
- Anchoring (independent root witness) — issue [#232](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/232).
- Browser/WASM/JS packaging — milestone #3.
