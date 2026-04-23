# Feature Specification: Cryptographic CSMT + MPF proof replay in Client.Verify

**Feature Branch**: `feat/cryptographic-proof-replay`
**Created**: 2026-04-23
**Status**: Draft
**Input**: Issue [#226](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/226) — "client: replay CSMT + MPF proofs in Client.Verify (cryptographic replay vs. advertised root)"

## User Scenarios & Testing *(mandatory)*

### A note on the Client.Verify DSL

Because these are the scenarios that anyone building on top of
`cardano-mpfs-client` will read first, this spec treats the end-to-end
tests as **the manual** for the library. The `Client.Verify` DSL must
therefore be:

- **Self-describing**: reading a single E2E scenario top-to-bottom
  explains how to consume a proof-bearing response, without needing to
  open any other file. Helpers carry intent-revealing names
  (`acceptsResponse`, `rejectsResponse`, `forgeWrongRoot`,
  `tamperTxOut`, `tamperTrieValue`, `dropToExclusion`, …) rather than
  plumbing names.
- **Symmetrically readable on both success and failure**: positive and
  negative assertions share the same shape
  (`response `shouldAccept` verifier`,
  `response `shouldRejectWith` CsmtReplayFailedMatcher{..}`), so a
  reader learns the contract from either direction.
- **Usable as example code**: every helper the E2E scenarios use is
  exported from `Cardano.MPFS.Client` (or a dedicated
  `Cardano.MPFS.Client.Verify.Examples` module) so a downstream wallet
  can import the same helpers and wire them straight into its signing
  flow.

### User Story 1 — Accept an honest proof-bearing response (Priority: P1) — positive path

A wallet receives a well-formed response from
`POST /tx/boot | /tx/request | /tx/retract | /tx/reject | /tx/end | /tx/update`
whose proofs are all correct. Running the per-endpoint verifier must
return `Right ()`, and the E2E scenario that exercises this path must
read as a narrative tutorial for a new consumer:

```
scenario "boot returns an accepted proof-bearing response" $ do
    response <- server `postsBoot` ownerAddress
    response `shouldAccept` verifyBootTxResponse
```

**Why this priority**: the positive path is the only one a real
downstream user ever wants to hit. If this scenario is not clearly
the first thing someone reads, the library is not documenting itself.

**Independent Test**: boot a token on a real devnet, call every
`POST /tx/…` endpoint, decode the response with the exported client
types, and assert `shouldAccept` succeeds for every envelope.

**Acceptance Scenarios**:

1. **Given** a freshly-booted token on devnet, **When** the client
   calls `POST /tx/boot` and `verifyBootTxResponse` on the decoded
   response, **Then** it returns `Right ()`.
2. **Given** a pending `request_insert` on devnet, **When** the
   client calls `POST /tx/request/insert` and runs
   `verifyRequestTxResponse`, **Then** it returns `Right ()`.
3. **Given** the token has one processed fact and one pending
   request, **When** the client calls `POST /tx/update` and runs
   `verifyUpdateTxResponse`, **Then** it returns `Right ()` and
   every `trie_read[i]` in `UpdateProof` is cryptographically
   replayed against `UpdateProof.trie_root`.
4. **Given** a retractable pending request, **When** the client
   calls `POST /tx/retract` and runs `verifyRetractTxResponse`,
   **Then** it returns `Right ()`.
5. **Given** a token in `End`-eligible state, **When** the client
   calls `POST /tx/end` and runs `verifyEndTxResponse`, **Then**
   it returns `Right ()`.
6. **Given** the `POST /tx/reject` endpoint targets a request whose
   processing deadline has elapsed, **When** the client runs
   `verifyRejectTxResponse`, **Then** it returns `Right ()`. (If the
   deadline plumbing is not yet wired in the E2E harness, this
   scenario is covered by a dedicated unit test that feeds a
   hand-crafted accepted `RejectTxResponse` through the verifier;
   tracked by issue [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224).)

---

### User Story 2 — Accept an honest `UpdateProof` batch (Priority: P1) — positive path for MPF

Every `TrieFact` in `UpdateProof.trie_read` is a real proof against
the advertised `trie_root`: inclusion for facts with `value = Just _`,
exclusion for facts with `value = Nothing`. The verifier accepts.

```
scenario "update accepts a batch with mixed inclusion/exclusion trie reads" $ do
    tid <- devnet `booted`
    _   <- devnet `insertsFact` (tid, "hello", "world")
    _   <- devnet `processesPending` tid
    _   <- devnet `requestsInsert` (tid, "bye", "moon")
    response <- server `postsUpdate` (tid, ownerAddress)
    response `shouldAccept` verifyUpdateTxResponse
```

**Why this priority**: `UpdateProof` is the batched flow that the
trust-minimised API story hinges on. A reader has to see the
positive path to understand what success looks like before the
negative scenarios teach them what to reject.

**Independent Test**: run the E2E scenario above on devnet; assert
every `trie_read[i]` ships either an inclusion proof (for
`value = Just _`) or an exclusion proof (for `value = Nothing`), and
the verifier returns `Right ()`.

**Acceptance Scenarios**:

1. **Given** an `UpdateProof` whose every `trie_read[i]` is a
   correct MPF inclusion proof for the advertised `key` and `value`
   against `trie_root`, **When** `verifyUpdateTxResponse` runs,
   **Then** it returns `Right ()`.
2. **Given** an `UpdateProof` whose `trie_read[i]` declares
   `value = Nothing` and ships a correct MPF exclusion proof for
   `key` against `trie_root`, **When** `verifyUpdateTxResponse`
   runs, **Then** it returns `Right ()`.
3. **Given** an `UpdateProof` that batches zero pending requests
   (empty `trie_read`), **When** `verifyUpdateTxResponse` runs,
   **Then** it returns `Right ()` and no replay is attempted.

---

### User Story 3 — Reject a forged `utxo_proof` before signing (Priority: P1)

A wallet or signer consumes the proof-bearing response from a
`POST /tx/boot | /tx/request | /tx/retract | /tx/reject | /tx/end | /tx/update`
endpoint. The server is assumed untrusted. Before presenting the
unsigned transaction to the human signer, the client verifier must
cryptographically replay every `WitnessedUtxo` against the advertised
`snapshot.utxo_root`. A response whose `utxo_proof` is well-formed hex
but does not actually prove the advertised `(tx_in, tx_out)` against
`utxo_root` must be rejected with a structured error that names the
field path and the reason.

**Why this priority**: this is the load-bearing property of the whole
proof-bearing API. Without cryptographic replay, a malicious or broken
server can ship a syntactically valid but semantically empty envelope
and the client will accept it. Every downstream scenario (binding to
tx, root anchor trust, offline signers) depends on replay being the
floor.

**Independent Test**: construct a forged response with hex-valid but
cryptographically wrong `utxo_proof` bytes; feed it to
`verify*TxResponse`; assert the result is
`Left (CsmtReplayFailed <field-path> <reason>)`. Repeat with (a) a
correct proof against a different root, (b) a correct proof for a
different `tx_in`, (c) a correct proof for the right `tx_in` but a
tampered `tx_out`.

**Acceptance Scenarios**:

```
scenario "boot rejects a response whose funding proof is random bytes" $ do
    response   <- server `postsBoot` ownerAddress
    tampered   <- response `forgingRandomUtxoProofAt` "boot.funding[0]"
    tampered `shouldRejectWith`
        CsmtReplayFailed "boot.funding[0].utxo_proof" _

scenario "retract rejects a response whose state-ref proof is against the wrong root" $ do
    response <- server `postsRetract` (utxoRef, ownerAddress)
    forged   <- response `forgingWrongRootAt` "retract.state_ref"
    forged `shouldRejectWith`
        CsmtReplayFailed "retract.state_ref.utxo_proof" _
```

1. **Given** a `BootTxResponse` whose `boot.funding[0].utxo_proof`
   decodes to random bytes, **When**
   `verifyBootTxResponse` runs, **Then** it returns
   `Left (CsmtReplayFailed "boot.funding[0].utxo_proof" _)`.
2. **Given** a `RetractTxResponse` whose `retract.state_ref.utxo_proof`
   is a correct CSMT proof but against a root that differs from
   `snapshot.utxo_root`, **When** `verifyRetractTxResponse` runs,
   **Then** it returns
   `Left (CsmtReplayFailed "retract.state_ref.utxo_proof" _)`.
3. **Given** an `UpdateTxResponse` whose `update.requests[0].txOut`
   field is tampered after the proof was generated, **When**
   `verifyUpdateTxResponse` runs, **Then** it returns
   `Left (CsmtReplayFailed "update.requests[0].utxo_proof" _)` because
   the in-proof value no longer equals the advertised `txOut`.

---

### User Story 4 — Reject a forged `mpf_proof` for a contributing request (Priority: P1)

For `POST /tx/update`, the response carries `UpdateProof.trie_root`
and a list of `TrieFact`s covering every request key whose insertion,
deletion, or update contributes to the batched `update`. The client
must cryptographically replay each `mpf_proof` against `trie_root`
and cross-check that the in-proof (key, value) matches the advertised
`TrieFact.key` and `TrieFact.value`.

**Why this priority**: `trie_root` is the datum the on-chain validator
reads. If an MPF inclusion / exclusion proof is forged, the client
cannot independently confirm that the batched update really corresponds
to the advertised pending requests — the server could claim any
request contributed to the batch.

**Independent Test**: construct an `UpdateProof` whose
`trie_read[0].mpf_proof` decodes but does not validate against
`trie_root`; assert the result is
`Left (MpfReplayFailed "update.trie_read[0].mpf_proof" _)`.

**Acceptance Scenarios**:

```
scenario "update rejects a batch with a tampered trie-read value" $ do
    tid      <- devnet `booted`
    _        <- devnet `insertsFact` (tid, "hello", "world")
    _        <- devnet `processesPending` tid
    _        <- devnet `requestsInsert` (tid, "bye", "moon")
    response <- server `postsUpdate` (tid, ownerAddress)
    tampered <- response `tamperingTrieValueAt` 0
    tampered `shouldRejectWith`
        MpfReplayFailed "update.trie_read[0].mpf_proof" _

scenario "update rejects an absence claim carrying an inclusion proof" $ do
    tid      <- devnet `booted`
    response <- server `postsUpdate` (tid, ownerAddress)
    forged   <- response `dropToExclusionAt` 0
    forged `shouldRejectWith`
        MpfReplayFailed "update.trie_read[0].mpf_proof" _
```

1. **Given** an `UpdateProof` whose `trie_read[0].value` is tampered
   (but the proof still decodes), **When** `verifyUpdateTxResponse`
   runs, **Then** it returns
   `Left (MpfReplayFailed "update.trie_read[0].mpf_proof" _)` because
   the replayed value no longer matches the advertised value.
2. **Given** an `UpdateProof` whose `trie_read[0]` declares
   `value = Nothing` but the MPF proof is actually an inclusion
   proof (or an exclusion proof against a different root), **When**
   `verifyUpdateTxResponse` runs, **Then** it returns
   `Left (MpfReplayFailed "update.trie_read[0].mpf_proof" _)`.
3. **Given** an `UpdateProof` whose `trie_read[0].mpf_proof` is a
   correct proof for the right key/value but against a root that
   differs from `UpdateProof.trie_root`, **When**
   `verifyUpdateTxResponse` runs, **Then** it returns
   `Left (MpfReplayFailed "update.trie_read[0].mpf_proof" _)`.

---

### User Story 5 — Offline verification stays cross-target compatible (Priority: P1)

The client verifier is intended to ship inside wallets, browser dApps
(GHC-JS), and WASM signers (GHC-WASM). Wiring in cryptographic replay
must not break any of those targets: no `cardano-ledger-*` or C-FFI
dependencies enter `cardano-mpfs-client`.

**Why this priority**: Constitution IX mandates GHC-native, GHC-WASM,
and GHC-JS byte-identical build of `cardano-mpfs-client`. A regression
here invalidates the whole trust-minimised API story.

**Independent Test**: the existing cross-target CI job
(`.#checks.<sys>.cardano-mpfs-client-cross-target`) builds the library
and runs the `Either VerifyError a` byte-identity suite on all three
backends with no new build-depends outside the pure-Haskell set
(`aeson`, `base`, `base16-bytestring`, `bytestring`, `cborg`, `text`,
and the already-pure `mts:csmt-verify` / `mts:mpf-write`).

**Acceptance Scenarios**:

1. **Given** the feature branch at `HEAD`, **When** the cross-target
   CI job runs, **Then** all three targets succeed and produce the
   same `VerifyError` bytes for the same inputs.
2. **Given** a future change that accidentally adds a C-FFI
   dependency (e.g. `crypton`) to `cardano-mpfs-client`, **When** CI
   runs, **Then** the GHC-WASM / GHC-JS targets fail the build
   (enforced by the `ghc-options` or `build-depends` audit in the
   existing cross-target check).

---

### Edge Cases

- `utxo_proof` / `mpf_proof` decodes as CBOR but its in-proof
  `proofKey` / `key` does not equal the advertised `tx_in` / `key`
  after hashing → **reject** with a binding error, not a generic
  replay error, so the field path identifies the mismatch.
- `UpdateProof.trie_root` is a 32-byte hash but is not the root
  committed in the unsigned tx's datum → **out of scope** here
  (covered by issue #227), the verifier only checks that
  `trie_read[i]` proofs replay against whatever `trie_root` the
  response advertises.
- An `UpdateProof` lists zero `trie_read` entries (batch touches
  no requests) → accept; there is nothing to replay. Structural
  checks (including `checkHash32 "update.trie_root"`) still run.
- A `TrieFact` declares `value = Just v` and its `mpf_proof` is
  syntactically an exclusion proof → **reject** as
  `MpfReplayFailed` (inclusion claim replayed as exclusion).
- `WitnessedUtxo.txOut` decodes to a CBOR-valid byte string whose
  first byte does not match the in-proof value → **reject** as
  `CsmtReplayFailed` (value-binding mismatch). Deeper `TxOut`
  schema validation is out of scope (covered by the shallow-decoder
  work in spec 177 / issue #227).
- `snapshot.utxo_root` is syntactically valid but all-zero bytes →
  accept at the structural layer (matches today's behaviour), but
  any non-trivial `WitnessedUtxo` will fail the replay against that
  root and surface as `CsmtReplayFailed`.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The client verifier MUST cryptographically replay every
  `WitnessedUtxo` carried by `BootTxResponse`, `RequestTxResponse`,
  `RetractTxResponse`, `RejectTxResponse`, `EndTxResponse`, and
  `UpdateTxResponse` against the advertised
  `VerificationSnapshot.utxoRoot` and return a structured error on
  mismatch.
- **FR-002**: For each replayed `WitnessedUtxo`, the verifier MUST
  confirm that the proof's in-proof key binds to the advertised
  `TxIn` and the proof's in-proof value binds to the advertised
  `TxOut` bytes, so that a correctly-rooted proof for a different
  UTxO still fails.
- **FR-003**: The client verifier MUST cryptographically replay every
  `TrieFact` carried by `UpdateProof.trie_read` against
  `UpdateProof.trie_root`, treating `value = Just _` as an inclusion
  claim and `value = Nothing` as an exclusion claim, and return a
  structured error on mismatch.
- **FR-004**: For each replayed `TrieFact`, the verifier MUST
  confirm that the proof's in-proof key binds to the advertised
  `key` and (for inclusion) its in-proof value binds to the
  advertised `value`.
- **FR-005**: `VerifyError` MUST grow at least two new constructors —
  `CsmtReplayFailed Text Text` and `MpfReplayFailed Text Text` —
  where the first field is the dotted field path rooted at the
  endpoint name (e.g. `"retract.state_ref.utxo_proof"`,
  `"update.trie_read[3].mpf_proof"`) and the second is a
  human-readable reason (e.g. `"root mismatch"`,
  `"key binding mismatch"`, `"value binding mismatch"`,
  `"malformed proof CBOR"`).
- **FR-006**: Existing structural-only verifiers (snapshot
  well-formedness, hex decode, 32-byte hashes, non-empty hex,
  funding list traversal) MUST continue to produce the same errors
  they produce today; cryptographic checks run only once structural
  checks pass.
- **FR-007**: `cardano-mpfs-client`'s `library` stanza MUST NOT gain
  any `build-depends` on `cardano-ledger-*`, `crypton`, `rocksdb*`,
  or any other C-FFI or native-only library. The only new
  dependencies allowed are `mts:csmt-verify`, `mts:mpf-write`,
  and (if strictly required for binding checks) `cborg`.
- **FR-008**: A new unit-test target MUST cover, at minimum, the
  forged-proof cases enumerated in User Stories 1 and 2: random
  bytes, correct proof against a different root, correct proof for
  a different `tx_in` / `key`, and correct proof with tampered
  `tx_out` / `value`. For MPF, both inclusion and exclusion forgeries
  must be covered.
- **FR-009**: The GHC-native, GHC-WASM, and GHC-JS cross-target CI
  job for `cardano-mpfs-client` MUST stay green on the feature branch,
  with byte-identical `Either VerifyError a` outputs across targets
  for the unit-test corpus.
- **FR-010**: `cardano-mpfs-client` MUST expose a
  `Cardano.MPFS.Client.Verify.DSL` module whose combinators make both
  positive (`shouldAccept`) and negative (`shouldRejectWith`) E2E
  assertions read as declarative tutorial code. Combinators MUST use
  intent-revealing names that pair honest and forged paths
  (`forgingRandomUtxoProofAt`, `forgingWrongRootAt`,
  `tamperingTxOutAt`, `tamperingTrieValueAt`, `dropToExclusionAt`,
  `promoteToInclusionAt`).
- **FR-011**: The proof-bundle E2E spec MUST exercise **every**
  per-endpoint verifier along both paths: a positive scenario that
  asserts `shouldAccept` and at least one negative scenario that
  asserts `shouldRejectWith` a structured `VerifyError`. Endpoints
  whose negative path cannot be hit on devnet (e.g. `/tx/reject`
  without an elapsed deadline) MUST be covered by a unit-test
  counterpart that feeds a hand-crafted response through the same
  DSL combinators.
- **FR-012**: Reading the E2E spec top-to-bottom MUST be enough for
  a new integrator to understand how to consume every proof-bearing
  response: every helper the spec uses is exported from
  `Cardano.MPFS.Client` (or a dedicated submodule) with Haddock that
  links back to the spec scenario that introduces it, so the spec
  doubles as the client-library manual.

### Key Entities *(include if feature involves data)*

- **VerifyError**: structured result of a client verification.
  Grows new cases for cryptographic-replay failures carrying the
  dotted field path and a reason string.
- **WitnessedUtxo**: a UTxO reference (`tx_in`), its CBOR-encoded
  `tx_out` bytes, and a CSMT inclusion proof. The proof binds the
  pair into a `utxo_root`.
- **TrieFact**: a trie read carrying a `key`, an optional `value`
  (presence vs. absence claim), and an MPF proof (inclusion or
  exclusion) against an `UpdateProof.trie_root`.
- **VerificationSnapshot**: the advertised `utxo_root` + indexed
  chain-point that every proof-bearing response roots its witnesses
  in. Trusting the root itself is explicitly out of scope here.
- **Client.Verify.DSL**: a thin combinator layer living in
  `cardano-mpfs-client` whose operators
  (`shouldAccept`, `shouldRejectWith`,
  `forgingRandomUtxoProofAt`, `forgingWrongRootAt`,
  `tamperingTxOutAt`, `tamperingTrieValueAt`,
  `dropToExclusionAt`, `promoteToInclusionAt`) let E2E and unit tests
  read as declarative tutorial code and are the extractable "manual"
  for any downstream wallet integrator.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of `WitnessedUtxo` values across all six
  per-endpoint response verifiers are cryptographically replayed
  before the verifier returns `Right ()`.
- **SC-002**: 100% of `TrieFact` values in `UpdateProof.trie_read`
  are cryptographically replayed before `verifyUpdateTxResponse`
  returns `Right ()`.
- **SC-003**: Forged-proof test corpus (at least 8 scenarios:
  4 CSMT + 4 MPF) fails verification with either
  `CsmtReplayFailed` or `MpfReplayFailed`, and no case falls through
  to `Right ()`.
- **SC-004**: The GHC-native, GHC-WASM, and GHC-JS cross-target
  check for `cardano-mpfs-client` remains green and the
  `cardano-mpfs-client:unit-tests` suite passes on all three targets
  with byte-identical `VerifyError` outputs for the forged-proof
  corpus.
- **SC-005**: No new dependency on `cardano-ledger-*`, `crypton`,
  or any C-FFI library is introduced in the
  `cardano-mpfs-client` library stanza (verified by `cabal check`
  and a `build-depends` audit in the cross-target check).
- **SC-006**: The proof-bundle E2E spec exercises each of the 6
  per-endpoint verifiers along both paths (`shouldAccept` +
  `shouldRejectWith`), for a total of at least 12 scenarios. Every
  helper the spec uses (including forgery helpers) is exported from
  `cardano-mpfs-client` with Haddock.
- **SC-007**: Manual readability bar: a reviewer who has never seen
  the codebase, reading only `E2E/ProofsSpec.hs`, can enumerate the
  happy path and every class of failure the verifier detects,
  without opening any other `.hs` file — validated by a short
  checklist walk-through added to the PR description.

## Assumptions

- Upstream `haskell-mts` main already ships WASM-safe `mts:csmt-verify`
  (CSMT inclusion + exclusion) and `mts:mpf-write` (MPF inclusion +
  exclusion via `MPF.Verify`). Consuming them pins the existing
  `source-repository-package` to a commit on `main` that has both.
- `InclusionProof` CBOR on the wire embeds `proofKey` and `proofValue`,
  so the verifier can bind the advertised `(tx_in, tx_out)` /
  `(key, value)` pair against the in-proof pair without a separate
  out-of-band channel.
- MPF proofs on the wire already distinguish inclusion vs. exclusion
  by their CBOR shape (Aiken-parity), so the verifier selects the
  right primitive from `MPF.Verify` based on whether the advertised
  `TrieFact.value` is `Just _` or `Nothing`.
- The advertised `snapshot.utxo_root` and `UpdateProof.trie_root`
  are treated as the "trusted roots" input to the cryptographic
  primitives. Deciding whether they are the _right_ roots (e.g.
  match an independent chain follower, or the datum embedded in
  the unsigned tx) is out of scope — covered by future work
  (trust-anchor ticket, and issue #227).
- The existing cross-target CI check for `cardano-mpfs-client`
  already audits `build-depends` and enforces `Either VerifyError a`
  byte identity across GHC-native, GHC-WASM, and GHC-JS; this
  feature only needs to keep that check green.
