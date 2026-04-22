# Implementation Plan: Proof-Carrying API Responses

**Branch**: `177-proof-carrying-api` | **Date**: 2026-04-17 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/177-proof-carrying-api/spec.md`

## Summary

Upgrade the affected query and transaction-building endpoints from bare
state/hex/CBOR payloads to proof-bearing JSON objects. Query endpoints
can mostly assemble bundles in the HTTP layer using existing `Context`
proof hooks, while transaction endpoints likely require a richer
`TxBuilder` return type so the server can return the unsigned
transaction plus the exact UTxO and MPF witnesses the builder relied on.
The design correction is that proof-bearing JSON must carry its own
`utxo_root` and indexed `chainpoint`; verification must not depend on a
separate metadata-discovery call. The first milestone is explicitly the
HTTP client read-verification scenario: fetch proof-bearing token data,
read the baked-in root and chain point, and verify it offline before
tackling transaction-signing flows.

## Technical Context

**Language/Version**: Haskell (GHC 9.8.4)
**Primary Dependencies**: servant, swagger2, cardano-ledger,
cardano-utxo-csmt, cardano-mpfs-onchain
**Storage**: RocksDB-backed index/state plus persistent trie manager
**Testing**: hspec unit tests, HTTP endpoint specs, E2E devnet tests,
Swagger freshness check via `just update-swagger`
**Target Platform**: Linux server (Nix-built service and Docker image)
**Project Type**: web-service for unsigned transaction building and trie
queries
**Performance Goals**: Preserve current synchronous request model while
adding proof generation bounded by the number of bundled UTxOs and trie
facts in the response
**Constraints**: One indexed root plus one indexed chain point per
response, unsigned txs only,
direct `/utxo/*` debugging endpoints remain valid, Swagger JSON must stay
fresh
**Scale/Scope**: 13 affected endpoints (`GET /status`, 4 token query
endpoints, 8 tx-building endpoints) plus shared schemas, docs, and
tests

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | Pass | Witness bundles should reuse existing `TxIn`, `TxOut`, and serialized ledger CBOR rather than invent shadow chain types |
| II. Records of Functions | Pass | `TxBuilder` may return a richer product type, but remains a record-of-functions boundary |
| III. Atomic Block Processing | N/A | No block application or RocksDB write-batch changes |
| IV. External Signing | Pass | Responses remain unsigned; richer metadata helps clients verify before signing |
| V. Aiken Compatibility | Pass | MPF proofs reuse existing trie proof encoding; UTxO proofs come from the indexed CSMT state |
| VI. Test Locally First | Pass | HTTP, unit, E2E, and Swagger freshness checks all run locally |
| VII. Nix Reproducibility | Pass | No new non-flake tooling required |

## Project Structure

### Documentation (this feature)

```text
specs/177-proof-carrying-api/
├── spec.md
├── research.md
└── plan.md
```

### Source Code Changes

```text
cardano-mpfs-offchain/lib/Cardano/MPFS/
├── Context.hs                 # Existing UTxO proof hooks used by HTTP/query layer
├── TxBuilder.hs               # Builder interface likely returns proof-bearing bundles
├── TxBuilder/Real.hs          # Wire richer builder results
├── TxBuilder/Real/Boot.hs     # Boot witness bundle (wallet inputs only)
├── TxBuilder/Real/Request.hs  # Request insert/delete/update witness bundles
├── TxBuilder/Real/Update.hs   # Update bundle with request/state MPF proofs
├── TxBuilder/Real/Retract.hs  # Retract bundle with request/state witnesses
├── TxBuilder/Real/Reject.hs   # Reject bundle for batched Phase 3 requests
├── TxBuilder/Real/End.hs      # End bundle with consumed inputs
├── HTTP/API.hs                # Response types for affected endpoints change from scalars to objects
├── HTTP/Types.hs              # Shared proof-bearing response schemas
├── HTTP/Server.hs             # Assemble query bundles and serialize tx bundles
└── HTTP/Swagger.hs            # Swagger description updates if schema naming/notes need tightening

cardano-mpfs-offchain/test/Cardano/MPFS/
├── HTTP/StatusSpec.hs         # `GET /status` root + snapshot contract
├── HTTP/TokenSpec.hs          # `GET /tokens/:id`
├── HTTP/TrieSpec.hs           # `GET /facts/:key`, `GET /proofs/:key`
├── HTTP/RequestsSpec.hs       # `GET /requests`
└── TxBuilderSpec.hs           # Rich tx bundle shape and proof association

cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/
├── HTTPLifecycleSpec.hs       # Inline proof contracts and direct `/utxo/*` cross-checks
└── CageFlowSpec.hs            # Update/request/reject/end flows with proof-bearing tx responses

docs/
└── assets/swagger.json        # Regenerated API contract
```

**Structure Decision**: Keep the implementation inside the existing
single Haskell service. Query proof bundling lives primarily in
`HTTP.*`; transaction proof bundling crosses `TxBuilder` and `HTTP.*`
because the builder is the only layer that knows which inputs and trie
facts justified the unsigned transaction.

## Implementation Phases

### Slice 1: Shared response model + status root

Add the shared proof-bearing response types in `HTTP.Types` and extend
`StatusResponse` with the current UTxO-CSMT root. Wire `statusHandler`
to surface the root from `Context.utxoRoot` alongside existing chain tip
and checkpoint metadata. Treat `GET /utxo/root` as an explicitly
supported sibling endpoint, not incidental duplication. Define the
shared `VerificationSnapshot` shape so later proof-bearing responses bake
in `utxo_root` and indexed `chainpoint`.

**Files**: `HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`test/Cardano/MPFS/HTTP/StatusSpec.hs`

**Goal**: Establish the verification snapshot contract before changing
the token and tx endpoints, and make `/status` and `/utxo/root` agree as
the same source of truth.

### Slice 2: Proof-bearing query endpoints

Convert the affected token query endpoints from scalar payloads to
structured objects:

- `GET /tokens/:id`
- `GET /tokens/:id/facts/:key`
- `GET /tokens/:id/proofs/:key`
- `GET /tokens/:id/requests`

Use the existing `Context.resolveUtxo`, `Context.utxoProof`, and trie
proof code to attach witnessed state/request UTxOs and MPF proofs to the
business payloads. Each proof-bearing response object must include the
exact `utxo_root` and indexed `chainpoint` it targets.

**Files**: `HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`test/Cardano/MPFS/HTTP/TokenSpec.hs`,
`test/Cardano/MPFS/HTTP/TrieSpec.hs`,
`test/Cardano/MPFS/HTTP/RequestsSpec.hs`

**Goal**: Clients can verify all read-side responses offline using the
snapshot baked into each response.

**Milestone**: After slices 1 and 2, the first client-side scenario is
complete without touching transaction-signing flows: read a proof-bearing
response, extract `utxo_root` and `chainpoint`, and verify.

### Slice 3: Rich transaction builder boundary

Replace the current `TxBuilder` return type of bare `Tx ConwayEra` with
a first-cut proof-bearing bundle type that carries:

- the unsigned transaction
- the set of consumed inputs as witnessed UTxOs
- baked-in `utxo_root` and indexed `chainpoint`
- optional trie proof payloads for trie-dependent operations

Update mocks and the top-level wiring in `TxBuilder/Real.hs`. This
slice landed as a flat `UnsignedTxBundle` with a single
`bundleInputs :: [WitnessedInput]` field; slice 4 narrows that shape
into per-endpoint proof records once the endpoint-specific roles
(state, pending request, state reference, funding) are clear.

**Files**: `TxBuilder.hs`, `TxBuilder/Real.hs`,
`Mock/TxBuilder.hs`, `test/Cardano/MPFS/TxBuilderSpec.hs`

**Why this slice exists**: Parsing the final tx in `HTTP.Server` is not
enough to reconstruct MPF proof intent or stable proof-to-request
association for batched flows. It starts only after the read-side
HTTP-client scenario is accepted.

### Slice 4: Per-endpoint proof shapes + WASM verifier + simple tx endpoints

This slice is structured as a typed vertical, not a flat migration.
It lands three coupled changes behind one merge:

1. **Typed per-endpoint proof shapes** replace the flat bundle from
   slice 3. `TxBuilder` methods return `ProofEnvelope p` where `p` is
   one of `BootProof`, `RequestProof`, `RetractProof`, `RejectProof`,
   or `EndProof`. Every `WitnessedInput` inside `p` has a named field
   (state / requests / state reference / funding) that documents its
   role, and the verifier walks those fields directly.

2. **WASM-compatible client verifier**. `cardano-mpfs-client` gains a
   pure-Haskell shallow `TxOut` decoder that extracts
   `(address bytes, ada lovelace)` from Conway `TxOut` CBOR without
   depending on `cardano-ledger-*` or any C FFI. A cross-check test
   suite in `cardano-mpfs-offchain` proves the shallow decoder agrees
   byte-for-byte with the authoritative ledger decoder across a dense
   generator and a pinned regression corpus. This is the prerequisite
   the #208 plan marks as a hard gate for slice 4 (see "Follow-up:
   cross-compile spike for `cardano-mpfs-client`"): the verifier has
   no right to exist until it can be cross-compiled to wallet runtimes
   that cannot pull ledger.

3. **Simple tx endpoints on the new shapes**. The five endpoints below
   migrate to the typed shapes, the new response schemas, and the
   matching per-endpoint verifiers in `cardano-mpfs-client`:

   - `POST /tx/boot`
   - `POST /tx/request/insert`
   - `POST /tx/request/delete`
   - `POST /tx/request/update`
   - `POST /tx/retract`
   - `POST /tx/reject`
   - `POST /tx/end`

   Boot omits MPF sections entirely; the others include them only when
   the builder actually relies on trie/state facts that the client must
   trust. All of them carry `utxo_root` and indexed `chainpoint`.

The slice opens with the shallow decoder and cross-check suite landing
first (as the narrowest bisect-safe commit). The Real-impl updates then
produce structurally valid proof payloads that thread through to the
HTTP response schema and the client verifier. A stub E2E test walks
each proof shape via the client verifier as soon as the shapes exist,
so the "traverse the proof" scenario is demonstrated before any of the
real CSMT proof bytes are wired through.

**Files**: `TxBuilder.hs`, `TxBuilder/Real/Boot.hs`,
`TxBuilder/Real/Request.hs`, `TxBuilder/Real/Retract.hs`,
`TxBuilder/Real/Reject.hs`, `TxBuilder/Real/End.hs`,
`HTTP/API.hs`, `HTTP/Types.hs`, `HTTP/Server.hs`,
`cardano-mpfs-client/lib/Cardano/MPFS/Client/TxOut.hs`,
`cardano-mpfs-client/lib/Cardano/MPFS/Client/Bundle.hs`,
`cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs`,
`test/Cardano/MPFS/Client/TxOutShallowSpec.hs`,
`test/Cardano/MPFS/TxBuilderSpec.hs`,
`e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs`,
`test/vectors/txout-regression-corpus/*.hex`

### Slice 5: `POST /tx/update` proof bundle

Handle the hardest endpoint separately. `POST /tx/update` batches
pending requests and applies trie changes, so the response must carry:

- witnessed state input
- witnessed request inputs
- MPF proofs for every request key/value the update relied on
- deterministic association between each request and its proof data

**Files**: `TxBuilder/Real/Update.hs`, `HTTP/Types.hs`,
`HTTP/Server.hs`, `test/Cardano/MPFS/TxBuilderSpec.hs`,
`e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs`

**Goal**: A client can inspect a batched update transaction and verify
every contributing request before signing.

### Slice 6: Swagger + end-to-end contract checks

Regenerate `docs/assets/swagger.json`, make the freshness check pass,
and add contract-style tests that cross-check inline proof data against
the existing direct `/utxo/*` endpoints for the same indexed snapshot.
That includes explicit root-consistency checks between `GET /status` and
`GET /utxo/root`, and explicit checks that proof-bearing JSON already
contains the root and chain point needed for external matching.

**Files**: `docs/assets/swagger.json`,
`HTTP/Types.hs`, `HTTP/Swagger.hs`,
`e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs`

## Architectural Principles

These principles are normative for the whole umbrella (#208) and
codified in the project [constitution](../../.specify/memory/constitution.md)
v1.1.0 (Principles VIII, IX, X). They are repeated here because they
shape every slice from #211 forward and explain *why* the response
shapes were chosen the way they were.

### Pure offline verification (Constitution VIII)

The whole point of proof-bearing responses is that the server is
*untrusted infrastructure*. Trust collapses to a single `utxo_root`
that the client obtains from a trusted external source — for example,
the same root attested by the on-chain UTxO-CSMT — and from there every
further check MUST be a pure fold over the proof data. No network, no
disk, no `IO`, no timeouts, no clocks.

Concretely, every verifier shipped in `cardano-mpfs-client` MUST have a
shape compatible with:

```haskell
verify :: TrustedRoot -> Bundle -> Either VerifyError a
```

and verifiers MUST compose as `Kleisli (Either VerifyError)` arrows.
Any verifier that needs `IO` is the wrong shape and MUST be redesigned;
any dependency that forces `IO` into the verifier MUST be swapped or
vendored.

### ProofGraph recursion: two levels of Merkle-ness

The data model has two stacked Merkle structures:

1. **UTxO-CSMT** — the global UTxO set as a Compact Sparse Merkle Tree.
   Its root is the `utxo_root` baked into every snapshot.
2. **Per-token MPF** — each MPFS token's datum carries a Merkle
   Patricia Forestry root over the facts it owns.

Verifying a proof-bearing response is therefore not a flat check but a
*recursion over a directed acyclic proof graph*. For a fact lookup the
chain is:

```
utxo_root  ─CSMT proof→  TxOut (datum)
                          │
                          └── mpfRoot  ─MPF proof→  fact value
```

For an unsigned-tx response the same shape extends to consumed inputs
and to MPF proofs for every key the builder relied on. When a datum
itself contains a sub-trie root (e.g. nested namespaces), the verifier
recurses again: the sub-trie root becomes the next `TrustedRoot` and
the next layer of the bundle is checked against it.

Implementation shape:

```haskell
data ProofGraph
    = CsmtLeaf  CsmtInclusionProof TxOut
    | MpfLeaf   MpfInclusionProof  Value
    | MpfUpdate MpfInclusionProof  ProofGraph
    -- … extended as new datum shapes appear

verifyNode
    :: TrustedRoot
    -> ProofGraph
    -> Either VerifyError TrustedRoot
```

`verifyNode` returns the *next* trusted root (the datum's `mpfRoot`,
or a nested sub-trie root) so the caller can keep folding. The
top-level verifier is a fold of `verifyNode` calls anchored at the
snapshot's `utxo_root`.

This is why the response types in slice 2 carry full `WitnessedUtxo` +
`MpfInclusionProof` values rather than scalars: the JSON shape mirrors
the recursion structure of `ProofGraph`, and adding a new on-chain
nesting level is a change to one verifier instead of every client.

### Unsigned-tx bundles as proof graphs (slices 3–5)

For tx-building endpoints the same `ProofGraph` machinery covers both
*input* witnesses (consumed UTxOs proven against `utxo_root`) and any
MPF facts the builder consulted. The unsigned tx itself is *not* what
the client trusts; the client trusts the proof graph that justifies
each input and each fact, and then re-derives that the tx body is the
unique transaction implied by those witnesses.

The slice-3 `UnsignedTxBundle` type therefore carries:

- the unsigned tx CBOR
- a `ProofGraph` rooted at the snapshot covering every consumed input
- per-request sub-graphs for batched flows (`update`, `reject`)

with explicit, deterministic association between request items and
their proof sub-graphs so a client can verify each one independently
before signing.

### One verifier, many targets (Constitution IX)

The verifier MUST exist exactly once, in Haskell, in the
`cardano-mpfs-client` package, and MUST be cross-compiled to every
runtime a client might live in:

- GHC native — server, CLI, Haskell tests
- GHC-WASM — browsers, Node, embedded wallets, hardware signers
- GHC-JS backend — environments that cannot load WASM

Re-implementing the verifier in TypeScript, JavaScript, Rust, or any
other language is forbidden. The whole trust model collapses if every
wallet vendor ships a different implementation; security fixes lag,
encodings drift silently, and "we have a proof-bearing API" stops
meaning anything.

Consequences for `cardano-mpfs-client`:

- No `IO`, no `unix`, no `process`, no native C FFI beyond pure
  hashing primitives.
- Every new dependency MUST clear the GHC-WASM and GHC-JS cross-compile
  matrix before landing.
- CI MUST build WASM and JS artifacts and run a cross-target QuickCheck
  suite asserting byte-identical `Either VerifyError a` outputs across
  GHC-native / GHC-WASM / GHC-JS for the same input.
- Releases MUST publish the npm package alongside Hackage; a release
  that ships Haskell but not WASM/JS is incomplete.

### Lean as source of truth (Constitution X)

The verifier's state machine MUST be formalized in Lean before it is
implemented in Haskell. The Lean predicates and preservation theorems
are the authoritative specification; the Haskell implementation exists
to match them. A QuickCheck property `prop_matchesLeanReference`
generates random inputs and asserts the Haskell implementation agrees
with the Lean-extracted reference, and the cross-target suite (above)
extends that property to the compiled WASM/JS artifacts.

For this umbrella the Lean artifacts to land before slice 4 are:

1. `ProofGraph` data type and `verifyNode` reference function.
2. Preservation theorems: a verified node returns a root that closes
   over the same fact / UTxO the Haskell verifier closes over.
3. The fold theorem: `verify = foldM verifyNode utxoRoot bundle`
   accepts iff every nested check accepts.

### Shallow `TxOut` decoder and cross-check test suite

Full ledger-native decoding on the client is ruled out: the `TxOut`
decoder inside `cardano-ledger-conway` transitively pulls
`cardano-crypto-class`, which binds to `libsodium`, `blst`, and
`secp256k1` via C FFI. None of those cross-compile to GHC-WASM or
GHC-JS, and swapping or vendoring them for the verifier adds more
attack surface than verifying the narrow slice of a `TxOut` we
actually look at.

The slice 4 answer is a shallow pure-Haskell decoder in
`cardano-mpfs-client` that extracts only `(address bytes, ada)` from a
Conway `TxOut` CBOR blob, plus a cross-check test suite in
`cardano-mpfs-offchain` that proves the shallow decoder agrees
byte-for-byte with the ledger decoder across the full Conway-era
generator. This earns the decoder its keep: the server-side test has
access to both decoders and asserts equality; the client ships only
the shallow one.

Test coverage:

- **Generator**: every Shelley address shape (payment × stake =
  KeyHash / ScriptHash / Pointer / absent), every value shape
  (ada-only, ada + 1 asset, ada + N assets across M policies), every
  datum variant (none, hash, short inline, long inline), and both
  presence and absence of a reference script.
- **Positive property**: for every generated `txOut`, the shallow
  decoder on `serialize' (natVersion @11) txOut` returns the same
  `(addr, ada)` pair as `(txOut ^. addrTxOutL, txOut ^. coinTxOutL)`.
- **Negative property**: every truncated prefix of a valid CBOR blob
  is rejected with `Left _` and never produces a `Right` or an
  exception.
- **Regression corpus**: pinned real hex blobs from preprod / mainnet
  are re-decoded on every ledger dependency bump, so silent CBOR
  layout drift fails CI rather than silently diverging verifier
  behaviour.
- **Run volume**: ≥ 1000 shrinks per property in CI.

Acceptance: any mismatch blocks the client. We either fix the shallow
decoder (widen its coverage) or discover an upstream ledger change
that requires a release bump of the decoder spec. Silent divergence is
not an option.

### Follow-up: cross-compile spike for `cardano-mpfs-client`

Principle IX is currently aspirational for this repo — slice 2 ships a
GHC-native verifier, but no WASM/JS build has been proven. Before
slice 4 merges we MUST run a short spike that:

- attempts a GHC-WASM build of `cardano-mpfs-client` end-to-end,
- attempts the GHC-JS backend build of the same package,
- pins or swaps any transitive dependency that fails to cross-compile,
- adds the `nix build` outputs and a minimal cross-target QuickCheck
  invocation to CI.

This spike is a hard prerequisite for slices 4–6: if the verifier
cannot be compiled to a wallet runtime, the proof-bearing tx
endpoints have no client. Because slice 4 introduces the shallow
`TxOut` decoder inside `cardano-mpfs-client`, the spike also verifies
that the decoder (and only pure-Haskell transitive deps) survives the
cross-target matrix.

## Risks

- **Snapshot drift during response assembly**: If the indexer advances
  between reading data and building proofs, the response can mix roots.
  The implementation must capture one `utxo_root` plus one indexed
  `chainpoint` per response and reject or retry inconsistent bundles.
- **TxBuilder boundary expansion**: Changing the builder return type
  touches every tx endpoint and the mock builder. The migration should be
  staged so each commit still compiles.
- **Batch response size**: `update` and `reject` can include many
  witnessed inputs. The API contract must keep proof-to-item association
  explicit even when payloads get large.
- **Swagger churn**: Several endpoints currently return `Hex`; changing
  them to objects affects schema names, examples, and the freshness
  check in one sweep.
