# Implementation Plan: Fact-provider pivot

**Branch**: `259-fact-provider-pivot` | **Date**: 2026-05-04 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/259-fact-provider-pivot/spec.md`

## Summary

Replace the MPFS server's `GET /transaction/{address}/{op}` endpoints
that return unsigned transactions with `POST /facts/{op}` endpoints
that return only the proof-bearing data the matching cage-protocol
operation needs at one indexer snapshot. Native clients (MOOG today)
build the unsigned transaction locally using the cage-protocol DSL
relocated to `cardano-node-clients`. Hard cutover across three
repositories — `cardano-mpfs-offchain` (server + verifier library),
`cardano-node-clients` (DSL host), `lambdasistemi/moog` (CLI client) —
landed in lockstep so neither default branch is broken between
landings.

The verifier surface in `cardano-mpfs-client` reduces to pure
proof-validity functions over facts bundles. The strict-template tx
verifier we were converging on (in #256's brainstorm) evaporates
because the wallet built the transaction and trivially knows its
shape. Protocol parameters travel in each facts response with
explicit "unverified" status; wallets enforce a documented
`WalletPolicy` of hard caps.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1 across all three
repositories.

**Primary Dependencies**:

- Server (`cardano-mpfs-offchain`):
  - `cardano-mpfs-offchain` lib (this repo) — server, indexer,
    verifier client.
  - `Cardano.MPFS.Indexer.Reads` — IndexerTx primitives from #249
    (PR #253). New primitives required by tier-2/tier-3 endpoints
    (`readStateUtxoAt`, `readRequestUtxosAt`,
    `readNamedRequestUtxo`, `readTrieFact`).
  - `cardano-utxo-csmt`, `chain-follower`, `haskell-mts` —
    indexer infrastructure unchanged.
  - `servant-server` — HTTP layer.
- DSL host (`cardano-node-clients`):
  - `Cardano.Node.Client.TxBuild` — operational-monad DSL,
    44/44 tests passing on native.
  - `cardano-ledger-conway`, `cardano-ledger-api`, `plutus-ledger-api`,
    `cardano-crypto-class` — already cross-target via
    `cardano-ledger-inspector`'s machinery (parked for the WASM
    artifact in cardano-node-clients#123).
- Client (`lambdasistemi/moog`):
  - `cardano-node-clients` (new dep at the wallet seam) — for the
    cage-protocol DSL.
  - HTTP client for the new `MPFS.Facts` module.

**Storage**: RocksDB on the server, unchanged. No schema change. The
new IndexerTx primitives operate over the same column families
introduced in #249.

**Testing**:

- Unit (server): tests for the new IndexerTx primitives + the
  per-endpoint facts-assembly functions.
- Unit (DSL host): existing 44/44 TxBuild tests, plus new property
  tests for the cage-protocol helpers (boot/request/retract/end/
  update/reject) producing byte-equal txs given equal inputs.
- Unit (verifier): the new pure proof-validity functions get
  golden-vector tests for each facts shape; tamper tests confirm
  rejection.
- Cross-target tests (Principle IX): the verifier package's
  property suite runs under native + GHC-WASM + GHC-JS and asserts
  byte-identical `Either VerifyError ()` outputs across targets.
- E2E: MOOG's existing devnet-backed integration spec is migrated;
  the same flows that today exercise the legacy `transaction/...`
  endpoints exercise the new `/facts/*` endpoints + local build +
  submission.

**Target Platform**: Linux server (amd64) inside `nix develop` for
the server. MOOG already runs as a native Linux/macOS Haskell
binary; the client-side DSL stays native here.

**Project Type**: Multi-repository web service + CLI client.
Three repositories move in lockstep.

**Performance Goals**:

- Server-side: facts response time bounded by the IndexerTx read
  cost — same `O(K)` over UTxOs at an address as today; tier-3
  endpoints add MPF-fact reads bounded by the number of pending
  requests in the batch, not by the trie size.
- Client-side (MOOG): local tx-build is pure CPU work bounded by
  the DSL's existing performance envelope (44/44 tests pass within
  the existing test-suite budget).

**Constraints**:

- Hard cutover across three repos in the same release window
  (FR-011); no coexistence period.
- Wire contracts of the new endpoints must accept the same
  parameters MOOG supplies today (FR-001). No new client-side input
  is needed beyond what the legacy endpoints already required.
- `cardano-mpfs-client` verifier may not import
  `Cardano.Ledger.Api.Tx` or any transaction-grammar type after the
  pivot (FR-007).
- `cardano-mpfs-offchain` server may not retain any of the legacy
  `transaction/...` endpoints (FR-006).
- swagger.json reflects only the new shape after the pivot (FR-006,
  SC-002).

**Scale/Scope**:

- Server (`cardano-mpfs-offchain`):
  - ~10 modules touched (HTTP/Server, HTTP/API, HTTP/Types,
    Indexer/Reads, Application, the existing TxBuilder/Real/* tree
    is gutted — its content moves to cardano-node-clients).
- DSL host (`cardano-node-clients`):
  - 1 new module family (`Cardano.Node.Client.TxBuild.Cage.{Boot,
    Request,Retract,End,Update,Reject}`) — pure cage-protocol
    builder helpers ported from the server-side `Real.*Core`
    modules. Each is ~50–150 lines.
- Client (`lambdasistemi/moog`):
  - New `MPFS.Facts` module replacing `MPFS.API`.
  - Every callsite that used the legacy `MPFS.API` migrated.
  - ~10 callsites across `Cli.hs`, `Effects.hs`, `Oracle/*`,
    `User/*`.
- Verifier (`cardano-mpfs-client`):
  - New per-endpoint `verifyXFacts` functions; cross-target tested.
  - Old `verifyXTxResponse` / `verifyConservation` etc. removed.

## Constitution Check

The constitution at `.specify/memory/constitution.md` enumerates
ten principles. The pivot interacts directly with **Principle IV**
("The API MUST return unsigned CBOR transactions") — the literal
wording is not preserved by the pivot. Below: per-principle gate
plus an explicit constitutional amendment.

### Principle I — Ledger-Native Types

**Gate**: PASS. The cage-protocol DSL helpers in `cardano-node-clients`
operate on `Tx ConwayEra` and friends. The server's facts response
carries indexer-resolved bytes that decode to ledger-native types
client-side. No shadow types.

### Principle II — Records of Functions

**Gate**: PASS. The MPFS service boundary (Provider, State,
TrieManager, Submitter) is unchanged. The cage-protocol DSL helpers
are pure functions, not service interfaces; no new typeclasses.

### Principle III — Atomic Block Processing

**Gate**: PASS. The chain follower's writer invariant is unchanged.
The new facts endpoints add new readers; each handler is one
`runIndexerTx ctx $ do { … }` block — exactly the discipline from
#249 (PR #253). New primitives required for tier-2/tier-3 endpoints
(`readStateUtxoAt`, `readRequestUtxosAt`, `readNamedRequestUtxo`,
`readTrieFact`) are added to `Cardano.MPFS.Indexer.Reads` under the
same discipline.

### Principle IV — External Signing — **AMENDED BY THIS SLICE**

The literal wording today reads: "The API MUST return unsigned CBOR
transactions." The pivot **replaces this**. After the pivot the
server does NOT serve unsigned transactions — it serves only the
proof-bearing material that clients need to build the transaction
themselves. The principle is renamed to "Client-Side Transaction
Construction" to reflect what the system now does.

The current Principle IV's spirit has two strands:

1. **Server returns transactions for clients to sign.** ← This
   strand is REMOVED. The server returns no transactions at all
   after the pivot. Clients build them locally from proof-bearing
   facts.
2. **Server holds no private keys.** ← This strand is PRESERVED
   and in fact strengthened. After the pivot the server never
   produces transactions, so it cannot — by structural absence
   of code paths — sign anything. The no-keys invariant holds
   trivially.

**Proposed amendment** (lands as a standalone PR BEFORE the pivot's
implementation work; see Phase 0 of tasks.md):

```text
### IV. Client-Side Transaction Construction

The MPFS server MUST NOT return unsigned transactions. The server
serves only proof-bearing material — snapshot, indexer-resolved
UTxOs with CSMT inclusion proofs, MPF facts where applicable, and
protocol parameters — anchored to a single indexer snapshot.
Clients verify the proofs against an independently-obtained
trusted root, build the unsigned transaction locally using the
shared cage-protocol DSL, and sign with their own keys.

The MPFS server MUST NOT hold or accept private keys. The
no-keys-on-server invariant follows trivially from the above:
since the server never produces transactions, it has no signing
code paths.
```

The same amendment PR also adds a **Sync Impact Report waiver
note** for Principle IX (cross-target verifier build): the "CI
MUST build the WASM and JS artifacts on every commit" clause is
explicitly deferred to a separate slice tracked in issue
https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258.
Until that slice lands, Principle IX's intent is preserved (the
verifier IS pure and IS structurally cross-compilable per the
spike result on issue
https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257)
but the CI-enforcement clause is operationally deferred.

Version bump: **MAJOR (1.1.0 → 2.0.0)** because Principle IV's
substance — what the server returns — is replaced.

The constitution update lands in the same merge as the pivot. If
this gate fails review, the pivot does not land.

### Principle V — Aiken Compatibility

**Gate**: PASS. The cage-protocol DSL helpers in
`cardano-node-clients` produce byte-equal `Tx ConwayEra` values
for equivalent inputs to today's server-side `*Core` modules. A
property test in the DSL host's test suite asserts: for the same
seed input, the boot tx body's CBOR is byte-equal between
`Cardano.Node.Client.TxBuild.Cage.Boot.bootTokenCore` (post-pivot)
and the pre-pivot `Cardano.MPFS.TxBuilder.Real.Boot.bootTokenCore`
(captured from the merge-base). Same for the seven other
endpoints' equivalents.

### Principle VI — Test Locally First

**Gate**: PASS. Full quality gate locally before each push, mirroring
the boot/request slice discipline:
`nix build .#offchain-tests .#e2e-tests .#cardano-mpfs-offchain
.#docker-image .#checks.x86_64-linux.swagger-up-to-date && just
format-check && just hlint && nix run .#unit-tests && nix run
.#e2e-tests`. MOOG's e2e suite is exercised in tandem (it's the
acceptance test for SC-001).

### Principle VII — Nix Reproducibility

**Gate**: PASS. No new system dependencies on the server side.
`cardano-node-clients` adopts no new system deps for the DSL
helpers; the existing native build is unchanged.

### Principle VIII — Pure Offline Verification

**Gate**: PASS / load-bearing. The post-pivot verifier surface in
`cardano-mpfs-client` is strictly the pure offline fold Principle
VIII originally specified — proof validity over a facts bundle and
a trusted root. The pivot **strengthens** Principle VIII by removing
the tx-shape grammar that was leaking into the verifier.

### Principle IX — One Verifier, Many Targets

**Gate**: PASS. The post-pivot verifier is strictly smaller than the
pre-pivot verifier (no tx-shape grammar). Cross-target compilation
under GHC-native + GHC-WASM + GHC-JS is at least as feasible. The
WASM artifact for the *cage-protocol DSL* (which Principle IX would
require if the DSL is to live cross-target) is deferred to
cardano-node-clients#123 — MOOG validates the architecture natively
first.

### Principle X — Lean as Source of Truth

**Gate**: NOT APPLICABLE for this slice. The Lean model formalises
verifier-side properties; the pivot's verifier is a strict subset
of the pre-pivot verifier (no new properties to formalise; some
properties to remove from the model, alongside the tx-shape
grammar that was previously specified).

**Re-check after Phase 1 design**: see end of this file.

## Project Structure

### Documentation (this feature)

```text
specs/259-fact-provider-pivot/
├── plan.md              # This file
├── research.md          # Phase 0 — design decisions
├── data-model.md        # Phase 1 — facts shapes + verifier surface
├── quickstart.md        # Phase 1 — run all eight operations end-to-end
├── contracts/
│   ├── facts-api.md         # Phase 1 — server's POST /facts/* shape
│   ├── cage-dsl.md          # Phase 1 — DSL helpers in cardano-node-clients
│   └── verifier.md          # Phase 1 — verifier in cardano-mpfs-client
├── checklists/
│   └── requirements.md  # Spec quality checklist (already complete)
└── tasks.md             # Phase 2 — generated by /speckit.tasks
```

### Source Code (across three repositories)

```text
cardano-mpfs-offchain/                    # this repo
├── lib/Cardano/MPFS/
│   ├── HTTP/API.hs                       # CHANGED — replace transaction/* paths
│   ├── HTTP/Server.hs                    # CHANGED — eight new fact handlers
│   ├── HTTP/Types.hs                     # CHANGED — new XFacts response types
│   ├── Indexer/Reads.hs                  # CHANGED — new primitives
│   └── Application.hs                    # CHANGED — drop TxBuilder field from Context
├── lib/Cardano/MPFS/TxBuilder/Real/      # REMOVED — moves to cardano-node-clients
└── docs/assets/swagger.json              # REGENERATED

cardano-mpfs-client/                      # same repo
└── lib/Cardano/MPFS/Client/
    ├── Verify.hs                         # CHANGED — purely proof-validity
    ├── Verify/Conservation.hs            # REMOVED — was for tx-shape; obsolete
    ├── Verify/Replay.hs                  # CHANGED — facts-bundle replay only
    └── Facts.hs                          # NEW — XFacts types + JSON

cardano-node-clients/                     # separate repo
└── lib/Cardano/Node/Client/TxBuild/
    └── Cage/                             # NEW — cage-protocol DSL helpers
        ├── Boot.hs                       # NEW — bootTokenCore (ported from server)
        ├── Request.hs                    # NEW — requestInsert/Delete/UpdateCore
        ├── Retract.hs                    # NEW — retractCore
        ├── End.hs                        # NEW — endCore
        ├── Update.hs                     # NEW — updateCore + MPF fold
        └── Reject.hs                     # NEW — rejectCore + MPF fold

lambdasistemi/moog/                       # separate repo
├── src/MPFS/
│   ├── API.hs                            # REMOVED
│   └── Facts.hs                          # NEW — HTTP client for /facts/*
└── src/                                  # CHANGED — every callsite migrated
    ├── Cli.hs
    ├── Effects.hs
    ├── Submitting.hs
    ├── Oracle/Process.hs
    ├── User/Agent/Cli.hs
    ├── User/Requester/Cli.hs
    └── …
```

**Structure Decision**: Three repositories move together. The
sequencing is:

1. **`cardano-node-clients`** lands first (port the cage-protocol
   DSL helpers from the server). PR is independent — adds new
   modules, breaks nothing.
2. **`cardano-mpfs-offchain`** lands next (replace endpoints +
   verifier; depends on cardano-node-clients's new modules being
   on main). Hard cutover commit removes the legacy server
   surface and the legacy verifier.
3. **`lambdasistemi/moog`** lands last (migrates client to the
   new shape; depends on the new server endpoints being on main).

The MOOG repo's main is broken between (2) and (3) — that is the
narrow cutover window. A one-commit "bump cardano-mpfs-offchain
pin to the cutover commit" PR follows the cardano-mpfs-offchain
merge, and the MOOG migration PR rebases on top.

A safer alternative — land all three in a single coordinated
multi-repo merge — is rejected because the repositories don't share
a CI pipeline and a synchronised merge is fragile. The narrow CI-
gated window above is operationally acceptable; FR-011's lockstep
constraint is satisfied by ensuring no production deploy uses the
broken-MOOG window.

## Phase 0: Outline & Research

See `research.md` for the resolved design questions:

- **Q0-1 — DSL helpers home**: confirmed `cardano-node-clients`
  (Shape B from the spike). The repo already hosts the
  operational-monad DSL with 44 tests; cage-protocol helpers are a
  natural extension. Rejected: hosting in `cardano-mpfs-offchain`
  (binds DSL evolution to server release) or in
  `cardano-ledger-inspector` (muddies that repo's narrow scope).
- **Q0-2 — Cage helpers signature shape**: each helper is a pure
  function `cfg + verifiedFacts + walletPolicy → Tx ConwayEra`.
  Rejected: returning a `TxBuild` program for the caller to run
  (forces every wallet to know about the DSL run-loop; the helper
  encapsulates that).
- **Q0-3 — IndexerTx primitives needed**: four new ones
  (`readStateUtxoAt`, `readRequestUtxosAt`,
  `readNamedRequestUtxo`, `readTrieFact`). All sit inside the
  existing `runIndexerTx` discipline; no new transaction shape.
- **Q0-4 — MPF fold home**: the server returns MPF facts (each
  with a proof against the trie root in the consumed state UTxO's
  datum). The wallet runs the fold *during transaction building*
  (inside `Cage.Update`'s helper) — which is correct because the
  fold result is the new state UTxO's `stateRoot` field. The
  fold logic moves from `cardano-mpfs-offchain/lib/.../Real/Update.hs`
  to `cardano-node-clients/lib/.../Cage/Update.hs`.
- **Q0-5 — Protocol-parameter shape on the wire**: the response
  carries the full Conway `PParams` object (CBOR-encoded for
  fidelity) plus a JSON envelope flagging it `"verified": false`.
  The wallet decodes the CBOR and uses it for fee/ExUnits
  calculation. Wallets that have their own pp source ignore the
  field.
- **Q0-6 — Verifier signature shape**: `verifyBootFacts`,
  `verifyRequestFacts`, etc. — one per operation. Each is
  `TrustedRoot -> XFacts -> Either VerifyError VerifiedXFacts`.
  Rejected: a single polymorphic verifier (the per-endpoint shapes
  differ enough — single-UTxO vs batch — that one signature would
  be awkward).
- **Q0-7 — Cross-repo sequencing**: confirmed the three-step
  sequence above. The narrow CI-gated window between server
  cutover and MOOG migration is acceptable; production deploys
  must not use commits in that window.

**Output**: `research.md`.

## Phase 1: Design & Contracts

**Prerequisites**: research.md complete.

### 1. Data model

See `data-model.md`. Key entities:

- Eight per-endpoint `XFacts` records with the shapes named in
  FR-003 of the spec.
- `BundleSnapshot` (existing from #249) — unchanged.
- `WalletPolicy` (new, client-side only) — hard-cap fields named
  in FR-009.
- `VerifiedXFacts` records (one per `XFacts`) — proof-token
  evidence that the proofs in the bundle have been validated.
- The cage-protocol DSL helpers' input/output types — pure
  functions consuming `cfg + VerifiedXFacts + WalletPolicy` and
  returning `Either BuildError (Tx ConwayEra)`.

### 2. Contracts

Three contract documents:

- `contracts/facts-api.md` — server-side wire contract: the eight
  `POST /facts/{op}` endpoints, request/response JSON schemas
  (mirroring swagger), and the protocol-parameters envelope. This
  is the public surface external integrators consume.
- `contracts/cage-dsl.md` — client-library contract: the eight
  cage-protocol DSL helpers in `cardano-node-clients`, their
  signatures, the `WalletPolicy` enforcement points, the
  byte-equality invariant against the legacy `*Core` server
  modules.
- `contracts/verifier.md` — `cardano-mpfs-client` verifier
  contract: the per-endpoint `verifyXFacts` functions, the
  invariants their `VerifiedXFacts` outputs imply, the cross-
  target byte-identity property (Principle IX).

### 3. Quickstart

See `quickstart.md` — bring up devnet + MPFS server, walk through
all eight operations end-to-end via MOOG using the new shape,
observe the cross-target verifier accepting each response.

### 4. Agent context update

`CLAUDE.md` already lists Haskell GHC 9.10.1, Servant, Nix,
cardano-mpfs-client, etc. This slice introduces no new tech; the
"Recent Changes" section gains a 259-fact-provider-pivot entry
when the implementation lands.

## Constitution Check (Re-evaluation, Post Phase 1 Design)

I, II, III, V, VI, VII, VIII, IX, X — unchanged from pre-Phase-0
evaluation. The Phase 1 design does not change the constitutional
posture for these principles.

**Principle IV — AMENDED to "Client-Side Transaction Construction"**:
the Phase 1 contracts confirm the shape of the amendment. The
server's wire contract (`contracts/facts-api.md`) returns no
unsigned transactions and no transaction-shaped objects of any
kind — only proof-bearing material (snapshot + CSMT-proven UTxOs
+ MPF facts + protocol parameters). Transaction construction
lives entirely on the client side, in the cage-protocol DSL
helpers (`contracts/cage-dsl.md`). The no-keys-on-server invariant
is preserved trivially because the server has no transaction-
producing code paths after the pivot. The proposed amendment text
above is the wording that lands with this slice's merge.

The constitution version bumps from 1.1.0 to 2.0.0 in the same
merge.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Principle IV literal wording amended | The pivot's value comes from removing the tx-shape verifier surface, which requires the server to stop returning unsigned txs as a wire shape | Keeping Principle IV's literal wording would block a load-bearing architectural improvement — the spirit (no keys on server) is preserved by the amended wording |
| Three-repo lockstep landing | `cardano-mpfs-offchain` and `lambdasistemi/moog` are independent repositories with independent CI; the cage DSL relocation also touches `cardano-node-clients` | A single coordinated merge across three repos is operationally fragile; the narrow CI-gated cutover window above is the safer alternative under FR-011's lockstep requirement |
