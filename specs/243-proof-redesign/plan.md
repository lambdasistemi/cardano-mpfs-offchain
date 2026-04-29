# Implementation Plan: Post-Split Proof Redesign

**Branch**: `243-proof-redesign` | **Date**: 2026-04-29 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/243-proof-redesign/spec.md`

## Summary

Redesign the entire MPFS HTTP surface around two principles enabled by the post-split on-chain protocol (PR #50 onchain, merged downstream as PR #241): (1) every read/write response that returns a UTxO carries its CSMT inclusion proof inline; (2) the offchain server stops being authoritative for the CSMT root and becomes a pointer + bytes service that the client verifies against an independently-obtained trusted root. Write endpoints reorganise by signer role (`/tx/oracle/...`, `/tx/requester/...`, top-level for boot/submit/public sweep). The new `requests_completeness_proof` field on `POST /tx/oracle/update` and `POST /tx/oracle/end` provides the cryptographic fairness guarantee against server curation that was the original motivation for the on-chain split.

Technical approach: introduce one uniform write response shape across all eleven `POST /tx/...` endpoints; restructure the four read endpoints we keep around proof-bearing payloads with split present/absent variants for `facts/:key`; remove six obsolete read endpoints; update the `cardano-mpfs-api` Servant types, the `cardano-mpfs-offchain` HTTP server, the `cardano-mpfs-client` verifiers (pure, native + WASM + JS), the Lean formal model for the new verifier state machine, the Swagger description, and every downstream consumer (MOOG, harvest, internal devnet harness) in a single coordinated release.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1 (offchain server, verifier library); Lean 4 (formal model — `lean/` directory)
**Primary Dependencies**: Servant (HTTP API), `cardano-mpfs-cage` (Aiken-derived on-chain types via PlutusV3 blueprint), `cardano-mpfs-client` (verifier package), `haskell-mts` (CSMT inclusion + prefix-completeness primitives, MPF inclusion + exclusion), `cardano-utxo-csmt` (CSMT runtime), `cardano-ledger-conway` (TxOut/Tx serialization), `cardano-node-clients` (N2C wiring), `chain-follower` (block stream)
**Storage**: RocksDB (existing CSMT + index column families)
**Testing**: `hspec` for unit and HTTP contract tests; bespoke E2E harness with subprocess `cardano-node` devnet; QuickCheck cross-target property `prop_matchesLeanReference` (constitution X) for verifier conformance
**Target Platform**: Linux for the offchain server; native GHC + GHC-WASM + GHC-JS for the verifier package (constitution IX)
**Project Type**: web service (offchain HTTP) plus shipped Haskell + npm verifier library, both grounded in a Lean specification (constitution X)
**Performance Goals**: HTTP latency dominated by CSMT proof construction; aim for `GET /tokens/:id` and the heaviest write endpoint (`POST /tx/oracle/update` with completeness witness) under 500ms p95 on the existing devnet workload
**Constraints**: verifier path stays pure (constitution VIII), no `IO` admitted into `cardano-mpfs-client`; Lean must compile with no `sorry` for every new predicate (constitution X); Aiken-encoding parity for any datum/redeemer touched (constitution V); the offchain server stops publishing `utxo_root` as authoritative — clients pin the trusted root externally (this feature's central trust shift)
**Scale/Scope**: 21 existing endpoints reviewed: 5 reads kept (restructured), 6 reads removed, 11 writes kept (new uniform shape, role-based paths). Pending request set per cage in scope: bounded — multi-tx bundle for unbounded sets is deferred to a follow-up.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|---|---|---|
| I. Ledger-Native Types | PASS | Every `txout_cbor` and `unsigned_tx_cbor` continues to be a `cardano-ledger-conway` serialization; no shadow types introduced. |
| II. Records of Functions | PASS | The HTTP layer reuses the existing `Indexer`, `TxBuilder`, `Submitter` records; new completeness witness construction goes via a new field on the existing `Indexer` record, not a new typeclass. |
| III. Atomic Block Processing | PASS | This feature does not touch the block-processing path; only response-construction is affected. |
| IV. External Signing | PASS | All `POST /tx/...` endpoints continue to return unsigned CBOR. |
| V. Aiken Compatibility | PASS | Proof encodings (CSMT inclusion, CSMT prefix-completeness, MPF inclusion, MPF exclusion) reuse the existing on-chain-compatible byte shapes from `haskell-mts`. |
| VI. Test Locally First | PASS | Existing e2e harness (subprocess `cardano-node`) covers each new shape. |
| VII. Nix Reproducibility | PASS | No new system deps; existing flake suffices. |
| VIII. Pure Offline Verification | PASS — explicit | FR-020 / FR-021 / FR-023 / FR-024 require every new verifier to be pure `Hex -> Bundle -> Either VerifyError ()`. |
| IX. One Verifier, Many Targets | PASS — explicit | FR-022 requires native + WASM + JS targets for every new shape. The `prop_matchesLeanReference` cross-target QuickCheck (constitution IX) extends to the new state machine. |
| X. Lean as Source of Truth | **GATE — Phase 1 prerequisite** | Every new verifier shape (read-side present/absent, completeness witness, write-side input cover with optional completeness extra) MUST appear as Lean predicates and preservation theorems in `lean/Phase4/Verify.lean` (or a successor file) BEFORE the Haskell implementation lands. The plan's Phase 1 design output explicitly produces the Lean predicate names and signatures; the proofs land in the implementation phase as prerequisites for the corresponding Haskell modules. |

No violations to justify; the Complexity Tracking table at the bottom is empty.

## Project Structure

### Documentation (this feature)

```text
specs/243-proof-redesign/
├── plan.md                 # This file
├── research.md             # Phase 0 output — resolves dependencies and unknowns
├── data-model.md           # Phase 1 output — response types, witness shapes, error vocabulary
├── quickstart.md           # Phase 1 output — end-to-end client verification walkthrough
├── contracts/
│   ├── api-shapes.md       # Phase 1 — new endpoint contracts (paths, request/response shapes, status codes)
│   └── verify-error.md     # Phase 1 — VerifyError vocabulary additions for the new shapes
├── checklists/
│   └── requirements.md     # Existing spec-quality checklist
└── tasks.md                # Phase 2 output — produced by /speckit.tasks
```

### Source Code (repository root)

The change touches the following existing top-level packages and directories. No new packages are introduced.

```text
cardano-mpfs-api/                                   # Servant API types — paths, request/response types, JSON instances, Swagger
├── lib/Cardano/MPFS/API.hs                         # Endpoint type aliases — paths restructured by role
└── lib/Cardano/MPFS/API/
    ├── Types.hs                                    # Response/request types — uniform write response, split read responses
    ├── Encoding.hs                                  # Hex newtype etc. (untouched)
    └── Schemas.hs (if present)                      # OpenAPI schemas — regenerated to reflect new shapes

cardano-mpfs-offchain/                              # HTTP server + handlers
├── lib/Cardano/MPFS/HTTP/
│   ├── API.hs                                       # Re-exports / aliases for the API type — kept thin
│   ├── Server.hs                                    # Handlers — new shapes constructed here
│   ├── Types.hs                                     # Server-internal helpers around Types.hs (if any)
│   └── Encoding.hs                                  # Encoding helpers
├── lib/Cardano/MPFS/TxBuilder/
│   ├── Real/{Boot,Request,Retract,Reject,Update,End,Sweep}.hs
│   │                                                # Each builder returns the uniform write response shape
│   └── ...
├── lib/Cardano/MPFS/Indexer/...                     # New helper: enumerate-all-at-script-prefix + completeness proof
├── lib/Cardano/MPFS/Application.hs                  # Wires the HTTP server to the indexer + tx-builder
├── e2e-test/                                        # New E2E specs cover honest + forgery cases per shape
└── test/                                            # Unit + HTTP contract tests, hspec

cardano-mpfs-client/                                # Pure verifier package (native + WASM + JS)
├── cardano-mpfs-client.cabal
├── lib/Cardano/MPFS/Client/
│   ├── Read.hs                                      # New typed read DTOs — TokensListResponse, TokenResponse, FactPresent, FactAbsent, ConfirmResponse
│   ├── Write.hs                                     # New typed write DTO — UnsignedTxResponse (uniform)
│   ├── Verify.hs                                    # Re-exports
│   └── Verify/
│       ├── Read.hs                                  # Verifiers for the four read responses
│       ├── Write.hs                                 # Verifier for the uniform write response
│       ├── Completeness.hs                          # Helper: verify CSMT prefix-completeness against trusted root
│       └── Replay.hs                                # Existing CSMT/MPF replay primitives — extended for new uses
└── test/                                            # Honest fixtures + forgery corpus per new shape

lean/                                               # Lean 4 formal model — constitution X
├── Phase4.lean
└── Phase4/
    ├── Verify.lean                                  # Existing replay state machine — extended with new predicates
    ├── ProofRedesign.lean                           # NEW — per-endpoint preservation theorems for the new shapes
    └── Completeness.lean                            # NEW — predicates and theorems for CSMT prefix-completeness witness

docs/
├── architecture/
│   ├── overview.md                                  # Update: post-split proof model, trust direction
│   └── testing.md                                   # Update: new verifier conformance + forgery corpus
└── assets/swagger.json                              # Regenerated via `just update-swagger`
```

**Structure Decision**: keep the existing four-package layout (`cardano-mpfs-api`, `cardano-mpfs-offchain`, `cardano-mpfs-client`, plus the on-chain pin `cardano-mpfs-cage`) and the standalone `lean/` Lake project unchanged. All new code lands in existing modules; new modules `Verify/Completeness.hs`, `lean/Phase4/ProofRedesign.lean`, `lean/Phase4/Completeness.lean` are added under existing trees. No new top-level package is created; the verifier remains a single Haskell package compiled to three targets per constitution IX.

## Phase 0: Outline & Research — `research.md`

Open questions to resolve before Phase 1 freezes contracts. Each becomes a section in `research.md`:

1. **CSMT prefix-completeness primitive in `haskell-mts`** — confirm the public API surface (function name, signature, byte layout of the proof, support for the empty leaf set), and whether it accepts a script-hash prefix directly or requires a pre-computed key bound. Cited by spec FR-002, FR-003, FR-015, FR-016.
2. **Empty-prefix completeness for `POST /tx/oracle/end`** — confirm `haskell-mts` produces and verifies a witness for "no leaves under this script-hash prefix", or document the workaround. Spec edge case + FR-016.
3. **Servant pattern for two HTTP-status-code response variants** — pick a stable Servant idiom for `GET /tokens/:id/facts/:key` (200 `FactPresent` / 404 `FactAbsent` with body / 404 no body for unknown token). Options: `Verb 200`/`Verb 404` with a sum type, custom `MimeRender`, or `WithStatus`. Decision needed before contracts/api-shapes.md.
4. **Trusted blueprint distribution for the client** — record assumption: client is given the trusted Aiken blueprint by the application that wraps `cardano-mpfs-client`; no in-band distribution from the offchain service. Cite where the blueprint is read in the verifier flow.
5. **Where the trusted UTxO-CSMT root comes from** — record assumption: a separate CSMT service the client trusts. Document the contract the client supplies to verifier entry points (`utxoRoot :: IO Hex` outside the verifier; the verifier itself takes the resolved `Hex` synchronously, preserving constitution VIII).
6. **Downstream consumers to migrate** — enumerate concrete code-paths to change in MOOG, harvest, internal devnet harness, mpfs-explorer. Each becomes a Phase-2 task line.
7. **Lean formalization scope for the new verifier state machine** — confirm the predicate-and-preservation-theorem scope before Haskell lands: read-side present/absent state, write-side input-cover state, completeness-witness state. Each maps to a `lean/Phase4/...` definition before the corresponding Haskell module is written.
8. **Public sweep validator semantics** — verify with the on-chain side that the global state validator permits a non-legitimate UTxO to be spent without the oracle's signature, and that no datum check is required for non-legit cases. Cited by FR-014 and US6.

**Output**: `research.md` lists each item above with `Decision`, `Rationale`, and `Alternatives considered`.

## Phase 1: Design & Contracts

Prerequisites: `research.md` complete with all eight decisions recorded.

### 1. Data model — `data-model.md`

Inventory of every type that crosses the API boundary or the verifier boundary. Includes:

- **`VerificationSnapshot`** — `{ utxo_root :: Hex, chainpoint :: ChainPoint }`. Identical to today's; documented here as the canonical anchor.
- **`UtxoEntry`** — `{ ref :: TxIn, txout_cbor :: Hex, inclusion_proof :: Hex }`. The leaf type appearing inside both read and write responses.
- **`UtxoSetWitness`** — `{ entries :: [UtxoEntryRefOnly], completeness_proof :: Hex }`, where `UtxoEntryRefOnly = { ref :: TxIn, txout_cbor :: Hex }`. The single completeness proof sits outside the list per the locked design (endpoint walkthrough confirmed).
- **`StatusResponse`** — tip-only after this feature: `{ tip_slot :: Word64, tip_block_id :: Hex }`. Every other field removed.
- **`TokensListResponse`** — `{ snapshot, tokens :: UtxoSetWitness }`. Replaces the today's `[TokenIdJSON]`.
- **`TokenResponse`** — `{ snapshot, state_utxo :: UtxoEntry, requests :: UtxoSetWitness }`. Folded the old `/tokens/:id/requests`.
- **`FactPresentResponse`** (HTTP 200) — `{ snapshot, state_utxo :: UtxoEntry, value :: Hex, mpf_inclusion_proof :: Hex }`.
- **`FactAbsentResponse`** (HTTP 404 with body) — `{ snapshot, state_utxo :: UtxoEntry, mpf_exclusion_proof :: Hex }`. Distinct from "token unknown" which is HTTP 404 no body.
- **`ConfirmResponse`** (HTTP 200 from `GET /tx/:txId`) — `{ snapshot, ref :: TxIn /* always (txId, 0) */, txout_cbor :: Hex, inclusion_proof :: Hex }`.
- **`UnsignedTxResponse`** — uniform write response: `{ unsigned_tx_cbor :: Hex, snapshot, inputs :: [UtxoEntry] }`. Optional fields by endpoint: `requests_completeness_proof :: Maybe Hex` set on `POST /tx/oracle/update` and `POST /tx/oracle/end`, absent elsewhere. Encoded as a present/absent JSON field, not a separate type.
- **Existing request bodies** (`BootRequest`, `InsertRequest`, `DeleteRequest`, `UpdateValueRequest`, `RetractRequest`, `RejectRequest`, `UpdateRequest`, `SweepRequest`, `EndRequest`, `SubmitRequest`) — kept as-is; only the response shapes change.
- **`VerifyError`** — extended; new constructors enumerated in `contracts/verify-error.md`.

### 2. Interface contracts — `contracts/api-shapes.md`, `contracts/verify-error.md`

`api-shapes.md` records, per endpoint:
- new path, HTTP method, request body type, response status codes, response body types per status,
- the verifier's required external input (`Hex` trusted UTxO-CSMT root) and what the verifier validates, and
- removal status for the six dropped read endpoints.

`verify-error.md` records the canonical `VerifyError` vocabulary additions for the new shapes — at minimum a closed list covering: trusted-root mismatch, address mismatch (decoded `txout_cbor` does not match locally-derived script address), NFT-policy mismatch, NFT-asset-name mismatch, malformed `state` datum, completeness-leaf-not-present, completeness-extra-leaf, MPF-inclusion-fail, MPF-exclusion-fail, unsigned-tx-input-not-covered, snapshot-chainpoint-stale (client-side optional check). Each constructor names the dotted field path in the response where the failure occurred (continuing the existing `<endpoint>.<role>[<index>]?.<leaf>` convention from #226).

### 3. Quickstart — `quickstart.md`

End-to-end walkthrough of one read flow and one write flow:

- **Read flow**: client obtains a trusted `utxo_root` for a chain point from a separate CSMT service; calls `GET /tokens/:id`; runs `verifyTokenResponse trustedRoot response`; on success, decodes the state datum and acts on the recovered trie root and pending request set.
- **Write flow**: client obtains a trusted `utxo_root`; calls `POST /tx/oracle/update`; runs `verifyUnsignedTxResponse trustedRoot response`; on success, decodes the unsigned tx, verifies the redeemer matches the operations carried by each consumed request UTxO's datum, applies operations to the prior trie root, compares to the new state UTxO's datum, signs.

### 4. Agent context update

Run the speckit agent script (`.specify/scripts/bash/update-agent-context.sh`) to refresh `CLAUDE.md`'s "Recent Changes" stanza with this feature's name and the verifier portability + Lean-first reminders.

## Re-evaluate Constitution Check (post-design)

After Phase 1 produces `data-model.md` + `contracts/`, the same gate table is re-checked. Expected status: unchanged from the pre-Phase-0 evaluation. Constitution X remains the only "active gate" — it shifts from "design output names the Lean predicates" (Phase 1) to "Lean predicates and theorems compile with no `sorry`" (Phase 2 implementation prerequisite).

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

(empty — no violations)
