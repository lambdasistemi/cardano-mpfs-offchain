# Implementation Plan: Atomic POST /tx/request/{insert,delete,update}

**Branch**: `256-atomic-request-handlers` | **Date**: 2026-05-03 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/256-atomic-request-handlers/spec.md`

## Summary

Apply the proven boot-slice pattern (#249, PR #253) to the three
request endpoints. Each handler reads its `BundleSnapshot` and the
wallet inputs at the requester address inside one
`runIndexerTx ctx $ do { … }` call composed from the existing
`Cardano.MPFS.Indexer.Reads` primitives (`readSnapshot`,
`readWalletInputsAt`). The three builder impls collapse into a single
shared pure `requestCore` (mirroring `bootTokenCore`) that returns a
`RequestCore` record (program + ledger pairs + funding + snapshot);
`Cardano.MPFS.TxBuilder.Real` holds the IO orchestrator
(`runRequestBuilder`) that fetches `pp`, runs the DSL `build` loop,
and assembles the envelope. `Provider.queryUTxOs` is removed from the
request build path entirely.

The request transaction shape itself is unchanged — the on-chain
validators in `cardano-mpfs-onchain` and the wire contracts are
untouched. Only the server-side construction path is reshaped.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1
**Primary Dependencies**:

- `cardano-mpfs-offchain` (this repo) — server, tx-builders, HTTP
  layer, indexer reads.
- `cardano-node-clients:Cardano.Node.Client.TxBuild` — the DSL we
  migrated boot to in PR #253; reused verbatim here.
- `cardano-mpfs-offchain:Cardano.MPFS.Indexer.Reads` — `IndexerTx`
  monad and primitives (`readSnapshot`, `readWalletInputsAt`)
  introduced in PR #253.
- `cardano-ledger-conway` — `TxOut` CBOR (de)serialization (already
  used by `Boot.Inputs.decodeAll`; either reuse or generalise to a
  shared `Wallet.Inputs` module).
- `servant-server` — the three `txInsertHandler`, `txDeleteHandler`,
  `txUpdateValueHandler` Servant handlers.

**Storage**: RocksDB with the existing 13-column-family unified
schema. No schema change.

**Testing**:

- Unit tests in `cardano-mpfs-offchain` — new property tests for the
  three pure `*Core` constructors using deterministic
  `[(TxIn, TxOut bytes, proof)]` triples.
- E2E tests in `cardano-mpfs-offchain` — extend `ProofsSpec` with
  three churn-test variants (insert/delete/update); the existing
  `HTTPLifecycleSpec` and `CageFlowSpec` already exercise the three
  endpoints under `followerEnabled = True`, so they validate FR-001
  end-to-end against the new shape.

**Target Platform**: Linux server (amd64) inside `nix develop`.

**Project Type**: Haskell multi-package web service.

**Performance Goals**:

- Same as boot slice: request endpoint median latency at K=2 wallet
  UTxOs and 1M total chain UTxOs ≤ 2× the median at 1k total chain
  UTxOs.

**Constraints**:

- Wire contracts MUST stay identical (FR-003).
- No `Provider.queryUTxOs` on the build path (FR-002, SC-002).
- Builder modules MUST be pure (FR-006, SC-003).
- One indexer transaction per request (FR-001, SC-005).
- Verifier purity preserved (FR-007).

**Scale/Scope**:

- ~3 modules touched in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/`
  (`Request.hs` rewrites, `Real.hs` adds the IO orchestrator, the
  `Internal.hs` helpers it reuses are unchanged).
- ~3 HTTP handlers rewritten in `Cardano/MPFS/HTTP/Server.hs`.
- Optional: `Boot/Inputs.hs` generalised to `Wallet/Inputs.hs` if
  shared decode/projection helpers move out — to be decided in
  Phase 0.
- 1 new unit-test module covering the three `*Core` constructors.
- E2E tests extended in-place (no new modules).
- No wire-schema or migration changes.

## Constitution Check

The constitution at `.specify/memory/constitution.md` enumerates
principles that govern architectural decisions. This slice's gates
are identical in structure to the boot slice's (#249, PR #253).

### Principle I — Ledger-Native Types

**Gate**: PASS. The shared `InputRow` carries `TxOut ConwayEra`
directly; the DSL's `build` returns `Tx ConwayEra`. No shadow types.

### Principle II — Records of Functions

**Gate**: PASS. No new typeclasses introduced. `RequestCore` is a
pure data record; `Context` already exposes `runIndexerTx` from
PR #253 and is reused as-is.

### Principle III — Atomic Block Processing

**Gate**: PASS. The chain follower's writer invariant is unchanged.
The three request handlers add three new readers, each one a single
`runIndexerTx ctx $ do { … }` block — exactly the discipline
introduced by PR #253. No new column families, no new write paths.

### Principle IV — External Signing

**Gate**: PASS. Returned txs are unsigned. Unchanged.

### Principle V — Aiken Compatibility

**Gate**: PASS. Request datum, redeemer, output address, and
on-chain reference shapes are unchanged from the existing
`requestImpl`. Only the construction mechanism (DSL program vs.
imperative lens updates) changes; the resulting `Tx` is byte-equal
when fed identical inputs.

### Principle VI — Test Locally First

**Gate**: PASS. Full quality gate locally before each push,
mirroring the boot slice's discipline:
`nix build .#offchain-tests .#e2e-tests .#cardano-mpfs-offchain
.#docker-image .#checks.x86_64-linux.swagger-up-to-date && just
format-check && just hlint && nix run .#unit-tests && nix run
.#e2e-tests`.

### Principle VII — Nix Reproducibility

**Gate**: PASS. No new system dependencies.

### Principle VIII — Pure Offline Verification

**Gate**: PASS. The verifier is unchanged. Producer-side fix is
what makes verifier acceptance reach 100% under churn (SC-001) for
the three request responses.

### Principle IX — One Verifier, Many Targets

**Gate**: PASS / not affected. No verifier changes; no new
`cardano-mpfs-client` deps.

### Principle X — Lean as Source of Truth

**Gate**: NOT APPLICABLE for this slice. Same reasoning as PR #253
— the Lean model formalizes verifier-side properties; producer-side
read coherence is captured by FR-001/SC-001, not by a Lean theorem.

**Re-check after Phase 1 design**: see end of this file.

## Project Structure

### Documentation (this feature)

```text
specs/256-atomic-request-handlers/
├── plan.md              # This file
├── research.md          # Phase 0 — design decisions
├── data-model.md        # Phase 1 — RequestCore + reuse map
├── quickstart.md        # Phase 1 — exercise the three endpoints
├── contracts/
│   └── request-core.md  # Phase 1 — internal contract
├── checklists/
│   └── requirements.md  # Spec quality checklist (already complete)
└── tasks.md             # Phase 2 — generated by /speckit.tasks
```

### Source Code (repository root)

```text
cardano-mpfs-offchain/
├── lib/Cardano/MPFS/
│   ├── HTTP/Server.hs                        # CHANGED — three request handlers rewritten
│   ├── TxBuilder/Real.hs                     # CHANGED — adds runRequestBuilder
│   ├── TxBuilder/Real/Request.hs             # CHANGED — collapses to pure requestCore + 3 wrappers
│   ├── TxBuilder/Real/Boot/Inputs.hs         # MAYBE MOVED to Wallet/Inputs.hs (Phase 0)
│   └── TxBuilder/Real/Internal.hs            # UNCHANGED — non-boot/request builders still use it
└── test/Cardano/MPFS/TxBuilder/
    └── RequestSpec.hs                        # NEW — unit tests for the three Cores
```

**Structure Decision**: The boot slice already established the
shape: one pure `*Core` module + IO orchestrator in `Real.hs` + DSL
program for the tx body. This slice repeats that shape for the
three request endpoints, sharing one `requestCore` function across
`requestInsertCore` / `requestDeleteCore` / `requestUpdateCore` —
the three already funnel through a shared `requestImpl` today, so
the parameterisation point already exists in the source.

The only architectural question to resolve in Phase 0 is whether
`Boot/Inputs.hs` (input decoding + `InputRow` + `decodeAll` +
`ledgerPair` + `rowToWitness`) generalises to a shared
`Wallet/Inputs.hs`, since the three request endpoints need exactly
the same decoding shape as boot.

## Phase 0: Outline & Research

See `research.md` for the resolved design questions:

- **Q0-1 — Share `Boot/Inputs.hs` or duplicate?** Resolved: rename
  to `Wallet/Inputs.hs`. Both Boot and Request consume identical
  data. No behaviour change — just a cross-handler home.
- **Q0-2 — One `requestCore` parameterised on operation, or three
  separate functions?** Resolved: keep one `requestCore` with an
  operation-kind argument (mirrors today's `requestImpl`); export
  three thin wrappers as the public API.
- **Q0-3 — Where does `runRequestBuilder` live?** Resolved:
  `Cardano.MPFS.TxBuilder.Real`, alongside `runBootBuilder`. Both
  hold the `Provider` and the DSL `build` call; co-locating keeps
  the IO surface searchable.
- **Q0-4 — DSL combinator coverage.** Resolved: `spend` (per
  wallet input), `payTo'` (request output with inline datum at the
  per-token request address), `collateral`. No `mint`, no
  `spendScript`. The existing DSL combinators are sufficient.
- **Q0-5 — Where does the per-token request address come from?**
  Resolved: looked up via `State.tokens` (an existing `State` field
  on `Context`) inside the IO orchestrator. The pure
  `requestCore` receives the resolved `requestAddr` as an argument
  rather than the `tokenId`, so it does not depend on `State`.
- **Q0-6 — Does `requestImpl` consult `Provider.queryUTxOs` only,
  or anything else from `Provider`?** Resolved by source reading:
  `requestImpl` calls `queryUTxOs prov addr` (the forbidden one)
  and `queryProtocolParams prov` (kept, called from the IO
  orchestrator) and `evaluateTx prov` (kept, called from the IO
  orchestrator inside the DSL `build` evaluator). Only
  `queryUTxOs` goes.

**Output**: `research.md`.

## Phase 1: Design & Contracts

**Prerequisites**: research.md complete.

### 1. Data model

See `data-model.md`. Key entities:

- `RequestCore` — record bundling `TxBuild` program + ledger
  pairs + funding + snapshot + the per-token request output
  destination address.
- `RequestCoreError` — sum type for decode-failure and empty-
  inputs cases (mirrors `BootCoreError`).
- `RequestOp` — operation discriminator carrying insert / delete /
  update payloads; reuses the existing `Operation` ADT in
  `Cardano.MPFS.Core.Types` plus the on-chain `OpInsert` /
  `OpDelete` / `OpUpdate` constructors in
  `Cardano.MPFS.Core.OnChain`.

### 2. Contracts

See `contracts/request-core.md` — internal contract for the
shared `requestCore` function and the three public wrappers.
There are no new HTTP contracts: the three request wire
contracts are unchanged (FR-003).

### 3. Quickstart

See `quickstart.md` — bring up devnet, exercise the three
endpoints, observe the atomicity property.

### 4. Agent context update

`CLAUDE.md` already lists the technologies. This slice introduces
no new tech.

## Constitution Check (Re-evaluation, Post Phase 1 Design)

I, II, III, IV, V, VI, VII, VIII, IX, X — unchanged from
pre-Phase-0 evaluation. The shape is a strict pattern application
of the boot slice; no new types, typeclasses, key handling, Aiken
changes, system deps, verifier changes, or Lean changes.

No violations. No entries needed in Complexity Tracking.

## Complexity Tracking

> No constitutional violations. Section intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none)    |            |                                     |
