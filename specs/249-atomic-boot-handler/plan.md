# Implementation Plan: Atomic POST /tx/boot

**Branch**: `249-atomic-boot-handler` | **Date**: 2026-05-02 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/249-atomic-boot-handler/spec.md`

## Summary

Make `POST /tx/boot` produce a proof-bearing response whose snapshot,
resolved input `TxOut` bytes, and per-input CSMT inclusion proofs all
come from a single coherent read of the local indexer's RocksDB. Drop
`Provider.queryUTxOs` from the boot path entirely — the indexer is the
single source of truth for UTxO state on every tx-build path. The wire
contract `POST /tx/boot { address }` is preserved; the server selects
inputs from the address's CSMT subtree, not from the request body and
not from cardano-node.

The mechanism is a new `AtomicCageReader` seam threaded into the boot
tx-builder. Production wires it to a single `RunTransaction` call over
`UnifiedColumns` that walks the address-prefix subtree of the UTxO CSMT
(reusing the existing `collectValues` primitive), reads each leaf's
`TxOut` bytes from `KVCol`, generates each leaf's inclusion proof
against `CSMTCol`, and returns the indexer's checkpoint as the
`BundleSnapshot` — all inside one transaction. Tests with
`followerEnabled = False` install an alternate `AtomicCageReader` via a
new `AppConfig.atomicCageReaderOverride` field that lets the harness
(which already has wallet-side `LocalStateQuery` access) supply a
working reader without involving the chain follower.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1
**Primary Dependencies**:

- `cardano-mpfs-offchain` (this repo) — server, tx-builders, HTTP layer
- `cardano-utxo-csmt` — CSMT runtime, `queryByAddress`, `Columns` GADT
  (`KVCol`, `CSMTCol`, …)
- `haskell-mts` — `collectValues`, `generateInclusionProof`,
  `FromKV` iso (note absolute-jump contract change merged 2026-04 —
  `Indirect.jump` is now the full key, no longer a delta)
- `rocksdb-kv-transactions` — snapshot-isolated read transactions over
  `UnifiedColumns`, `mapColumns InUtxo`
- `chain-follower` — supplies the checkpoint that becomes the snapshot
- `cardano-ledger-conway` — `TxOut` CBOR (de)serialization
- `cardano-node-clients` — used by `Provider`, but explicitly not on
  the boot build path after this change
- `servant-server` — `txBootHandler` wiring

**Storage**: RocksDB with 13 column families (6 UTxO from
cardano-utxo-csmt + 6 cage/trie + 1 composed-rollback). All boot reads
go through `UnifiedColumns` projected to `InUtxo` for the CSMT walk and
the KV lookups.

**Testing**:

- Unit (`cardano-mpfs-offchain` test suite) — boot tx-builder
  property-tested with a deterministic in-memory `AtomicCageReader`
- E2E (`cardano-mpfs-offchain` e2e-test suite) — devnet subprocess,
  exercises the real wiring with `followerEnabled = True`
- Fixture path with `followerEnabled = False` — verified end-to-end via
  the existing CageSpec/CageFlowSpec harness using the test-seam stub
- Cross-target client verifier QuickCheck (`cardano-mpfs-client`) —
  proves the response check is pure (not affected here, but the
  pre-condition for SC-001)

**Target Platform**: Linux server (amd64) inside `nix develop`. Verifier
side is not touched by this change.

**Project Type**: Haskell multi-package web service (single repo,
multiple cabal packages).

**Performance Goals**:

- Boot endpoint median latency at K=2 wallet UTxOs and 1M total chain
  UTxOs ≤ 2× the median at 1k total chain UTxOs (SC-003).
- Throughput: not changed — boot is a per-wallet, low-rate path.

**Constraints**:

- Wire contract MUST stay `POST /tx/boot { address }` (FR-003).
- No `Provider.queryUTxOs` on the build path (FR-002, SC-002).
- One indexer transaction per boot response (FR-001, SC-005).
- Test seam configurable at startup, not at runtime (FR-006).
- Verifier purity preserved — this change touches only the producer
  side (FR-007).

**Scale/Scope**:

- ~5 modules touched in `cardano-mpfs-offchain` (Context, Application,
  TxBuilder, TxBuilder/Real, TxBuilder/Real/Boot, plus HTTP wiring).
- 1 new test helper in the e2e/integration harness.
- No schema or wire changes; no migration.

## Constitution Check

The constitution at `.specify/memory/constitution.md` enumerates ten
principles. Each gate below records how this feature satisfies it; any
violation must appear in Complexity Tracking with a justification.

### Principle I — Ledger-Native Types

**Gate**: PASS. The atomic reader returns the same `TxOut ConwayEra`
CBOR bytes the indexer already stores in `KVCol` (deserialized only at
the use-site by `bootTokenImpl`). No shadow types.

### Principle II — Records of Functions

**Gate**: PASS. `AtomicCageReader m` is a record-of-functions field on
`Context m` (or, equivalently, threaded into `mkRealTxBuilder`). No new
typeclasses introduced.

### Principle III — Atomic Block Processing

**Gate**: PASS / load-bearing. The boot read is a *reader* over the
same `UnifiedColumns` the chain follower writes. Crash-safety of the
follower's "one block = one write batch" invariant is preserved
because we add no writers and no new transaction shape; we only
introduce a single multi-column read transaction that observes a
snapshot of that batch.

### Principle IV — External Signing

**Gate**: PASS. The tx returned by boot is unsigned; the server holds
no keys. Unchanged.

### Principle V — Aiken Compatibility

**Gate**: PASS. The boot tx body, redeemer, and datum encoding are
unchanged — only the *source* of the wallet UTxOs that fund the tx
changes. Asset-name derivation from the seed `TxIn`, the cage policy,
and the on-chain validator semantics are untouched.

### Principle VI — Test Locally First

**Gate**: PASS. The full quality gate (build all targets, format-check,
hlint, unit-tests, e2e) runs before every push; the speckit task list
will explicitly close out only after the e2e suite is green locally on
the merge-base of `origin/main`.

### Principle VII — Nix Reproducibility

**Gate**: PASS. No new system dependencies. CI mirrors local
`just ci` plus `nix build .#offchain-tests .#e2e-tests
.#cardano-mpfs-offchain .#docker-image
.#checks.x86_64-linux.swagger-up-to-date`.

### Principle VIII — Pure Offline Verification

**Gate**: PASS. The verifier (`cardano-mpfs-client`) is unchanged. The
producer-side fix here is what makes verifier acceptance reach 100%
(SC-001) under chain churn — the verifier was already pure; it was the
producer that was racy.

### Principle IX — One Verifier, Many Targets

**Gate**: PASS / not affected. No verifier changes; no new
`cardano-mpfs-client` deps.

### Principle X — Lean as Source of Truth

**Gate**: NOT APPLICABLE for this slice. The Lean model formalizes the
verifier state machine, which this change does not touch. The atomicity
property of the producer is an operational, not a verifier-fold,
property — it is captured by the FR-001/SC-001 acceptance criteria, not
by a Lean theorem. If a future slice extends Lean to model producer-side
read coherence, this feature's invariant will be added there at that
time.

**Re-check after Phase 1 design**: see end of this file.

## Project Structure

### Documentation (this feature)

```text
specs/249-atomic-boot-handler/
├── plan.md              # This file
├── research.md          # Phase 0 — design decisions and rationale
├── data-model.md        # Phase 1 — AtomicCageReader and its callers
├── quickstart.md        # Phase 1 — how to exercise the boot path locally
├── contracts/
│   └── atomic-cage-reader.md  # Phase 1 — internal contract for the seam
├── checklists/
│   └── requirements.md  # Spec quality checklist (already complete)
└── tasks.md             # Phase 2 — generated by /speckit.tasks
```

### Source Code (repository root)

```text
cardano-mpfs-offchain/
├── lib/Cardano/MPFS/
│   ├── Application.hs              # CHANGED — wires AtomicCageReader prod impl
│   ├── Context.hs                  # CHANGED — adds atomicCageReader field
│   ├── TxBuilder.hs                # UNCHANGED — public interface preserved
│   ├── TxBuilder/Real.hs           # CHANGED — threads reader to bootTokenImpl
│   └── TxBuilder/Real/Boot.hs      # CHANGED — drops queryUTxOs, uses reader
├── exe/
│   └── Serve.hs                    # CHANGED — atomicCageReaderOverride = Nothing
├── test/Cardano/MPFS/
│   ├── HTTP/TokenSpec.hs           # CHANGED — install stub reader for boot tests
│   └── TxBuilder/BootSpec.hs       # NEW — unit-tests bootTokenImpl with stub
└── e2e-test/Cardano/MPFS/E2E/
    ├── ProofsSpec.hs               # MAY CHANGE — verify SC-001 under churn
    └── CageFlowSpec.hs             # CHANGED — uses test-seam reader

cardano-mpfs-client/                # UNCHANGED — verifier is already pure
.specify/memory/constitution.md     # UNCHANGED
```

**Structure Decision**: This is a Haskell multi-package web service.
The change is localized to `cardano-mpfs-offchain`. The key new module
boundary is `AtomicCageReader`, defined in `Cardano.MPFS.Context`
(record-of-functions, per Principle II) and constructed in two places:

- Production: `Cardano.MPFS.Application.withApplication` builds the
  reader as a closure over `RunTransaction`, `mapColumns InUtxo`,
  `collectValues`, `query KVCol`, `generateInclusionProof`, and the
  current `latestRollbackPoint`. A single `run $ do { … }` block
  performs all reads.
- Tests with `followerEnabled = False`: the harness supplies an
  override via `AppConfig.atomicCageReaderOverride :: Maybe (AtomicCageReader IO)`.
  The override is queried before the production builder; if `Nothing`,
  production wires the real reader.

The existing `Provider.queryUTxOs` is *kept* as a Provider field for
non-boot paths (e.g. wallet-side use during tests) but has a Haddock
warning attached — and zero call sites on tx-build paths after this
slice. Subsequent slices in #250/#252 remove it from the remaining
build paths.

## Phase 0: Outline & Research

See `research.md` for the resolved design questions:

- Q0-1 — *Where does the atomic transaction live?* Resolved:
  `withApplication` constructs the closure; it is a sibling to
  `exists` / `resolve` / `proof` and shares the `utxoRt`
  `RunTransaction`.
- Q0-2 — *How do we walk the address subtree?* Resolved: reuse the
  existing `queryByAddress` shape from `cardano-utxo-csmt` —
  `collectValues CSMTCol [] addressKey`, then for each `Indirect{jump}`
  look up `KVCol jump` and call `generateInclusionProof fkv KVCol
  CSMTCol jump` inside the same transaction. No new primitive.
- Q0-3 — *What is the exact return shape?* Resolved:
  `AtomicCageReader m = Addr -> m (Either AtomicReaderError (BundleSnapshot, [(TxIn, ByteString, ByteString)]))`
  where each tuple is `(input ref, TxOut CBOR, CSMT proof)`. The error
  ADT distinguishes the four spec edge cases (no checkpoint, no UTxOs,
  KV-missing-for-leaf, snapshot-not-readable).
- Q0-4 — *How does the test seam look?* Resolved: a new optional
  `AppConfig.atomicCageReaderOverride :: Maybe (AtomicCageReader IO)`
  field, defaulted to `Nothing` in `Serve.hs`, set to `Just …` by test
  harnesses that disable the follower.
- Q0-5 — *Why is removing `queryUTxOs` from boot the right reason?*
  Resolved: cardano-node's `GetUTxOByAddress` runs in
  `O(total UTxOs in ledger)`, not `O(K)`. Memory recorded; details in
  research.md.
- Q0-6 — *What about the `requireBundleSnapshot` handler-level read?*
  Resolved: removed from the boot path. The snapshot now comes from
  the same atomic read the proofs come from; the HTTP handler no
  longer reads the snapshot before calling the builder. Other handlers
  retain their current shape (their slices of #250 will fold them into
  their own atomic readers).

**Output**: `research.md` (this directory).

## Phase 1: Design & Contracts

**Prerequisites**: research.md complete.

### 1. Data model

See `data-model.md`. Key entities:

- `AtomicCageReader m` — record-of-functions seam.
- `AtomicReaderError` — sum type encoding the four FR-004 edge cases.
- `BundleSnapshot` — already exists in `Cardano.MPFS.TxBuilder`,
  produced by the reader rather than by `requireBundleSnapshot`.
- `WitnessedInput` — already exists; the reader supplies the inputs
  to the field.
- `WalletInput` (internal alias) — `(TxIn, TxOut ConwayEra, ByteString)`
  representing the deserialized form `bootTokenImpl` consumes.

### 2. Contracts

See `contracts/atomic-cage-reader.md` — the internal contract for
the new seam. There are no new HTTP contracts: the wire contract
`POST /tx/boot { address }` is unchanged (FR-003). The Swagger schema
does not move.

### 3. Quickstart

See `quickstart.md` — how to bring up a devnet, exercise the boot
endpoint, and observe the new atomicity property end-to-end.

### 4. Agent context update

The repo's `CLAUDE.md` already lists Haskell GHC 9.10.1, Servant,
Nix, etc. This feature does not introduce new technologies, so the
agent context file does not need new entries. The "Recent Changes"
section will be updated when the implementation lands.

## Constitution Check (Re-evaluation, Post Phase 1 Design)

- I/II/IV/V/VI/VII/VIII/IX/X — unchanged from pre-Phase-0 evaluation;
  no new types, no new typeclasses, no key handling, no Aiken-side
  changes, no new system deps, no verifier changes, no Lean change.
- III — confirmed by data-model.md: the reader is a single
  `RunTransaction` invocation. The Application.hs change places the
  reader's closure body in one `run $ do { … }` block, which is
  greppable and review-checkable (SC-005).

No violations. No entries needed in Complexity Tracking.

## Complexity Tracking

> No constitutional violations. Section intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none)    |            |                                     |
