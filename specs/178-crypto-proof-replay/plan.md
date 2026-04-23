# Implementation Plan: Cryptographic CSMT + MPF proof replay in Client.Verify

**Branch**: `feat/cryptographic-proof-replay` (spec dir `178-crypto-proof-replay`) | **Date**: 2026-04-23 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/178-crypto-proof-replay/spec.md`
**Issue**: [#226](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/226)

## Summary

Wire the upstream WASM-safe CSMT + MPF verifiers (`mts:csmt-verify`,
`mts:mpf-write`) into `Cardano.MPFS.Client.Verify` so every proof-bearing
response is cryptographically replayed — not merely structurally
well-formed — before `cardano-mpfs-client` accepts it. Ship a small
`Cardano.MPFS.Client.Verify.DSL` with intent-revealing combinators
(`shouldAccept`, `shouldRejectWith`, `forgingRandomUtxoProofAt`,
`forgingWrongRootAt`, `tamperingTxOutAt`, `tamperingTrieValueAt`,
`dropToExclusionAt`, `promoteToInclusionAt`) so the E2E spec doubles as
the client-library manual, with paired positive (`shouldAccept`) and
negative (`shouldRejectWith`) scenarios for every endpoint.

## Technical Context

**Language/Version**: Haskell — GHC 9.10.1 (native) plus GHC-WASM and
GHC-JS cross-targets; language edition `GHC2021`; fourmolu 70-char
limit.
**Primary Dependencies**:

- New: `mts:csmt-verify` (already available via the `cabal.project`
  pin, WASM-safe), `mts:mpf-write` (new WASM-safe sublibrary,
  introduced on `haskell-mts` main via PR #147).
- Possibly new: `cborg` (only if binding checks need to decode
  `InclusionProof` CBOR to cross-check the in-proof key/value).
- Unchanged: `aeson`, `base`, `base16-bytestring`, `bytestring`,
  `text`.
- Forbidden by Principle IX: `cardano-ledger-*`, `crypton`,
  `rocksdb*`, any native-C-FFI library.

**Storage**: N/A. Verifier is a pure
`Hex -> Bundle -> Either VerifyError a`.
**Testing**: `hspec` unit tests in `cardano-mpfs-client/test/` (new
`Cardano.MPFS.Client.VerifySpec` for cryptographic replay and
forgery corpus); `hspec` E2E in
`cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/ProofsSpec.hs`
extended with paired positive + negative DSL scenarios; existing
cross-target QuickCheck suite covers GHC-native / GHC-WASM / GHC-JS
byte identity of `Either VerifyError a`.
**Target Platform**: `cardano-mpfs-client` ships to GHC-native,
`wasm32-wasi`, and GHC-JS; server runs on GHC-native Linux only.
**Project Type**: Multi-package Haskell project
(`cardano-mpfs-offchain/` server + pure `cardano-mpfs-client/`
library + shared MTS upstream).
**Performance Goals**: Replay cost is O(depth × log) per proof —
CSMT is a binary trie, MPF is 16-ary; each endpoint verifier
handles ≤ a few dozen witnesses in practice. Target: verifying a
complete `UpdateTxResponse` with 16 `trie_read` entries and 32
`WitnessedUtxo` values in < 50 ms native, < 200 ms WASM. Not a hot
path; no micro-benchmarking required.
**Constraints**:

- Principle VIII (Pure Offline Verification): no `IO`, no
  networking, no disk, no non-determinism; pure fold over the
  proof data, composable as `Kleisli (Either VerifyError)`.
- Principle IX (One Verifier, Many Targets): GHC-native + WASM +
  JS all build and produce byte-identical `Either VerifyError a`.
- Principle X (Lean as Source of Truth): before the Haskell
  verifier changes, the Lean model grows a `Verify` module that
  formalises CSMT + MPF replay and proves key-binding /
  value-binding preservation; a QuickCheck
  `prop_matchesLeanReference` asserts the Haskell matches.
- Principle V (Aiken compatibility): MPF proofs on the wire carry
  Aiken-parity encoding; use `MPF.Verify` primitives exactly as
  Aiken consumes them, with no local re-encoding.

**Scale/Scope**: ~200–400 LOC added in `cardano-mpfs-client` (new
`Verify.Replay`, `Verify.DSL`, expanded `VerifyError`), ~300 LOC
of tests (unit + 12 E2E scenarios), ~150 LOC of Lean
(`Phase4/Verify.lean` grows to cover replay preservation).
`haskell-mts` is **not** modified — only its existing main-branch
sublibraries are consumed.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | PASS | Client doesn't touch ledger types; server-side envelopes continue to use `cardano-ledger-*` for `Tx ConwayEra`. Proof bytes travel as opaque `ByteString`. |
| II. Records of Functions | PASS | Verifier stays a plain pure function; no typeclasses introduced. DSL combinators are plain functions. |
| III. Atomic Block Processing | N/A | No block-processing or RocksDB write path touched. |
| IV. External Signing | PASS | Server still returns unsigned tx CBOR; the change is strictly on the offline verifier. |
| V. Aiken Compatibility | PASS | `MPF.Verify` from `mts:mpf-write` is Aiken-parity; we consume its primitives rather than re-implementing MPF locally. |
| VI. Test Locally First | PASS | Unit tests run without devnet; E2E reuses the existing `cardano-node` devnet bracket; no CI-only dependency added. |
| VII. Nix Reproducibility | PASS | Only new `build-depends` are already in the upstream `haskell-mts` flake; no system-level or out-of-flake dep added. |
| VIII. Pure Offline Verification | PASS | Every new combinator is a pure fold; the primitives we consume (`verifyInclusionProof`, `verifyExclusionProof`, `MPF.Verify.*`) are themselves pure. No IO leaks. |
| IX. One Verifier, Many Targets | PASS (guarded) | Gate: the cross-target CI check must stay green on the feature branch. We additionally audit the new `build-depends` (nothing C-FFI). |
| X. Lean as Source of Truth | GUARDED | Before any Haskell edit, grow the Lean model under `lean/Phase4/Verify.lean` (or equivalent) with a state machine that captures replay preservation; then add `prop_matchesLeanReference` on the Haskell side. Documented as Phase 1.5 below. |

No violations. No entries required in the Complexity Tracking
table. Principle X imposes a precondition honoured by the Phase 2
task ordering: Lean before Haskell, Haskell before tests.

## Project Structure

### Documentation (this feature)

```text
specs/178-crypto-proof-replay/
├── plan.md                # This file
├── spec.md                # Already present
├── research.md            # Phase 0 output (this command)
├── data-model.md          # Phase 1 output (this command)
├── quickstart.md          # Phase 1 output (this command)
├── contracts/             # Phase 1 output (this command)
│   ├── verify-error.md
│   ├── replay-primitives.md
│   └── dsl.md
├── checklists/
│   └── requirements.md    # Already present
└── tasks.md               # Phase 2 output (NOT created here)
```

### Source Code (repository root)

```text
cardano-mpfs-client/
├── lib/
│   └── Cardano/MPFS/Client/
│       ├── Bundle.hs           # (touched — expose accessors if needed)
│       ├── Snapshot.hs         # (unchanged)
│       ├── Verify.hs           # (extended — new error cases, calls into Replay)
│       ├── Verify/
│       │   ├── Replay.hs       # NEW — CSMT/MPF cryptographic replay + binding
│       │   ├── DSL.hs          # NEW — shouldAccept/shouldRejectWith/forging combinators
│       │   └── Examples.hs     # NEW — example snippets re-exported for docs
│       └── Client.hs           # (touched — re-export Verify.DSL + new error ctors)
├── test/
│   └── Cardano/MPFS/Client/
│       ├── VerifySpec.hs       # NEW — forgery corpus (≥ 8 cases)
│       └── Verify/DSLSpec.hs   # NEW — DSL combinator properties
└── cardano-mpfs-client.cabal   # (touched — new build-deps on mts:csmt-verify / mts:mpf-write)

cardano-mpfs-offchain/
└── e2e-test/
    └── Cardano/MPFS/E2E/
        └── ProofsSpec.hs       # (extended — paired shouldAccept / shouldRejectWith scenarios per endpoint)

lean/
├── Phase4.lean
└── Phase4/
    ├── Verify.lean             # NEW — replay state machine + preservation theorems
    └── …                       # existing Phase4 modules unchanged
```

**Structure Decision**: layer the WASM-safe client library as
`Client.Verify` (orchestrates per-endpoint response walk) →
`Client.Verify.Replay` (pure CSMT / MPF replay primitives, calls into
`mts:csmt-verify` / `mts:mpf-write`) → `Client.Verify.DSL` (the
tutorial-shaped combinators). Forgery helpers live in the DSL module
so downstream consumers can import them. E2E stays in
`cardano-mpfs-offchain/e2e-test/` because it needs a live devnet.

## Phase 1.5: Lean before Haskell (Principle X gate)

Before any code in `cardano-mpfs-client` changes, the Lean model must
cover the new invariant. Specifically, `lean/Phase4/Verify.lean` grows:

- `Proof : Type` — abstract handle for a proof-bytes blob.
- `verifyCsmt : Root → Key → Value → Proof → Prop` — inclusion.
- `verifyCsmtAbsence : Root → Key → Proof → Prop` — exclusion.
- `verifyMpf` / `verifyMpfAbsence` — same shape for MPF proofs.
- State transitions `replayWitness`, `replayTrieFact` acting on a
  `VerifiedEnvelope` state; preservation theorems such as:
  - `replay_binds_key` — a proof accepted against root `r` for
    advertised key `k` cannot be accepted against `r` for any
    `k' ≠ k`.
  - `replay_binds_value` — same shape for `value`.
  - `replay_preserves_root_trust` — adding a witness never changes
    which root the envelope was rooted in.
- Matching Haskell reference via `prop_matchesLeanReference` in the
  unit-test suite: generate random envelopes, assert Haskell's
  `Either VerifyError ()` agrees with the Lean-extracted reference.

Lean theorems compile with no `sorry`, no custom axioms, and no
`native_decide` on large terms. The Haskell implementation is
written to match the Lean signatures.

## Phase 2 preview (NOT executed by this command)

`tasks.md` will enumerate the slices in this order:

1. **Lean**: `Phase4/Verify.lean` predicates + preservation theorems.
2. **Upstream pin bump**: `cabal.project` `haskell-mts` tag → commit
   on `main` that carries both `mts:csmt-verify` (PR #141) and
   `mts:mpf-write` (PR #147); update `--sha256:` comment.
3. **Client cabal**: add `build-depends` on `mts:csmt-verify`,
   `mts:mpf-write`; audit remains WASM-safe.
4. **`Verify.Replay` module**: wrap `verifyInclusionProof` /
   `verifyExclusionProof` (CSMT) and the `MPF.Verify.*` primitives,
   plus key/value-binding cross-checks that decode the wire
   `InclusionProof` CBOR and compare `proofKey` / `proofValue`
   against the advertised `TxIn` / `TxOut` / `TrieFact.key` /
   `TrieFact.value`.
5. **`VerifyError` extension**: add `CsmtReplayFailed Text Text`
   and `MpfReplayFailed Text Text` constructors.
6. **`Verify.hs`**: thread replay into `checkWitnessedUtxo` and a
   new `checkTrieFactBinding` used by `verifyUpdateTxResponse`;
   preserve the existing error order (structural → binding →
   replay).
7. **`Verify.DSL` module**: combinators
   `shouldAccept`, `shouldRejectWith`, forgery helpers
   (`forgingRandomUtxoProofAt`, `forgingWrongRootAt`,
   `tamperingTxOutAt`, `tamperingTrieValueAt`,
   `dropToExclusionAt`, `promoteToInclusionAt`).
8. **Forgery unit tests** `Cardano.MPFS.Client.VerifySpec`: ≥ 8
   scenarios (4 CSMT + 4 MPF) plus the DSL-level positive
   counterparts, matching the acceptance scenarios in the spec.
9. **QuickCheck `prop_matchesLeanReference`**: random envelopes +
   Lean-extracted reference.
10. **E2E** `Cardano.MPFS.E2E.ProofsSpec`: extend with paired
    `shouldAccept` / `shouldRejectWith` scenarios for each of the 6
    per-endpoint responses (6 positive + 6 negative = 12+ scenarios).
    `/tx/reject` negative coverage stays in the unit test if devnet
    cannot expose an elapsed-deadline request (tracked by #224).
11. **Cross-target CI** audit: run the existing
    `cardano-mpfs-client-cross-target` check; confirm byte-identical
    outputs and no new C-FFI deps.
12. **Docs**: Haddock on every new combinator linking back to the
    spec scenario; swagger regeneration only if the wire contract
    changed (it should not, since this is a client-side replay).

## Post-design Constitution re-check

Re-evaluated after Phase 1 artifacts (data-model, contracts,
quickstart) were produced:

- **Principle VIII** — confirmed: the `Verify.Replay` contract is a
  total pure function, no `IO` in the Haskell signatures; the
  `IO`-suffixed forgery helpers in `Verify.DSL` are *test-time
  helpers*, not verifier code, and have deterministic `'` variants
  for property tests.
- **Principle IX** — confirmed: the only new client build-depends
  are `mts:csmt-verify`, `mts:mpf-write`, and (if strictly required)
  `cborg`, all already WASM-safe in the `haskell-mts` toolchain.
  The DSL module itself has no cross-target-unsafe deps.
- **Principle X** — the Phase-1.5 gate is explicit in the task
  ordering; `Phase4/Verify.lean` lands *before* any Haskell change.
- **Principle V** — `aikenKeyPath` is the same helper the server
  uses via `mts:mpf-write`, so the client never re-implements the
  Aiken key-path.

No violations surfaced during Phase 1 design; the Complexity
Tracking table stays empty.

## Complexity Tracking

> Fill ONLY if Constitution Check has violations that must be justified.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)*  | —          | —                                    |
