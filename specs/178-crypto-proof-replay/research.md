# Research: Cryptographic CSMT + MPF proof replay

**Feature**: 178-crypto-proof-replay
**Date**: 2026-04-23

## R1 — CSMT wire proof format and how to bind `(tx_in, tx_out)` to the proof

**Decision**: consume `mts:csmt-verify`'s `verifyInclusionProof :: ByteString -> ByteString -> Bool` and `verifyExclusionProof :: ByteString -> ByteString -> Bool`. For key/value binding, extract the `proofKey` / `proofValue` from the wire CBOR by re-using `CSMT.Core.CBOR.parseProof :: ByteString -> Maybe (InclusionProof Hash)`, then compare:

- `proofKey` against the Blake2b-256 (pure, from `CSMT.Verify.Blake2b`) of the CBOR-serialised `TxIn` (`tx_id || tx_ix` in the exact bytes the UTxO-CSMT writer uses — mirrored in `CSMT.Hashes.mkHash`).
- `proofValue` against the raw `tx_out` bytes advertised in the response.

**Rationale**: the `InclusionProof` CBOR already embeds `proofKey` and `proofValue` ([`lib/csmt-core/CSMT/Core/Proof.hs`](https://github.com/lambdasistemi/haskell-mts/blob/main/lib/csmt-core/CSMT/Core/Proof.hs)). Replaying the proof proves "this (key, value) pair is in a tree whose root is `R`"; the binding step proves "the (key, value) pair in the proof is the pair the response claims". Together they constitute "the advertised (tx_in, tx_out) is in the UTxO-CSMT rooted at `snapshot.utxo_root`".

**Alternatives considered**:

- **Trust the server's `tx_in`/`tx_out` fields and skip binding**: rejected — a server could ship a correct proof for a different UTxO and pass replay; the whole trust-minimisation story collapses.
- **Re-implement CSMT decoding locally in `cardano-mpfs-client`**: rejected — violates Principle IX (one verifier, one place) and Principle V (Aiken parity) and duplicates work already done in `mts:csmt-verify`.
- **Expose `proofKey` / `proofValue` accessors from `mts:csmt-verify` and skip the CBOR decode**: *accept this upstream later* — for now we decode the CBOR locally using `cborg` (already WASM-safe) because adding an accessor to upstream is a separate PR.

## R2 — MPF wire proof format and inclusion vs. exclusion dispatch

**Decision**: use the `MPF.Verify` primitives from `mts:mpf-write` (introduced by `haskell-mts` PR #147). Select by advertised `TrieFact.value`:

- `Just v`  → `MPF.Verify.verifyInclusionProof trustedRoot key v proofBytes :: Bool` (exact Aiken-parity CBOR).
- `Nothing` → `MPF.Verify.verifyExclusionProof trustedRoot key proofBytes :: Bool`.

Cross-check the in-proof key against the advertised `TrieFact.key` (after the Aiken `blake2b_256(key)` path hashing — exposed by the helper added in PR #147 and routed through the MPF write path) and the in-proof value against `TrieFact.value`.

**Rationale**: mpf-write is pure-Haskell, WASM-safe (`buildable: True` under `flag(wasm)`), and its `MPF.Verify` routes through the same Aiken key-path helper the server uses. Keeps Principle V (Aiken compatibility) trivially satisfied.

**Alternatives considered**:

- **Parse the MPF proof by hand**: rejected — Aiken parity is maintained in one place (upstream `MPF.Verify`); forking it here would drift.
- **Consume `mts:mpf`**: rejected — `mpf` is `buildable: False` under `flag(wasm)` (carries the RocksDB backend). `mpf-write` is the WASM-safe subset.

## R3 — Pinning `haskell-mts` so both sublibraries are present

**Decision**: bump the `cabal.project` `source-repository-package` tag for `https://github.com/lambdasistemi/haskell-mts` from `a37b352041a1f90c00940ede0336dcc8d85140ee` to a `main`-branch commit that includes both `mts:csmt-verify` (#141) and `mts:mpf-write` (#147) — as of 2026-04-23 the tip is `9a51067`. Re-compute the `--sha256:` via `nix flake prefetch` + `nix hash convert --to nix32` and pin it. Pins-main-only (memory rule).

**Rationale**: the current pin predates `mts:mpf-write` (merged by #147, landing after `a37b352`). Without the bump, the Haskell build cannot see `MPF.Verify`.

**Alternatives considered**:

- **Add `mts:mpf-write` as a separate `source-repository-package`**: rejected — same repo, would cause duplicate package conflicts.
- **Use `cabal-debug.project` to override with a local checkout**: use this *during development* for faster iteration, but the merged PR must pin a main commit (rule `pins_main_only`).

## R4 — Where to decode the wire `InclusionProof` without pulling in non-WASM deps

**Decision**: use `cborg` (already in the workspace and in the `haskell-mts` WASM demo). Either:

(a) reuse the already-WASM-safe `CSMT.Core.CBOR.parseProof` and `MPF.Hashes.CBOR.parseFact` / equivalent by build-depending on `mts:csmt-verify` (which re-exports `CSMT.Core.CBOR`) and `mts:mpf-write` directly; or

(b) write a tiny local decoder in `Cardano.MPFS.Client.Verify.Replay`.

Start with (a) — no duplicate decoder — and only fall back to (b) if the re-export surface is insufficient.

**Rationale**: keeps us off the `cardano-ledger-*` / `crypton` path (Principle IX) and reuses the same decoder the Aiken validator effectively consumes.

**Alternatives considered**:

- **`binary`**: usable but we already use `cborg` via `mts:*`; no benefit to pulling in both.
- **`aeson` alone**: insufficient — the proofs are CBOR, not JSON, inside the JSON envelope (the JSON envelope hex-encodes the CBOR).

## R5 — DSL shape for tests-as-manual (spec FR-010, FR-011, FR-012)

**Decision**: expose a single module `Cardano.MPFS.Client.Verify.DSL` with the following surface:

```haskell
-- Positive + negative entry points
shouldAccept      :: (a -> Either VerifyError ()) -> a -> Expectation
shouldRejectWith  :: (a -> Either VerifyError ()) -> ErrorMatcher -> a -> Expectation

-- Error matchers that let the scenario read as prose
csmtReplayFailedAt :: Text -> ErrorMatcher
mpfReplayFailedAt  :: Text -> ErrorMatcher

-- Forgery helpers: return a *new* response with the targeted field tampered
forgingRandomUtxoProofAt :: Text -> a -> IO a
forgingWrongRootAt       :: Text -> a -> IO a
tamperingTxOutAt         :: Text -> a -> IO a
tamperingTrieValueAt     :: Int  -> a -> IO a
dropToExclusionAt        :: Int  -> a -> IO a
promoteToInclusionAt     :: Int  -> a -> IO a
```

All helpers operate through lens-style field paths so the negative scenarios and the positive scenarios share the same response type. The DSL module is itself pure — the `IO` in the forgery helpers is only there to draw fresh random bytes for "random" forgeries; variants that take an explicit seed are available for property tests.

**Rationale**: the user's requirement — "DSL expressive enough so anyone can read the end-to-end test and extract a manual from them" — is satisfied when `response `shouldAccept` verifyBootTxResponse` and `response `shouldRejectWith` csmtReplayFailedAt "boot.funding[0].utxo_proof"` read as English. Intent-revealing verb names (`forging…`, `tampering…`, `dropTo…`, `promoteTo…`) pair naturally with the failure name in the assertion.

**Alternatives considered**:

- **Reuse hspec's `shouldBe` directly**: rejected — exposes the `Either VerifyError ()` plumbing to every scenario reader.
- **QuickCheck-only DSL**: rejected — the spec wants E2E scenarios that a human reads top-to-bottom; QuickCheck complements rather than replaces this.
- **Tie DSL combinators to individual response types** (`bootShouldAccept`): rejected — duplicates the entry points 6× without clarifying anything; the single polymorphic `shouldAccept` over all `a` works.

## R6 — Ordering of structural, binding, and replay errors

**Decision**: keep the existing structural errors (`MalformedHex`, `WrongHexLength`, `EmptyBlockId`, `MalformedTxCbor`) as the *first* layer. On structural success, run a new *binding* layer (compare in-proof key/value against advertised `(tx_in, tx_out)` / `(key, value)`). On binding success, run the *replay* layer against the root.

Report the earliest failure: a tampered hex length returns `WrongHexLength`, a key-binding mismatch returns `CsmtReplayFailed "<path>" "key binding mismatch"`, a genuine root mismatch returns `CsmtReplayFailed "<path>" "root mismatch"`. The `Text` reason is the disambiguator inside `CsmtReplayFailed` / `MpfReplayFailed` rather than a separate constructor, because field-path granularity is enough and the error ADT stays flat.

**Rationale**: symmetric to how today's verifier reports structural errors first. A single replay-failure constructor with a human-readable reason keeps the `Either VerifyError a` surface small and backwards-compatible for downstream pattern-matchers that only care about "structural vs replay".

**Alternatives considered**:

- **Separate constructors per reason** (`CsmtKeyBindingMismatch`, `CsmtRootMismatch`, …): rejected — explodes the ADT; tests can match on the `Text` reason directly via the DSL matchers.
- **Run replay before binding**: rejected — a root-mismatch would mask a key-binding mismatch, hiding the more informative error; binding-first gives the signer more actionable feedback.

## R7 — Can `/tx/reject` negative coverage live on devnet?

**Decision**: tentative **no**. Devnet `/tx/reject` needs a pending request whose processing deadline has elapsed; the existing E2E harness only creates fresh requests. Keep the `/tx/reject` negative path in `cardano-mpfs-client/test/Cardano/MPFS/Client/VerifySpec.hs` as a hand-crafted `RejectTxResponse` fed through `shouldRejectWith`. Track E2E coverage in issue [#224](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/224) (which already exists).

**Rationale**: avoids extending the devnet harness with a clock-skew helper for a single negative scenario. The unit test is enough to satisfy FR-011 because the DSL combinators are the same.

**Alternatives considered**:

- **Add a time-travel feature to devnet**: out of scope; tracked elsewhere.
- **Manually set the deadline shorter than the request time**: requires server-side wiring that is not present today; deferred.

## R8 — Lean model of replay (Principle X compliance)

**Decision**: add `lean/Phase4/Verify.lean` with abstract primitives (`verifyCsmt`, `verifyCsmtAbsence`, `verifyMpf`, `verifyMpfAbsence`), a `VerifiedEnvelope` state, and three preservation theorems (`replay_binds_key`, `replay_binds_value`, `replay_preserves_root_trust`). Theorems compile with no `sorry`. Derive a QuickCheck-extractable reference via the existing Lean-to-Haskell pattern used for `lean/Phase4/Theorems.lean`.

**Rationale**: satisfies Principle X — the state machine + preservation properties are the source of truth for the Haskell shape. The Haskell signatures mirror the Lean ones.

**Alternatives considered**:

- **Skip Lean and rely on QuickCheck alone**: rejected — violates Principle X outright.
- **Model CSMT/MPF internals in Lean**: out of scope — the existing mts / csmt-core already has its own Lean story; we only model the client-side *wrapper* and binding.

## R9 — Performance floor

**Decision**: no micro-benchmark gate. Rely on a single hspec "performance sanity" test that verifies an `UpdateTxResponse` with 16 trie-reads + 32 witnesses in < 100 ms on GHC-native (the existing CI boxes). Skip on WASM/JS where measurement variance dwarfs the cost.

**Rationale**: replay cost scales linearly with witness count and logarithmically with trie depth. There is no realistic use case at a thousand witnesses; the current client verifier runs offline during signing, where 100 ms is invisible.

**Alternatives considered**:

- **Criterion benchmarks**: overkill for this change; add later if we observe drift.
- **No performance check at all**: fine in theory, but the sanity assertion is a cheap regression guard against accidentally `O(n²)` implementations.

## R10 — Scope boundary with issues #227 and #223

**Decision**:

- #227 ("bind proof-bundle content to the unsigned tx") is **out of scope** here. This feature validates the proof is *about what it says it's about* (key/value binding) and *rooted where it says it is* (root replay); #227 will validate that what the proof says matches what the *tx* says.
- #223 ("QuickCheck properties for proof-data consistency in MPFS responses") is **complementary** but lives in its own PR. The unit-test corpus we land here is example-based; #223 adds random-generation properties. The DSL combinators we ship are usable by both.

**Rationale**: keeps this PR's diff small and independently shippable. Adds the DSL surface #223 will want anyway.

**Alternatives considered**:

- **Bundle #227 into this PR**: rejected — that adds a tx-CBOR decoder (or a server-emitted summary) whose design is a separate ticket.
- **Bundle #223**: rejected — the QuickCheck generators deserve their own review cycle.
