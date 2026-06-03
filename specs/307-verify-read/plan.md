# Plan — #307 read-side verifiers (`Client/Verify/Read`)

## Goal

Populate the empty stub
`cardano-mpfs-verify/lib/Cardano/MPFS/Client/Verify/Read.hs` with two
pure, IO-free verifiers that mirror the `Client/Facts` `verify*Facts`
pattern (opaque `Verified*` newtype, hidden constructor, `verified*`
extractor, `TrustedRoot` threaded in, structured `VerifyError`):

- `verifyTokenState`  — verifies a `TokenResponse` (`GET /tokens/:id`).
- `verifyTokenFacts`  — verifies a `FactsResponse`
  (`GET /tokens/:id/facts`) by reconstructing the MPF root from the
  full enumerated `[FactEntry]` and asserting it equals the on-chain
  trie root in the (verified) `WitnessedTokenState`. This is the
  completeness proof.

Consumer: `cardano-foundation/moog` #155.

## Relationship to specs/243-proof-redesign

The 243 spec models a *per-key* facts endpoint
(`verifyTokenResponse` / `verifyFactPresentResponse`) anchored on
CSMT completeness proofs (tasks T013/T014, blocked upstream on
`lambdasistemi/haskell-mts#153`). Issue #307 supersedes that for the
read path: it verifies the *bulk* `[FactEntry]` endpoint (#305/#306)
by root reconstruction, which needs no upstream change. We follow the
issue, keep the issue's names, and leave the 243 read-side tasks as
they are.

## Tech notes

- `verifyTokenState`: check trusted-root length (32), assert
  `vsUtxoRoot trSnapshot == trustedRoot`, verify the snapshot
  (`verifyVerificationSnapshot`), and replay the state UTxO inclusion
  proof (`wtsUtxo trState`) against the trusted root via
  `replayWitnessedUtxo`. Mirrors `verifyBootFacts` + `replayFactState`.
- `verifyTokenFacts`: verify the embedded state anchoring (reuse
  `verifyTokenState`'s core over `frsSnapshot`/`frsState`), extract the
  on-chain trie root `root (wtsState frsState)`, reconstruct the MPF
  root from `frsFacts`, assert equality.
- Pure MPF reconstruction (no monadic MTS backend — package is
  IO-free per constitution VIII): map each `FactEntry` to
  `(aikenKeyPath feKey, mkMPFHash feValue)`, then
  `buildComposeFromList` → `scanMPFCompose [] mpfHashing` →
  `mpfRootFromNode mpfHashing` → `renderMPFHash`. Empty fact set →
  `nullHash`. All from `mts:mpf-write` (already a dependency).
- New `VerifyError` use: reconstruction/equality mismatch reported via
  an MPF-completeness error (reuse `MpfReplayFailed` or add a dedicated
  completeness constructor — decided in slice 2).

## Slices (one bisect-safe commit each)

- **Slice 1 (T001)** — `verifyTokenState` + `VerifiedTokenState` +
  `verifiedTokenState`, exports, honest fixture (accept) + forgeries
  (wrong trusted root, tampered state `tx_out`, tampered proof).
- **Slice 2 (T002)** — `verifyTokenFacts` + `VerifiedTokenFacts` +
  `verifiedTokenFacts`, reuse slice 1's anchoring; honest fixture
  (accept) + completeness forgeries (drop a fact, add a spurious fact,
  tamper a fact value → reconstructed-root mismatch rejects).

## Proof

TDD against `Cardano.MPFS.Client.Fixtures` honest fixtures (built from
the *real* MPF backend via `MPF.Test.Lib`, so reconstruction is an
independent check — not circular) plus the `TrieForge` forgery DSL.
No Lean for this PR (operator decision); the reconstruction-completeness
invariant is left as a Lean follow-up.

## Gate

`./gate.sh` — `just unit-client` + `just format-check` + `just hlint`.
