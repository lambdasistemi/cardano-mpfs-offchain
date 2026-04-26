# Implementation Plan: Typed read-side endpoints + verifiers

**Branch**: `feat/231-typed-read-side-verifiers` | **Date**: 2026-04-26 | **Spec**: [spec.md](./spec.md)
**Issue**: [#231](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/231)

## Summary

Add typed Haskell mirrors for the four proof-bearing read responses
(`/tokens/<id>`, `/tokens/<id>/facts/<key>`, `/tokens/<id>/proofs/<key>`,
`/tokens/<id>/requests`) and a pure offline verifier per response that
reuses the existing `replayWitnessedUtxo` and `replayTrieFact`
primitives. Add fixtures and forgery-DSL tests mirroring the
write-side `VerifySpec.hs` structure. Re-export the surface from
`Cardano.MPFS.Client`.

## Technical Context

**Language/Version**: Haskell, repo-pinned GHC through the existing Nix
development shell.
**Primary Dependencies**: `aeson`, `bytestring`, `text`, plus the
existing replay primitives. No new external dependencies.
**Storage**: N/A.
**Testing**: `cardano-mpfs-client:unit-tests` extended with
read-response fixtures + forgery-DSL coverage.
**Target Platform**: Same as the rest of the verifier — GHC native
plus future GHC-WASM and GHC-JS targets.
**Project Type**: Haskell client library in a multi-package flake.
**Performance Goals**: Constant-factor over write-side verification —
one structural pass plus one `replayWitnessedUtxo` per witness plus,
for `/facts`/`/proofs`, one `replayTrieFact`.
**Constraints**: Keep verifiers pure (no `IO`). Do not import
`cardano-ledger-*` or `cardano-mpfs-offchain` into the client.
**Scale/Scope**: One new module (`Cardano.MPFS.Client.Read`), four
new verifiers added to `Cardano.MPFS.Client.Verify`, fixtures + tests
extension, top-level re-export update.

## Constitution Check

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | PASS | Read DTOs mirror the existing `cardano-mpfs-api` wire format; no shadow ledger types. |
| II. Records of Functions | PASS | No service typeclass added; new verifiers are plain functions. |
| III. Atomic Block Processing | N/A | Indexer not touched. |
| IV. External Signing | N/A | Read endpoints; no signing path. |
| V. Aiken Compatibility | PASS | No proof / datum encoding changes. The trie root carried in the state datum is consumed as a hex byte string. |
| VI. Test Locally First | PASS | New tests are local unit tests over pure CSMT + MPF backends. |
| VII. Nix Reproducibility | PASS | No new dependency. |
| VIII. Pure Offline Verification | PASS | Each new verifier is `Response -> Either VerifyError ()`. |
| IX. One Verifier, Many Targets | PASS | Same dependency set as the write-side verifier; cross-target packaging tracked in milestone #3. |
| X. Lean as Source of Truth | PASS | The new verifiers reuse the existing `replayWitnessedUtxo` / `replayTrieFact` preservation theorems; no new invariants are introduced. |

No justified violations.

## Project Structure

### Documentation

```text
specs/231-typed-read-side-verifiers/
├── spec.md
├── plan.md
└── tasks.md
```

### Source Code

```text
cardano-mpfs-client/
├── lib/Cardano/MPFS/Client.hs               (re-exports updated)
├── lib/Cardano/MPFS/Client/Read.hs          (NEW: read response DTOs)
├── lib/Cardano/MPFS/Client/Verify.hs        (extended verifiers)
└── test/Cardano/MPFS/Client/
    ├── Fixtures.hs                          (extended with read fixtures)
    ├── ReadSpec.hs                          (NEW: read-side coverage)
    └── Main.hs                              (registers ReadSpec)
```

**Structure Decision**: A new sibling `Cardano.MPFS.Client.Read`
module hosts the read-side DTOs to keep `Cardano.MPFS.Client.Bundle`
(write-side) focused. Read verifiers live alongside the existing
write verifiers in `Cardano.MPFS.Client.Verify` because they share
helpers (`checkWitnessedUtxoStructural`, `checkTrieFactStructural`,
`replayWitnessedUtxos`, `replayTrieFacts`, `traverseIndexed`).

## Phase 1: Design

### Read DTOs (`Cardano.MPFS.Client.Read`)

```haskell
data TokenState = TokenState
    { owner       :: Text
    , root        :: Hex      -- trie root
    , tip         :: Integer
    , processTime :: Integer
    , retractTime :: Integer
    }

data WitnessedTokenState = WitnessedTokenState
    { utxo  :: WitnessedUtxo
    , state :: TokenState
    }

data Request = Request
    { token       :: Hex
    , owner       :: Text
    , key         :: Hex
    , operation   :: Text
    , value       :: Maybe Hex
    , fee         :: Integer
    , submittedAt :: Integer
    }

data WitnessedRequest = WitnessedRequest
    { utxo    :: WitnessedUtxo
    , request :: Request
    }

data FactWitness = FactWitness
    { state    :: WitnessedTokenState
    , mpfProof :: Hex
    }

data TokenResponse = TokenResponse
    { snapshot :: VerificationSnapshot
    , state    :: WitnessedTokenState
    }

data FactResponse = FactResponse
    { snapshot :: VerificationSnapshot
    , value    :: Hex
    , fact     :: FactWitness
    }

data ProofResponse = ProofResponse
    { snapshot :: VerificationSnapshot
    , fact     :: FactWitness
    }

data RequestsResponse = RequestsResponse
    { snapshot :: VerificationSnapshot
    , requests :: [WitnessedRequest]
    }
```

### Verifier signatures

```haskell
verifyTokenResponse    :: TokenResponse    -> Either VerifyError ()
verifyFactResponse     :: FactResponse     -> Either VerifyError ()
verifyProofResponse    :: ProofResponse    -> Either VerifyError ()
verifyRequestsResponse :: RequestsResponse -> Either VerifyError ()
```

### Field path conventions

| Endpoint   | Role path examples                                              |
|-----------|------------------------------------------------------------------|
| `/tokens/<id>`               | `token.state.utxo_proof`, `token.state.tx_out`, `token.snapshot.utxo_root` |
| `/tokens/<id>/facts/<key>`   | `fact.state.utxo_proof`, `fact.state.root`, `fact.mpf_proof`               |
| `/tokens/<id>/proofs/<key>`  | `proof.state.utxo_proof`, `proof.mpf_proof`                                |
| `/tokens/<id>/requests`      | `requests.requests[i].utxo_proof`                                          |

### Replay flow per verifier

* `verifyTokenResponse`
  * structural: snapshot, `state.utxo` witness, `state.state.root`
    32-byte hash;
  * replay `state.utxo` against `snapshot.utxo_root`.
* `verifyFactResponse`
  * structural: snapshot, `fact.state.utxo`, `fact.state.state.root`,
    `value`, `fact.mpf_proof`;
  * replay `fact.state.utxo` against `snapshot.utxo_root`;
  * replay `TrieFact { key = (server-provided), value = Just value,
    mpfProof = fact.mpf_proof }` against `fact.state.state.root`.
  * Note: the verifier shape needs the queried key on the side of
    the request, not on the response wire. Servers in this codebase
    return `value` and `fact` only; the *client* knows the key it
    asked for. The verifier therefore takes the queried key as an
    extra argument:

    ```haskell
    verifyFactResponse  :: Hex -> FactResponse  -> Either VerifyError ()
    verifyProofResponse :: Hex -> Maybe Hex -> ProofResponse -> Either VerifyError ()
    ```

    The extra `Hex` is dotted-path-rooted as `fact.key` /
    `proof.key` / `proof.value` for diagnostics.
* `verifyProofResponse`
  * structural: snapshot, `fact.state.utxo`, `fact.state.state.root`,
    `fact.mpf_proof`, plus the queried key (and, for inclusion claims,
    queried value) the caller passes;
  * replay `fact.state.utxo` against `snapshot.utxo_root`;
  * replay `TrieFact { key, value, mpfProof }` against
    `fact.state.state.root`.
* `verifyRequestsResponse`
  * structural: snapshot, every `requests[i].utxo`;
  * replay every `requests[i].utxo` against `snapshot.utxo_root`.
  * The decoded `Request` payload is opaque to the verifier — it is
    surfaced for downstream consumption only.

## Phase 2: Implementation

1. Add `Cardano.MPFS.Client.Read` module with DTOs + `Aeson` instances.
2. Extend `Cardano.MPFS.Client.Verify` with the four read verifiers,
   reusing the existing structural and replay helpers.
3. Update `Cardano.MPFS.Client` re-exports.
4. Add `Cardano.MPFS.Client.Fixtures` honest read fixtures
   (`honestTokenResponse`, `honestFactResponse`, `honestProofResponse`,
   `honestRequestsResponse`) plus the queried key constants needed for
   `verifyFactResponse` / `verifyProofResponse`.
5. Add `Cardano.MPFS.Client.ReadSpec` covering positive and forgery
   paths for every verifier; register it in the test suite.
6. Run the full local quality gate: `just unit`, `just format-check`,
   `just hlint`, `cabal check`.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)*  | -          | -                                   |
