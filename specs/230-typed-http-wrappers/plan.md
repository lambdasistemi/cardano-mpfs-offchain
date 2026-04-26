# Implementation Plan: Typed HTTP wrappers for MOOG

**Branch**: `feat/client-typed-http-wrappers-cardanompfsclienthttp` | **Date**: 2026-04-26 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/230-typed-http-wrappers/spec.md`
**Issue**: [#230](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/230)

## Status

**Completed**: The WASM/JS portability spike has been moved out of the
MOOG-ready milestone into the separate
`WASM/WASI MPFS API Client` milestone. Issue #230 now targets the native
Haskell CLI path MOOG needs.

**Current**: Design the typed HTTP wrapper surface and create the initial
speckit artifacts before implementation.

**Blockers**: None known. The main design risk is accidentally importing
the server package or ledger-heavy request types into
`cardano-mpfs-client`.

## Summary

Add `Cardano.MPFS.Client.Http`, a native Haskell transport layer for
MOOG. It owns typed request bodies for write endpoints, posts JSON to an
MPFS base URL through a caller-supplied HTTP manager, decodes existing
proof-bearing response envelopes, and optionally runs the existing pure
offline verifier before returning success.

## Technical Context

**Language/Version**: Haskell, repo-pinned GHC through the existing Nix
development shell.
**Primary Dependencies**: Existing `aeson`, `bytestring`, `text`, and
verifier dependencies; add native HTTP transport dependencies only to
`cardano-mpfs-client`.
**Storage**: N/A.
**Testing**: `cardano-mpfs-client:unit-tests` with mocked local HTTP
responses.
**Target Platform**: Native Haskell CLI for MOOG. Browser/WASM/WASI
transport is milestone #3.
**Project Type**: Haskell client library in a multi-package flake.
**Performance Goals**: One JSON POST/response decode per call. No client
retry loop in this module.
**Constraints**: Do not import `cardano-mpfs-offchain` or
`cardano-ledger-*` into the client package. Keep verifiers pure. Caller
owns manager, TLS, retry, and timeout policy.
**Scale/Scope**: One new client module, cabal dependency/module export
updates, top-level re-exports, and focused unit tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | PASS | Server remains ledger-native. Client request DTOs are wire-format types, not domain replacements. |
| II. Records of Functions | PASS | No service typeclasses added. HTTP config is an explicit record. |
| III. Atomic Block Processing | N/A | No persistence/indexer path touched. |
| IV. External Signing | PASS | Wrapper returns unsigned tx responses; signing remains client-side. |
| V. Aiken Compatibility | PASS | No datum/proof encoding changes. |
| VI. Test Locally First | PASS | Wrapper tests are local unit tests with mocked HTTP responses. |
| VII. Nix Reproducibility | PASS | New dependencies must come from the existing flake package set. |
| VIII. Pure Offline Verification | PASS | Network code stays outside verifier modules. |
| IX. One Verifier, Many Targets | GUARDED | The verifier remains one pure implementation. This issue adds native MOOG transport; browser/WASI transport and artifact gates are milestone #3. |
| X. Lean as Source of Truth | PASS | No new verifier invariant is introduced. |

No justified violations. The guarded Principle IX note records the
milestone split: transport is not the verifier, and cross-runtime
packaging is tracked separately.

## Project Structure

### Documentation

```text
specs/230-typed-http-wrappers/
├── spec.md
├── research.md
├── plan.md
├── quickstart.md
└── tasks.md
```

### Source Code

```text
cardano-mpfs-client/
├── cardano-mpfs-client.cabal
├── lib/Cardano/MPFS/Client.hs
├── lib/Cardano/MPFS/Client/Http.hs
└── test/Cardano/MPFS/Client/
    ├── HttpSpec.hs
    └── Main.hs
```

**Structure Decision**: define client-owned request DTOs in
`Cardano.MPFS.Client.Http` for this issue. Move them to a sibling module
only if the module grows too large during implementation.

## Phase 0: Research

Record the server write endpoint paths and request JSON shapes from
`Cardano.MPFS.HTTP.API` and `Cardano.MPFS.HTTP.Types`. Confirm that the
client can mirror the JSON contract without importing the server
package.

## Phase 1: Design

HTTP surface:

```haskell
data VerifierMode = RunVerifier | SkipVerifier

data MpfsHttp = MpfsHttp
    { manager :: Manager
    , baseUrl :: BaseUrl
    , verifier :: VerifierMode
    }
```

One function per write endpoint returns `IO (Either ClientError a)`.
Each function delegates to a shared `postJson` helper and an
endpoint-specific verifier hook.

## Phase 2: Implementation

1. Add dependencies and exposed module.
2. Add request parameter DTOs and JSON instances.
3. Add base URL/path joining and shared HTTP POST helper.
4. Add endpoint functions and verifier-mode handling.
5. Add focused unit tests with mocked HTTP responses.
6. Re-export the HTTP surface from `Cardano.MPFS.Client`.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)* | - | - |
