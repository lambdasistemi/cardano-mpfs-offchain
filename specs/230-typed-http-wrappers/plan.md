# Implementation Plan: Typed HTTP wrappers for MOOG

**Branch**: `feat/client-typed-http-wrappers-cardanompfsclienthttp` | **Date**: 2026-04-26 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/230-typed-http-wrappers/spec.md`
**Issue**: [#230](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/230)

## Status

**Completed**: The WASM/JS portability spike has been moved out of the
MOOG-ready milestone into the separate
`WASM/WASI MPFS API Client` milestone. Issue #230 now targets the native
Haskell CLI path MOOG needs. The implementation extracts the lightweight
`cardano-mpfs-api` package, derives write-endpoint clients from
`TxWriteAPI` with `servant-client`, keeps server-only ledger conversion
helpers in `cardano-mpfs-offchain`, and covers the client wrapper with
mocked HTTP unit tests.

**Current**: Local validation and PR documentation update.

**Blockers**: None known.

## Summary

Add a shared Servant wire-contract package and
`Cardano.MPFS.Client.Http`, a native Haskell transport layer for MOOG.
The client derives endpoint paths and wire request/response types from
the shared `TxWriteAPI`, runs through a caller-supplied HTTP manager,
bridges decoded wire responses into the existing client verifier DTOs,
and optionally runs the pure offline verifier before returning success.

## Technical Context

**Language/Version**: Haskell, repo-pinned GHC through the existing Nix
development shell.
**Primary Dependencies**: Existing `aeson`, `bytestring`, `text`, and
verifier dependencies; add `cardano-mpfs-api`, `servant-client`, and
native HTTP transport dependencies to `cardano-mpfs-client`.
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
**Scale/Scope**: One shared API package, server compatibility shims,
one client HTTP module, cabal dependency/module export updates,
top-level re-exports, and focused unit tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Ledger-Native Types | PASS | Server remains ledger-native. Shared DTOs are wire-format types; conversion helpers stay in the server package. |
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
cardano-mpfs-api/
├── cardano-mpfs-api.cabal
└── lib/Cardano/MPFS/
    ├── API.hs
    └── API/
        ├── Encoding.hs
        └── Types.hs
cardano-mpfs-offchain/
└── lib/Cardano/MPFS/HTTP/
    ├── API.hs
    ├── Encoding.hs
    └── Types.hs
cardano-mpfs-client/
├── cardano-mpfs-client.cabal
├── lib/Cardano/MPFS/Client.hs
├── lib/Cardano/MPFS/Client/Http.hs
└── test/Cardano/MPFS/Client/
    ├── HttpSpec.hs
    └── Main.hs
```

**Structure Decision**: define client-owned request DTOs in
`cardano-mpfs-api` owns the shared Servant API and wire DTOs.
`Cardano.MPFS.HTTP.*` remains the server import path through
compatibility shims. `Cardano.MPFS.Client.Http` owns the MOOG-facing
request parameter records and derives its transport from `TxWriteAPI`.

## Phase 0: Research

Record the server write endpoint paths and request JSON shapes from
`Cardano.MPFS.HTTP.API` and `Cardano.MPFS.HTTP.Types`. Extract the
stable wire contract into `cardano-mpfs-api` so both server and client
can depend on it without introducing a client dependency on
`cardano-mpfs-offchain` or ledger packages.

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
Each function delegates to the generated Servant client for the matching
wire endpoint and then to an endpoint-specific verifier hook.

## Phase 2: Implementation

1. Add the `cardano-mpfs-api` package with shared API, encoding, and
   wire types.
2. Add offchain compatibility shims and keep ledger conversion helpers
   in `Cardano.MPFS.HTTP.Types`.
3. Add client request parameter DTOs and JSON instances.
4. Derive write endpoint functions from `TxWriteAPI` using
   `servant-client`.
5. Add verifier-mode handling and focused mocked HTTP tests.
6. Re-export the HTTP surface from `Cardano.MPFS.Client`.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *(none)* | - | - |
