# Feature Specification: Cross-Target Client Verifier Builds

**Feature Branch**: `spike/spike-prove-cardano-mpfs-client-cross-compiles-to-`  
**Created**: 2026-04-25  
**Status**: Draft  
**Input**: Issue [#221](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/221) - "spike: prove cardano-mpfs-client cross-compiles to GHC-WASM and GHC-JS"

## User Scenarios & Testing

### User Story 1 - Prove the verifier is portable (Priority: P1)

A wallet integrator needs to know whether the same
`cardano-mpfs-client` verifier can run outside native GHC. The project
must produce repeatable GHC-WASM and GHC-JS builds, or document the exact
dependency blockers that prevent that result.

**Why this priority**: Constitution Principle IX requires one verifier
across native, WASM, and JS targets. If this fails, the later MOOG-ready
HTTP/read/anchor tickets cannot honestly claim wallet portability.

**Independent Test**: Run the documented Nix build commands for the
client WASM and JS targets from a clean checkout.

**Acceptance Scenarios**:

1. **Given** a clean checkout, **When** the client WASM build command
   runs, **Then** it either produces a client verifier artifact or fails
   with a documented dependency blocker.
2. **Given** a clean checkout, **When** the client JS build command runs,
   **Then** it either produces a client verifier artifact or fails with a
   documented dependency blocker.
3. **Given** a documented blocker, **When** a maintainer reads the
   research notes, **Then** they can see which dependency, toolchain, or
   package stanza must change next.

### User Story 2 - Gate future changes in CI (Priority: P1)

A reviewer needs CI to prevent native-only dependencies from slipping
back into `cardano-mpfs-client`. The repository must expose stable Nix
targets for the cross builds and wire those targets into the PR gate once
they are buildable.

**Why this priority**: A one-time local spike does not enforce the
constitution. CI is the mechanism that turns the portability proof into a
maintenance guard.

**Independent Test**: Open a PR and verify the GitHub CI run builds the
cross-target client outputs, or marks the spike as blocked with a
failing/explicit non-mergeable task list.

**Acceptance Scenarios**:

1. **Given** a PR that changes `cardano-mpfs-client`, **When** CI runs,
   **Then** the cross-target client build checks execute.
2. **Given** a native-only dependency is added to the client package,
   **When** CI reaches the cross-target build checks, **Then** the PR
   fails before merge.

### User Story 3 - Prepare release packaging (Priority: P2)

A JavaScript or browser consumer needs a predictable package shape, even
before the first full wallet integration. The repository should include
the minimal npm package skeleton or document why packaging is blocked by
the build proof.

**Why this priority**: Buildability is the security gate; packaging is
the release ergonomics layer that can follow once the artifacts exist.

**Independent Test**: Inspect the package skeleton and confirm it points
at the generated WASM/JS artifacts without duplicating verifier logic.

**Acceptance Scenarios**:

1. **Given** a successful WASM or JS build, **When** the npm package is
   assembled, **Then** it references those artifacts as generated
   outputs.
2. **Given** the cross build is blocked, **When** a maintainer reads the
   package notes, **Then** the missing artifact boundary is explicit.

## Edge Cases

- A dependency may build natively through Cabal but fail in the
  haskell.nix cross package set because a public sublibrary is not
  exposed in the cross package database.
- `cardano-mpfs-client` may stay portable while the `mpfs-verify`
  executable does not; the library is the hard gate, the native CLI is
  secondary.
- WASM and JS may fail for different reasons; blockers must be recorded
  separately.
- CI must not depend on local build artifacts or a developer's package
  database.
- Cross-target parity tests should use the existing honest/forged vector
  corpus where possible, not hand-written examples that bypass the real
  verifier surface.

## Requirements

### Functional Requirements

- **FR-001**: The repository MUST expose a repeatable Nix target for the
  GHC-WASM build of the `cardano-mpfs-client` library.
- **FR-002**: The repository MUST expose a repeatable Nix target for the
  GHC-JS build of the `cardano-mpfs-client` library.
- **FR-003**: The build targets MUST keep `cardano-mpfs-client` free of
  `cardano-ledger-*`, `crypton`, native C FFI, `unix`, `process`, and
  other verifier-host dependencies.
- **FR-004**: Any cross-target blocker MUST be documented with the
  failing command, observed error, root cause, and next action.
- **FR-005**: CI MUST include the successful cross-target build outputs
  before this issue is closed as complete.
- **FR-006**: If parity execution is feasible in this slice, CI MUST run
  a small cross-target verifier corpus and compare verdicts against the
  native verifier.
- **FR-007**: If parity execution is not feasible in this slice, the
  plan MUST document the smallest remaining step needed to run the
  corpus after artifacts build.
- **FR-008**: Release packaging MUST avoid a second verifier
  implementation; generated WASM/JS artifacts are the only verifier code
  shipped to npm.

## Success Criteria

- **SC-001**: Maintainers can run one documented command for each of
  native, WASM, and JS client verifier builds.
- **SC-002**: CI blocks PRs that break any cross-target client verifier
  build that was proven by this spike.
- **SC-003**: The research notes identify every dependency/toolchain
  blocker left after the spike, with no ambiguous "does not work" entry.
- **SC-004**: `cardano-mpfs-client:unit-tests` continues to pass
  natively after cross-target Nix changes.

## Assumptions

- The deliverable can be a spike PR that either lands working
  cross-target outputs or lands precise blockers with the repository
  changes needed to continue.
- The library build is the first portability proof; the CLI executable
  and npm packaging can remain secondary if they would obscure the core
  verifier result.
- The existing verifier fixtures are enough for the first parity corpus;
  the larger conformance-vector suite belongs to issue #233.
