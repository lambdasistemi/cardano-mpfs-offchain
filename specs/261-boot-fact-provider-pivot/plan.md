# Implementation Plan: Boot fact-provider pivot

**Branch**: `261-boot-fact-provider-pivot` | **Date**: 2026-05-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/261-boot-fact-provider-pivot/spec.md`

## Summary

Ship the boot slice of the fact-provider pivot. The offchain server adds
`POST /facts/boot { address }`, returning only a coherent snapshot,
proof-bearing wallet UTxOs, and explicitly unverified protocol
parameters. The client library verifies those facts against an
independently trusted UTxO root, then builds the unsigned boot
transaction locally with `bootCageTx`. The legacy server-built boot
transaction endpoint is removed in this PR. Non-boot write endpoints
remain unchanged until their own child issues.

The work is split into bisect-safe vertical slices:

1. Boot facts wire type and proof-only verifier.
2. Client-side boot cage helper, wallet policy, build errors, and
   legacy byte-equivalence vector.
3. Server hard swap from legacy boot tx route to `POST /facts/boot`.
4. E2E/docs/swagger/paired-MOOG release-window alignment.

The orchestrator owns this plan, tasks, contracts, quickstart, gate
script, PR metadata, and review. Behavior-changing implementation
slices are handed off only after the plan and tasks are accepted and
after subagent execution is explicitly authorized.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1 for `cardano-mpfs-api`,
`cardano-mpfs-client`, and `cardano-mpfs-offchain`.

**Primary Dependencies**: Servant and Aeson for the HTTP wire contract;
`cardano-ledger-*` and `cardano-node-clients` TxBuild for local
transaction construction; `cardano-utxo-csmt` and `haskell-mts` for
CSMT proof generation and replay; RocksDB through the existing
`IndexerTx` column-family transaction layer.

**Storage**: RocksDB, unchanged. Boot uses the existing snapshot and
wallet UTxO read primitives and introduces no new column family.

**Testing**: `nix develop --quiet -c just ci` remains the base PR gate.
Boot-specific proof adds focused client verifier tests, boot helper
byte-equivalence/golden tests, HTTP handler/API tests, and an e2e boot
facts flow that verifies, locally builds, signs, submits, and observes
indexing.

**Target Platform**: Linux server and native Haskell client library.
The verifier remains pure and shaped for future GHC-WASM/GHC-JS work
tracked by issue #258; cross-target CI is not newly required in this
slice because the repository constitution already carries that waiver.

**Project Type**: Multi-package Haskell repository plus a paired
downstream MOOG CLI PR. This PR owns the offchain server, shared API
package, client verifier/helper package, docs, and verification assets.

**Performance Goals**: Facts assembly remains O(K) in wallet UTxOs at
the requested address. Local boot build is pure CPU work and should stay
within the existing boot tx test budget.

**Constraints**:

- Server returns facts only for boot. It does not return an unsigned boot
  transaction from the new endpoint.
- The legacy boot transaction endpoint is gone at HEAD of this PR.
- Non-boot write endpoints remain live and unchanged.
- Every facts response is assembled inside one `runIndexerTx ctx`
  transaction.
- `verifyBootFacts` is pure and does not import transaction grammar
  modules.
- `VerifiedBootFacts` cannot be constructed by downstream clients except
  through `verifyBootFacts`.
- `bootCageTx` enforces `WalletPolicy` before signing can occur.
- `docs/assets/swagger.json` reflects the boot hard swap.
- The PR stays draft until the paired MOOG boot migration is ready for
  the same release window.

**Scale/Scope**:

- `cardano-mpfs-api`: add boot facts DTOs and replace only the boot
  write route in the shared Servant API.
- `cardano-mpfs-client`: add facts types, verifier output newtype,
  wallet policy/build error modules, `Cardano.MPFS.Client.Cage.Boot`,
  and focused tests.
- `cardano-mpfs-offchain`: add facts boot handler, remove legacy boot
  handler route, remove boot from server-side real tx builder exports,
  regenerate Swagger, and add e2e proof.
- `lambdasistemi/moog`: paired PR migrates boot call sites from legacy
  tx response to facts verification plus local build/sign/submit.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Ledger-Native Types

PASS. Local construction returns `Tx ConwayEra`; facts carry CBOR bytes
that decode to ledger-native `TxOut ConwayEra` values at the builder
boundary. No shadow ledger representation is introduced.

### II. Records of Functions

PASS. Existing server boundaries remain records of functions. New client
helpers are pure functions, not service interfaces or typeclasses.

### III. Atomic Block Processing

PASS. Boot facts are read through one `runIndexerTx ctx` block using
`readSnapshot` and `readWalletInputsAt`; no writer path changes.

### IV. Client-Side Transaction Construction

PASS. This slice directly implements the amended principle for boot:
server returns facts, client verifies and builds locally, server holds no
keys and returns no unsigned boot transaction.

### V. Aiken Compatibility

PASS with required proof. `bootCageTx` must produce byte-identical boot
transaction CBOR to the captured legacy boot vector for equivalent
inputs.

### VI. Test Locally First

PASS. `./gate.sh` currently runs `nix develop --quiet -c just ci`. It
will be extended after boot-specific tests exist so the gate fails loudly
at the new API and e2e boundaries.

### VII. Nix Reproducibility

PASS. All verification commands run through `nix develop` or the
repository's Nix apps.

### VIII. Pure Offline Verification

PASS with required proof. `verifyBootFacts` is a pure function from
trusted root and boot facts to `Either VerifyError VerifiedBootFacts`.
It does not use IO, networking, disk, time, or transaction inspection.

### IX. One Verifier, Many Targets

PASS under the existing #258 waiver. This slice must not add verifier
dependencies that make future GHC-WASM/GHC-JS compilation harder.

### X. Lean as Source of Truth

PASS by reuse of the existing CSMT replay invariant from the proof
redesign. No new state-machine invariant is introduced for boot facts;
the slice narrows the boot verifier to snapshot/root equality plus CSMT
inclusion replay. If implementation needs a new proof invariant, update
Lean before accepting that implementation slice.

## Project Structure

### Documentation (this feature)

```text
specs/261-boot-fact-provider-pivot/
|-- spec.md
|-- plan.md
|-- research.md
|-- data-model.md
|-- quickstart.md
|-- contracts/
|   |-- boot-client.md
|   `-- facts-boot-api.md
|-- checklists/
|   `-- requirements.md
`-- tasks.md
```

### Source Code (repository root)

```text
cardano-mpfs-api/
|-- lib/Cardano/MPFS/API.hs
`-- lib/Cardano/MPFS/API/Types.hs

cardano-mpfs-client/
|-- lib/Cardano/MPFS/Client/Facts.hs
|-- lib/Cardano/MPFS/Client/Verify.hs
|-- lib/Cardano/MPFS/Client/Cage/Boot.hs
|-- lib/Cardano/MPFS/Client/Cage/BuildError.hs
|-- lib/Cardano/MPFS/Client/Cage/Policy.hs
`-- test/Cardano/MPFS/Client/...

cardano-mpfs-offchain/
|-- lib/Cardano/MPFS/HTTP/API.hs
|-- lib/Cardano/MPFS/HTTP/Server.hs
|-- lib/Cardano/MPFS/HTTP/Types.hs
|-- lib/Cardano/MPFS/TxBuilder/Real.hs
|-- lib/Cardano/MPFS/TxBuilder/Real/Boot.hs
|-- lib/Cardano/MPFS/TxBuilder/Real/Boot/Inputs.hs
|-- lib/Cardano/MPFS/TxBuilder/Real/Boot/Transaction.hs
|-- test/Cardano/MPFS/...
`-- e2e-test/Cardano/MPFS/E2E/...

docs/assets/swagger.json
specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor
gate.sh
```

**Structure Decision**: Keep wire DTOs in `cardano-mpfs-api`, pure
verification and local construction in `cardano-mpfs-client`, and
server facts assembly in `cardano-mpfs-offchain`. Move only boot
construction out of the server-side `Real.Boot` tree in this slice;
non-boot `Real.*` modules remain until their child issues.

## Phase 0: Research Decisions

See [research.md](./research.md).

## Phase 1: Design And Contracts

See [data-model.md](./data-model.md),
[facts-boot-api.md](./contracts/facts-boot-api.md),
[boot-client.md](./contracts/boot-client.md), and
[quickstart.md](./quickstart.md).

## Plan Review Additions

### Orchestrator / Implementation Ownership

- Orchestrator owns specs, plan, tasks, contracts, quickstart,
  `gate.sh`, PR body, review, task stamping, and finalization.
- Implementation workers own one behavior-changing slice at a time and
  must not edit `specs/`, `gate.sh`, PR metadata, or the paired MOOG
  plan unless explicitly assigned.

### Vertical Slices And Proof Strategy

1. **Facts and verifier**: RED tests for `verifyBootFacts` happy path,
   snapshot mismatch, trusted-root mismatch, and proof tamper; GREEN
   implementation in facts/verifier modules.
2. **Client builder and vector**: RED golden/byte-equivalence test
   against captured legacy boot CBOR; GREEN `bootCageTx`, `WalletPolicy`,
   and `BuildError` implementation.
3. **Server hard swap**: RED API/handler tests proving `/facts/boot`
   exists and legacy boot tx route is absent; GREEN server route,
   handler, Swagger, and deletion of server-side boot tx route.
4. **E2E/docs/release window**: RED e2e boot facts flow against the live
   HTTP boundary; GREEN flow plus docs/PR metadata for paired MOOG.

Each implementation slice must be one bisect-safe commit with tests and
code together.

### Live-Boundary Diagnostic

The system boundary unit tests cannot exercise is the live HTTP/server
to indexer to client-verification to local-build boundary. The final
offchain slice must include an e2e proof that calls the HTTP
`POST /facts/boot` route, verifies the response client-side, builds the
transaction locally, signs/submits it, and observes indexing. The paired
MOOG PR must run the same operator flow from the CLI.

### Gate Representation

The current gate proves the baseline (`just ci`). Once the focused boot
tests exist, `gate.sh` must be extended with the accepted focused test
commands and the boot e2e proof. Until those tests exist, adding their
commands would make the gate red for the wrong reason.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | N/A |

## Post-Design Constitution Recheck

PASS. The design keeps server facts, client verification, and client
construction in separate packages, preserves atomic indexer reads, and
defines concrete RED/GREEN proof for every behavior-changing slice.
