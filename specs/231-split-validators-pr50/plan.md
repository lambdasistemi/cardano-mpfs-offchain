# Implementation Plan: Adopt split state + request validators (upstream PR #50)

**Branch**: `231-split-validators-pr50` | **Date**: 2026-04-28 |
**Spec**: [`specs/231-split-validators-pr50/spec.md`](spec.md)
**Input**: [`specs/231-split-validators-pr50/spec.md`](spec.md)

**Note**: This plan is filled in by the `/speckit.plan` command. See
`.specify/templates/plan-template.md` for the execution workflow.

## Summary

Mirror the upstream `cardano-mpfs-onchain` PR #50 redesign — which
splits the previous single-validator cage into a global state validator
and a per-cage request validator — into the offchain repo. Pins to the
upstream tip
[`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f)
are already on disk in `cabal.project`, `flake.nix`, `flake.lock`, and
`Blueprint.hs`'s re-exports. The remaining work is offchain-only: adapt
`TxBuilder/Real/*`, the indexer's address-following topology, and the
HTTP "list requests for token" lookup so each affected user flow
(Requester, Oracle, Owner-Sweep, Indexer/HTTP) builds the new
two-validator transaction shape and queries the new per-cage request
addresses, byte-for-byte against the upstream cage test vectors at the
pinned commit (Constitution Principle V — Aiken Compatibility).

## Technical Context

**Language/Version**: Haskell, GHC 9.10.1 (per repo `CLAUDE.md`).
**Primary Dependencies**:

- `cardano-mpfs-onchain` pinned at `cf3a8bdc` (PR #50 tip), exposing
  `applyDataParam`, `applyBytesParam`, `applyOutputRef`,
  `applyRequestParams` and the `mkRequestScript` /
  `requestAddrFromCfg` / `onChainTokenId` shapes used by the upstream
  cage tests.
- `cardano-ledger-*` for native domain types (Constitution Principle I).
- `merkle-patricia-forestry` (this repo) for trie hashing — unchanged
  by this feature.

**Storage**: RocksDB column families behind the existing indexer (state
+ per-cage request UTxOs); no schema change for this feature, only an
expansion of the set of subscribed addresses.
**Testing**: `just unit`, `just unit-offchain`, `just e2e`. The E2E
suite spins up a `cardano-node` subprocess devnet and exercises the
HTTP server end-to-end (`CageSpec`, `CageFlowSpec`, `ChainSyncSpec`,
`HTTPLifecycleSpec`, `IndexerSpec`, `ProofsSpec`). Devnet is the
arbiter for transaction validity in this feature (Constitution
Principle VI — Test Locally First).
**Target Platform**: Linux server, GHC native.

> Note on Principles VIII / IX / X: this feature does not touch the
> `cardano-mpfs-client` verifier, so the pure-offline-verification,
> WASM/JS cross-target, and Lean-as-source-of-truth principles are
> not directly engaged. They remain in force for any code path the
> client crosses; the indexer/HTTP changes here stay server-side.

**Project Type**: Multi-package Haskell project (`cardano-mpfs-api`,
`cardano-mpfs-offchain`, `cardano-mpfs-client`,
`merkle-patricia-forestry`) under one `cabal.project`.
**Performance Goals**: No new performance budget. Indexer must keep up
with devnet block times under N+1 address subscription (no measurable
regression vs the pre-split topology on the existing E2E suite).
**Constraints**:

- Conway era only, PlutusV3 scripts, ledger-native types
  (Constitution Principles I + Cardano Constraints).
- Server stays signing-free (Constitution Principle IV); only
  unsigned tx CBOR is built and returned.
- All redeemer payloads and address derivations match the upstream
  cage test vectors at `cf3a8bdc` byte-for-byte (Constitution
  Principle V).
- The project's `just ci` recipe covers build → unit →
  unit-offchain → format-check → hlint, but **does not** include
  E2E (`just e2e` is a separate recipe in `justfile` that boots a
  cardano-node subprocess). Per Constitution Principle V — which
  makes byte-for-byte tx parity load-bearing for this feature —
  E2E coverage is not optional. The `GATE` for this stack is
  therefore `just ci && just e2e`, captured as one shell command
  in `notes/gate.md` and run on every patch in the series
  (Workflow + Setup step 1 of the `pr` skill).

**Scale/Scope**: ~12 library modules touched in
`cardano-mpfs-offchain/lib`, plus ~6 E2E spec files and the call sites
in `exe/`. Estimated ~10–14 vertical commits — see Phase 2 task plan.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1
design.*

| Principle | Status | Notes |
|---|---|---|
| I. Ledger-Native Types | PASS | New helpers (`requestAddrFromCfg`, `onChainTokenId`) operate over existing ledger types; no shadow representations introduced. |
| II. Records of Functions | PASS | TxBuilder, Indexer, and HTTP boundaries remain records-of-functions; this feature only adds entries / changes implementations. |
| III. Atomic Block Processing | PASS | Indexer follows N+1 addresses; per-block writes still atomic across CFs. The set of CFs is unchanged. |
| IV. External Signing | PASS | All TxBuilder additions return unsigned CBOR; Sweep follows the same shape. |
| **V. Aiken Compatibility** | **PASS — load-bearing for this feature** | The upstream cage test vectors at [`cf3a8bdc`](https://github.com/cardano-foundation/cardano-mpfs-onchain/commit/cf3a8bdcd1414aa62d490c8fa51c2ef87336179f) — concretely, `mkRequestScript`, `requestAddrFromCfg`, `onChainTokenId`, the `Modify` / `Contribute(stateRef)` / `Sweep(stateRef)` redeemers, and the `Burning(onChainTokenId tid)` end-shape — are the **byte-for-byte arbiter** for every redeemer payload, address derivation, and attached-script choice introduced here. Any divergence is a critical bug per the constitution. |
| VI. Test Locally First | PASS | Acceptance is by `just e2e` on the devnet; no CI-only paths. |
| VII. Nix Reproducibility | PASS | Pin already in `flake.nix` + `flake.lock`; no system-level deps added. |
| VIII. Pure Offline Verification | N/A | Feature does not touch `cardano-mpfs-client`. |
| IX. One Verifier, Many Targets | N/A | Same. |
| X. Lean as Source of Truth | N/A | No new verifier state machine introduced. |

**Constitution-Check verdict**: PASS. No principles require
justification under Complexity Tracking.

## Project Structure

### Documentation (this feature)

```text
specs/231-split-validators-pr50/
├── spec.md                  # /speckit.specify output (already on disk)
├── plan.md                  # this file (/speckit.plan output)
├── checklists/
│   └── requirements.md      # spec quality checklist (already on disk)
├── research.md              # Phase 0 output (this command)
├── data-model.md            # Phase 1 output (this command)
├── quickstart.md            # Phase 1 output (this command)
├── contracts/               # Phase 1 output (this command)
│   ├── tx-shapes.md         # one entry per affected tx flow
│   └── http-endpoints.md    # request-listing endpoint shape
└── tasks.md                 # /speckit.tasks output (NOT created here)
```

### Source Code (repository root)

```text
cardano-mpfs-offchain/
├── lib/Cardano/MPFS/
│   ├── Core/
│   │   ├── Blueprint.hs        # already updated — re-exports applyData/Bytes/OutputRef/RequestParams
│   │   └── OnChain.hs          # drop Mint(..); update hardcoded cageScriptHash to global state validator hash
│   ├── TxBuilder/
│   │   ├── Config.hs           # add `requestScriptBytes :: ShortByteString`
│   │   └── Real/
│   │       ├── Internal.hs     # add mkRequestScript, requestAddrFromCfg, onChainTokenId, requestScriptBytesFromCfg
│   │       ├── Boot.hs         # redeemer becomes `Minting onChainRef` (drop Mint wrapper)
│   │       ├── End.hs          # redeemer becomes `Burning (onChainTokenId tid)`
│   │       ├── Request.hs      # pay request UTxO to per-cage request address
│   │       ├── Update.hs       # split queryContext; attach state + per-cage request scripts
│   │       ├── Reject.hs       # same shape as Update
│   │       ├── Retract.hs      # request UTxO at request address; state UTxO referenced; attach request script
│   │       ├── Sweep.hs        # **NEW** owner-only spend at per-cage request address with Sweep(stateRef)
│   │       └── Real.hs         # re-export Sweep entry point
│   ├── Indexer/                # Backend / CageFollower / Follower / ComposedInv: split-validator topology (N+1 addresses)
│   └── HTTP/                   # Types / API / Server / Encoding: per-token request lookup → per-cage request address
├── cardano-mpfs-offchain.cabal # add Sweep module to exposed-modules
├── exe/                        # devnet, server, bootstrap-genesis: extend CageConfig with requestScriptBytes
├── e2e-test/Cardano/MPFS/E2E/  # Cage / CageFlow / ChainSync / HTTPLifecycle / Indexer / Proofs Spec — adapt to new shape
└── test/Cardano/MPFS/          # OnChainSpec / TxBuilderSpec — drop Mint test, add Sweep round-trip, update hash literal
```

**Structure Decision**: Existing multi-package `cabal.project` layout
is unchanged. All offchain edits live under
`cardano-mpfs-offchain/`; one new module
(`Cardano.MPFS.TxBuilder.Real.Sweep`) is added to the library's
`exposed-modules`. Pins (`cabal.project`, `flake.nix`, `flake.lock`)
and `Core/Blueprint.hs` are already updated on disk and ride along
with the first commit of the implementation stack.

## Phase 0 — Research

Three open questions resolved up front so Phase 1 contracts and Phase 2
task plan have firm ground:

### R-001 — Per-cage request address derivation (canonical recipe)

- **Decision**: The offchain `requestAddrFromCfg cfg tokenName network`
  helper applies the unapplied request validator UPLC
  (`requestScriptBytes` from `CageConfig`) to two parameters in the
  upstream order — first the global `statePolicyId`, then the
  `cageTokenName` — using `applyBytesParam` from the upstream
  blueprint, hashes the resulting script to a script hash, and turns
  it into an address with the configured `network`. The
  `onChainTokenId` helper produces the value upstream uses for burn
  redeemers from a token name.
- **Rationale**: Identical to the `mkRequestScript` /
  `requestAddrFromCfg` shape in the upstream PR #50 cage test fixtures
  at `cf3a8bdc`. Constitution Principle V mandates byte-for-byte
  parity; reusing the same parameterisation order and the same
  blueprint helpers is the only way to get there.
- **Alternatives considered**: (a) deriving the address from the
  pre-applied bytes shipped in the existing config — rejected because
  pre-application freezes a single cage and breaks the per-cage
  parameterisation; (b) hashing the unapplied bytes directly —
  rejected because the on-chain script address is the **applied**
  script hash.

### R-002 — Indexer subscription topology (N+1 model)

- **Decision**: The `Cardano.MPFS.Indexer.Backend` follower set is
  parameterised by:
  - exactly one global state address (one for the deployment,
    corresponding to upstream's unparameterised
    `validator state { ... }`; the cage seed travels in the boot
    mint's `Minting(seed)` redeemer, not in a validator parameter),
    plus
  - one per-cage request address per known cage token, derived as in
    R-001 from `(statePolicyId, tokenName)`.

  When the global state policy emits a boot mint, the indexer derives
  the new cage's per-cage request address and adds it to the follower
  set in the same atomic block batch in which the boot is recorded
  (Constitution Principle III). No restart, no operator action.

- **Rationale**: Matches FR-007 / FR-008 and the spec's User Story 4.
  Atomic addition keeps "one block = one batch" intact: either the
  boot and the new follower entry both land or neither does. There is
  no half-state where the boot is committed but the address is not
  yet followed.

- **Alternatives considered**: (a) one global request address with a
  filter — impossible by design (different parameterisations produce
  different addresses); (b) periodic resync of the follower set —
  rejected as it admits a window in which a freshly booted cage is
  unreachable from HTTP, violating SC-003.

### R-003 — HTTP "list requests for token T" lookup

- **Decision**: `Cardano.MPFS.HTTP.Server` resolves the per-token
  endpoint by deriving the per-cage request address (R-001) and
  reading from the indexer's per-address index. The endpoint shape
  itself is unchanged for clients; only the server's resolution
  changes.

- **Rationale**: Preserves the public HTTP contract while moving the
  internal lookup to the new topology. Acceptance Story 4 requires
  the same set of pending requests to come back; only the server's
  internal address resolution shifts.

- **Alternatives considered**: An explicit "address" query parameter
  exposed to clients — rejected because it bleeds on-chain layout
  into the public API and forces every client to re-derive the
  address.

**Output**: `research.md` (written below).

## Phase 1 — Design & Contracts

### data-model.md (offchain types)

The on-chain entities are listed in the spec (Global state validator,
Per-cage request validator, Cage token, Pending request UTxO). Phase 1
captures the **offchain type-level changes** that mirror them:

| Type | Module | Change |
|---|---|---|
| `CageConfig` | `Cardano.MPFS.TxBuilder.Config` | Add `requestScriptBytes :: ShortByteString` (unapplied request UPLC). Do not add `cageSeed` — wallet picks the seed at runtime (spec Assumptions). |
| `CageScripts` (or equivalent in `OnChain.hs`) | `Cardano.MPFS.Core.OnChain` | Drop `Mint(..)` from exports + import. Update hardcoded `cageScriptHash` from PR #48's per-token hash to the current global state validator hash `ad0a8eeeec8b0a5ee9930be5d6ea2e80b285fc2f3e9675a13a392dd5`. |
| `RequestAddress` (concept; reuses ledger `Address`) | `Cardano.MPFS.TxBuilder.Real.Internal` | Introduce `requestAddrFromCfg :: CageConfig -> TokenName -> NetworkId -> Address` and `mkRequestScript :: CageConfig -> TokenName -> Script` and `onChainTokenId :: TokenName -> OnChainTokenId`. |
| Boot redeemer | `Cardano.MPFS.TxBuilder.Real.Boot` | `Minting onChainRef` (drop `Mint` wrapper). |
| End/Burn redeemer | `Cardano.MPFS.TxBuilder.Real.End` | `Burning (onChainTokenId tid)`. |
| Update / Reject query context | `Cardano.MPFS.TxBuilder.Real.Update` / `Reject` | Split into `stateUtxoFromCfg` (global state address) and `requestUtxosFromCfg cfg tid` (per-cage request address). Attach **two** scripts as witnesses. |
| Retract query context | `Cardano.MPFS.TxBuilder.Real.Retract` | Request UTxO at per-cage request address; state UTxO referenced (not consumed). Attach request script only. |
| Sweep entry point (NEW) | `Cardano.MPFS.TxBuilder.Real.Sweep` | Owner-only spend of one UTxO at per-cage request address with `Sweep(stateRef)`; state UTxO referenced. Attach request script only. |

### contracts/

Two contract documents — none of these are RPC contracts; they are the
**transaction shape and HTTP shape** acceptance contracts that the E2E
suite verifies against the devnet:

#### `contracts/tx-shapes.md`

Per-flow tx shape table (one row per spec acceptance scenario):

| Flow | Inputs | Outputs | Redeemers | Attached scripts |
|---|---|---|---|---|
| Boot | seed UTxO from wallet | state UTxO at global state address; cage token paid forward | mint policy: `Minting onChainRef` | mint policy + global state validator |
| Request{Insert,Delete,Update} | wallet UTxOs | request UTxO **at per-cage request address** | none on consumed inputs (wallet UTxOs); request datum on output | none (paying to script address only) |
| Retract | request UTxO at per-cage request address; state UTxO **referenced** | requester refund | request validator: retract redeemer | per-cage request validator |
| Update | state UTxO at global state address; request UTxOs at per-cage request address | new state UTxO at global state address; per-request payouts | state validator: `Modify`; per request: `Contribute(stateRef)` | global state validator + per-cage request validator |
| Reject | as Update | refunds to requesters | as Update | as Update |
| Sweep | one non-legitimate UTxO at per-cage request address; state UTxO **referenced** | sweep payout to owner | request validator: `Sweep(stateRef)` | per-cage request validator |
| End/Burn | state UTxO at global state address; cage token UTxO | none (cage retired) | mint policy: `Burning (onChainTokenId tid)` | mint policy + global state validator |

Each row's redeemer payload and address derivation is checked against
the upstream cage test vectors at `cf3a8bdc` per Constitution
Principle V.

#### `contracts/http-endpoints.md`

| Endpoint | Public shape | Internal resolution change |
|---|---|---|
| `GET /requests/{token}` (or whichever path the existing server uses for per-token request listings) | unchanged for clients — same JSON envelope, same set of pending requests | server now derives the per-cage request address from `(statePolicyId, token)` and queries the indexer's per-address index, instead of filtering a single global address |

No new endpoints. No removals. Sweep is a TxBuilder entry point only;
it does not need a public HTTP route in this feature unless the
existing server already has a generic "build tx for this redeemer"
shape.

### quickstart.md

A short walkthrough that an operator runs end-to-end on the devnet
post-implementation, tracking the four user stories:

1. `just build`.
2. `just e2e` — full E2E suite passes on devnet (SC-001).
3. Boot a cage; observe the global state policy id is unchanged
   across boots and the per-cage request address differs per token
   name.
4. Submit insert/delete/update requests; observe the request UTxOs
   land at the per-cage request address (Story 1, FR-002).
5. Drive Update; observe the resulting tx spends both validators
   (Story 2, FR-003).
6. Pay a non-legitimate UTxO to the per-cage address from a
   non-owner wallet; drive Sweep from the owner; observe the
   non-legitimate UTxO is consumed and legitimate requests are not
   (Story 3, FR-005, SC-004).
7. Boot a second cage **while the server is running**; submit a
   request against it; observe `GET /requests/{newToken}` returns
   the request without a server restart (Story 4, FR-008, SC-003).

### Agent context update

The repo's `CLAUDE.md` is current as of 2026-04-25 and already covers
the technologies in play (GHC 9.10.1, Nix flakes, Cabal, Fourmolu,
HLint). No new technology is introduced by this feature, so the
auto-update step is a no-op for the agent file. The "Recent Changes"
trailer in `CLAUDE.md` will be updated by the implementing agent at
the end of the implementation phase.

### Constitution re-check (post-design)

| Principle | Status (post-design) | Notes |
|---|---|---|
| I–IV, VI–VII | PASS | Unchanged from pre-design. |
| **V. Aiken Compatibility** | **PASS** | Phase 1 contracts above pin every redeemer / address / script-witness choice to the upstream cage test vectors at `cf3a8bdc`. The contracts/tx-shapes.md table is the explicit byte-for-byte arbiter required by the constitution. |
| VIII–X | N/A | Verifier surface untouched. |

No new violations. Complexity Tracking remains empty.

## Phase 2 — Task plan (preview only — `/speckit.tasks` produces tasks.md)

The implementation will be laid out as vertical, bisect-safe stgit
patches, one per concern, in roughly this order (final list belongs
in `tasks.md`):

1. `chore: pin upstream cardano-mpfs-onchain at cf3a8bdc` (already
   on disk: `cabal.project`, `flake.nix`, `flake.lock`,
   `Core/Blueprint.hs` re-exports). First patch in the stack so the
   rest of the series compiles against the new upstream.
2. `core: drop Mint, update global state validator hash literal`
   (`Core/OnChain.hs` + matching test fixture in
   `test/Cardano/MPFS/OnChainSpec.hs`).
3. `txbuilder: add requestScriptBytes to CageConfig` (`TxBuilder/Config.hs` plus
   call sites in `exe/` and E2E `e2e-test/`).
4. `txbuilder: add per-cage request address helpers`
   (`TxBuilder/Real/Internal.hs`).
5. `txbuilder: route Boot/End redeemer shapes to upstream PR #50`
   (`TxBuilder/Real/Boot.hs`, `TxBuilder/Real/End.hs`).
6. `txbuilder: pay Request{Insert,Delete,Update} to per-cage address`
   (`TxBuilder/Real/Request.hs`).
7. `txbuilder: split Retract query context, reference state UTxO`
   (`TxBuilder/Real/Retract.hs`).
8. `txbuilder: split Update query context, attach both scripts`
   (`TxBuilder/Real/Update.hs`).
9. `txbuilder: split Reject query context, attach both scripts`
   (`TxBuilder/Real/Reject.hs`).
10. `txbuilder: introduce Sweep entry point` (NEW
    `TxBuilder/Real/Sweep.hs` + cabal `exposed-modules` + Real.hs
    re-export + `OnChainSpec` round-trip test).
11. `indexer: follow N+1 addresses with atomic boot-time addition`
    (`Indexer/Backend.hs` and friends).
12. `http: derive per-cage request address for per-token listings`
    (`HTTP/Server.hs` + matching `HTTPLifecycleSpec` E2E case).
13. `e2e: cover sweep + dynamic boot scenarios in existing specs`
    (CageSpec / CageFlowSpec / IndexerSpec / HTTPLifecycleSpec /
    ProofsSpec).
14. `test: drop Mint round-trip, add Sweep round-trip, update hash
    literal` (`OnChainSpec`, `TxBuilderSpec`).

The `pr` skill's setup step 1 (capture `GATE`, run on
`origin/main`) MUST be completed before this stack is laid out; per
WIP.md the agent picking this up will run that step before
`stg init`.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

(none — Constitution Check passes both pre- and post-design.)
