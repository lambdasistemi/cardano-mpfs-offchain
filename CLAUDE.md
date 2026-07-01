# cardano-mpfs-offchain Development Guidelines

Auto-generated from active feature plans and maintained by hand where
repo-wide guidance is stable. Last updated: 2026-07-01.

## Source of Truth

- Project constitution: `.specify/memory/constitution.md`
- Architecture overview: `docs/architecture/overview.md`
- Testing guide: `docs/architecture/testing.md`
- Public API surface: `docs/assets/swagger.json`

The constitution is authoritative for architectural decisions. Feature
plans must include a Constitution Check before research and re-check it
after design.

## Active Technologies
- Haskell GHC 9.10.1 (offchain server, verifier library); Lean 4 (formal model — `lean/` directory) + Servant (HTTP API), `cardano-mpfs-cage` (Aiken-derived on-chain types via PlutusV3 blueprint), `cardano-mpfs-client` (verifier package), `haskell-mts` (CSMT inclusion + prefix-completeness primitives, MPF inclusion + exclusion), `cardano-utxo-csmt` (CSMT runtime), `cardano-ledger-conway` (TxOut/Tx serialization), `cardano-node-clients` (N2C wiring), `chain-follower` (block stream) (243-proof-redesign)
- RocksDB (existing CSMT + index column families) (243-proof-redesign)
- RocksDB with 13 column families (6 UTxO from (249-atomic-boot-handler)
- Haskell GHC 9.10.1 across all three (259-fact-provider-pivot)
- RocksDB on the server, unchanged. No schema change. The (259-fact-provider-pivot)
- Haskell GHC 9.10.1; Servant/Aeson boot facts API; pure
  `cardano-mpfs-client` boot verifier and local cage helper; RocksDB
  IndexerTx reads unchanged (261-boot-fact-provider-pivot)
- wasm32-wasi targets: `mpfs-verify-reactor` (verifier closure) +
  `mpfs-cage-reactor` (cage tx builders — boot/assemble/end/request-*/
  retract/reject/update ops via WASI stdio envelope); built via
  `nix/wasm-targets.nix` mirroring `cardano-ledger-inspector`;
  `cardano-mpfs-cage-tx` package carves cage tx builders out of client
  as a WASM-portable sublibrary (258-cage-helper-wasm)

- Haskell with GHC 9.10.1 for native builds.
- `cardano-mpfs-client` verifiers must remain compatible with native
  GHC, GHC-WASM, and GHC-JS targets.
- `cardano-mpfs-cage-tx` cage tx builders must remain compatible with
  native GHC and GHC-WASM (wasm32-wasi) targets.
- Nix flakes provide the development shell and CI environment.
- Cabal drives Haskell builds inside the nix shell.
- Fourmolu and HLint enforce formatting and linting.

## Project Structure

```text
cardano-mpfs-offchain/   Main service, API, e2e tests, docs
cardano-mpfs-client/     Offline proof-bearing response verifier
merkle-patricia-forestry/
                         MPF trie implementation and tests
docs/                    Architecture, API docs, Swagger UI assets
specs/                   Speckit feature specs, plans, and tasks
```

## Commands

Use `just` recipes from `nix develop`:

```bash
just build                 # Build all components
just unit                  # MPF/client unit tests
just unit-offchain         # Offchain interface/unit tests
just e2e                   # E2E tests with cardano-node subprocess
just format                # Apply Fourmolu formatting
just format-check          # Check formatting
just hlint                 # Run HLint
just ci                    # Full local CI mirror
just update-swagger        # Regenerate docs/assets/swagger.json
```

## Core Constraints

- Use ledger-native Cardano types; do not introduce shadow ledger
  representations.
- Service boundaries use records of functions, not typeclasses.
- Block processing must be atomic across RocksDB column families.
- The server is a fact-provider: it serves only proof-bearing material
  (snapshot + UTxOs with CSMT proofs + MPF facts + protocol parameters)
  anchored to a single indexer snapshot. The server MUST NOT return
  unsigned transactions; the client builds and signs them locally.
- Proof encoding, trie hashing, and datum/redeemer construction must stay
  compatible with the Aiken validators in `cardano-mpfs-onchain`.
- Client verifiers are pure offline functions. No `IO`, networking,
  filesystem, time, or non-determinism in verifier paths.
- Verifier dependencies must cross-compile to GHC-WASM and GHC-JS before
  they are admitted.

## Spec Kit Workflow

Every issue starts with speckit artifacts before implementation:

1. `spec.md` states the user-visible requirement and acceptance criteria.
2. `plan.md` records the technical approach and Constitution Check.
3. `tasks.md` decomposes the implementation into ordered, testable work.
4. Implementation follows the task list, updating status as work lands.

## Recent Changes
- 258-cage-helper-wasm: Added wasm32-wasi cross-compilation targets
  (`wasm-mpfs-verify` via `nix/wasm-targets.nix`); `cardano-mpfs-cage-tx`
  package carving cage tx builders as a WASM-portable library;
  `mpfs-cage-reactor` WASI stdio-envelope reactor covering all cage ops
  (boot, assemble, end, request-insert/update/delete, retract, reject,
  update); build infrastructure mirrors `cardano-ledger-inspector` pattern
- 261-boot-fact-provider-pivot: Added boot facts API planning,
  proof-only verifier and local cage-helper design, and paired MOOG
  cutover constraints
- 259-fact-provider-pivot: Added Haskell GHC 9.10.1 across all three
- 249-atomic-boot-handler: Added Haskell GHC 9.10.1
- 243-proof-redesign: Added Haskell GHC 9.10.1 (offchain server, verifier library); Lean 4 (formal model — `lean/` directory) + Servant (HTTP API), `cardano-mpfs-cage` (Aiken-derived on-chain types via PlutusV3 blueprint), `cardano-mpfs-client` (verifier package), `haskell-mts` (CSMT inclusion + prefix-completeness primitives, MPF inclusion + exclusion), `cardano-utxo-csmt` (CSMT runtime), `cardano-ledger-conway` (TxOut/Tx serialization), `cardano-node-clients` (N2C wiring), `chain-follower` (block stream)

  cryptographic replay constraints, and verifier portability principles.

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
