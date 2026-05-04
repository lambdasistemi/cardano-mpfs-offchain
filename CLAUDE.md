# cardano-mpfs-offchain Development Guidelines

Auto-generated from active feature plans and maintained by hand where
repo-wide guidance is stable. Last updated: 2026-05-02.

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

- Haskell with GHC 9.10.1 for native builds.
- `cardano-mpfs-client` verifiers must remain compatible with native
  GHC, GHC-WASM, and GHC-JS targets.
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
- 249-atomic-boot-handler: Added Haskell GHC 9.10.1
- 243-proof-redesign: Added Haskell GHC 9.10.1 (offchain server, verifier library); Lean 4 (formal model — `lean/` directory) + Servant (HTTP API), `cardano-mpfs-cage` (Aiken-derived on-chain types via PlutusV3 blueprint), `cardano-mpfs-client` (verifier package), `haskell-mts` (CSMT inclusion + prefix-completeness primitives, MPF inclusion + exclusion), `cardano-utxo-csmt` (CSMT runtime), `cardano-ledger-conway` (TxOut/Tx serialization), `cardano-node-clients` (N2C wiring), `chain-follower` (block stream)

- `178-crypto-proof-replay`: added proof-bearing response verification,
  cryptographic replay constraints, and verifier portability principles.

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
