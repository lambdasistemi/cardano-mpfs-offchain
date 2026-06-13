# Tasks: Shared Ledger WASM Kernel for MPFS Verify

**Input**: Design documents from `specs/359-shared-ledger-wasm/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`
**PR**: #360

One behavior-changing commit per slice. The orchestrator marks these tasks
done by amending the reviewed slice commit.

## Slice S1 — Pin and Align Released Kernel

Commit: `build: pin shared cardano-ledger-wasm kernel`
Owned files: `cabal.project`, `flake.nix`, `flake.lock`

- [ ] T001 Add `source-repository-package` for
      `lambdasistemi/cardano-ledger-wasm` tag
      `845877fde0907b58b150a2c8302033b4e73e9061` with nix32 hash
      `1gamv01par1zgj6wr1lldk51fpad1jw4pwf5jyfvi18x01jnvplx` in
      `cabal.project`.
- [ ] T002 Align `cabal.project` CHaP index-state and `flake.lock` CHaP input
      to `cardano-ledger-wasm` v0.1.1 (`2026-04-15T11:20:53Z`,
      `00c90c10812a98ef9680f4bfa269d42366d46d89`).
- [ ] T003 Add the pinned `cardano-ledger-wasm` flake input in `flake.nix`
      and update `flake.lock`.
- [ ] T004 Prove the native verifier still resolves with
      `nix develop --quiet -c just unit-client "Verify"`.

## Slice S2 — Replace Local WASM Builder with Shared Builder

Commit: `build: use shared ledger wasm builder for mpfs verify`
Owned files: `flake.nix`, `nix/wasm-targets.nix`, `cabal-wasm.project`,
`nix/wasm/default.nix`, `nix/wasm/cabal-project-fragment.nix`,
`nix/wasm/mkCardanoLedgerWasm.nix`, `nix/wasm/forks.json`,
`nix/wasm/c-libs/default.nix`, `nix/wasm/c-libs/libsodium.nix`,
`nix/wasm/c-libs/secp256k1.nix`, `nix/wasm/c-libs/blst.nix`

- [ ] T005 Rewire `nix/wasm-targets.nix` so `wasm-mpfs-verify` calls
      `cardano-ledger-wasm.lib.wasm.mkCardanoLedgerWasm`.
- [ ] T006 Keep only MPFS-specific wasm project/SRP content locally; inherit
      ledger-kernel forks, flags, and C library wiring from
      `cardano-ledger-wasm`.
- [ ] T007 Delete duplicated local ledger wasm builder and fork source files
      that are no longer imported by this repo.
- [ ] T008 Prove the independent Plutus repin is gone with repository search
      and build `nix build --quiet .#wasm-mpfs-verify`.

## Slice S3 — Verification, SPA Smoke, and CI Coverage

Commit: `ci: cover shared mpfs verify wasm kernel`
Owned files: `gate.sh`, `.github/workflows/ci.yml`, `justfile`,
`specs/359-shared-ledger-wasm/tasks.md`

- [ ] T009 Ensure the PR-local `gate.sh` and CI cover native verifier tests,
      `wasm-mpfs-verify`, `mpfs-spa`, and repository CI.
- [ ] T010 Run `nix build --quiet .#mpfs-spa` and verify it consumes the real
      `mpfs-cage-reactor.wasm` from `wasm-mpfs-verify`.
- [ ] T011 Run `nix develop --quiet -c just e2e-spa` for the Playwright
      reactor smoke, or record an environmental live-boundary blocker with
      exact failure output.
- [ ] T012 Run `./gate.sh` at HEAD and capture `GATE-PASS` before
      finalization.

## Finalization

- [ ] T013 Update PR #360 body with delivered behavior and verification.
- [ ] T014 Drop `gate.sh` in `chore: drop gate.sh (ready for review)`.
- [ ] T015 Mark PR #360 ready after local gate and GitHub CI are green.

## Dependencies & Execution Order

- S1 blocks S2 because CHaP and source pins determine the shared kernel input.
- S2 blocks S3 because SPA and CI proof must run against the migrated build.
- Finalization starts only after all implementation slices are checked.

## Parallel Opportunities

- S1 and S2 touch overlapping Nix/Cabal files and must run sequentially.
- S3 verification commands can be run in parallel by the driver after S2 is
  accepted, but the slice has one final commit.
