# Tasks: WASM + JS Cross-Target Verifier Build

Feature `258-wasm-cross-target` | Issue #258 | Epic #287 (Child 4).
One commit per task; bisect-safe; native `just ci` green at every step.

## Slice S1 — carve `cardano-mpfs-api:wire` (B2)

- [ ] T1.1 Add public sublibrary `cardano-mpfs-api:wire` (hs-source-dirs
  for the wire types + Aeson/CBOR encoding), portable deps only
  (no servant, no swagger2). Move the type + Aeson/CBOR modules in;
  leave `ToSchema`/`FromHttpApiData` + the Servant API in the main lib,
  which now depends on `:wire`.
- [ ] T1.2 Verify no orphan-instance warning leaks into `:wire`; the main
  lib still exposes the full schema/API surface. `cabal check` passes.
- [ ] T1.3 GREEN: native build + offchain swagger generation unchanged
  (`just build`, `just update-swagger` diff-clean).

## Slice S2 — carve `cardano-mpfs-client:verify` (B1)

- [ ] T2.1 Add public sublibrary `cardano-mpfs-client:verify` with the
  pure verifier modules (Verify.*, Facts, Bundle, Snapshot, TrustedRoot,
  Cage.Config, Cage.Identity), depending on `cardano-mpfs-api:wire` and
  the portable set. No http-client/servant-client/cardano-node-clients.
- [ ] T2.2 Main `cardano-mpfs-client` library re-exports `:verify` so all
  current import paths resolve unchanged; IO surface (Http, Cage
  builders, Read/Write/Client) stays in the main lib.
- [ ] T2.3 GREEN: `just unit` (client unit tests) + `mpfs-verify` CLI
  build unchanged; `cabal check` passes.

## Slice S3 — WASI-reactor entry point

- [ ] T3.1 Add `app-wasm/Main.hs` (in `:verify` or a thin exe depending
  only on `:verify`) reading the JSON op-envelope on stdin and writing a
  deterministic `Either VerifyError Verified*` JSON on stdout. Dispatch
  table covers the full `verify*Facts` family + `verifyVerificationSnapshot`.
- [ ] T3.2 Shared deterministic envelope encode/decode + `Arbitrary`
  generators live in `:verify` (single source of truth for all targets).
- [ ] T3.3 GREEN: native run of the reactor over fixture envelopes
  matches calling `verify*Facts` directly (RED test first).

## Slice S4 — nix WASM target (mirror inspector)

- [ ] T4.1 Add `ghc-wasm-meta` flake input; port
  `nix/wasm/{forks.json,c-libs/,cabal-project-fragment.nix,
  mkCardanoLedgerWasm.nix}` adapted to mpfs SRP set + package list.
- [ ] T4.2 Add `cabal-wasm.project` (or fragment splice) pinning the
  wasm forks; build-verify `mts` + `cardano-mpfs-cage` cross-compile.
  If either fails → Q-file, pause (no silent vendoring).
- [ ] T4.3 `nix build .#packages.x86_64-linux.wasm-mpfs-verify` →
  real `mpfs-verify.wasm`. Smoke it under wasmtime on a fixture.

## Slice S5 — nix GHC-JS target

- [ ] T5.1 Wire `js-mpfs-verify` via haskell.nix `projectCross.ghcjs`
  (or ghc-wasm-meta JS if projectCross can't resolve forks → escalate).
- [ ] T5.2 `nix build .#packages.x86_64-linux.js-mpfs-verify` → real JS
  artifact. Smoke it under node on a fixture.

## Slice S6 — cross-target byte-identity QuickCheck

- [ ] T6.1 QuickCheck suite: random verifier inputs → native verdict,
  wasm verdict (wasmtime), js verdict (node) → assert all three
  serialized `Either VerifyError a` byte-identical. RED first (e.g.
  with a deliberately diverging stub), then GREEN.
- [ ] T6.2 Add `cross-target-verify-check` flake check invoking the suite
  with wasmtime + node on PATH.

## Slice S7 — CI wiring

- [ ] T7.1 CI builds `wasm-mpfs-verify` + `js-mpfs-verify` and runs
  `cross-target-verify-check` on every commit.
- [ ] T7.2 Update `CLAUDE.md` / docs: Principle IX waiver lifted note;
  cross-target build commands.

## Phase 2 follow-on (NOT this ticket — tracked here per epic rule)

- [ ] P2.1 When `cardano-node-clients` merges the pure `tx-build`
  sublibrary (branch `041-extract-txbuild`, `b1853c9`) and the pin is
  bumped: point the `Cage.{Boot,End,Request,Retract,Update,Reject}`
  builders at `cardano-node-clients:tx-build` so `cardano-mpfs-workflows`
  (#289) becomes cross-compilable.
- [ ] P2.2 Add `wasm-mpfs-workflows` + `js-mpfs-workflows` targets and a
  `fetch`-based `HttpClient` JS shim seam (workflows owns the shim; #258
  provides the build).
- [ ] P2.3 Extend the cross-target QuickCheck to exercise
  `cardano-mpfs-workflows`' verification path (epic acceptance: both
  packages' verifiers).
