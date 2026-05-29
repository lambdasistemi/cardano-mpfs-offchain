# Feature Specification: WASM + JS Cross-Target Verifier Build (Principle IX)

Child 4 of epic #287 (client front-ends for cardano-mpfs-client).
Issue: lambdasistemi/cardano-mpfs-offchain#258.

## User Story

As a wallet/SPA integrator, I run the one canonical Haskell verifier
(`cardano-mpfs-client`) unchanged in the browser — compiled to WASM
(`wasm32-wasi`) and to GHC-JS — and I am guaranteed the verdict it
produces is byte-identical to the native build, so a fact response that
verifies on the server verifies in the browser and vice versa. This is
the infrastructure prerequisite for the PureScript SPA (#291), which
replaces its current PureScript reimplementation of the verifier with
the WASM-compiled Haskell one (Principle IX: One Verifier, Many Targets).

## Scope

In scope (Phase 1 — this ticket's acceptance):

- Cross-target build infrastructure mirroring `cardano-ledger-inspector`'s
  `nix/wasm/` shape (ghc-wasm-meta toolchain, two-phase FOD dependency
  builder, C-libs for libsodium/secp256k1/blst, cabal project fragment
  for `if arch(wasm32)` source-repository-package forks).
- The pure verifier surface of `cardano-mpfs-client`
  (`verify*Facts` family + `verifyVerificationSnapshot`) carved into a
  cross-compilable component, free of `http-client`/`servant-client`
  (the `Http` module) and free of `swagger2`/`Servant.API`
  (the `cardano-mpfs-api` wire-type instance tangle).
- A WASI-reactor entry point exposing the verifier family over a
  deterministic JSON request/response envelope, built to:
  - `wasm-mpfs-verify` → `mpfs-verify.wasm`
  - `js-mpfs-verify` → a GHC-JS artifact
- A cross-target QuickCheck suite asserting byte-identical
  `Either VerifyError a` outputs across native + WASM + JS for the
  `verify*Facts` family.
- CI builds both artifacts on every commit and runs the cross-target
  suite.

Out of scope (Phase 2 — follow-on, tracked in tasks.md):

- Cross-compiling `cardano-mpfs-workflows` (#289) including its
  unsigned-transaction building path. Blocked on
  `cardano-node-clients` exposing its already-extracted pure
  `tx-build` sublibrary (branch `041-extract-txbuild`, commit
  `b1853c9`) on a merged commit, then bumping the pin here. The
  cross-target QuickCheck is extended to cover the workflows
  verification path when that lands.
- The npm package publish (Principle IX mentions it for releases; the
  release wiring is the epic's concern, not this build slice).

## Acceptance Criteria

1. `nix build .#packages.x86_64-linux.wasm-mpfs-verify` produces a real
   `mpfs-verify.wasm` — a genuine wasm32-wasi compilation of the
   verifier surface, not a stub or a native wrapper.
2. `nix build .#packages.x86_64-linux.js-mpfs-verify` produces a real
   GHC-JS artifact of the same surface.
3. A QuickCheck property generates random verifier inputs, runs each
   `verify*Facts` on native, on the WASM artifact (via wasmtime), and on
   the JS artifact (via node), and asserts all three produce
   byte-identical serialized `Either VerifyError a`. A divergence fails
   the suite (merge block, per Principle IX).
4. CI builds `wasm-mpfs-verify` and `js-mpfs-verify` and runs the
   cross-target suite on every commit.
5. The native `cardano-mpfs-client` library, its `mpfs-verify` CLI, and
   `just ci` continue to pass unchanged — the carve does not regress the
   native build or its public API.

## Non-Goals / Constraints

- No verifier logic is rewritten in any language (Principle IX). The
  WASM/JS artifacts are compilations of the existing Haskell.
- No transitive dependency is silently shimmed or vendored. A dependency
  that does not cross-compile is surfaced as a Q-file
  (`/tmp/epic-287/258/questions/`) and paused on.
- Verifier paths stay pure: no `IO`, networking, filesystem, time, or
  non-determinism reachable from `verify*Facts` (Principle VIII/IX).
