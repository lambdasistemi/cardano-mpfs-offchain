# Implementation Plan: WASM + JS Cross-Target Verifier Build

Feature: `258-wasm-cross-target` | Issue #258 | Epic #287 (Child 4).

## Technical Context

- Languages: Haskell GHC 9.10.1 (native), ghc-wasm-meta GHC (wasm32-wasi),
  GHC-JS (js-unknown-ghcjs) via haskell.nix `projectCross`.
- Prior art: `cardano-ledger-inspector` `nix/wasm/` — generic WASM ledger
  port. We mirror its *shape* (toolchain, FOD builder, c-libs, fragment).
  It is WASM-only; the GHC-JS target is new infrastructure for this repo.
- Verifier surface (from survey): `Cardano.MPFS.Client.Verify`
  (`verify*Facts`, `verifyVerificationSnapshot`) plus its pure transitive
  modules `Facts`, `Bundle`, `Snapshot`, `TrustedRoot`,
  `Verify.{Replay,Snapshot,TxView,Completeness,Write,DSL,Read}`,
  `Cage.{Config,Identity}`.

## Constitution Check

- **IX. One Verifier, Many Targets** — this feature *is* Principle IX.
  Single Haskell implementation, compiled to native + WASM + JS, with
  byte-identity enforced in CI. PASS by construction.
- **VIII. Verifier Shape** — `verify*Facts` are pure
  `Either VerifyError`. The carve must not introduce `IO`. PASS.
- **I. Ledger-Native Types** — no shadow types introduced; the carve
  only moves existing modules across a component boundary. PASS.
- **II. Records of Functions / no orphan-instance hazards** — B2
  relocates `ToSchema`/`FromHttpApiData` instances out of the wire-type
  modules. Risk: orphan instances. Mitigation: keep the instances in the
  same package, in a module that owns either the type or the class
  context only where non-orphan; where unavoidable, isolate behind the
  non-wasm component so the wasm component never compiles them. Re-check
  after design. WATCH.
- **Hackage-ready / `cabal check`** — new components must pass
  `cabal check`. PASS target.

Complexity tracking: the only added complexity is component splitting
(sublibraries) in `cardano-mpfs-client` and `cardano-mpfs-api`. Justified:
it is the minimal structural change that lets the existing verifier
cross-compile without rewriting it (Principle IX) and without dragging
`http-client`/`servant-client`/`swagger2` into a browser artifact.

## Approach

### B1 — carve the pure verifier component (`cardano-mpfs-client`)

Add a public sublibrary `cardano-mpfs-client:verify` containing only the
pure verifier modules listed above. The main library re-exports it and
adds the IO surface (`Http`, `Client`, `Read`, `Write`, the
`Cage.{Boot,End,Request,Retract,Update,Reject}` tx-builders). Native
consumers and the `mpfs-verify` CLI keep their current import paths via
re-export — no public API change.

`verify` sublibrary build-depends (portable set): base, bytestring,
base16-bytestring, aeson, cborg, containers, text, microlens, operational,
plutus-tx, cardano-crypto-class, cardano-ledger-{core,api,conway,mary,
alonzo,allegra,babbage,binary}, cardano-slotting, cardano-strict-containers,
cardano-mpfs-cage, `mts:{csmt-core,csmt-verify,mpf-write}`, and the
`cardano-mpfs-api:wire` sublibrary (B2). NO http-client, servant-client,
servant-client-core, http-types, cardano-node-clients.

### B2 — separate wire types from swagger/servant (`cardano-mpfs-api`)

The verifier needs the wire *types* + Aeson + CBOR encoding, not
`Data.Swagger` `ToSchema` or `Servant.API` `FromHttpApiData`. Add a
`cardano-mpfs-api:wire` public sublibrary with the type + Aeson/CBOR
modules and portable deps only (aeson, base16-bytestring, bytestring,
text, + cborg as needed). Keep `ToSchema`/`FromHttpApiData` instances and
the Servant API definition in the main `cardano-mpfs-api` library, which
depends on `:wire`. If those instances are orphans relative to `:wire`,
that is acceptable here because only the non-wasm main library compiles
them; the wasm component never sees them. Re-check orphan exposure.

### Entry point — WASI reactor

`app-wasm/Main.hs`: read a JSON envelope on stdin
`{ "op": "verifyBootFacts" | "verifyRequestInsertFacts" | … ,
   "trusted_root": "<hex>", "facts": { … } }`, dispatch to the matching
`verify*Facts`, and write a deterministic JSON response encoding
`Either VerifyError Verified*` on stdout (stable key order, no spurious
whitespace). Mirrors the inspector's reactor model. Built with
`-no-hs-main -optl-mexec-model=reactor` for WASM. The same `Main` builds
under GHC-JS (node reads stdin / writes stdout). Native test harness
links the same dispatch directly for the byte-identity comparison.

### Nix

Mirror `nix/wasm/{forks.json,c-libs/,cabal-project-fragment.nix,
mkCardanoLedgerWasm.nix}` from the inspector, adapted to the mpfs SRP set
(plutus, cborg, hs-memory, network if pulled, foundation as needed) and a
narrower package list. Add `ghc-wasm-meta` flake input. Add `nix/wasm.nix`
producing `wasm-mpfs-verify`. Add `nix/js.nix` using the existing
haskell.nix project's `projectCross.ghcjs` to produce `js-mpfs-verify`.
Add a `cross-target-verify-check` flake check that runs the QuickCheck
suite against native + wasmtime + node.

### Cross-target QuickCheck

A test executable in the `verify` component generates `Arbitrary`
verifier inputs, computes the native verdict, serializes the input
envelope, invokes the wasm artifact under wasmtime and the js artifact
under node with that envelope, and asserts all three serialized verdicts
are equal. Shared `Arbitrary`/serialization lives in the `verify`
component so all three targets agree on encoding.

## Risks / Watch

- `mts` (haskell-mts CSMT/MPF) and `cardano-mpfs-cage` cross-compile:
  unverified at plan time. Build-verify early; if either fails, Q-file
  (no silent vendoring).
- GHC-JS via haskell.nix `projectCross.ghcjs` is unproven in this repo
  (inspector has no JS target). If it does not resolve the same SRP
  forks, escalate.
- B2 orphan-instance exposure — re-check after the sublibrary split.

## Phasing

Phase 1 (this ticket): B1, B2, entry point, wasm + js targets,
cross-target QuickCheck for `cardano-mpfs-client`, CI.
Phase 2 (follow-on, tasks.md): extend to `cardano-mpfs-workflows` once
`cardano-node-clients:tx-build` is merged and pinned.
