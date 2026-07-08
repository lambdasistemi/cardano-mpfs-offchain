# Implementation Plan: On-chain #76 Pin and State Policy Parameter

## Technical Context

- Language/tooling: Haskell GHC 9.12.3 through Nix flakes, Cabal, Hspec,
  Fourmolu, HLint.
- Relevant package surfaces:
  - `cardano-mpfs-verify/lib/Cardano/MPFS/Client/Cage/Config.hs`
  - `cardano-mpfs-verify/lib/Cardano/MPFS/Client/Cage/Identity.hs`
  - `cardano-mpfs-verify/lib/Cardano/MPFS/Client/Verify/Reactor.hs`
  - `cardano-mpfs-client/test/**` config fixtures that extract `state.`
  - `cardano-mpfs-cli/lib/Cardano/MPFS/CLI/Cage.hs`
  - `cardano-mpfs-offchain/e2e-test/**` boot/e2e config fixtures that
    extract `state.`
  - `flake.nix` and `flake.lock`
- Upstream helper shape from on-chain #76:
  `applyPreviousPolicies pids = applyDataParam (List (map B pids))`.
  The offchain verifier ports this helper through existing `applyDataParam`
  surface without adding dependency manifests.

## Constitution Check

- Ledger-native types remain in use. No shadow policy-id/address types are
  introduced.
- Verifier paths stay pure: parameter application is a deterministic bytes-to-
  bytes transformation.
- Proof encoding and script identity remain compatible with the Aiken
  validators by deriving the state policy id from the applied PlutusV3 program.
- No server API, DB schema, or live-chain behavior is changed in this ticket.

## Design

1. RED setup:
   - Bump the on-chain flake input and lock to the resolved `e37e33e` commit.
   - Add a focused regression assertion under the client cage/verifier tests
     that compares locally derived genesis state identity to the bumped
     on-chain helper's `applyPreviousPolicies []` output.
   - Run the focused test and record the expected failure before any
     implementation change.
2. GREEN implementation:
   - Expose a verifier-facing `applyPreviousPolicies` helper from
     `Cardano.MPFS.Client.Cage.Config`, preferably as a wrapper around the
     bumped `Cardano.MPFS.Cage.Blueprint.applyPreviousPolicies`.
   - Apply `applyPreviousPolicies []` once when building a genesis
     `CageConfig` from raw state bytes. Use the applied bytes for both
     `cageScriptBytes` and `cfgScriptHash`.
   - Update the reactor JSON config parser so incoming raw state bytes are
     applied before storing/hash derivation.
   - Update every matching test helper site discovered by
     `rg 'cfgScriptHash = computeScriptHash (scriptBytes|stateBytes)'`.
3. Verification:
   - Re-run the RED focused test and the affected `just unit-client` matchers.
   - Run `./gate.sh`.
   - Commit one bisect-safe implementation commit with a `Tasks:` trailer.
4. Expanded CLI/e2e adoption:
   - Apply the same genesis `previousPolicies = []` parameter in live CLI and
     e2e boot paths that build a `CageConfig` from raw state bytes.
   - Prefer central constructors: CLI has `buildCageConfig`, and e2e already
     has `Cardano.MPFS.E2E.Helpers.Boot` as a shared helper module.
   - Prove the e2e boot path with `just e2e` using a real `MPFS_BLUEPRINT` so
     the affected specs do not skip.

## Slice Plan

Two implementation slices are required after scope clarification A-001:

1. Pin bump plus verifier/client genesis state parameter adoption. The pin
   bump, lock update, RED test, and verifier parameter application are coupled.
2. CLI plus e2e boot adoption. These are live boot paths and must be proven by
   an e2e run against the freshly built bumped blueprint, not compile-only
   evidence.

## Gate

The PR gate is `./gate.sh`, currently covering:

- `git diff --check`
- `nix develop --quiet -c cabal build all -O0 --enable-tests --enable-benchmarks`
- focused `just unit-client` matchers for cage boot/request/update/end/retract/
  reject, end facts, read-side verifiers, and reactor
- `nix build --quiet .#cardano-mpfs-offchain .#wasm-mpfs-verify
  .#checks.x86_64-linux.swagger-up-to-date`
- `just format-check`
- `just hlint`
- second-slice proof: `MPFS_BLUEPRINT=<bumped blueprint JSON> just e2e
  "Boot facts E2E"` at minimum; broaden to the affected e2e matchers when the
  focused run passes.

Baseline evidence before behavior changes: `./gate.sh` exited 0.
