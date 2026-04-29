# Handoff — 231-split-validators-pr50

**Branch:** `231-split-validators-pr50`
**PR:** https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/241 (draft)
**Head at pickup:** `a39b0f6`; e2e green-up and the `just e2e`
recipe fix completed locally on 2026-04-29.
**Upstream pin:** `cardano-foundation/cardano-mpfs-onchain@cf3a8bdc` (PR #50 tip)

## What is done

Library + API surface for adopting upstream PR #50 (split state +
request validators) is committed. Stack from base of `origin/main`
upward:

| commit | task | what it lands |
|---|---|---|
| `eee1683` | pin | `cabal.project`, `flake.nix`, `flake.lock`, `Blueprint.hs` re-exports pinned at `cf3a8bdc` |
| `6b44446` | T011 | drop `Mint(..)`, drop dead-code identity helpers in `Core.OnChain`, fix Boot/End redeemer shapes (`Minting onChainRef` / `Burning (onChainTokenId tid)`), stub all 9 `applyVersion` call sites to identity + switch validator-name prefix `cage.` → `state.` |
| `ebd6fea` | T012 | `requestScriptBytes :: ShortByteString` on `CageConfig`, threaded as `SBS.empty` through every constructor |
| `e8339a4` | T013 | `mkRequestScript` / `requestAddrFromCfg` / `onChainTokenId` / `requestScriptBytesFromCfg` helpers in `TxBuilder.Real.Internal` |
| `6e64a83` | T022 | `Request.hs` pays request UTxO output to `requestAddrFromCfg cfg tid (network cfg)` |
| `3d18957` | T023 | `Retract.hs` splits `queryContext` (state at global / request at per-cage), attaches request validator script |
| `7a468b7` | T030 | `Update.hs` splits queryContext, attaches BOTH state and request validator scripts |
| `5f5e557` | T031 | `Reject.hs` mirrors T030 |
| `562a776` | T040 | `Cardano.MPFS.TxBuilder.Real.Sweep` (owner-only), exposed through `TxBuilder.Real`, listed in `cardano-mpfs-offchain.cabal` exposed-modules |
| `318fd49` | T022-fixup | test fixture `cardano-mpfs-offchain/test-data/request.uplc.hex` (real flat-encoded request UPLC from `cf3a8bdc`), `testReqAddr` helper, mock providers route request UTxOs to per-cage address |
| `9655db9` / `2c481d2` | T041 | `POST /tx/sweep`: `SweepRequest` / `SweepTxResponse` types in `cardano-mpfs-api`, `TxSweepAPI` wired into `TxWriteAPI` + the full `API` chain, `txSweepHandler` in `HTTP.Server`, `Context.cfgCage` (lazy), Swagger schemas + regenerated `docs/assets/swagger.json`, `sweepTx` wrapper in `cardano-mpfs-client` |
| `68ba77b` | T050 | `Cardano.MPFS.Indexer.Event.detectCageEvents` relaxed-predicate detector: request outputs are recognised by `RequestDatum` shape at any script address; spent UTxOs at any script address get redeemer-dispatched |

## CI status after e2e green-up

| Check | Result |
|---|---|
| Build Gate (Nix derivations + swagger up-to-date) | ✅ |
| build (unit tests, 369/0) | ✅ |
| deploy (docs) | ✅ |
| e2e | ✅ GitHub e2e and local `just e2e` both pass, 22 examples / 0 failures |

## Resolved e2e blocker

The previous full-suite failure was:

```
uncaught exception: DeserialiseFailure
DeserialiseFailure 0 "end of input"
```

Root cause was not the N2C codec or the `applyVersion` removal. The
runtime and E2E configs loaded only the global state validator from the
blueprint and set `requestScriptBytes = SBS.empty`. Once request routes
derived/attached the per-cage request validator, the empty script bytes
could be forced through `PlutusBinary`, producing the position-0 CBOR
failure.

Fix:

- `Cardano.MPFS.Core.Blueprint.loadCageScripts` now loads both
  `state.state` and `request.request` compiled code from the pinned
  split-validator blueprint.
- `mpfs-serve`, `mpfs-devnet-server`, and all E2E `CageConfig`
  constructors pass the real request validator bytes instead of
  `SBS.empty`.
- E2E assertions/resolvers now query the derived per-cage request
  address when checking or resolving request UTxOs.
- `just e2e` now runs the `.#e2e-tests` flake app through `nix run`.
  The app wraps the built E2E executable with the blueprint, devnet
  genesis, `cardano-node`, `cardano-cli`, and `aiken` runtime
  environment instead of relying on the stale `mpfs-bootstrap-genesis`
  Cabal target.
- The same flake-app standard is used for non-Docker CI commands:
  `.#unit-tests`, `.#format-check`, `.#hlint`, and `.#e2e-tests` run
  through `nix run`; Docker remains a `nix build .#docker-image`
  artifact.

## Layout notes

- The full speckit folder is at `specs/231-split-validators-pr50/`:
  `spec.md`, `plan.md`, `tasks.md`, `research.md`, `data-model.md`,
  `quickstart.md`, `contracts/{tx-shapes,http-endpoints}.md`,
  `checklists/requirements.md`, plus `notes/{gate.md,gate-baseline.md}`.
- The PR description on GitHub mirrors the task plan and tracks
  per-task status through checkboxes.
- `tasks.md` Phase Dependencies section lists T024, T032, T042, T052,
  T060 — these were placeholders for E2E coverage extensions and the
  drop-Mint-test polish. Of those, only T060 is library-relevant
  (TxBuilderSpec hash-literal fold-back) and is small. The E2E
  coverage tasks are no longer blocked by the suite-level green-up.

## Quality gate (re-runnable)

`specs/231-split-validators-pr50/notes/gate.md` captures the local
gate command:

```bash
just ci && just e2e
```

`just ci` does NOT include e2e — the recipe is `nix build` for the
library/swagger check, then `unit → unit-offchain → format-check →
hlint`. The `unit`, `format-check`, `hlint`, and `e2e` recipes run
flake apps.

## Outstanding tasks (in priority order)

1. **T060** — drop the obsolete `Mint` test in
   `test/Cardano/MPFS/TxBuilderSpec.hs`, update any remaining hash
   literals. Small. Parts of T060 already absorbed into T011 / T022-fixup.
2. **T024 / T032 / T052** — extend the E2E coverage to assert on the
   per-cage routing and dual-witness shape per `tasks.md` acceptance
   scenarios. No longer blocked on e2e being green.
3. **PR description sweep** — replace the in-progress checklist with a
   final, reviewer-facing summary once e2e is green.
4. **stgit cleanup** — empty placeholder patches for T024/T032/T042/T052/T060
   exist in the local stack but are not in remote history. Either
   fill them with real work or `stg delete` them at finalisation.
5. **Verify byte-for-byte parity (SC-005)**. The fixture under
   `cardano-mpfs-offchain/test-data/request.uplc.hex` was generated by
   `aiken build` against the pinned upstream source tree at session
   time. Add a small test (or document the reproduction recipe) that
   re-extracts the bytes from `dist-newstyle/src/cardano-m_-…` to
   detect drift if the pin is bumped.

## Local reproduction steps

```bash
# Enter dev shell once
nix develop

# Build everything
just build

# Unit tests (green: 369/0)
just unit-offchain

# E2E (green: 22/0 after e2e green-up)
just e2e

# Direct flake app form
nix run .#e2e-tests
```

For a single targeted run with verbose output:

```bash
just e2e "Cage E2E/boot, request, update, retract"
```

## Spec compliance checkpoint

| Functional req | Status |
|---|---|
| FR-001 derive per-cage request address | ✅ T013 |
| FR-002 pay Request to per-cage address | ✅ T022 |
| FR-003 Update/Reject attach both validators with `Modify` + `Contribute(stateRef)` | ✅ T030 / T031 |
| FR-004 Retract attaches request script + references state | ✅ T023 |
| FR-005 owner-only Sweep flow with `Sweep(stateRef)` | ✅ T040 + T041 |
| FR-006 End/Burn carries `OnChainTokenId` | ✅ T011 |
| FR-007 indexer follows N+1 addresses | ✅ T050 (relaxed-predicate detector) |
| FR-008 auto-add per-cage on boot | ✅ T050 (atomic via the same detector path; no separate follower set to update) |
| FR-009 HTTP per-token resolves via per-cage address | ✅ covered by T050 (indexer keys requests by token id) + existing `requestsByToken` |
| FR-010 `requestScriptBytes` on `CageConfig` | ✅ T012 |
| FR-011 byte-for-byte parity with upstream cage test vectors | ✅ unit tests pass against the canonical request UPLC fixture; full E2E now confirms the split-validator byte handling against the pinned blueprint |
