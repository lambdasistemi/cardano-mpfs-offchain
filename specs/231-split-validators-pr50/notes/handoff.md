# Handoff — 231-split-validators-pr50

**Branch:** `231-split-validators-pr50`
**PR:** https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/241 (draft)
**Head:** `68ba77b` (T050)
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

## CI status on `68ba77b`

| Check | Result |
|---|---|
| Build Gate (Nix derivations + swagger up-to-date) | ✅ |
| build (unit tests, 369/0) | ✅ |
| deploy (docs) | ✅ |
| e2e | ❌ 22 examples, 11 failures — **same failure shape since the moment e2e could run** |

## Known-red e2e

Every e2e test fails identically:

```
uncaught exception: DeserialiseFailure
DeserialiseFailure 0 "end of input"
```

No Haskell call site is exposed by Hspec. Local reproduction with the
canonical `nix build .#e2e-tests && ./result/bin/e2e-tests` confirms.

**What I ruled out:**

- Test-side typo or bad assertion — same error pre- and post-T050.
- Bad redeemer constructor — `Minting`/`Burning`/`Modify`/`Contribute`/`Sweep` shapes match upstream `Cage.Types.MintRedeemer` / `UpdateRedeemer` at `cf3a8bdc` (verified via `cardano-mpfs-onchain` source in `dist-newstyle/src/cardano-m_-...`).
- Mock-provider routing — unit tests fully green at 369/0 with the per-cage route through `requestAddrFromCfg testCageConfig testTid Testnet` against the real request UPLC.
- StatusSpec mock — fixed via `Context.cfgCage :: ~CageConfig` (lazy under `StrictData`).

**What is suspect (next person to pick this up should start here):**

- The error originates in a CBOR codec, almost certainly inside one of:
  - `cardano-node-clients/lib/Cardano/Node/Client/N2C/Codecs.hs` (the only `DeserialiseFailure` raise in the dependency tree).
  - `cardano-utxo-csmt/lib/Cardano/UTxOCSMT/Ouroboros/Codecs.hs` (the chain-follower's wire codec).
- Likely path: the `cf3a8bdc` pin pulled in a transitive dep change that shifted the wire shape between offchain and node by a single byte boundary, hence "position 0, end of input" — the decoder runs to completion expecting more bytes than the message carries.
- Alternative: the `applyVersion`-removal stub in T011 produced a script-bytes shape that the node can't deserialise when running the script. If the node submits a `MsgRejectTx` / `MsgScriptFailure` that the offchain decodes wrongly, you'd see this exact error.

## How to make progress on e2e

1. **Get a real stack trace.** The default Nix build of `e2e-tests` is
   not profiled, so `+RTS -xc -RTS` is rejected. Either rebuild
   profiling-enabled (`cabal build --enable-profiling`) or temporarily
   thread `traceIO` markers through the chain-follower start path:
   - `Cardano.MPFS.Application.withApplication` (entry).
   - `Cardano.UTxOCSMT.Application.ChainSyncN2C.mkN2CChainSyncApplication`
     (where chain-sync starts).
   - `Cardano.Node.Client.N2C.Connection.runNodeClient` (where the
     N2C codec runs).
2. **Bisect by reverting T011's `applyVersion`-stub change.** The
   commit replaced `applyVersion 1 sb` with `sb` in 9 call sites. If
   `applyVersion` was actually CBOR-wrapping the flat UPLC bytes, then
   the script attached to the boot tx is a raw flat program where a
   CBOR-wrapped Plutus script is expected, and the node can't decode
   it. Swap one call site back to a stub that produces CBOR-wrapped
   bytes (e.g. `serialize'` from `cardano-ledger-binary`) and see if
   the boot tx makes it past validation.
3. **Read upstream cage's E2E test setup.** Upstream
   `cardano-mpfs-cage`'s
   `dist-newstyle/src/cardano-m_-…/haskell/e2e-test/Cardano/MPFS/Cage/E2E/CageSpec.hs`
   shows how the cage tests load the script bytes and submit a boot —
   verify our offchain mirrors the EXACT same bytecode handling
   (especially around the boot `Minting(seed)` redeemer).

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
  coverage tasks are blocked on the e2e green-up.

## Quality gate (re-runnable)

`specs/231-split-validators-pr50/notes/gate.md` captures the local
gate command:

```bash
nix develop --command bash -c '
  just ci \
  && find . -name "*.cabal" -not -path "./dist-newstyle/*" | xargs cabal-fmt -c \
  && just e2e
'
```

`just ci` does NOT include e2e — the recipe is `build → unit →
unit-offchain → format-check → hlint`. Add `cabal-fmt -c` to match
CI exactly.

## Outstanding tasks (in priority order)

1. **e2e green-up** (blocking merge). See "How to make progress on
   e2e" above.
2. **T060** — drop the obsolete `Mint` test in
   `test/Cardano/MPFS/TxBuilderSpec.hs`, update any remaining hash
   literals. Small. Parts of T060 already absorbed into T011 / T022-fixup.
3. **T024 / T032 / T052** — extend the E2E coverage to assert on the
   per-cage routing and dual-witness shape per `tasks.md` acceptance
   scenarios. Blocked on e2e being green.
4. **PR description sweep** — replace the in-progress checklist with a
   final, reviewer-facing summary once e2e is green.
5. **stgit cleanup** — empty placeholder patches for T024/T032/T042/T052/T060
   exist in the local stack but are not in remote history. Either
   fill them with real work or `stg delete` them at finalisation.
6. **Verify byte-for-byte parity (SC-005)**. The fixture under
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

# E2E (red: 22/11)
nix build .#e2e-tests
E2E_GENESIS_DIR=cardano-mpfs-offchain/e2e-test/genesis \
  nix develop --quiet -c ./result/bin/e2e-tests --match "Cage E2E"
```

For a single targeted run with verbose output:

```bash
E2E_GENESIS_DIR=cardano-mpfs-offchain/e2e-test/genesis \
  nix develop --quiet -c ./result/bin/e2e-tests \
    --match "Cage E2E/boot, request, update, retract" \
    --format=specdoc --print-cpu-time --fail-fast
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
| FR-011 byte-for-byte parity with upstream cage test vectors | ⚠ unit tests pass against the canonical request UPLC fixture; e2e cannot confirm until the codec issue above is resolved |
