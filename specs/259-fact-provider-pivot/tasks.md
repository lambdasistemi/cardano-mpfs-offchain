---
description: "Task list for 259-fact-provider-pivot implementation — per-endpoint slices"
---

# Tasks: Fact-provider pivot — per-endpoint slices

**Input**: Design documents in `/specs/259-fact-provider-pivot/`
**Prerequisites**: spec.md, plan.md, research.md, data-model.md,
contracts/{facts-api.md, cage-dsl.md, verifier.md}, quickstart.md

**Tests**: Included. Each slice ships unit + property + e2e
coverage for its endpoint. SC-001 (end-to-end MOOG flow) becomes
the Phase 5 gate, exercising the union of all eight slices.

**Organization**: Phase 3 is decomposed into **eight per-endpoint
vertical slices**, one per child issue of #257
([#261](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/261),
[#264](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/264)–[#270](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/270)).
Each slice is a paired
**(cardano-mpfs-offchain PR + lambdasistemi/moog PR)** landing in
lockstep — the offchain PR hard-swaps `POST /tx/{op}` → `POST /facts/{op}`
and the moog PR migrates the matching call site. Slices are
**independent**: each one carries whatever scaffolding (modules,
helpers, types) it needs; no shared foundation PR.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other [P] tasks in the same slice.
- **[Story]**: User-story tag (US1..US4) or `SETUP` for cross-cutting work.
- Every task description names the exact files / repos it touches.

## Path conventions

- `cardano-mpfs-offchain` worktree (spec PR + each slice): `/code/cardano-mpfs-offchain-fact-provider` (for #260 spec) or a fresh worktree per slice (recommended).
- `lambdasistemi/moog` worktree: per-slice, created at the start of each slice's moog companion PR.

## Terminology (canonical)

- **slice** — one paired (offchain, moog) PR pair for a single endpoint. Eight slices total.
- **facts bundle** — the conceptual entity carrying a snapshot + proof-bearing data + protocol parameters for one operation.
- **`XFacts`** — the Haskell type names for each per-endpoint bundle (`BootFacts`, `RequestInsertFacts`, …).
- **facts response** — the HTTP-level wire shape of a `POST /facts/{op}` reply.
- **hard swap** — the offchain slice PR removes `POST /tx/{op}` in the same commit that adds `POST /facts/{op}`. No coexistence period.
- **lockstep release window** — the time between merging an offchain slice PR and merging its paired moog slice PR. Production deploys are gated against this window (see "Production deploy gating" below).

Use these terms uniformly across all artifacts and source.

---

## Phase 0: Constitution amendment — DONE

Phase 0 amended the constitution to v2.0.0 (Principle IV →
"Client-Side Transaction Construction"; Principle IX CI waiver).
Merged in [PR #259](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/259);
historical record only.

---

## Phase 1: Setup (pre-slice baseline)

**Purpose**: Confirm baselines are green and merge the speckit
artifacts (this PR, [#260](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/260)) before slice work begins. Each
individual slice captures its own legacy CBOR vector at the
moment its legacy handler is removed.

- [ ] T001 [SETUP] Verify the full local quality gate is green on
  `origin/main` of `cardano-mpfs-offchain`. Gate:
  `nix build .#offchain-tests .#e2e-tests .#cardano-mpfs-offchain
  .#docker-image .#checks.x86_64-linux.swagger-up-to-date &&
  just format-check && just hlint && nix run .#unit-tests &&
  nix run .#e2e-tests`. If red, STOP and surface to the user —
  pre-existing failures are not for the pivot to fix.
- [ ] T002 [SETUP] Merge [#260](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/260) (this PR) to main as
  spec-only. After this, each per-endpoint slice PR opens against
  main referencing these artifacts.

**Checkpoint**: Baselines green; speckit artifacts on main.

---

## Phase 3: Per-endpoint slices

Each slice is a paired (offchain PR, moog PR) landing in
lockstep. The eight slices are:

| Order | Slice | Issue | Tier | Notes |
|-------|-------|-------|------|-------|
| S1 | boot | [#261](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/261) | 1 | First slice; introduces shared scaffolding (WalletPolicy, BuildError, verifier framework) used by later slices. |
| S2 | request-insert | [#264](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/264) | 1 | |
| S3 | request-delete | [#265](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/265) | 1 | |
| S4 | request-update | [#266](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/266) | 1 | |
| S5 | retract | [#267](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/267) | 2 | Adds `readNamedRequestUtxo` indexer primitive. |
| S6 | end | [#268](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/268) | 2 | Adds `readStateUtxoAt`; requires empty-set completeness proof. |
| S7 | update | [#269](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/269) | 3 | Adds tier-3 plumbing: `readTrieFact`, `TrieFact` response type, MPF fold helper. Blocked by [#248](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248). |
| S8 | reject | [#270](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/270) | 3 | Reuses tier-3 plumbing from S7. Blocked by [#248](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248). |

Order is the recommended landing order — earlier slices introduce
shared modules later slices reuse. Slices may be reordered if a
team prefers (e.g., S5/S6 before S2–S4) provided each slice
respects its named "this slice introduces" scope.

### Slice template (applies to S1–S8 unless overridden)

Each slice produces two PRs in lockstep:

**Offchain PR** (`cardano-mpfs-offchain`):

1. Capture golden CBOR vector for the legacy `/tx/{op}` output at the slice's base commit. Save to `specs/259-fact-provider-pivot/test-vectors/legacy-{op}.cbor`. This is the byte-equality reference for the slice's helper.
2. Add `Cardano.MPFS.HTTP.Types.{Op}Facts` response type per `data-model.md` + JSON instances per `contracts/facts-api.md`.
3. Add cage DSL helper module `Cardano.MPFS.Client.Cage.{Op}` in `cardano-mpfs-client`. Port from the legacy `Cardano.MPFS.TxBuilder.Real.{Op}` if present; otherwise implement per `contracts/cage-dsl.md`.
4. Add verifier `Cardano.MPFS.Client.Verify.verify{Op}Facts` per `contracts/verifier.md` with `Verified{Op}Facts` newtype (constructor not exported).
5. Add new indexer primitive(s) in `Cardano.MPFS.Indexer.Reads` if the slice requires any (see per-slice notes). Same atomicity discipline as PR #253.
6. Add handler `facts{Op}Handler` in `Cardano.MPFS.HTTP.Server`. One `runIndexerTx ctx $ do { … }` composition + assemble response.
7. Add `/facts/{op}` path to `Cardano.MPFS.HTTP.API`; **remove** the legacy `/tx/{op}` path in the same commit.
8. Remove the legacy handler `tx{Op}Handler` from `Cardano.MPFS.HTTP.Server`.
9. Delete the legacy `Cardano.MPFS.TxBuilder.Real.{Op}` module. Update the `Real.hs` re-export.
10. Property test: `{op}CageTx` output byte-equal to `legacy-{op}.cbor` for the same inputs (Principle V).
11. Unit tests for `verify{Op}Facts` covering happy path, snapshot tamper, trusted-root tamper, proof tamper (and trie-fact tamper for tier-3).
12. Unit tests for the new indexer primitives (if any) and the new handler (happy path + 404/503/400/500 edge cases per `spec.md`).
13. Cross-target QuickCheck for the verifier per `contracts/verifier.md` §"Cross-target byte identity" (native + GHC-WASM + GHC-JS byte-identical output).
14. Extend `Cardano.MPFS.E2E.ProofsSpec` to exercise `POST /facts/{op}` honest + tamper variants.
15. Run `just update-swagger`. Confirm diff: `/facts/{op}` added; `/tx/{op}` (legacy) removed. Commit regenerated `docs/assets/swagger.json`.
16. Run full local quality gate (`nix build .#... && just format-check && just hlint && nix run .#unit-tests && nix run .#e2e-tests`).
17. Push branch; open PR titled `feat({op}): hard-swap POST /facts/{op} for POST /tx/{op} (#{issue})`. Cross-reference #257 and the matching slice issue.
18. After CI green + review + paired moog PR ready: merge. **Slice cutover window opens for this op.**

**Moog PR** (`lambdasistemi/moog`):

M1. Create worktree from `origin/main`.
M2. Add client function `MPFS.Facts.{op}` (Servant) in `src/MPFS/Facts.hs` (create the module if first slice; extend it otherwise).
M3. Migrate every moog call site that uses `MPFS.API.{op}` to the new pipeline: `verify{Op}Facts` → `{op}CageTx` → sign → submit.
M4. If first slice introducing them: add `Wallet.Policy` module with default `WalletPolicy`; add CLI flag for overrides.
M5. Migrate moog's `{op}` integration test against the post-pivot server (run in CI against the slice's offchain PR before merge).
M6. Bump `cardano-mpfs-offchain` source-repository-package pin to the offchain slice's merge-commit SHA (per `feedback_pins_main_only`: pin to a main commit SHA, not a branch ref). `nix flake update` if necessary.
M7. Run moog's full local quality gate.
M8. Push branch; open PR titled `feat({op}): migrate moog to POST /facts/{op}`. Cross-reference the offchain slice PR.
M9. After CI green + review: merge in the same release window as the offchain PR (typically the same day; max same release tag). **Slice cutover window closes when this commit is on moog main.**

**Slice checkpoint**: `/facts/{op}` live on offchain main; legacy `/tx/{op}` gone; moog's `{op}` path migrated; no production deploy used a commit in the slice's cutover window.

---

### Slice S1 — boot ([#261](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/261))

**This slice also introduces** (because it lands first):

- `Cardano.MPFS.Client.Cage.Policy` (`WalletPolicy`, `mainnetDefaultWalletPolicy`, `enforcePolicy`) per `data-model.md`.
- `Cardano.MPFS.Client.Cage.BuildError` ADT per `data-model.md`.
- `Cardano.MPFS.Client.Facts` module skeleton (`BundleSnapshot`, `UnverifiedPParams`, common JSON helpers).
- `Cardano.MPFS.Client.Verify` framework: `TrustedRoot` newtype, `VerifyError` ADT, `verifyCsmtInclusion` helper.

**Indexer primitives needed**: none new (relies on existing `readWalletUtxosAt` from #253; verify presence in T001).

**Per-slice tasks**: S1.1–S1.18 follow the offchain template (S1.{1..18}). Moog: S1.M1–S1.M9 follow the moog template. M4 (Wallet.Policy + CLI flag) lands in S1 since it is the first slice.

**Slice cutover window**: open at offchain S1.18 merge; close at moog S1.M9 merge.

---

### Slice S2 — request-insert ([#264](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/264))

**Indexer primitives needed**: none new.

Tasks follow the offchain + moog templates (`{op} = request-insert`,
`{Op} = RequestInsert`). The cage helper reuses
`Cardano.MPFS.Client.Cage.Request` if S2 introduces it;
subsequent S3/S4 extend the same module.

---

### Slice S3 — request-delete ([#265](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/265))

**Indexer primitives needed**: none new.

Tasks follow the templates (`{op} = request-delete`, `{Op} = RequestDelete`). Extends `Cardano.MPFS.Client.Cage.Request`.

---

### Slice S4 — request-update ([#266](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/266))

**Indexer primitives needed**: none new.

Tasks follow the templates (`{op} = request-update`, `{Op} = RequestUpdate`). Extends `Cardano.MPFS.Client.Cage.Request`.

---

### Slice S5 — retract ([#267](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/267))

**Indexer primitives needed** (new in this slice):

- `Cardano.MPFS.Indexer.Reads.readNamedRequestUtxo :: TxIn -> IndexerTx (Maybe ResolvedRequestUtxo)`.

Tasks follow the templates (`{op} = retract`, `{Op} = Retract`). The cage helper consumes the named request UTxO + funding and emits the refund to the requester.

---

### Slice S6 — end ([#268](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/268))

**Indexer primitives needed** (new in this slice):

- `Cardano.MPFS.Indexer.Reads.readStateUtxoAt :: TokenId -> IndexerTx (Maybe ResolvedStateUtxo)`.

**Extra**: `EndFacts` carries `requests_completeness_proof` (must attest the per-cage request set is empty). The slice also adds the CSMT empty-prefix-completeness primitive in `cardano-mpfs-client` if `haskell-mts` doesn't already expose one — confirm before slice work starts.

Tasks follow the templates (`{op} = end`, `{Op} = End`).

---

### Slice S7 — update ([#269](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/269), tier-3)

**Blocked by** [#248](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248) (read-side fact value fix) — confirm closed before starting.

**Indexer primitives needed** (new in this slice):

- `Cardano.MPFS.Indexer.Reads.readRequestUtxosAt :: TokenId -> IndexerTx [ResolvedRequestUtxo]`.
- `Cardano.MPFS.Indexer.Reads.readTrieFact :: TokenId -> ByteString -> IndexerTx (Maybe TrieFact)`.

**This slice also introduces** (tier-3 plumbing reused by S8):

- `Cardano.MPFS.Client.Facts.TrieFact` response type + JSON.
- MPF fold helper in `Cardano.MPFS.Client.Cage.Update`.

**Extra**: `UpdateFacts` carries `requests_completeness_proof` (must attest the full pending set so the oracle's policy decisions run over an unforgeable input — see `spec.md` §"Why completeness on update and end").

Tasks follow the templates (`{op} = update`, `{Op} = Update`).

---

### Slice S8 — reject ([#270](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/270), tier-3)

**Blocked by** [#248](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248). Reuses tier-3 plumbing from S7.

**Indexer primitives needed**: none new (reuses S5's `readNamedRequestUtxo` + S7's `readTrieFact`).

Tasks follow the templates (`{op} = reject`, `{Op} = Reject`).

---

## Phase 5: Verification & closure

Runs after all eight slices have merged on both offchain and moog main.

- [ ] T_V1 [P] [US2] Cross-repo grep verification — scoped to live binary surface:
  - `cardano-mpfs-offchain`: zero `transaction/{address}` paths in `lib/Cardano/MPFS/HTTP/{API,Server}.hs`; zero `Cardano.Ledger.Api.Tx` imports in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify*`; `docs/assets/swagger.json` has zero `transaction` paths; `lib/Cardano/MPFS/TxBuilder/Real/` directory empty (or removed entirely if `Real.hs` no longer needed).
  - `lambdasistemi/moog`: zero `MPFS.API` imports in `src/`; the old `MPFS.API` module file deleted.
- [ ] T_V2 [P] [SC-005] Confirm no production deploy used a commit inside any per-slice cutover window. The eight cutover windows are typically same-day each; cross-check release tags against deploy logs.
- [ ] T_V3 [P] [SC-001] Run end-to-end devnet exercise from `quickstart.md` §9 against post-pivot binaries: boot, three requests, retract, end, update, reject — all via moog against the new server. Every step verifies. Every transaction lands on-chain.
- [ ] T_V4 [SETUP] Doc sweep: `docs/architecture/overview.md` describes the post-pivot architecture (server as fact provider; client builds tx; verifier as pure proof check; pp gap + WalletPolicy). `grep -rn 'transaction/{address}\|GET /transaction\|verifyBootTxResponse\|verifyRequestTxResponse\|verifyRetractTxResponse\|verifyEndTxResponse\|verifyUpdateTxResponse\|verifyRejectTxResponse' docs/` returns only intentional migration-notes mentions. (Per-slice swagger regen already handles the API surface.)
- [ ] T_V5 [SETUP] Update [#257](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/257) with the pivot's completion record:
  - Phase 0 PR merge SHA (constitution amendment, already merged in [#259](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/259)).
  - Eight per-slice merge SHA pairs (offchain + moog), one row per slice.
  - Cutover-window timestamps for each slice.
  - Link to the post-pivot architecture doc.
  - Confirm SC-001..SC-005 all met.
  - Close [#256](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/256) (verifier-completeness-for-tx-shape; obsolete after pivot).
  - Re-tag [#254](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/254) (multi-band snapshots) against the new shape (optional `snapshot=band` parameter on `/facts/{op}`).
  - [#258](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/258) (Principle IX cross-target build infra) stays open under the constitution waiver.

---

## Dependency graph

```text
Phase 0 (Constitution) — DONE in PR #259
    ↓
Phase 1 (Setup) — T001..T002 (this PR #260 merges)
    ↓
Phase 3 (Per-endpoint slices):
    S1 boot (#261) ──────────────┐
    S2 request-insert (#264) ────┤
    S3 request-delete (#265) ────┼─→ (slices independent;
    S4 request-update (#266) ────┤    recommended landing order
    S5 retract (#267) ───────────┤    is S1..S8; each slice is
    S6 end (#268) ───────────────┤    one offchain PR + one moog
    S7 update (#269, tier-3) ────┤    PR landing in lockstep)
    S8 reject (#270, tier-3) ────┘
        ↑ S7, S8 blocked by #248
    ↓
Phase 5 (Verification & closure) — T_V1..T_V5
```

Notes:
- Slices are independent. Each one carries its own scaffolding; the only sequencing constraint is S7/S8 are blocked by [#248](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/248) (read-side fact value fix).
- The recommended order (S1..S8) lands the broadest shared modules in S1 so subsequent slices add less code per PR; teams may reorder freely as long as each slice covers its own scaffolding.
- Within a slice, the offchain and moog PRs must land in lockstep (typically same day).

## Acceptance criteria mapping

| Spec ID | Criterion | Closing tasks |
| ------- | --------- | ------------- |
| FR-001 | Eight `POST /facts/{op}` endpoints | One per slice (S1.7, S2.7, …, S8.7) |
| FR-002 | Each response carries snapshot + per-endpoint data + pp | S1.2 + S{n}.2 |
| FR-003 | Per-endpoint facts shape matches data-model.md | S{1..8}.2 |
| FR-004 | Every CSMT/MPF proof verifies against snapshot's roots | S{1..8}.4, S{1..8}.13 |
| FR-005 | One `runIndexerTx` per handler; new primitives inside same discipline | S{n}.5 (slices that add primitives), S{1..8}.6 |
| FR-006 | Legacy endpoints removed; swagger reflects new shape only | S{1..8}.{7,8,9,15}, T_V1 |
| FR-007 | Verifier surface has zero `Cardano.Ledger.Api.Tx` imports | S1.4 framework + S{1..8}.4 + T_V1 |
| FR-008 | Cage helpers byte-equal to legacy `*Core` for equivalent inputs | S{1..8}.{1,10} |
| FR-009 | pp returned with `verified: false`; `WalletPolicy` documented + enforced | S1.2 (`UnverifiedPParams`) + S1.M4 (moog defaults) |
| FR-010 | moog's `MPFS.API` removed; every callsite migrated | S{1..8}.M{2,3} + T_V1 |
| FR-011 | All repos move in coordinated release windows; neither broken between landings | Per-slice (S{1..8}.18 ↔ S{1..8}.M9), T_V2 |
| SC-001 | moog end-to-end exercises all eight ops via new endpoints | T_V3 |
| SC-002 | Zero `transaction/{address}` matches; swagger contains zero `transaction` paths | T_V1 |
| SC-003 | Zero `Cardano.Ledger.Api.Tx` matches in verifier | T_V1 |
| SC-004 | Default `WalletPolicy` rejects stubbed inflated pp | S1.M4 + S1's policy regression test (added in S1.M5 alongside the integration test) |
| SC-005 | Both repo defaults move within coordinated release windows | T_V2 |

## Production deploy gating

Each slice opens its own cutover window between the offchain PR's
merge and the matching moog PR's merge. During each per-slice
window:

- `cardano-mpfs-offchain` main publishes `/facts/{op}` (new) but no longer publishes `/tx/{op}` (legacy).
- `lambdasistemi/moog` main still calls `/tx/{op}` (legacy) for *that* op until the paired moog PR merges. The moog binary built from a commit inside the window cannot reach the server for the `{op}` operation.

**Discipline** (per slice):

- No production MPFS server deploy may use a post-offchain-merge commit until the paired moog PR is merged.
- No production moog deploy may use a pre-moog-merge commit against a post-offchain-merge server, for that operation.

The cutover release for each slice deploys both at once. Typical
cadence is same-day; max same release tag.

This is the per-slice operational corollary of FR-011 / SC-005 —
the constitutional commitment that both repos move in lockstep,
applied eight times (once per endpoint) instead of once globally.
