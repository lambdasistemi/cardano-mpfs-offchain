# Cross-artifact analysis: issue #275

**Status**: report committed; corrections applied to `plan.md` + `tasks.md` in this commit. Verdict was *ready for implementation*; the orchestrator addressed the HIGH item (C1) and the LOW items (A1, C3, I1) before dispatch.

## Findings

| ID | Cat | Sev | Where | Summary | Resolution |
|----|---|---|---|---|---|
| C1 | coverage gap | HIGH | spec.md FR-009 ↔ tasks.md T030 | T030 said "update PR description with production evidence" without pinning the three timestamps or their per-clause mapping. | **Applied**: T030 now lists `13:56:28.599Z`, `14:11:04Z`, `14:33:37Z` with the FR-### each maps to. |
| C2 | coverage gap | MEDIUM | spec.md US1-6 | Two-`withApplication`-cycle test (third-restart shape) not in tasks; plan acknowledges it as a residual risk. | **Kept as residual risk** in plan.md "Risks and edge cases" and "Out of scope". PR description (T030) will flag it as an operator follow-up. |
| C3 | coverage gap | MEDIUM | spec.md FR-005 ↔ plan.md TraceReady | FR-005 requires the "no-op recovery" decision to be logged. Plan conflated this with `TraceReady`. | **Applied**: plan.md `TraceReady` paragraph now explicitly states it IS the recovery-decision marker (fires on both the long restoration path and the synchronous-`toFollowing` no-op path). |
| C4 | coverage gap | LOW | spec.md SC-006 | "restoration end" event not explicitly mapped. | **Implicit**: restoration end ≡ first `runner_phase_transition` emission. Documented in plan.md TraceRunner block. |
| A1 | ambiguity | LOW | plan.md | Open question about whether `/status` should also return 503. | **Applied**: open question struck; decision A+C is final. |
| I1 | inconsistency | LOW | plan.md gate pattern | Original pattern `'armageddonCleanup\|setup '` would not match either existing multi-line `setup` call. | **Applied**: gate switched to a count assertion (`grep -cE '^[[:space:]]+\$?[[:space:]]*setup$' …` must equal 2). Pre-verified on `ffc8dfe`: returns 2. |
| I2 | inconsistency | LOW | spec Key Entities lists `seeding` phase | No corresponding seeding trace event in plan. | **Acceptable**: FR-004 only requires seeding to run before phase 1, not a separate event. Tracked in `TraceStartupClassification`'s `fresh_db` flag. |
| U1 | underspecification | LOW | tasks.md subagent brief | Cabal expose-module verification left to subagent runtime. | **Acceptable**: subagent has the worktree and will verify directly. |

## Coverage matrix

| FR | Slice | Task | Notes |
|----|---|---|---|
| FR-001 startup classification | 1 | T010 | `TraceStartupClassification` |
| FR-002 phase boundary events | 1 | T010 | `TraceRunner` lifts upstream `RunnerEvent` |
| FR-003 readiness fail-closed | 1 | T010 | `/ready` 503 → 200 |
| FR-004 fresh-DB walks phases 1–3 | 1 | T010 | Test exercises fresh-DB path |
| FR-005 no-op decision logged | 1 | T010 | `TraceReady` is the marker (C3 resolution) |
| FR-006 fail closed on replay failure | 1 | T010 (happy path) | Negative-path test is out of scope for this slice; documented as residual risk |
| FR-007 devnet regression test | 1 | T010 RED + GREEN | |
| FR-008 fails on `ffc8dfe` | 1 | T010 RED | Verified in subagent's `WIP.md` |
| FR-009 PR description timestamps | – | T030 (orchestrator) | C1 resolution |
| FR-010 autoheal non-goal | – | T030 (orchestrator) | |
| FR-011 no armageddon in fix | 1 + gate | T010 + T020 | `TraceArmageddon` assertion + count-of-`setup` gate |
| FR-012 test asserts zero `TraceArmageddon` | 1 | T010 | |
| NFR-001 fresh-DB latency | 1 | T010 (full e2e suite) | Implicit via existing tests |
| NFR-002 single JSON-line stream | 1 | T010 | Subagent verifies no tracer doubling |
| NFR-003 CI budget | 1 | T010 | 90s timeout consistent with `CrashRecoverySpec` |
| SC-001..SC-007 | mix | T010 / T020 / T030 | All covered |

## Constitution

No conflicts with `.specify/memory/constitution.md`. Fix is purely additive (HTTP endpoint + tracer + TVar). No signing, verifier, or Plutus-version concerns. Principles VI (test locally first) and VII (Nix reproducibility) honoured via `nix develop --quiet -c …` commands in the subagent brief.

## Verdict

**Ready for implementation.** Proceed to dispatch slice 1 (T010).
