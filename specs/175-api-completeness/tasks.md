# Tasks: API Completeness — Reject and Request Update

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md)

## Phase 1: Request Update (US2 — simpler, no new module)

**Goal**: Add `POST /tx/request/update` for submitting `OpUpdate(old, new)` requests.

**Independent Test**: Insert a key, process it, submit update request, process update, verify root.

- [ ] T001 [US2] Add `requestUpdate` to `TxBuilder` record in `lib/Cardano/MPFS/TxBuilder.hs`
- [ ] T002 [US2] Implement `requestUpdateImpl` in `lib/Cardano/MPFS/TxBuilder/Real/Request.hs` — copy `requestDeleteImpl`, use `OpUpdate oldVal newVal`
- [ ] T003 [US2] Wire `requestUpdateImpl` in `lib/Cardano/MPFS/TxBuilder/Real.hs`
- [ ] T004 [US2] Add mock stub in `lib/Cardano/MPFS/Mock/TxBuilder.hs`
- [ ] T005 [P] [US2] Add `UpdateValueRequest` type in `lib/Cardano/MPFS/HTTP/Types.hs` — fields: token, key, old_value, new_value, address
- [ ] T006 [US2] Add `TxRequestUpdateAPI` type in `lib/Cardano/MPFS/HTTP/API.hs`
- [ ] T007 [US2] Add `txUpdateValueHandler` in `lib/Cardano/MPFS/HTTP/Server.hs` and wire into `mkApp`
- [ ] T008 [US2] Add Swagger schema for the new endpoint
- [ ] T009 [US2] Unit test in `test/Cardano/MPFS/TxBuilderSpec.hs` — request-update tx structure
- [ ] T010 [US2] E2E test in `e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs` — insert → update value → verify root
- [ ] T011 [US2] Preprod script `scripts/e2e-request-update.sh`

**Checkpoint**: `POST /tx/request/update` works end-to-end. Commit and verify.

---

## Phase 2: Reject (US1 — new module, different tx structure)

**Goal**: Add `POST /tx/reject` for cleaning up Phase 3 requests.

**Independent Test**: Submit request, wait for Phase 3, reject, verify refund and unchanged root.

- [ ] T012 [US1] Add `rejectRequests` to `TxBuilder` record in `lib/Cardano/MPFS/TxBuilder.hs`
- [ ] T013 [US1] Create `lib/Cardano/MPFS/TxBuilder/Real/Reject.hs` — implement `rejectRequestsImpl`:
  - Find state UTxO + pending requests
  - Filter to Phase 3 requests (submitted_at + process_time + retract_time < now)
  - State redeemer: `Reject` (Constr 4 [])
  - Request redeemers: `Contribute stateRef`
  - New state output: same root, same params
  - Refund outputs: (locked ADA - fee) to each request owner
  - Validity lower bound: after latest Phase 3 deadline
  - Use PastHorizon fallback from Update.hs for slot conversion
- [ ] T014 [US1] Wire `rejectRequestsImpl` in `lib/Cardano/MPFS/TxBuilder/Real.hs`
- [ ] T015 [US1] Add mock stub in `lib/Cardano/MPFS/Mock/TxBuilder.hs`
- [ ] T016 [P] [US1] Add `RejectRequest` type in `lib/Cardano/MPFS/HTTP/Types.hs` — fields: token, address
- [ ] T017 [US1] Add `TxRejectAPI` type in `lib/Cardano/MPFS/HTTP/API.hs`
- [ ] T018 [US1] Add `txRejectHandler` in `lib/Cardano/MPFS/HTTP/Server.hs` and wire into `mkApp`
- [ ] T019 [US1] Add Swagger schema for the new endpoint
- [ ] T020 [US1] Unit test in `test/Cardano/MPFS/TxBuilderSpec.hs` — reject tx structure (root unchanged, refunds, redeemers)
- [ ] T021 [US1] E2E test in `e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs` — request → wait Phase 3 → reject → verify
- [ ] T022 [US1] Preprod script `scripts/e2e-reject.sh`

**Checkpoint**: `POST /tx/reject` works end-to-end. Stuck preprod UTxOs can be recovered.

---

## Phase 3: Preprod Cleanup and Polish

- [ ] T023 Run `scripts/e2e-reject.sh` on preprod to recover stuck UTxOs from tokens `0cbe`, `b222`, `de7c`
- [ ] T024 Update `scripts/e2e-lib.sh` memory reference with reject endpoint
- [ ] T025 Run `just ci` locally — fourmolu, hlint, cabal-check, unit tests, E2E tests
- [ ] T026 Deploy to preprod, verify all scripts work
- [ ] T027 Update PR description, push, merge

---

## Dependencies

```
T001 → T002 → T003 (TxBuilder record → impl → wire)
T005 can run parallel with T001-T003 (different files)
T006 → T007 (API type → handler)
T009, T010 after T007

T012 → T013 → T014 (TxBuilder record → impl → wire)
T016 can run parallel with T012-T014
T017 → T018
T020, T021 after T018

T023 after T022 (need reject on preprod first)
T025 after all implementation
```

## Notes

- Request Update (Phase 1) is trivial — follows existing pattern exactly
- Reject (Phase 2) is the critical path — new tx structure, new module
- Phase 3 timing in E2E: use short process_time/retract_time (15s each) so Phase 3 is reachable in ~30s
- Each phase can be committed independently
