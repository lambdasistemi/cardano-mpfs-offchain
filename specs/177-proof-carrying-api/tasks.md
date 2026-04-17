# Tasks: Proof-Carrying API Responses

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md)

## Slice 1: Shared response model + status root (US3, #210)

- [ ] T001 Add shared proof-bearing response types to `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`
- [ ] T002 Change `GET /status` response schema in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` to include the UTxO-CSMT root
- [ ] T003 Update `statusHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to read `Context.utxoRoot`
- [ ] T004 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/StatusSpec.hs` for the new status contract
- [ ] T005 Verify the HTTP test suite still compiles and `StatusSpec` passes

**Checkpoint**: Clients can discover the verification snapshot from
`GET /status`.

---

## Slice 2: Proof-bearing query endpoints (US1, #211)

- [ ] T006 Change `GET /tokens/:id` to a structured proof-bearing response in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T007 Change `GET /tokens/:id/facts/:key` to return the fact plus state witness and MPF proof in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T008 Change `GET /tokens/:id/proofs/:key` to return the MPF proof plus state witness in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T009 Change `GET /tokens/:id/requests` to return witnessed pending requests in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T010 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/TokenSpec.hs` for the token-state witness contract
- [ ] T011 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/TrieSpec.hs` for fact/proof response objects
- [ ] T012 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/RequestsSpec.hs` for witnessed request responses
- [ ] T013 Verify the HTTP test suite passes for token, trie, and request endpoints

**Checkpoint**: Read-side responses are verifiable offline against one
reported snapshot.

---

## Slice 3: Rich transaction builder boundary (US2 foundation, #212)

- [ ] T014 Add a proof-bearing unsigned-transaction bundle type to `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs`
- [ ] T015 Change the `TxBuilder` record in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs` to return the new bundle type instead of bare `Tx ConwayEra`
- [ ] T016 Update the real wiring in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs` to propagate bundle results
- [ ] T017 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/TxBuilder.hs` to compile with the new interface
- [ ] T018 Add or update bundle-shape tests in `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs`
- [ ] T019 Verify unit tests compile after the interface change

**Checkpoint**: The builder boundary can carry unsigned txs plus their
verification metadata.

---

## Slice 4: Simple proof-bearing tx endpoints (US2, #213)

- [ ] T020 Implement witnessed-input bundle generation for `POST /tx/boot` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`
- [ ] T021 Implement witnessed-input bundle generation for `POST /tx/request/insert`, `POST /tx/request/delete`, and `POST /tx/request/update` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs`
- [ ] T022 Implement witnessed-input bundle generation for `POST /tx/retract` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs`
- [ ] T023 Implement witnessed-input bundle generation for `POST /tx/reject` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Reject.hs`
- [ ] T024 Implement witnessed-input bundle generation for `POST /tx/end` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs`
- [ ] T025 Change transaction endpoint response schemas in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` for boot, request, retract, reject, and end
- [ ] T026 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to serialize bundle responses instead of bare hex CBOR for those endpoints
- [ ] T027 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` with coverage for proof-bearing bundles on the simple tx endpoints
- [ ] T028 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` to verify inline bundle data and cross-check against `/utxo/*`
- [ ] T029 Verify unit and E2E coverage for the simple tx endpoints

**Checkpoint**: All tx-building endpoints except `POST /tx/update`
return proof-bearing bundles.

---

## Slice 5: `POST /tx/update` proof bundle (US2, #214)

- [ ] T030 Implement proof-bearing update bundles in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs`, including witnessed state input, witnessed request inputs, and MPF proofs for contributing request keys
- [ ] T031 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` for the `POST /tx/update` response object
- [ ] T032 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` to verify proof-to-request association in batched update bundles
- [ ] T033 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs` to verify update bundles before signing
- [ ] T034 Verify update bundle tests pass

**Checkpoint**: The hardest batch update flow is fully verifiable before
signing.

---

## Slice 6: Swagger + contract checks (US3, #215)

- [ ] T035 Update Swagger `ToSchema` coverage in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` and tighten docs in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Swagger.hs` if needed
- [ ] T036 Regenerate `docs/assets/swagger.json` with `just update-swagger`
- [ ] T037 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` with cross-endpoint contract checks for snapshot/root consistency
- [ ] T038 Run `just ci`
- [ ] T039 Update the PR description, push, and wait for CI before merge

**Checkpoint**: The proof-bearing API contract is documented, tested,
and ready for review.

---

## Dependencies

```text
T001-T004 -> T005
T005 -> T006-T013
T005 -> T014-T019
T014-T019 -> T020-T029
T014-T019 -> T030-T034
T020-T029 -> T035-T039
T030-T034 -> T035-T039
```

## Notes

- Slice 1 is the narrowest vertical slice and should land first.
- Slice 3 is the main architectural refactor; slices 4 and 5 depend on
  it even if they touch different endpoint families.
- `POST /tx/update` is intentionally isolated from the other tx
  endpoints because batching and proof association make it the highest
  risk implementation area.
- The direct `/utxo/*` endpoints remain part of the verification story
  and should be used by tests as the cross-check oracle for inline
  bundles.
