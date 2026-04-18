# Tasks: Proof-Carrying API Responses

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md)

## Slice 1: Shared response model + status root (US3, #210)

- [ ] T001 Add shared proof-bearing response types to `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, including a reusable snapshot shape with baked-in `utxo_root` and indexed `chainpoint`
- [ ] T002 Change `GET /status` response schema in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` to include the UTxO-CSMT root
- [ ] T003 Update `statusHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to read `Context.utxoRoot`
- [ ] T004 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/StatusSpec.hs` for the new status contract and root agreement with `GET /utxo/root`
- [ ] T005 Verify the HTTP test suite still compiles and `StatusSpec` passes

**Checkpoint**: Clients can discover the verification snapshot from
`GET /status`.

---

## Slice 2: Proof-bearing query endpoints (US1, #211)

- [ ] T006 Change `GET /tokens/:id` to a structured proof-bearing response in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T007 Change `GET /tokens/:id/facts/:key` to return the fact plus state witness and MPF proof in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T008 Change `GET /tokens/:id/proofs/:key` to return the MPF proof plus state witness in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T009 Change `GET /tokens/:id/requests` to return witnessed pending requests in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T010 Ensure every read-side proof-bearing response embeds its own `utxo_root` and indexed `chainpoint`, rather than relying on a prior `GET /status` call
- [ ] T011 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/TokenSpec.hs` for the token-state witness contract
- [ ] T012 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/TrieSpec.hs` for fact/proof response objects
- [ ] T013 Update `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/RequestsSpec.hs` for witnessed request responses
- [ ] T014 Verify the HTTP test suite passes for token, trie, and request endpoints

**Checkpoint**: Read-side responses are verifiable offline against one
reported snapshot.

**First milestone**: This is the first HTTP-client scenario we should
implement end-to-end before starting transaction-bundle refactors.

---

## Slice 3: Rich transaction builder boundary (US2 foundation, #212)

- [ ] T015 Add a proof-bearing unsigned-transaction bundle type to `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs`
- [ ] T016 Change the `TxBuilder` record in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs` to return the new bundle type instead of bare `Tx ConwayEra`
- [ ] T017 Ensure the bundle type always carries baked-in `utxo_root` and indexed `chainpoint`
- [ ] T018 Update the real wiring in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real.hs` to propagate bundle results
- [ ] T019 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/TxBuilder.hs` to compile with the new interface
- [ ] T020 Add or update bundle-shape tests in `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs`
- [ ] T021 Verify unit tests compile after the interface change

**Checkpoint**: The builder boundary can carry unsigned txs plus their
verification metadata.

---

## Slice 4: Simple proof-bearing tx endpoints (US2, #213)

- [ ] T022 Implement witnessed-input bundle generation for `POST /tx/boot` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs`
- [ ] T023 Implement witnessed-input bundle generation for `POST /tx/request/insert`, `POST /tx/request/delete`, and `POST /tx/request/update` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs`
- [ ] T024 Implement witnessed-input bundle generation for `POST /tx/retract` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs`
- [ ] T025 Implement witnessed-input bundle generation for `POST /tx/reject` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Reject.hs`
- [ ] T026 Implement witnessed-input bundle generation for `POST /tx/end` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs`
- [ ] T027 Change transaction endpoint response schemas in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` for boot, request, retract, reject, and end
- [ ] T028 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to serialize bundle responses instead of bare hex CBOR for those endpoints
- [ ] T029 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` with coverage for proof-bearing bundles on the simple tx endpoints
- [ ] T030 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` to verify inline bundle data and cross-check against `/utxo/*`
- [ ] T031 Verify unit and E2E coverage for the simple tx endpoints

**Checkpoint**: All tx-building endpoints except `POST /tx/update`
return proof-bearing bundles.

---

## Slice 5: `POST /tx/update` proof bundle (US2, #214)

- [ ] T032 Implement proof-bearing update bundles in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs`, including witnessed state input, witnessed request inputs, baked-in `utxo_root`, baked-in indexed `chainpoint`, and MPF proofs for contributing request keys
- [ ] T033 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` for the `POST /tx/update` response object
- [ ] T034 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` to verify proof-to-request association in batched update bundles
- [ ] T035 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs` to verify update bundles before signing
- [ ] T036 Verify update bundle tests pass

**Checkpoint**: The hardest batch update flow is fully verifiable before
signing.

---

## Slice 6: Swagger + contract checks (US3, #215)

- [ ] T037 Update Swagger `ToSchema` coverage in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` and tighten docs in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Swagger.hs` if needed
- [ ] T038 Regenerate `docs/assets/swagger.json` with `just update-swagger`
- [ ] T039 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` with cross-endpoint contract checks for snapshot/root consistency, including `/status` versus `/utxo/root` and baked-in `chainpoint` matching
- [ ] T040 Run `just ci`
- [ ] T041 Update the PR description, push, and wait for CI before merge

**Checkpoint**: The proof-bearing API contract is documented, tested,
and ready for review.

---

## Dependencies

```text
T001-T004 -> T005
T005 -> T006-T014
T014 -> T015-T021
T015-T021 -> T022-T031
T015-T021 -> T032-T036
T022-T031 -> T037-T041
T032-T036 -> T037-T041
```

## Notes

- Slice 1 is the narrowest vertical slice and should land first.
- Slices 1 and 2 together are the first client-visible milestone:
  proof-carrying token read endpoints whose JSON already contains
  `utxo_root` and `chainpoint`.
- `GET /utxo/root` remains a first-class sibling root endpoint for
  debugging and isolated deployments; tests should keep it aligned with
  `/status`.
- Slice 3 is the main architectural refactor; it starts only after the
  read-side milestone is settled, and slices 4 and 5 depend on it even
  if they touch different endpoint families.
- `POST /tx/update` is intentionally isolated from the other tx
  endpoints because batching and proof association make it the highest
  risk implementation area.
- The direct `/utxo/*` endpoints remain part of the verification story
  and should be used by tests as the cross-check oracle for inline
  bundles.
