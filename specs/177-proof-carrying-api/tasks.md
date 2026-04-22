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

## Slice 4: Per-endpoint proof shapes + WASM verifier + simple tx endpoints (US2, #213)

- [ ] T022 Replace `UnsignedTxBundle` in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder.hs` with `ProofEnvelope p` plus per-endpoint proof records `BootProof`, `RequestProof`, `RetractProof`, `RejectProof`, `EndProof`, and `UpdateProof`; update the `TxBuilder` record so every method returns `m (ProofEnvelope <EndpointProof>)`
- [ ] T023 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/Mock/TxBuilder.hs` to match the new per-endpoint return types (polymorphic `error` is enough)
- [ ] T024 Add pure-Haskell shallow `TxOut` decoder in `cardano-mpfs-client/lib/Cardano/MPFS/Client/TxOut.hs` (extracts `(address bytes, ada lovelace)` via `cborg` / `binary` only — no `cardano-ledger-*`, no C FFI)
- [ ] T025 Add cross-check test suite in `cardano-mpfs-offchain/test/Cardano/MPFS/Client/TxOutShallowSpec.hs` with positive property (matches ledger decoder across Shelley address × value × datum × ref-script generator), negative property (truncated prefixes and random bytes return `Left`), and regression corpus under `cardano-mpfs-offchain/test/vectors/txout-regression-corpus/`
- [ ] T026 Add per-endpoint proof envelopes and JSON contracts in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Bundle.hs` (Haskell mirror of the server-side response types: `BootTxResponse`, `RequestTxResponse`, `RetractTxResponse`, `RejectTxResponse`, `EndTxResponse`)
- [ ] T027 Add per-endpoint verifiers in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs` following the shape `verify :: TrustedRoot -> ProofEnvelope p -> Either VerifyError ()`; the documented check list per endpoint (snapshot well-formedness, inputs-match-witnesses, per-witness ownership via shallow decode + CSMT path, output/datum/mint checks) lives beside the verifier source
- [ ] T028 Change transaction endpoint response schemas for boot, request, retract, reject, and end in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs` and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` to per-endpoint response types; the endpoint URL is the discriminator, no tagged-union wrapper
- [ ] T029 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to serialize per-endpoint proof responses instead of bare hex CBOR for those endpoints
- [ ] T030 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs` to emit `ProofEnvelope BootProof` with real `TxIn`/`TxOut` on every `WitnessedInput` (CSMT proof bytes may start empty if not yet wired through the CSMT view)
- [ ] T031 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs` to emit `ProofEnvelope RequestProof` for `insert`, `delete`, and `update`
- [ ] T032 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs` to emit `ProofEnvelope RetractProof` with named state-reference + request + funding witnesses
- [ ] T033 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Reject.hs` to emit `ProofEnvelope RejectProof` with named state + rejected-requests + funding witnesses
- [ ] T034 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs` to emit `ProofEnvelope EndProof` with named state + funding witnesses
- [ ] T035 Wire the UTxO-CSMT view access through the builder so `witnessedCsmtProof` carries real proof bytes against `envSnapshot.snapshotUtxoRoot` for every witness in the five migrated endpoints
- [ ] T036 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` with coverage for proof-bearing envelopes on the simple tx endpoints, including named-field presence, inputs-match-witnesses set equality, and deterministic ordering for list-valued fields
- [ ] T037 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` with a stub traversal test that calls each migrated endpoint, parses the per-endpoint response, walks every named `WitnessedInput` via the client verifier, and confirms inline proof data cross-checks against `/utxo/*`
- [ ] T038 Verify unit and E2E coverage for the simple tx endpoints
- [ ] T039 Run the `cardano-mpfs-client` cross-compile spike (GHC-WASM and GHC-JS) and record the outcome; slice 4 MUST NOT merge until the shallow decoder and verifiers build successfully on both targets

**Checkpoint**: All tx-building endpoints except `POST /tx/update`
return typed per-endpoint proof envelopes; a WASM-safe client verifier
walks every proof shape; the shallow `TxOut` decoder is cross-checked
against the ledger decoder.

---

## Slice 5: `POST /tx/update` proof bundle (US2, #214)

- [ ] T040 Implement proof-bearing update envelopes in `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs`, emitting `ProofEnvelope UpdateProof` with witnessed state input, witnessed request inputs, funding inputs, the trie root from the consumed state datum, and MPF proofs for every contributing request key
- [ ] T041 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs`, and `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` for the `POST /tx/update` response object
- [ ] T042 Extend `cardano-mpfs-client/lib/Cardano/MPFS/Client/Bundle.hs` and `Verify.hs` with the `UpdateProof` JSON contract and the per-endpoint verifier (walks state input, batched requests, funding, and every `TrieFact` against the datum-encoded trie root)
- [ ] T043 Extend `cardano-mpfs-offchain/test/Cardano/MPFS/TxBuilderSpec.hs` to verify proof-to-request association in batched update envelopes
- [ ] T044 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/CageFlowSpec.hs` to verify update envelopes end-to-end via the client verifier before signing
- [ ] T045 Verify update envelope tests pass

**Checkpoint**: The hardest batch update flow is fully verifiable before
signing by the same WASM-safe client that covers the simple endpoints.

---

## Slice 6: Swagger + contract checks (US3, #215)

- [ ] T046 Update Swagger `ToSchema` coverage in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs` and tighten docs in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Swagger.hs` if needed
- [ ] T047 Regenerate `docs/assets/swagger.json` with `just update-swagger`
- [ ] T048 Extend `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/HTTPLifecycleSpec.hs` with cross-endpoint contract checks for snapshot/root consistency, including `/status` versus `/utxo/root` and baked-in `chainpoint` matching
- [ ] T049 Wire the `cardano-mpfs-client` cross-target QuickCheck suite (GHC-native / GHC-WASM / GHC-JS byte-identity of `Either VerifyError a`) into CI per Constitution IX
- [ ] T050 Run `just ci`
- [ ] T051 Update the PR description, push, and wait for CI before merge

**Checkpoint**: The proof-bearing API contract is documented, tested,
and ready for review on every runtime the verifier ships to.

---

## Dependencies

```text
T001-T004 -> T005
T005 -> T006-T014
T014 -> T015-T021
T015-T021 -> T022-T039
T015-T021 -> T040-T045
T022-T039 -> T046-T051
T040-T045 -> T046-T051
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
