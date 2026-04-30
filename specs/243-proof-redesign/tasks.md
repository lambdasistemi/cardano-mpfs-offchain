---

description: "Task breakdown for post-split proof redesign"
---

# Tasks: Post-Split Proof Redesign

**Input**: Design documents from `/specs/243-proof-redesign/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md
**Branch**: `243-proof-redesign`

**Tests**: included — every endpoint shape ships with honest + forgery unit fixtures and an E2E spec, per spec acceptance and constitution principles VIII–X.

**Organization**: tasks grouped by user story so each story is independently testable. Foundational phase covers types and Lean predicates that all stories depend on; Polish phase covers Swagger, docs, and downstream consumer migration.

## Format: `[ID] [P?] [Story] Description with file path`

- **[P]**: can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: which user story this task belongs to — required for stories phases only

## Path Conventions

- API types: `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`, `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
- Server: `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`, `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`
- TxBuilder: `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/*.hs`
- Indexer: `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/...`
- Client verifier: `cardano-mpfs-client/lib/Cardano/MPFS/Client/...`
- Lean: `lean/Phase4/...`
- E2E: `cardano-mpfs-offchain/e2e-test/...`
- Swagger: `docs/assets/swagger.json`

---

## Phase 1: Setup

**Purpose**: prepare module scaffolding and Lake project entries before any logic lands.

- [X] T001 Add new modules to `cardano-mpfs-client/cardano-mpfs-client.cabal`: `Cardano.MPFS.Client.Read`, `Cardano.MPFS.Client.Write`, `Cardano.MPFS.Client.Verify.Read`, `Cardano.MPFS.Client.Verify.Write`, `Cardano.MPFS.Client.Verify.Completeness`, `Cardano.MPFS.Client.TrustedRoot` (stub modules also created so each commit compiles cleanly)
- [X] T002 [P] Add new Lean files to `lean/lakefile.lean` exports: `Phase4/ProofRedesign.lean`, `Phase4/Completeness.lean` (no-op — Lake's `lean_lib Phase4` auto-discovers `Phase4/*.lean`; files will materialise in T004)
- [X] T003 [P] Update `WIP.md` status section to "Phase 1 — Setup in progress"

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Lean predicates and shared types every story depends on. **No user story work begins until this phase is complete (constitution principle X).**

- [X] T004 Lean predicate / state machine in `lean/Phase4/Completeness.lean` — `CompletenessEnvelope`, `init`, `replayLeaf` (mirrors the structural-replay style of `Phase4.Verify`; cryptographic prefix-completeness predicate stays opaque per #226's pattern)
- [X] T005 [P] Lean theorems `replayLeaf_records_leaf` + `replayLeaf_preserves_count` in `lean/Phase4/Completeness.lean` — every accepted leaf is recorded verbatim and grows the list by one (refines the original "forge_extra_leaf" framing into structural recordkeeping invariants the Haskell verifier must mirror)
- [X] T006 [P] Lean theorems `replayLeaf_preserves_root_trust` + `replayLeaf_preserves_script_prefix` in `lean/Phase4/Completeness.lean` — replay never rewrites the trusted root or the prefix (refines the original "forge_missing_leaf" framing similarly)
- [X] T007 [P] Lean theorem `empty_witness_records_no_leaves` in `lean/Phase4/Completeness.lean` — empty witness records no leaves; the load-bearing primitive for `POST /tx/oracle/end` (US4)
- [ ] T008 Confirmation test in `cardano-mpfs-client/test/Cardano/MPFS/Client/CompletenessSpec.hs` exercises `haskell-mts`'s `CSMT.Proof.Completeness.generateProof`/verifier on the empty-leaf-set case under a known prefix and asserts success; corresponding forgery (claiming empty when leaves exist) fails with named error
- [X] T009 Add `TrustedRoot` newtype in `cardano-mpfs-client/lib/Cardano/MPFS/Client/TrustedRoot.hs`
- [X] T010 Add `Blueprint` data type in `cardano-mpfs-client/lib/Cardano/MPFS/Client/TrustedRoot.hs` (kept beside `TrustedRoot`; both are out-of-band trust inputs to the verifier) with `bpStatePolicyId`, `bpStateScriptAddress`, `bpRequestScriptAddress` fields and supporting `Address` / `AssetName` newtypes
- [X] T011 Add `UtxoEntry`, `UtxoEntryRefOnly`, `UtxoSetWitness` types with JSON instances in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` (also adds `UtxoRef` since the new vocabulary uses `ref` not `tx_in`; `ToSchema` instances included)
- [X] T012 Add `VerificationSnapshot` JSON instance reuse + new `UnsignedTxResponse` (uniform write response — completeness-bearing variant for `oracle/update` and `oracle/end` deferred to those slices to avoid the optional-field bridge pattern) in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`
- [ ] T013 Add `verifyCompleteness :: TrustedRoot -> Address -> UtxoSetWitness -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs`. **Blocked by upstream `lambdasistemi/haskell-mts#153`** (csmt-verify must expose `CompletenessProof` CBOR codec + pure `verifyCompletenessProof` before this can land — Principle IX forbids pulling `csmt-write` into the WASM/JS-portable client lib).
- [ ] T014 Add `verifyCompletenessEmpty :: TrustedRoot -> Address -> UtxoSetWitness -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs`. **Blocked by upstream `lambdasistemi/haskell-mts#153`** (same reason as T013; the empty-leaf-set check is a special case of the same primitive).
- [X] T015 [P] Add new `VerifyError` constructors per `contracts/verify-error.md` (US1 subset: `SnapshotMismatch`, `TrustedRootMismatch`, `StateAddressMismatch`, `RequestAddressMismatch`, `StateNftPolicyMismatch`, `StateNftNameMismatch`, `StateNftNotUnique`, `StateDatumMalformed`, `CompletenessProofInvalid`, `CompletenessExtraLeaf`, `CompletenessMissingLeaf`, `MpfInclusionInvalid`, `MpfExclusionInvalid`, `TokenUnknown`) added to `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Replay.hs` (where `VerifyError` lives); write-side and end-side constructors deferred to US2/US3/US4 slices
- [ ] T016 Add helper `enumerateAtScriptPrefix :: ScriptHash -> Indexer m UtxoSetWitness` to `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Interface.hs` and a default implementation against the existing CSMT in `cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Default.hs`
- [ ] T017 Update `cardano-mpfs-offchain/lib/Cardano/MPFS/Application.hs` to wire `enumerateAtScriptPrefix` through to handlers

**Checkpoint**: Foundation ready — user story implementation can begin.

---

## Phase 3: User Story 1 — Oracle reads a cage's current state without trusting the server (Priority: P1) 🎯 MVP

**Goal**: A trust-minimised oracle calls `GET /tokens/:id` and verifies the response offline using a separately-obtained trusted UTxO-CSMT root, recovering the state UTxO and the full pending request set with cryptographic certainty. Also covers the `GET /tokens/:id/facts/:key` lookup with present/absent split.

**Independent Test**: spec US1 acceptance scenarios — drive `GET /tokens/:id` against an instrumented server, verify offline given a trusted root.

- [ ] T018 [US1] Lean predicate `tokenResponseValid` in `lean/Phase4/ProofRedesign.lean` covering snapshot agreement + state-UTxO check + per-cage requests completeness
- [ ] T019 [P] [US1] Lean theorem `forge_token_response_state_utxo_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T020 [P] [US1] Lean theorem `forge_token_response_completeness_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T021 [US1] Lean predicates `factPresentResponseValid`, `factAbsentResponseValid` in `lean/Phase4/ProofRedesign.lean` covering MPF inclusion / exclusion
- [ ] T022 [P] [US1] Lean theorem `forge_fact_present_value_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T023 [P] [US1] Lean theorem `forge_fact_absent_present_key_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T024 [US1] Add `TokenResponse` (new shape with folded-in requests witness) to `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`; remove the old `TokenStateJSON` and `RequestsResponse` types
- [ ] T025 [US1] Add `FactPresentResponse` and `FactAbsentResponse` types with JSON instances in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`
- [ ] T026 [US1] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`: keep `TokenAPI` returning `TokenResponse`; replace `TokenFactAPI` with `UVerb` carrying `FactPresentResponse` (200) / `FactAbsentResponse` (404 with body) / `NoContent` (404 no body); remove `TokenProofAPI`, `TokenRootAPI`, `TokenRequestsAPI` from the API record
- [ ] T027 [US1] Update `tokenHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to construct the new `TokenResponse` with state UTxO inclusion proof + per-cage requests completeness witness via `enumerateAtScriptPrefix`
- [ ] T028 [US1] Update `factHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to dispatch present/absent and return the matching response with status code
- [ ] T029 [US1] Remove `tokenProofHandler`, `tokenRootHandler`, `tokenRequestsHandler` from `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T030 [US1] Add typed `TokenResponse`, `FactPresentResponse`, `FactAbsentResponse` to `cardano-mpfs-client/lib/Cardano/MPFS/Client/Read.hs`
- [ ] T031 [US1] Add `verifyTokenResponse :: TrustedRoot -> Blueprint -> TokenIdJSON -> TokenResponse -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Read.hs`
- [ ] T032 [P] [US1] Add `verifyFactPresentResponse` and `verifyFactAbsentResponse` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Read.hs`
- [ ] T033 [P] [US1] Honest fixture + forgery corpus for `verifyTokenResponse` (forgeries: wrong root, address mismatch, NFT policy mismatch, NFT name mismatch, malformed datum, completeness invalid) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Read/TokenSpec.hs`
- [ ] T034 [P] [US1] Honest fixtures + forgery corpus for `verifyFactPresentResponse`/`verifyFactAbsentResponse` (MPF inclusion forge, exclusion forge for present key) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Read/FactSpec.hs`
- [ ] T035 [P] [US1] E2E spec `TokenReadSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/` covering `GET /tokens/:id` honest + forged responses

**Checkpoint**: US1 complete — oracle can read any cage trust-minimised. **MVP** is shippable here for read-only consumers.

---

## Phase 4: User Story 2 — Requester submits an unsigned-tx request with verifiable inputs (Priority: P1)

**Goal**: All requester-side write endpoints (`POST /tx/boot`, `POST /tx/requester/{insert,delete,update,retract}`) return the uniform `UnsignedTxResponse`; client verifies every consumed input is one they recognise before signing.

**Independent Test**: spec US2 acceptance scenarios — call any requester endpoint, decode unsigned tx, cross-check inputs.

- [ ] T036 [US2] Lean predicate `unsignedTxInputCoverValid` in `lean/Phase4/ProofRedesign.lean` covering the input-cover invariant
- [ ] T037 [P] [US2] Lean theorem `forge_input_not_covered_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T038 [P] [US2] Lean theorem `forge_extra_input_in_response_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [~] T039 [US2] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`: move request endpoints to `/tx/requester/{insert,delete,update,retract}`; keep `/tx/boot` at top level; all return `UnsignedTxResponse`. **Boot subset done** (this slice); requester paths await their own slice.
- [X] T040 [US2] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs` to construct the uniform `UnsignedTxResponse` with `inputs` covering every spent and reference input (re-uses `mkBootTxResponse` rather than touching the builder itself, since the funding inputs are already plumbed; the helper now returns `UnsignedTxResponse`).
- [ ] T041 [P] [US2] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs` (covers insert, delete, update-value) to construct the uniform `UnsignedTxResponse`
- [ ] T042 [P] [US2] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Retract.hs` to construct the uniform `UnsignedTxResponse`
- [~] T043 [US2] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` `bootHandler`, `requestInsertHandler`, `requestDeleteHandler`, `requestUpdateHandler`, `retractHandler` to return the new shape on the new paths; remove the old `BootTxResponse`/`RequestTxResponse`/`RetractTxResponse` plumbing. **Boot handler done** (this slice); requester handlers await their own slice; legacy types stay alive until Polish T095.
- [ ] T044 [US2] Add `UnsignedTxResponse` to `cardano-mpfs-client/lib/Cardano/MPFS/Client/Write.hs`
- [ ] T045 [US2] Add `verifyUnsignedTxResponse :: TrustedRoot -> Blueprint -> UnsignedTxResponse -> Either VerifyError ()` (input-cover only, no per-cage extras) in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Write.hs`
- [ ] T046 [P] [US2] Honest fixtures + forgery corpus (wrong root, input-not-covered, extra-input-in-response, txout-cbor mismatch, decode-fail) for `verifyUnsignedTxResponse` in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Write/BaseSpec.hs`
- [ ] T047 [P] [US2] E2E spec `RequesterWriteSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/` covering boot + each requester endpoint, honest + forged

**Checkpoint**: US2 complete — requesters can submit verifiable requests for any operation; combined with US1 the read+request loop is trust-minimised.

---

## Phase 5: User Story 3 — Oracle signs an update batch with cryptographic fairness (Priority: P1)

**Goal**: `POST /tx/oracle/update` returns `UnsignedTxResponse` with `requests_completeness_proof` populated; client verifies the proof attests the full pending request set so a server-curated subset is detectable.

**Independent Test**: spec US3 acceptance scenarios including the malicious-server scenario (3): hide a pending request, verifier rejects with `CompletenessProofInvalid` or `CompletenessMissingLeaf`.

- [ ] T048 [US3] Lean predicate extension `unsignedTxUpdateValid` in `lean/Phase4/ProofRedesign.lean` adding `requestsCompletenessProofValid` over the per-cage prefix
- [ ] T049 [P] [US3] Lean theorem `forge_hidden_pending_request_breaks_update_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T050 [P] [US3] Lean theorem `forge_consumed_input_outside_attested_set_breaks_update_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T051 [US3] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` to mount `TxUpdateAPI` at `/tx/oracle/update`; ensure response type is `UnsignedTxResponse` with the optional field present
- [ ] T052 [US3] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Update.hs` to attach the per-cage `requests_completeness_proof` via `enumerateAtScriptPrefix` for the per-cage request address
- [ ] T053 [US3] Update `updateHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to surface the new field
- [ ] T054 [US3] Add `verifyUnsignedTxUpdateExtras :: TrustedRoot -> Blueprint -> TokenIdJSON -> UnsignedTxResponse -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Write.hs`
- [ ] T055 [P] [US3] Honest fixture + forgery corpus for the update extras (hidden pending request, extra leaf, consumed input outside attested set) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Write/UpdateSpec.hs`
- [ ] T056 [P] [US3] E2E spec `OracleUpdateSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/` covering honest update + forgeries

**Checkpoint**: US3 complete — the central new fairness property of this redesign is shipped and verifiable.

---

## Phase 6: User Story 4 — Oracle ends a cage with verifiable empty-pending state (Priority: P2)

**Goal**: `POST /tx/oracle/end` requires the per-cage request address to be empty and ships a completeness proof attesting it.

**Independent Test**: spec US4 acceptance scenarios — end on empty cage succeeds; end on cage with pending request is rejected.

- [ ] T057 [US4] Lean predicate `unsignedTxEndValid` in `lean/Phase4/ProofRedesign.lean` requiring `requestsCompletenessEmpty`
- [ ] T058 [P] [US4] Lean theorem `forge_non_empty_completeness_breaks_end_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T059 [US4] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` to mount `TxEndAPI` at `/tx/oracle/end`; ensure response type is `UnsignedTxResponse` with the optional field present
- [ ] T060 [US4] Update `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/End.hs` to attach the per-cage `requests_completeness_proof` (empty leaf set) and refuse to build when the per-cage address is non-empty
- [ ] T061 [US4] Update `endHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T062 [US4] Add `verifyUnsignedTxEndExtras :: TrustedRoot -> Blueprint -> TokenIdJSON -> UnsignedTxResponse -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Write.hs` requiring the witness to be empty
- [ ] T063 [P] [US4] Honest empty-set fixture + forgery (non-empty witness, completeness-required-but-missing) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Write/EndSpec.hs`
- [ ] T064 [P] [US4] E2E spec `OracleEndSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/` covering empty-cage end + refused-end with pending request

**Checkpoint**: US4 complete — destructive operation is protected by the same fairness primitive as update.

---

## Phase 7: User Story 5 — Cage discovery via verifiable global state listing (Priority: P2)

**Goal**: `GET /tokens` returns the full set of UTxOs at the global state validator address with a single completeness proof; client classifies legitimate vs garbage.

**Independent Test**: spec US5 acceptance scenarios — verify completeness, decode each entry, classify.

- [ ] T065 [US5] Lean predicate `tokensListResponseValid` in `lean/Phase4/ProofRedesign.lean` covering completeness over the global state script address
- [ ] T066 [P] [US5] Lean theorem `forge_extra_token_in_listing_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T067 [P] [US5] Lean theorem `forge_missing_token_in_listing_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T068 [US5] Add `TokensListResponse` type with JSON instance in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`
- [ ] T069 [US5] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` `TokensAPI` to return `TokensListResponse`
- [ ] T070 [US5] Update `tokensHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to enumerate UTxOs at the global state script address with completeness via `enumerateAtScriptPrefix`
- [ ] T071 [US5] Add typed `TokensListResponse` to `cardano-mpfs-client/lib/Cardano/MPFS/Client/Read.hs`
- [ ] T072 [US5] Add `verifyTokensListResponse :: TrustedRoot -> Blueprint -> TokensListResponse -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Read.hs`
- [ ] T073 [P] [US5] Honest fixture + forgery corpus (extra leaf, missing leaf, address mismatch on entry) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Read/TokensListSpec.hs`
- [ ] T074 [P] [US5] E2E spec `TokensListSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/`

**Checkpoint**: US5 complete — discovery is trust-minimised.

---

## Phase 8: User Story 6 — Public sweep of non-legitimate UTxOs at the global state address (Priority: P2)

**Goal**: `POST /tx/sweep` (top-level) builds an unsigned tx that spends a non-legitimate UTxO at the global state address with no required signer.

**Independent Test**: spec US6 acceptance scenarios — anyone-can-spend non-legit UTxO; tx body has no required `extra_signatories`.

- [ ] T075 [US6] Add `GlobalSweepRequest` type (`{ utxo_ref, refund_address }`) with JSON instance in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`
- [ ] T076 [US6] Add `TxGlobalSweepAPI` to `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` mounted at `POST /tx/sweep`; the per-cage owner sweep moves to `POST /tx/oracle/sweep`
- [ ] T077 [US6] Add `cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/GlobalSweep.hs` with `buildGlobalSweep` returning `UnsignedTxResponse`
- [ ] T078 [US6] Update `sweepHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` (per-cage owner sweep) and add `globalSweepHandler` for the public path
- [ ] T079 [P] [US6] Honest fixture + forgery for `verifyUnsignedTxResponse` covering a global-sweep payload in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Write/SweepSpec.hs`
- [ ] T080 [P] [US6] E2E spec `PublicSweepSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/` covering anyone-can-spend at the global state address

**Checkpoint**: US6 complete — public sweep is implemented and verifiable.

---

## Phase 9: User Story 7 — Verifiable confirmation of a submitted transaction (Priority: P3)

**Goal**: `GET /tx/:txId?timeout=N` returns `ConfirmResponse` with a CSMT inclusion proof on success; HTTP 408 no body on timeout.

**Independent Test**: spec US7 acceptance scenarios — submit tx, await, verify offline.

- [ ] T081 [US7] Lean predicate `confirmResponseValid` in `lean/Phase4/ProofRedesign.lean`
- [ ] T082 [P] [US7] Lean theorem `forge_confirm_ref_breaks_validity` in `lean/Phase4/ProofRedesign.lean`, no `sorry`
- [ ] T083 [US7] Add `ConfirmResponse` type with JSON instance in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`
- [ ] T084 [US7] Update `cardano-mpfs-api/lib/Cardano/MPFS/API.hs` `TxAwaitAPI` to return `UVerb '[ WithStatus 200 ConfirmResponse, WithStatus 408 NoContent ]`
- [ ] T085 [US7] Update `awaitHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to return `ConfirmResponse` on success
- [ ] T086 [US7] Add typed `ConfirmResponse` to `cardano-mpfs-client/lib/Cardano/MPFS/Client/Read.hs`
- [ ] T087 [US7] Add `verifyConfirmResponse :: TrustedRoot -> Hex -> ConfirmResponse -> Either VerifyError ()` in `cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Read.hs`
- [ ] T088 [P] [US7] Honest fixture + forgery (wrong txid, ref.tx_ix != 0) in `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/Read/ConfirmSpec.hs`
- [ ] T089 [P] [US7] E2E spec `ConfirmSpec.hs` in `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/`

**Checkpoint**: US7 complete — submission confirmation is trust-minimised.

---

## Phase 10: Polish & Cross-cutting Concerns

**Purpose**: trim removed surfaces, regenerate docs, migrate downstream consumers, re-check constitution gates, run local CI gate.

- [ ] T090 Remove `TokenRootAPI`, `TokenProofAPI`, `TokenRequestsAPI`, `UtxoResolveAPI`, `UtxoProofAPI`, `UtxoRootAPI` from `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
- [ ] T091 Remove the corresponding handlers and helpers from `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- [ ] T092 Trim `StatusResponse` in `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`: drop `checkpointSlot`, `checkpointBlockId`, `currentUtxoRoot`; update JSON instances accordingly
- [ ] T093 Update `statusHandler` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs` to emit only tip fields
- [ ] T094 Re-export new client surface from `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs`: `TrustedRoot`, `Blueprint`, every new response type, every new verifier
- [ ] T095 Drop the old per-endpoint `*TxResponse` types (`BootTxResponse`, `RequestTxResponse`, `RetractTxResponse`, `RejectTxResponse`, `UpdateTxResponse`, `SweepTxResponse`, `EndTxResponse`) from `cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs` and the old `ProofResponse`/`RequestsResponse`
- [ ] T096 Drop `Cardano.MPFS.Client.Bundle` if its types no longer have callers; otherwise update for the new shapes
- [ ] T097 Run `nix develop --quiet -c just update-swagger` and commit the regenerated `docs/assets/swagger.json`
- [ ] T098 Update `docs/architecture/overview.md` to describe the post-split trust model: server is non-authoritative for UTxO-CSMT root, proofs ride with data, role-based write paths
- [ ] T099 [P] Update `docs/architecture/testing.md` with the new honest-fixture + forgery-corpus pattern for each verifier
- [ ] T100 Migrate MOOG to the new endpoint paths and verifier signatures (verify with MOOG's e2e harness; out-of-tree change tracked in MOOG's repo)
- [ ] T101 [P] Migrate harvest to the new endpoint paths and verifier signatures (out-of-tree)
- [ ] T102 [P] Migrate mpfs-explorer to the new endpoint paths (out-of-tree)
- [ ] T103 Re-evaluate Constitution Check in `specs/243-proof-redesign/plan.md` after implementation; document any deviations in the Complexity Tracking table
- [ ] T104 Run `nix develop --quiet -c just ci` locally; resolve any failures
- [ ] T105 Update PR description to enumerate new shapes, removed endpoints, role-based paths, completeness-witness coverage; link to spec, plan, research, contracts, quickstart

---

## Dependencies & Story Completion Order

- **Phase 1 (Setup)** → blocks Phase 2.
- **Phase 2 (Foundational)** → blocks every story phase. Constitution X requires Lean predicates and theorems before any Haskell module they govern is accepted.
- **Phase 3 (US1)** is the MVP — independent of US2–US7. After T035 the read-only consumer flow is shippable.
- **Phase 4 (US2)** depends only on Phase 2; independent of US1.
- **Phase 5 (US3)** depends on Phase 4 (it extends the uniform write verifier).
- **Phase 6 (US4)** depends on Phase 4 (same write shape) and Phase 2 (empty-prefix completeness primitive).
- **Phase 7 (US5)** depends only on Phase 2.
- **Phase 8 (US6)** depends on Phase 4 (uniform write shape).
- **Phase 9 (US7)** depends only on Phase 2.
- **Phase 10 (Polish)** depends on every preceding phase.

Within a story phase, Lean tasks (T-Lean) MUST land before the Haskell module they govern (T-API, T-Server, T-Client, T-Test) per constitution X.

## Parallel execution opportunities

- **Within Phase 2**: T005, T006, T007 in parallel (separate Lean theorems); T015 in parallel with type additions; T002, T003 in parallel.
- **Within US1 (Phase 3)**: T019, T020, T022, T023 in parallel (separate Lean theorems); T032, T033, T034, T035 in parallel (separate test files / e2e specs).
- **Within US2 (Phase 4)**: T037, T038 in parallel (Lean theorems); T041, T042 in parallel (separate TxBuilder modules); T046, T047 in parallel (test + e2e files).
- **Within US3 (Phase 5)**: T049, T050 in parallel; T055, T056 in parallel.
- **Within US4 (Phase 6)**: T058 alone for Lean; T063, T064 in parallel.
- **Within US5 (Phase 7)**: T066, T067 in parallel; T073, T074 in parallel.
- **Within US6 (Phase 8)**: T079, T080 in parallel.
- **Within US7 (Phase 9)**: T088, T089 in parallel.
- **Across stories (after Phase 2)**: US1, US2, US5, US7 phases can run on parallel branches if multi-agent execution is desired; US3 must wait for US2; US4 must wait for US2 and Phase 2's empty-prefix primitive.

## Implementation strategy

- **MVP (US1 only)** — read-only trust-minimised consumers. Ship after Phase 3 if downstream signing flows can lag.
- **First full release** — Phases 1–5 + Polish slice. Read + requester writes + oracle update with completeness. The central fairness property of the redesign is in.
- **Second release** — add Phases 6–9 (end, discovery, public sweep, confirmation).
- **Throughout**: every push runs `just ci` locally before being pushed (constitution VI + workflow rule). The `prop_matchesLeanReference` cross-target QuickCheck (constitution IX) extends naturally as new predicates land in Phase 2 and per-story phases.

## Validation

- Total tasks: **105**.
- Per-phase counts: Setup 3 (T001–T003); Foundational 14 (T004–T017); US1 18 (T018–T035); US2 12 (T036–T047); US3 9 (T048–T056); US4 8 (T057–T064); US5 10 (T065–T074); US6 6 (T075–T080); US7 9 (T081–T089); Polish 16 (T090–T105).
- Independent test criteria: each story phase ends with at least one E2E spec exercising honest + forged inputs.
- Format check: every task is `- [ ] T### [P?] [US?] description with file path` (Setup/Foundational/Polish omit `[US]`).
