# Tasks: Request-Insert Fact Provider Pivot

- [ ] T001 Add `RequestInsertFacts` wire type, JSON instances, and Swagger schema.
- [ ] T002 Add shared/offchain `FactsRequestInsertAPI` and server handler for `POST /facts/request/insert`.
- [ ] T003 Add client `VerifiedRequestInsertFacts`, `verifyRequestInsertFacts`, and verifier tests for happy path, snapshot tamper, trusted-root mismatch, and wallet proof tamper.
- [ ] T004 Add `requestInsertCageTx` with byte-equality proof against `legacy-request-insert.cbor`.
- [ ] T005 Remove legacy `POST /tx/request/insert` from shared API, offchain server, client wrapper, and tests while preserving delete/update request routes.
- [ ] T006 Regenerate Swagger and prove `/facts/request/insert` exists while `/tx/request/insert` is absent.
- [ ] T007 Record MOOG boundary status for request-insert in issue #264 and parent epic #257.
- [ ] T008 Run `./gate.sh`, review the branch, then drop `gate.sh` only at final ready-for-review time.
