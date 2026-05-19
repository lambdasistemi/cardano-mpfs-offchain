# Tasks: Request-Delete Fact Provider Pivot

- [ ] T001 Add `RequestDeleteFacts` wire type, JSON instances, and Swagger schema.
- [ ] T002 Add shared/offchain `FactsRequestDeleteAPI` and server handler for `POST /facts/request/delete`.
- [ ] T003 Add client `VerifiedRequestDeleteFacts`, `verifyRequestDeleteFacts`, and verifier tests for happy path, snapshot tamper, trusted-root mismatch, and wallet proof tamper.
- [ ] T004 Add `requestDeleteCageTx` with byte-equality proof against `legacy-request-delete.cbor`.
- [ ] T005 Remove legacy `POST /tx/request/delete` from shared API, offchain server, client wrapper, active docs, and tests while preserving request-update and later write routes.
- [ ] T006 Regenerate Swagger and prove `/facts/request/delete` exists while `/tx/request/delete` is absent.
- [ ] T007 Record MOOG boundary status for request-delete in issue #265 and parent epic #257.
- [ ] T008 Run `./gate.sh`, review the branch, then drop `gate.sh` only at final ready-for-review time.
