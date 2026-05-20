# Tasks: Local-Cluster Facts API Coverage Matrix

- [x] T000 Bootstrap issue #278 worktree, gate, draft PR, and orchestration contract.
- [x] T001 Add a named E2E matrix entry point for migrated facts endpoints.
- [x] T002 Implement the boot matrix row or adapt the existing boot facts E2E into the shared matrix shape.
- [x] T003 Implement the request-insert matrix row via `POST /facts/request/insert -> verifyRequestInsertFacts -> requestInsertCageTx -> submit -> request indexed`.
- [x] T004 Implement the request-delete matrix row via `POST /facts/request/delete -> verifyRequestDeleteFacts -> requestDeleteCageTx -> submit -> delete request indexed`, including the process/update proof needed to observe fact removal.
- [x] T005 Strengthen the end row so `/facts/end -> verifyEndFacts -> endCageTx -> submit -> token removed` is proved with the same matrix columns.
- [x] T006 Add live HTTP legacy-route absence checks for migrated rows.
- [x] T007 Expose the matrix through a named command or documented test selector and wire the practical part into `./gate.sh`.
- [x] T008 Run the focused matrix and `./gate.sh`, then record the command/transcript evidence in the PR before finalization.
