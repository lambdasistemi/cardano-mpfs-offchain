# Tasks: Request-Update Fact Provider Pivot

- [X] T001 [US1] RED: add request-update facts JSON/schema and verifier tests
      covering round-trip, happy path, snapshot tamper, trusted-root mismatch,
      and wallet proof tamper in
      `cardano-mpfs-client/test/Cardano/MPFS/Client/RequestUpdateFactsSpec.hs`.
- [X] T002 [US1] GREEN: add `RequestUpdateFacts` to
      `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs` and
      `VerifiedRequestUpdateFacts` / `verifyRequestUpdateFacts` to
      `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`, with exports in
      `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs` and test-suite wiring.
- [X] T003 [US1] RED: extend
      `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/RequestSpec.hs` with
      `requestUpdateCageTx` empty-funding, wallet-policy, structural, and
      byte-equality tests against
      `specs/266-request-update-fact-provider-pivot/test-vectors/legacy-request-update.cbor`.
- [X] T004 [US1] GREEN: capture `legacy-request-update.cbor` from the legacy
      request-update shape, add `requestUpdateCageTx` to
      `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Request.hs`, and keep
      request-insert/delete byte vectors passing.
- [X] T005 [US1] RED: add HTTP tests proving `POST /facts/request/update` is
      routed, returns facts without transaction CBOR, documents the facts path,
      and omits `/tx/request/update` in
      `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/RequestUpdateFactsSpec.hs`.
- [X] T006 [US1] GREEN: add `FactsRequestUpdateAPI`,
      `factsRequestUpdateHandler`, `mkRequestUpdateFacts`, typed client
      `requestUpdateFacts`, remove `TxRequestUpdateAPI` /
      `txUpdateValueHandler` / `requestUpdateTx`, regenerate
      `docs/assets/swagger.json`, and keep reject/update/sweep routes intact.
- [X] T007 [US1] RED: extend
      `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
      so the matrix fails without a request-update row and fails if
      `/tx/request/update` remains reachable.
- [X] T008 [US1] GREEN: implement the request-update matrix row:
      `POST /facts/request/update -> verifyRequestUpdateFacts ->
      requestUpdateCageTx -> submit -> request indexed`, then process the
      request if needed to leave later rows with clean preconditions.
- [X] T009 [US1] Record request-update MOOG boundary status in this spec and
      the PR body as deferred to cardano-foundation/moog#96 unless a
      request-update canary/staged-port proof exists.
- [X] T010 [US1] Run focused verifier, cage, HTTP, matrix, and final
      `./gate.sh` verification; leave `gate.sh` present for parent
      finalization.

## Worker Slice Briefs

### Slice A: Request-Update Facts And Verifier

Worker owns T001 and T002. Write RED tests first, observe the focused verifier
test fail for missing request-update facts/verifier support, then implement the
wire type and pure verifier. The verifier surface for request-update must not
import `Cardano.Ledger.Api.Tx`. Do not edit server route behavior or cage
builder code in this slice.

### Slice B: Request-Update Cage Helper And Golden

Worker owns T003 and T004. Write RED cage tests first, capture the golden vector
under `specs/266-request-update-fact-provider-pivot/test-vectors/`, then add
`requestUpdateCageTx` using `OpUpdate oldValue newValue`. Do not remove the
legacy HTTP route in this slice; the vector must be captured before deletion.

### Slice C: Server Hard Swap And Swagger

Worker owns T005 and T006. Write RED HTTP route/Swagger tests first, then add
the facts endpoint and remove only the request-update legacy transaction route
from shared API, offchain server wiring, typed client wrappers, active tests,
and Swagger. Preserve reject, update, and sweep.

### Slice D: Local-Cluster Matrix And Boundary Status

Worker owns T007, T008, and T009. Extend the matrix with request-update
coverage and `/tx/request/update` live absence, run the named matrix command,
and update PR/spec boundary wording. Do not claim MOOG production readiness.
