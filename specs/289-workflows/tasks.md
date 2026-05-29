# Tasks — #289 `cardano-mpfs-workflows`

One slice = one bisect-safe commit. Each commit body carries
`Tasks: T289-S<n>`. Boxes flip to `[X]` in the same amended commit
that the orchestrator accepts.

## Slice S1 — client `serializeCageTx` export

- [X] T289-S1 Add `Cardano.MPFS.Client.Cage.Serialize` exporting
  `serializeCageTx :: Tx ConwayEra -> ByteString`
  (`serialize' (natVersion @11)`); list it in
  `cardano-mpfs-client.cabal` `exposed-modules`; add a client unit test
  asserting a built tx serializes to non-empty CBOR that re-decodes.

## Slice S2 — workflows package + `registerToken`

- [X] T289-S2 Create `cardano-mpfs-workflows/` (cabal: lib + hspec
  test). Wire into `cabal.project`, `nix/project.nix` (component +
  runner + package + app), `flake.nix` packages, `justfile`
  (`unit-workflows` recipe + add to `ci`). Implement
  `Cardano.MPFS.Workflows.Internal` (`HttpClient`, `HttpError`,
  `UnsignedTx`, `WorkflowsConfig`, `WorkflowError`, `runFactsWorkflow`)
  and `Cardano.MPFS.Workflows` exporting the named surface +
  `registerToken`. Tests: stub-`HttpClient` routing (path+body),
  `WorkflowHttpError`, `WorkflowDecodeError`, `WorkflowVerifyError`
  (root mismatch), `WorkflowBuildError` (root-matching empty-wallet
  boot → `EmptyFunding`). Green under `just unit-workflows`.

## Slice S3 — requester request workflows

- [ ] T289-S3 `insertFact` (`/facts/request/insert`), `updateFact`
  (`/facts/request/update`), `deleteFact` (`/facts/request/delete`)
  over `runFactsWorkflow`; re-export `InsertRequest`,
  `UpdateValueRequest`, `DeleteRequest`. Tests: routing + body per op,
  verify-error propagation per op.

## Slice S4 — oracle apply

- [ ] T289-S4 `applyRequests` (`/facts/update`, `UpdateRequest`,
  `verifyUpdateFacts`, `updateCageTx`). Tests: routing + body +
  verify-error propagation.

## Slice S5 — retract + reject

- [ ] T289-S5 `retractRequest` (`/facts/retract`, `RetractRequest`,
  `verifyRetractFacts`, `retractCageTx`) and `rejectExpired`
  (`/facts/reject`, `RejectRequest`, `verifyRejectFacts`,
  `rejectCageTx`). Tests: routing + body + verify-error per op.

## Slice S6 — end cage

- [ ] T289-S6 `endCage` (`/facts/end`, `EndRequest`,
  `verifyEndFacts wcCage wcTrustedRoot`, `endCageTx`). Tests: routing +
  body + verify-error propagation.

## Deferred (NOT in this PR — gated on #288)

- [ ] T289-S7 Live integration test: run a server, exercise each
  workflow end-to-end with real proofs, sign the `UnsignedTx`, POST to
  `/submit`, assert acceptance. **Blocked by #288 (`/submit`).** File
  as a follow-on issue / add when #288 merges. Do not fake submission.

## Notes

- `applyRequests` (8th workflow) added per operator decision
  (2026-05-29): the brief named 7; the oracle update is the 8th.
- No `http-client` / `servant-*` / `cardano-ledger-*` in the workflows
  package — enforced by the cabal dep list and `-Wunused-packages`.
