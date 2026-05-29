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

- [X] T289-S3 `insertFact` (`/facts/request/insert`), `updateFact`
  (`/facts/request/update`), `deleteFact` (`/facts/request/delete`)
  over `runFactsWorkflow`; re-export `InsertRequest`,
  `UpdateValueRequest`, `DeleteRequest`. Tests: routing + body per op,
  verify-error propagation per op.

## Slice S4 — oracle apply

- [X] T289-S4 `applyRequests` (`/facts/update`, `UpdateRequest`,
  `verifyUpdateFacts`, `updateCageTx`). Tests: routing + body +
  verify-error propagation.

## Slice S5 — retract + reject

- [X] T289-S5 `retractRequest` (`/facts/retract`, `RetractRequest`,
  `verifyRetractFacts`, `retractCageTx`) and `rejectExpired`
  (`/facts/reject`, `RejectRequest`, `verifyRejectFacts`,
  `rejectCageTx`). Tests: routing + body + verify-error per op.

## Slice S6 — end cage

- [X] T289-S6 `endCage` (`/facts/end`, `EndRequest`,
  `verifyEndFacts wcCage wcTrustedRoot`, `endCageTx`). Tests: routing +
  body + verify-error propagation.

## Slice S7 — live-boundary integration through /submit

- [X] T289-S7 `Cardano.MPFS.E2E.WorkflowsIntegrationSpec`: one e2e row
  per workflow, each driving the real `cardano-mpfs-workflows.<name>`
  against a live devnet via an in-process WAI `HttpClient`, decoding
  the `UnsignedTx`, signing with the genesis key, POSTing to `/submit`
  (#288), awaiting the txId, and asserting the on-chain effect (token
  indexed / request queued / fact materialised / request drained /
  token burned). No scaffolding fakes. Run:
  `just e2e WorkflowsIntegration` (8 examples, 0 failures). #288 merged
  to main, so this landed in this PR rather than as a follow-on.

## Notes

- `applyRequests` (8th workflow) added per operator decision
  (2026-05-29): the brief named 7; the oracle update is the 8th.
- No `http-client` / `servant-*` / `cardano-ledger-*` in the workflows
  package — enforced by the cabal dep list and `-Wunused-packages`.
