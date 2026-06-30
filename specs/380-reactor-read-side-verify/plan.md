# Implementation Plan: Reactor Read-Side Verify Ops

## Scope

Add read-side operation arms to
`cardano-mpfs-verify/lib/Cardano/MPFS/Client/Verify/Reactor.hs`, backed by
the existing read verifier surface in
`Cardano.MPFS.Client.Verify.Read` and proof replay primitives in
`Cardano.MPFS.Client.Verify.Replay`.

## Design

- Keep the envelope shape used by write-side operations:
  `{ op, trusted_root, facts, cage_config? }`.
- Decode `facts` into the raw API response type for each operation:
  `TokensResponse`, `RequestsResponse`, `FactResponse`, and
  `FactsResponse`.
- Parse `cage_config` for `verify_tokens` and `verify_snapshot`
  because those paths verify prefix completeness using cage/request
  prefixes.
- Include the token id in the envelope for `verify_snapshot` if needed
  by `verifyTokenRequests`; the raw `/tokens/:id/requests` body does not
  repeat the path token id.
- Prefer existing `verifyTokens`, `verifyTokenRequests`, and
  `verifyTokenFacts` wrappers. For per-fact inclusion, wrap the existing
  anchoring/replay primitives already used by read/write verification;
  if the exported function is not obvious, file a Q with the ticket
  orchestrator instead of inventing new proof logic.
- Preserve verdict rendering through `run` or a local equivalent that
  returns exactly `verify_ok`, `verify_error: ...`, `bad_facts: ...`,
  `bad_envelope: ...`, or `unknown_op: ...`.

## Test Strategy

- Extend `cardano-mpfs-client/test/Cardano/MPFS/Client/Verify/ReactorSpec.hs`.
- Capture actual non-empty responses from `https://umpfs.plutimus.com`:
  `/tokens`, `/tokens/:id`, `/tokens/:id/facts/:key`,
  `/tokens/:id/facts`, and `/tokens/:id/requests`.
- Store the captured real-data fixtures in the client test tree. The
  fixtures must include the trusted root and cage config used for the
  envelope.
- RED first: add tests for the new ops before reactor dispatch handles
  them; they should fail with `unknown_op`.
- GREEN: add dispatch arms and only the minimum wrapper/export changes
  required to make the tests pass.
- Tamper each fixture in a way that keeps JSON decoding valid and
  forces a verifier failure, not `bad_facts`.

## Verification Commands

- `nix develop --quiet -c just unit-client "runEnvelope"`
- `nix build .#wasm-mpfs-verify --fallback`
- `./gate.sh`

## Risks

- The exact per-fact inclusion wrapper may require a small exported
  read-side function in `Read.hs` because the raw `FactResponse` combines
  trusted-root anchoring with MPF inclusion proof replay. That wrapper is
  acceptable only if it composes existing helpers/primitives and adds no
  new proof algorithm.
- Live UMPFS data can change. Fixtures should be committed as captured
  bytes/JSON, with provenance comments in the test module, so tests are
  deterministic after capture.

## Slices

1. `S1`: Add reactor read-side operations and real-data reactor tests.
2. `S2`: Final audit, PR metadata, and drop the temporary `gate.sh`.
