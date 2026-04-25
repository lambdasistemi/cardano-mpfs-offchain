# Research: E2E /tx/reject proof verification

## Decision: wait past the existing devnet deadline

`ProofsSpec` already uses a test-only `CageConfig` with:

- `defaultProcessTime = 5_000`
- `defaultRetractTime = 5_000`

`rejectRequestsImpl` filters pending requests with:

```text
now > requestSubmittedAt + stateProcessTime + stateRetractTime
```

Because the scenario already submits and awaits a second pending request
before exercising write endpoints, the smallest reliable path is to wait
slightly past `defaultProcessTime + defaultRetractTime` and then build
the reject transaction through HTTP.

## Alternatives considered

### Add a devnet-only config knob

Rejected for this ticket. The local deadline is already 10 seconds, so a
new knob would add production API or harness plumbing for only a few
seconds of savings.

### Use CageFlowSpec instead

Rejected. `CageFlowSpec` already proves a submitted reject transaction
can consume expired requests, but issue #224 is specifically about the
proof-bearing HTTP response in `ProofsSpec` and the client verifier DSL.

### Hand-crafted client fixture only

Rejected. The client fixture already covers `RejectTxResponse`; the
missing coverage is the server HTTP path.

## Runtime impact

The scenario adds one wait just past the 10 second reject deadline. The
expected runtime increase is about 11 to 12 seconds, plus normal devnet
block/indexing jitter.
