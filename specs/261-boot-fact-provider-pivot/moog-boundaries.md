# MOOG / MPFS v2 Boundary Map

## Decision

On 2026-05-18 the paired MOOG work stopped being treated as a normal
client migration. The new MPFS on-chain validators change enough of the
state-machine surface that a green compile/unit migration in MOOG is not
evidence that the old MOOG requester, oracle, agent, and operations
flows are compatible.

The current paired MOOG PR is therefore a boundary spike:

- MOOG spike PR: https://github.com/cardano-foundation/moog/pull/95
- Follow-up MOOG boundary issue:
  https://github.com/cardano-foundation/moog/issues/96

This offchain PR remains draft. The offchain boot facts implementation
has local and live e2e evidence, but the cross-repo completion condition
now requires a canary-backed MPFS-v2 boundary proof or an explicit
MOOG-v2 migration decision from the follow-up issue.

## Boundaries

### 1. Wire/API Boundary

Surface:

- `GET /status`
- `POST /facts/boot`
- token read endpoints
- transaction submission
- transaction status polling

This boundary proves that a client can speak to the MPFS-v2 HTTP surface
and receive facts instead of server-built transactions.

Current evidence: the offchain PR proves this for boot through unit and
e2e tests. The MOOG spike compiles and locally gates against the new boot
facts API.

### 2. Transaction-Construction Boundary

Surface:

- MPFS blueprint selection
- wallet address bytes
- policy ID and cage configuration
- `verifyBootFacts`
- `bootCageTx`
- signing and submit shape

This boundary proves that a client can turn verified facts into a
submit-valid transaction body.

Current evidence: offchain e2e proves the shared client builder can
build, sign, submit, and observe boot indexing. The MOOG spike proves
MOOG can call the same builder locally, but it has not submitted a live
MOOG boot transaction against the paired branch.

### 3. On-Chain Semantic Boundary

Surface:

- datum and redeemer shapes
- token IDs and policy assumptions
- valid state transitions
- validator-specific request/update rules

This is the riskiest boundary. The old MOOG tests may fail here even
when the HTTP client and boot builder compile, because the validators
are not just a transport dependency.

Required next evidence: document the old MOOG assumptions about MPFS
token and request state before porting non-boot behavior.

### 4. MOOG Domain Boundary

Surface:

- requester facts
- oracle validation
- agent report/result flow
- Antithesis test-run lifecycle
- encrypted/decrypted report URL handling

This boundary decides whether the old MOOG domain model still maps onto
the new MPFS-v2 state machine.

Required next evidence: migrate one simple request lifecycle after the
boot canary proves the lower boundaries.

### 5. Operations Boundary

Surface:

- release binary
- role wallets
- oracle and agent daemons
- email/report polling
- recovery commands

This boundary must not be pulled into the first port. It belongs after
the canary and first request lifecycle are live-boundary proven.

## Preferred Migration Track

1. Keep PR #95 draft as a boundary spike.
2. Build a tiny MPFS-v2 canary in MOOG or a separate canary component:

   ```text
   wallet -> /status -> /facts/boot -> verify -> bootCageTx -> sign
   -> submit -> wait /tx/:id -> inspect /tokens/:id
   ```

3. Use the canary as the proof target for the boot boundary.
4. Port MOOG slice by slice:

   - Slice 0: read-only status/token/facts plumbing.
   - Slice 1: boot only through the canary-proven path.
   - Slice 2: one simplest request lifecycle through facts/local build.
   - Slice 3: oracle update and validation flow.
   - Slice 4: agent report/result flow.
   - Slice 5: production release and operational scripts.

5. After Slice 2, decide whether to continue the staged port or start a
   replacement MOOG application with a clean MPFS-v2 domain model.

