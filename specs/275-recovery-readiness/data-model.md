# Data model — 275 recovery, liveness, readiness

Fields, relationships, validation, and state invariants. No encodings beyond
the wire shapes that are part of the published contract.

## D-1 `IndexerPhase`

The indexer's coarse phase, as already distinguished by the follower.

| Constructor | Meaning |
|---|---|
| `Restoring` | rebuilding in KV-only mode; the full CSMT is not yet correct |
| `Following` | full CSMT; blocks are applied incrementally |

State invariant: `Restoring → Following` on the follower's existing
within-stability-window transition, and `Following → Restoring` on the
armageddon/rollback-impossible reset. Both directions must be observable;
a one-way encoding reintroduces the #276 latch.

## D-2 `FollowerMode`

`FollowerEnabled | FollowerDisabled`, derived from `followerEnabled`.

Validation: with `FollowerDisabled` there is no chain tip, so currency is
vacuously satisfied and must not be evaluated.

## D-3 `BootStage`

Where the process is in its boot sequence, published by M-3.

| Constructor | Meaning |
|---|---|
| `Booting BootReason` | the listener is up; the application context does not exist yet |
| `Started` | the context is published; readiness is decided from indexer state |

`BootReason` is `Opening | Recovering | Replaying (Maybe Word64)`, the last
carrying remaining replay units when the tracer has reported them.

State invariant: `Booting → Started` exactly once per process. This is a
boot-sequence fact, not a readiness latch — readiness after `Started` is
recomputed per request and may return to false at any time.

## D-4 `ServerPhase`

The published cell M-3 owns and M-2 reads.

| Constructor | Carries |
|---|---|
| `PhaseBooting BootReason` | current boot reason |
| `PhaseServing` | the application context |

Relationship: exactly one transition per process, `PhaseBooting →
PhaseServing`. There is no transition back; a failure terminates the process
instead (INV-R7).

## D-5 `ReadyVerdict`

The result of the readiness decision.

| Constructor | Carries |
|---|---|
| `Ready` | — |
| `NotReady ReadyReason` | why |

`ReadyReason`:

| Value | Wire code | Meaning |
|---|---|---|
| `ReasonOpening` | `opening` | database opening, context not yet built |
| `ReasonRecovering` | `recovering` | CSMT crash recovery in progress |
| `ReasonReplaying` | `replaying` | retained journal replay in progress |
| `ReasonRestoring` | `restoring` | indexer rebuilding from origin |
| `ReasonNoChainTip` | `no-chain-tip` | following, but no tip observed yet |
| `ReasonBehind` | `behind` | following and correct, but outside the stability window of the tip |
| `ReasonProofsUnavailable` | `proofs-unavailable` | proof reads momentarily inconsistent |

Validation: the reason is diagnostic only. Consumers gate on the HTTP status
code; the contract published to M2-T101 says so explicitly, so that adding a
reason later is not a breaking change.

## D-6 `LiveResponse`

Wire shape of `GET /live`, always HTTP 200:

```json
{"live": true}
```

Invariant: no field of this response is derived from indexer or chain state.
A liveness answer that could be affected by recovery is not a liveness answer.

## D-7 `ReadyResponse`

Wire shape of `GET /ready`. HTTP 200 when ready, HTTP 503 otherwise; the body
shape is the same in both cases so a consumer parses one type.

```json
{
  "ready": false,
  "reason": "replaying",
  "checkpoint_slot": 3715222,
  "tip_slot": 123518063,
  "replay_remaining": 1307033
}
```

| Field | Type | Notes |
|---|---|---|
| `ready` | bool | mirrors the status code; the status code is authoritative |
| `reason` | string or null | null exactly when `ready` is true |
| `checkpoint_slot` | int or null | null when no checkpoint is persisted yet |
| `tip_slot` | int or null | null when no tip has been observed |
| `replay_remaining` | int or null | null outside retained journal replay |

Rationale: this body is what preserves operator observability (FR-5) once
`/status` and `/metrics` are gated. It reports progress; it never reports a
root. A 503 carrying progress cannot be mistaken for a successful data read.

## D-8 Gated-route error body

Wire shape returned by the gate for any route outside the allowlist while not
ready, HTTP 503:

```json
{"error": "not ready", "reason": "restoring"}
```

Invariant: the gate never emits 404 for a gated route, and never emits a body
containing a root, a checkpoint-derived proof, or any indexer-resolved value.

## D-9 Readiness relation

`Ready` holds exactly when all of:

1. `BootStage` is `Started`;
2. `IndexerPhase` is `Following`;
3. proof reads are internally consistent;
4. currency holds — either `FollowerDisabled`, or a tip has been observed and
   `checkpointSlot + stabilityWindowSlots >= tipSlot`.

Reason precedence when several fail, most fundamental first: boot stage,
phase, currency, proof consistency. Precedence is fixed so the reported reason
is deterministic and testable.

State invariant (INV-R5): this is a relation over current observations, not a
stored flag. Nothing in the system may cache a `Ready` verdict across
requests.
