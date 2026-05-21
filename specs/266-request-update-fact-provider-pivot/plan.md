# Implementation Plan: Request-Update Fact Provider Pivot

## Architecture

Request-update is a Tier 1 facts endpoint. The server performs one atomic
indexer read for the current snapshot and the requester wallet UTxOs at the
submitted address, then queries protocol parameters and chooses the current
submission timestamp. It does not build or return a transaction.

The wire shape mirrors request-delete with one additional value:

- `snapshot`
- `token`
- `key`
- `old_value`
- `new_value`
- `address`
- `submitted_at`
- `wallet_utxos`
- `protocol_parameters`

The client verifier checks the trusted-root and wallet UTxO CSMT proofs only.
The submitted timestamp and protocol parameters remain unverified inputs that
are constrained later by wallet policy inside `requestUpdateCageTx`.

The local builder extends `Cardano.MPFS.Client.Cage.Request` and reuses the
existing request builder path with `OpUpdate oldValue newValue`. Byte equality
is proved against a captured `legacy-request-update.cbor` vector using the
same deterministic fixture shape as request-insert and request-delete.

## Shared Surfaces

- `cardano-mpfs-api/lib/Cardano/MPFS/API.hs`
- `cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Request.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`
- `cardano-mpfs-client/lib/Cardano/MPFS/Client.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/RequestUpdateFactsSpec.hs`
- `cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/RequestSpec.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`
- `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types/Facts.hs`
- `cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/RequestUpdateFactsSpec.hs`
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`
- `docs/assets/swagger.json`
- `gate.sh`

Do not change reject, oracle update, or sweep except where removing
request-update from the shared legacy write API requires tuple-order fixes.

## Orchestrator / Worker Split

The ticket-owner orchestrator owns this plan, task list, gate growth, PR body,
MOOG boundary note, review, and final verification. Behavior-changing Haskell
source, tests, fixtures, Swagger generation, and matrix implementation are
owned by visible Codex tmux workers. Each worker produces one bisect-safe
commit with a `Tasks:` trailer. The orchestrator reviews the diff, reruns the
focused proof or `./gate.sh`, then stamps the matching `tasks.md` checkboxes by
amending the worker commit.

## Vertical Slices

1. Client facts/verifier slice: add request-update facts DTO, JSON/schema,
   opaque verifier witness, exports, and focused verifier tests.
2. Cage/golden slice: capture `legacy-request-update.cbor`, add
   `requestUpdateCageTx`, and prove byte equality.
3. Server hard-swap slice: add `POST /facts/request/update`, remove
   `/tx/request/update`, update typed HTTP wrappers, HTTP tests, and Swagger.
4. Local-cluster matrix slice: add a request-update row and include the legacy
   route absence check at the live WAI boundary.
5. MOOG/status/finalization slice: record boundary status, run the gate, and
   prepare the branch for parent finalization.

## Verification

- Focused verifier RED/GREEN:
  `nix develop --quiet -c just unit-client "/verifyRequestUpdateFacts/"`.
- Focused cage RED/GREEN:
  `nix develop --quiet -c just unit-client "/requestUpdateCageTx/"`.
- Focused HTTP route RED/GREEN:
  `nix develop --quiet -c just unit-offchain "/POST /facts/request/update/"`.
- Focused matrix proof:
  `nix develop --quiet -c just e2e-facts-matrix`.
- Swagger refresh:
  `nix develop --quiet -c just update-swagger`.
- Final PR gate:
  `./gate.sh`.

## Live-Boundary Smoke Decision

The unit suites prove JSON, proof replay, route wiring, and byte equality, but
they cannot prove the live server boundary fetches facts, verifies them, builds
locally, submits, and observes the indexer. The #278 matrix is the required
live-boundary smoke for this ticket. It is slow, so `gate.sh` carries static
presence/absence and wiring checks, while the matrix command must be run and
recorded before the PR leaves draft.

## MOOG Boundary

Request-update boundary status is recorded as deferred to
cardano-foundation/moog#96 unless a request-update canary/staged-port proof is
created before finalization. The PR must not claim MOOG production readiness;
#275 remains the boundary blocker for such a claim.
