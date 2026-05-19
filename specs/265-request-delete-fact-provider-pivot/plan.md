# Implementation Plan: Request-Delete Fact Provider Pivot

## Architecture

This is a Tier 1 facts endpoint. The server performs one atomic indexer
read for the current snapshot and wallet UTxOs at the requester address,
then queries protocol parameters. It does not build or return a
transaction.

The client verifier checks:

- trusted root is 32 bytes,
- snapshot root is 32 bytes and equals the trusted root,
- every wallet UTxO witness replays against that root.

The client builder decodes the verified wallet UTxOs and protocol
parameters, applies `WalletPolicy`, builds the request-delete
transaction locally, and returns `Tx ConwayEra` for wallet signing.

## Shared Surfaces

- `Cardano.MPFS.API` and `Cardano.MPFS.HTTP.API`
- `Cardano.MPFS.API.Types.Facts`
- `Cardano.MPFS.HTTP.Server`
- `Cardano.MPFS.Client.Facts`
- `Cardano.MPFS.Client.Http`
- `Cardano.MPFS.Client.Cage.Request`
- `docs/assets/swagger.json`

Do not touch request-update or later Tier 2/Tier 3 routes except to keep
their legacy routes compiling after request-delete is removed from the
shared write API.

## Verification

- Focused client facts/verifier tests.
- Focused client cage builder byte-equality test.
- Focused HTTP server facts test.
- Swagger freshness.
- `./gate.sh`.

## MOOG Boundary

This offchain PR records boundary status only. MOOG v2 experiments are
blocked as production readiness evidence by #275 until that replay/serving
bug is fixed or superseded by a documented rollout decision.
