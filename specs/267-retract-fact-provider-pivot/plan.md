# Implementation Plan: Retract Fact Provider Pivot

## Architecture

This is a Tier 2 facts endpoint. The server performs one atomic
indexer read for the current snapshot, the named request UTxO at the
per-cage request address, the state UTxO at the cage state address,
and the wallet UTxOs at the requester address. It then queries
protocol parameters and converts the request's Phase 2 POSIX time
window into a slot interval. It does not build or return a
transaction.

The client verifier checks:

- trusted root is 32 bytes,
- snapshot root is 32 bytes and equals the trusted root,
- the named request UTxO witness replays against that root,
- the state UTxO witness replays against that root,
- every wallet UTxO witness replays against that root.

The client builder decodes the verified request and state UTxOs,
extracts the on-chain request datum's owner key hash, applies
`WalletPolicy`, builds the retract transaction locally with the
server-derived Phase 2 validity interval, and returns
`Tx ConwayEra` for owner signing.

The server-derived validity slot bounds are unverified facts: a
malicious server can only push the local builder to produce a
transaction whose Phase 2 validity window fails on-chain. This is
fail-closed and matches the existing treatment of unverified
protocol parameters.

## Shared Surfaces

- `Cardano.MPFS.API` and `Cardano.MPFS.HTTP.API`
- `Cardano.MPFS.API.Types.Facts`
- `Cardano.MPFS.HTTP.Server`
- `Cardano.MPFS.HTTP.Types.Facts`
- `Cardano.MPFS.Indexer.Reads`
- `Cardano.MPFS.Client.Facts`
- `Cardano.MPFS.Client.Cage.Request` (or a sibling
  `Cardano.MPFS.Client.Cage.Retract`)
- `docs/assets/swagger.json`
- `cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs`

Do not touch request-update or later Tier 2/Tier 3 routes except to
keep their legacy routes compiling after retract is removed from the
shared write API.

## Verification

- Focused client facts/verifier tests for retract.
- Focused client cage builder byte-equality test against
  `legacy-retract.cbor`.
- Focused HTTP server facts test for retract.
- Swagger freshness.
- Extended `./gate.sh` checks: presence of `/facts/retract`, absence
  of `/tx/retract` everywhere.
- Local-cluster #278 matrix extended with a retract row.
- `./gate.sh`.

## MOOG Boundary

This offchain PR records boundary status only. MOOG v2 experiments
are blocked as production readiness evidence by #275 until that
replay/serving bug is fixed or superseded by a documented rollout
decision. The retract operation defers to cardano-foundation/moog#96
for the MOOG-v2 staged port or replacement surface decision.
