# Plan — 387

## Strategy

`boot` established the accepted shape in the July commit: sort wallet rows by
lovelace descending, reserve the largest row for collateral only, spend the
rest, and reject a one-row wallet. The remaining work applies that same shape
to `retract` and closes two gate defects measured on 2026-09-01/02.

## Constraints

- `cardano-mpfs-cage-tx` must stay compatible with native GHC and
  `wasm32-wasi`; no `IO`, networking, filesystem, or clock in builder paths.
- `cardano-foundation/moog` is a read-only consumer boundary for this ticket.
- Datum, redeemer, and script-integrity construction must stay compatible with
  the Aiken validators in `cardano-mpfs-onchain`.
- `retract`'s reserved collateral row must still satisfy the Conway collateral
  requirement, which is why the largest row — not an arbitrary one — is
  reserved.

## Slices (bisect-safe, ordered)

- **S-INV3** — R-3. Test-only. Add the named example that drives the
  evaluator's collateral-resolution guard to its rejecting branch. No
  production change; passes on the current tree once written.
- **S-RETRACT** — R-1, R-2, R-4, R-5 for `retract`. Behaviour-changing.
  Includes re-authoring the pre-change assertion and fixture in
  `RetractSpec.hs`, which positively pin the overlap this ticket removes.

## Live boundary

The composed `moog-v2` build is the live consumer boundary. It currently fails
for both PR #387 and offchain `main` with one identical GHC error caused by
the `cardano-mpfs-onchain` pin skew, so it can only establish
*attributable-clean* status for this ticket. The full boundary proof reruns
after M2-T101 enforces C-DEP-PINS.

## Constitution check

Ledger-native types only; no shadow ledger representation; builders stay pure
and WASM-portable; the server remains a fact provider and is untouched. Passes
before and after design.
