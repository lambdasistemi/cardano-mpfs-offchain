# Tasks — 387

## Slice S-INV3 — prove the evaluator's collateral guard (R-3)

- [ ] T387-01 Add a named example that drives the collateral-resolution guard
      to its rejecting branch and asserts the rejection, so the guard is
      proved able to fail rather than only exercised on the happy path.

## Slice S-RETRACT — widen the fix to retract (R-1, R-2, R-4, R-5)

- [ ] T387-02 Re-author the pre-change assertion and one-row fixture in
      `RetractSpec` to the post-change world, preserving the pre-change
      assertion in the RED bundle as the defect witness.
- [ ] T387-03 Add retract coverage for the one-row rejection (R-2), for
      collateral/input disjointness (R-1), and for funding completeness (R-5).
- [ ] T387-04 Reserve a disjoint collateral row in the retract builder and keep
      every other wallet row available as funding.
- [ ] T387-05 Confirm full retract evaluation still succeeds (R-4).

## Ticket-level

- [ ] T387-06 Refresh the PR body to describe the widened branch.
