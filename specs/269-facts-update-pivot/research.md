# Research: Update Fact Provider Runtime Boundary

## Q-001 Decision

Parent answer `A-001-update-byte-equality-inputs.md` chooses structural
equality plus same-new-root proof instead of literal byte equality for
`updateCageTx`.

## Why Byte Equality Is Not A Fact-Contract Check

The legacy server-side update builder uses two provider/runtime services that
are not present in `UpdateFacts`:

- `Provider.posixMsToSlot` derives the validity upper slot from request
  deadlines and current chain time.
- `Provider.evaluateTx` derives per-redeemer ExUnits during local transaction
  building.

Those values are not snapshot-anchored facts about UTxOs or trie state. Adding
them to `UpdateFacts` would move local build/runtime derivations across the
server boundary and weaken the epic invariant that the server returns facts,
not unsigned transactions or provider outputs.

## Resulting Test Shape

The update cage tests must compare every transaction field that can be known
from verified facts: inputs, outputs including the continuing state output,
mint, certificates, withdrawals, datums, redeemer structure, required signers,
network id, collateral, and reference inputs.

The tests must explicitly exclude only validity-upper-slot and per-redeemer
ExUnits from structural parity, then separately prove that the MPF fold over
`TrieFact` inputs produces the same new state root as the legacy update fold for
the same inputs.

## Reuse For Reject

Reject (#270) should inherit the same boundary. Shared `TrieFact` and MPF fold
helpers prove fact-driven state-root behavior; provider-runtime slot and script
budget derivations remain local client build concerns.

## Q-002 Revision

Parent answer `A-002-update-validity-live-matrix.md` revises the Q-001
boundary after the S5 live-boundary smoke. Q-001 was correct for the
unit-level evidence available at the time: whole-transaction byte equality
could not be required while `UpdateFacts` lacked validity slot conversion and
per-redeemer ExUnits. S5 then showed validity-upper-slot is not only a byte
parity field; without it, an unmodified fact-derived `updateCageTx` cannot
submit on the local cluster.

The revised boundary separates two provider/runtime fields:

- Validity-upper-slot is an era-schedule lookup applied to the request POSIX
  deadline. It is now treated as a chain-era fact at facts-fetch time, like
  protocol parameters, and becomes part of `UpdateFacts`.
- Per-redeemer ExUnits are script-evaluator output. They remain client-local
  build output and stay excluded from whole-transaction byte equality.

S4b adds `validity_upper_slot` to the update facts wire shape, computes it on
the server with the same provider conversion used by the legacy update path,
verifies it, and has `updateCageTx` consume it. The structural parity test
stays expressed as field projection rather than whole-body byte equality
because ExUnits still differ.

Reject (#270) likely inherits the same slot-fact need. If S4b introduces a
validated-slot helper or newtype, it should live in a module reject can reuse.
