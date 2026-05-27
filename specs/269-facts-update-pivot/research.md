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
