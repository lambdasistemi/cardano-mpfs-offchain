# Quickstart: E2E /tx/reject proof verification

## Prerequisites

- Enter the repository dev shell with `nix develop`.
- Set `MPFS_BLUEPRINT` to the cage blueprint used by the E2E suite.

## Focused validation

Run the proof scenario only:

```bash
just e2e "Proof-bearing reads E2E"
```

If running through Cabal directly, keep development builds at `-O0`.

## Expected behavior

The scenario should:

1. Start a local Cardano devnet.
2. Boot a token.
3. Submit an insert request and update it.
4. Submit a second insert request and wait past its reject deadline.
5. Verify read-side proof envelopes.
6. Verify write-side proof envelopes for boot, request, update,
   retract, reject, and end.
7. Confirm a tampered reject response fails at an explicit CSMT proof
   path.

The reject branch adds about 11 to 12 seconds to the scenario.
