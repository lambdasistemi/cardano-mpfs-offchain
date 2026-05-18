# Research: End fact-provider pivot

## Boot Decision Applied To End

Boot issue #261 changed the MOOG boundary. The paired MOOG work is a boundary spike and canary track, not production migration evidence. End issue #268 therefore records boundary status and links cardano-foundation/moog#96 instead of making MOOG production migration part of this offchain PR.

## Completeness Proof

`CSMT.Verify.verifyCompletenessProof` verifies a proof against:

- trusted UTxO root bytes,
- a prefix `Key`,
- the absolute leaves under that prefix,
- serialized completeness proof bytes.

For end, the prefix is the per-cage request validator address. The verifier derives it locally from client cage configuration and token id, then requires an empty leaf list. This proves there are no pending request UTxOs under that request address at the snapshot root.

## State UTxO Read

The state UTxO is found by scanning the global state validator address prefix in the UTxO CSMT and filtering decoded `TxOut` values for the configured state policy id plus the requested token asset name. The read returns `(TxIn, TxOut CBOR, inclusion proof)` so the server does not need a node UTxO query.

## Protocol Parameters

Protocol parameters remain unverified facts. `endCageTx` decodes them and enforces `WalletPolicy` caps before returning a transaction.
