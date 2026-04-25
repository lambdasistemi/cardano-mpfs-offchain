# Quickstart: Tx/proof binding

## Focused validation

Run the client verifier tests:

```bash
nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct
```

## Expected behavior

- Honest fixtures still pass every `verify*TxResponse` function.
- Replacing only the `tx` field with a valid transaction whose inputs do
  not match the proof roles causes a `TxBindingFailed` error.
- The verifier remains pure and does not require a node, database, or
  server summary.
