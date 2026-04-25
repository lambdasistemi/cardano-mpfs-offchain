# Quickstart: Cross-Target Client Verifier Builds

From the repository root:

```bash
nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct
```

After the Nix outputs are added, the expected local proof commands are:

```bash
nix build .#cardano-mpfs-client-wasm --quiet
nix build .#cardano-mpfs-client-js --quiet
```

If a target fails, update `research.md` with:

- exact command
- first failing package or derivation
- shortest useful error excerpt
- root-cause hypothesis
- next action

Before merge, run the native client tests and every cross-target output
that the PR claims as working.
