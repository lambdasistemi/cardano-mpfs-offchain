# mpfs-cli

A scriptable command-line front-end for the
[MPFS](https://lambdasistemi.github.io/cardano-mpfs-offchain/) server:
register tokens and manage facts with a local Bech32 `.skey`. Protocol
logic comes from [`cardano-mpfs-workflows`](../cardano-mpfs-workflows);
the CLI owns argument parsing, key loading, signing, submission, and
JSON output.

## Quick start

```bash
nix develop
cabal build mpfs-cli                          # or: nix build .#mpfs-cli
mpfs-cli register-token --server http://localhost:3000 --owner-key owner.skey
mpfs-cli token list      --server http://localhost:3000 | jq
```

`--owner-key` is a Bech32 ed25519 signing key (`ed25519_sk1…`) funded on
the target network. Write commands resolve the cage blueprint from
`--cage-config` or `$MPFS_BLUEPRINT`, and the trusted root from
`--trusted-root` or the server's `/status`.

## Documentation

Full docs — overview, command cheat sheet, an asciinema walkthrough, the
trust model, and troubleshooting — live on the documentation site:

**https://lambdasistemi.github.io/cardano-mpfs-offchain/cli/**

Every subcommand also has `--help`.
