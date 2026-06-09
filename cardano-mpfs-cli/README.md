# mpfs-cli

A scriptable command-line front-end for the
[MPFS](https://lambdasistemi.github.io/cardano-mpfs-offchain/) server:
register tokens and manage facts with a local Bech32 `.skey`. Protocol
logic comes from [`cardano-mpfs-workflows`](../cardano-mpfs-workflows);
the CLI owns argument parsing, key loading, signing, submission, and
verified output.

## Quick start

```bash
nix develop
cabal build mpfs-cli                          # or: nix build .#mpfs-cli
export MPFS_SERVER=http://localhost:3000
export MPFS_SIGNER_WALLET=owner.skey
mpfs-cli register-token
mpfs-cli token list --json | jq
```

`MPFS_SIGNER_WALLET` points at a Bech32 ed25519 signing key
(`ed25519_sk1...`) funded on the target network. Write commands resolve
the server from `--server` or `$MPFS_SERVER`, the cage blueprint from
`--cage-config` or `$MPFS_BLUEPRINT`, and the trusted root from
`--trusted-root` or the server's `/status`.

Owner commands include `register-token`, `token process TOKEN`, `fact
reject`, and `token end`. Requester commands include `fact insert`,
`fact update`, `fact delete`, and `fact retract`. Verified read commands
include `token list`, `token get TOKEN`, `fact list TOKEN`, `fact get
TOKEN KEY`, and `requests list TOKEN`. Human-readable output is the
default; pass `--json` before or after a command for machine output.

For short-lived devnet lifecycles, `register-token` also accepts
`--process-time-ms` and `--retract-time-ms`; omitting them keeps the
CLI's default testnet/devnet timing profile.

## Documentation

Full docs — overview, command cheat sheet, an asciinema walkthrough, the
trust model, and troubleshooting — live on the documentation site:

**https://lambdasistemi.github.io/cardano-mpfs-offchain/cli/**

Every subcommand also has `--help`.
