# CLI (mpfs-cli)

`mpfs-cli` is a scriptable command-line front-end for the MPFS server.
With a Bech32 `.skey` and a running server it registers tokens and
manages facts end-to-end — fetch facts, verify the proof-bearing
response, build the transaction, sign locally, submit, and await —
writing JSON to stdout and logs to stderr.

All MPFS protocol logic comes from `cardano-mpfs-workflows`; the CLI
owns argument parsing, key loading, local signing, submission, and
output formatting. See the [walkthrough](walkthrough.md) for a recorded
session and [troubleshooting](troubleshooting.md) for known issues.

## Install

Release builds are attached to
[GitHub Releases](https://github.com/lambdasistemi/cardano-mpfs-offchain/releases):

- `mpfs-cli-<version>-x86_64-linux.tar.gz`
- `mpfs-cli-<version>-aarch64-darwin.tar.gz`

Unpack the tarball for your platform and put `bin/mpfs-cli` on your
`PATH`.

On macOS with Homebrew:

```bash
brew tap lambdasistemi/tap
brew install mpfs-cli
```

## Quick start

```bash
nix develop                                    # tools + $MPFS_BLUEPRINT
cabal build mpfs-cli                            # or: nix build .#mpfs-cli
mpfs-cli register-token --server http://localhost:3000 --owner-key owner.skey
mpfs-cli token list      --server http://localhost:3000 | jq
```

`--owner-key` is a Bech32 ed25519 signing key (`ed25519_sk1…`) whose
enterprise address is funded on the target network.

## Common commands

| Operation | Command |
|---|---|
| Register (boot) a token | `mpfs-cli register-token --server URL --owner-key KEY [--cage-config FILE]` |
| Request a fact insert | `mpfs-cli fact insert --server URL --token TOK --key HEX --value HEX --owner-key KEY` |
| Request a fact update | `mpfs-cli fact update --server URL --token TOK --key HEX --old-value HEX --new-value HEX --owner-key KEY` |
| Request a fact delete | `mpfs-cli fact delete --server URL --token TOK --key HEX --value HEX --owner-key KEY` |
| Retract a pending request | `mpfs-cli fact retract --server URL --token TOK --request-id TXHASH#IX --owner-key KEY` |
| Reject expired requests | `mpfs-cli fact reject --server URL --token TOK --owner-key KEY` |
| End (close) a token | `mpfs-cli token end --server URL --token TOK --owner-key KEY` |
| List token ids (read-only) | `mpfs-cli token list --server URL` |
| Look up a fact (read-only) | `mpfs-cli fact get --server URL --token TOK --key HEX` |

Write commands also accept `--cage-config FILE` and `--trusted-root HEX`
(see the trust model below). Every subcommand has `--help`.

## Output contract

- **stdout**: exactly one JSON object per successful invocation.
- **stderr**: all diagnostics.
- **exit code**: non-zero on any failure, with stdout left empty so a
  caller never parses a half-result.

```bash
mpfs-cli token list --server http://localhost:3000 | jq '.[]'
```

## Trust model

`mpfs-cli` resolves two anchors for write commands; each has a sensible
default for running against your own server, plus a flag for paranoid or
third-party deployments.

- **Trusted UTxO root** — `--trusted-root HEX`, or, by default, fetched
  from the server's `GET /status`. The default trusts the server to
  report a faithful snapshot. With the flag, the CLI verifies the
  proof-bearing facts against an independently-obtained anchor — the
  verifier then earns its keep.
- **Cage blueprint** — `--cage-config FILE`, or, by default, the path in
  `$MPFS_BLUEPRINT`. The blueprint carries the validator scripts; the
  CLI parses it locally and derives the script hash, so the client owns
  validator-script provenance and never trusts the server for it. If
  neither the flag nor the env var is set, write commands fail with a
  clear message.

Network and timing default to a testnet/devnet profile; a mainnet flag
is a future addition.

## Scope

`mpfs-cli` is **requester-facing**. Its subcommands are the requester's
surface: request operations (insert/update/delete), inspection
(list/get), wind-down (retract/reject/end), and booting a cage you own.

`insert → get` will not surface the fact until an oracle service
processes the pending request via the server's `applyRequests` path
(which advances the trie root). That oracle path is a separate,
non-CLI concern.

`fact delete --value` and `fact retract --request-id` (`txhash#ix`) are
on-chain protocol requirements — the request datum binds the value, and
a retract spends one specific request UTxO — not added ergonomics.

## Key format

Bech32 ed25519 signing keys only (CIP-5 `ed25519_sk1…`). No hardware
wallet, no encrypted keystore, no TextEnvelope JSON.
