# CLI (mpfs-cli)

`mpfs-cli` is a scriptable command-line front-end for the MPFS server.
With a Bech32 `.skey` and a running server it drives both roles:
requesters submit fact requests, owners register tokens, process pending
requests into the trie, reject expired requests, and end tokens. Write
commands fetch facts, verify the proof-bearing response, build the
transaction, sign locally, submit, and await. Read commands verify the
proof-bearing query response before printing it.

All MPFS protocol logic comes from `cardano-mpfs-workflows`; the CLI
owns argument parsing, key loading, local signing, submission, and
output formatting. See the [walkthrough](walkthrough.md) for a recorded
local-devnet lifecycle and [troubleshooting](troubleshooting.md) for
common timing and configuration failures.

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
nix run .#mpfs-devnet-server -- --port 3000    # one shell

nix develop                                    # another shell: tools + $MPFS_BLUEPRINT
export MPFS_SERVER=http://localhost:3000
export MPFS_SIGNER_WALLET=/path/to/funded-devnet.ed25519_sk
mpfs-cli --json token list | jq '{verified,result}'
mpfs-cli --json register-token | jq
```

`MPFS_SIGNER_WALLET` points at a Bech32 ed25519 signing key
(`ed25519_sk1...`) whose enterprise address is funded on the target
network.

For short devnet demonstrations, `register-token` accepts
`--process-time-ms` and `--retract-time-ms`. Use a retract time greater
than the process time if you plan to demonstrate `fact retract`.

## Common commands

| Operation | Command |
|---|---|
| Register (boot) a token | `mpfs-cli register-token --server URL --owner-key KEY [--cage-config FILE] [--process-time-ms MS --retract-time-ms MS]` |
| Request a fact insert | `mpfs-cli fact insert --server URL --token TOK --key HEX --value HEX --owner-key KEY` |
| Request a fact update | `mpfs-cli fact update --server URL --token TOK --key HEX --old-value HEX --new-value HEX --owner-key KEY` |
| Request a fact delete | `mpfs-cli fact delete --server URL --token TOK --key HEX --value HEX --owner-key KEY` |
| Retract a pending request | `mpfs-cli fact retract --server URL --token TOK --request-id TXHASH#IX --owner-key KEY` |
| Process pending requests | `mpfs-cli token process TOK --server URL --owner-key KEY [--request-id TXHASH#IX ...]` |
| Reject expired requests | `mpfs-cli fact reject --server URL --token TOK --owner-key KEY [--request-id TXHASH#IX ...]` |
| End (close) a token | `mpfs-cli token end --server URL --token TOK --owner-key KEY` |
| List token ids (verified read-only) | `mpfs-cli token list --server URL` |
| Get token state (verified read-only) | `mpfs-cli token get TOK --server URL` |
| Enumerate facts (verified read-only) | `mpfs-cli fact list TOK --server URL` |
| Look up a fact (verified read-only) | `mpfs-cli fact get TOK KEY --server URL` |
| List pending request ids and deadlines (verified read-only) | `mpfs-cli requests list TOK --server URL` |

Write commands also accept `--cage-config FILE` and `--trusted-root HEX`
(see the trust model below). `token list` and `requests list` also need
the cage config to derive the verifier address locally. Every subcommand
has `--help`.

## Output contract

- **stdout**: human-readable output by default, or exactly one JSON
  object with `--json`.
- **stderr**: all diagnostics.
- **exit code**: non-zero on any failure, with stdout left empty so a
  caller never parses a half-result.

```bash
mpfs-cli token list --server http://localhost:3000 --json | jq '.result.tokens[]'
```

## Trust model

`mpfs-cli` resolves the same anchors for writes and verified reads; each
has a sensible default for running against your own server, plus a flag
for paranoid or third-party deployments.

- **Server** — `--server URL`, or, by default, `$MPFS_SERVER`.
- **Signer** — `--owner-key KEYFILE`, or, by default,
  `$MPFS_SIGNER_WALLET`. Despite the flag name, this is the local
  signing key for the current write action: owner actions sign as the
  owner, requester actions sign as the requester.
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

## Both-role lifecycle

`fact insert`, `fact update`, and `fact delete` create pending request
UTxOs. The fact will not materialize until the owner runs
`token process TOK`, which folds pending requests into the token trie.
After processing, `fact get TOK KEY` verifies the read proof and prints
the materialized value or a verified absence.

`requests list TOK` shows pending request ids plus process and retract
deadlines. Use those ids with `token process --request-id`,
`fact reject --request-id`, or `fact retract --request-id`.
`fact retract` is only valid after the process window has elapsed and
before the retract deadline. `fact reject` is for expired pending
requests after the configured deadline.

`fact delete --value` and `fact retract --request-id` (`txhash#ix`) are
on-chain protocol requirements: the request datum binds the value, and a
retract spends one specific request UTxO.

## Key format

Bech32 ed25519 signing keys only (CIP-5 `ed25519_sk1…`). No hardware
wallet, no encrypted keystore, no TextEnvelope JSON.
