# mpfs-cli

A command-line front-end for the [MPFS](../README.md) server. With a
Bech32 `.skey` file and a running server, `mpfs-cli` registers tokens
and manages facts end-to-end — fetch facts, verify the proof-bearing
response, build the transaction, sign locally, submit, and await — and
prints structured JSON to stdout (logs go to stderr, so it scripts
cleanly).

All MPFS protocol logic lives in
[`cardano-mpfs-workflows`](../cardano-mpfs-workflows); the CLI only owns
argument parsing, key loading, local signing, submission, and output
formatting.

## Build

```bash
nix develop
cabal build mpfs-cli
# or: nix build .#mpfs-cli
```

## Anchors and the trust model

`mpfs-cli` resolves two anchors for write commands:

- **Trusted UTxO root** — `--trusted-root HEX`, or, by default, fetched
  from the server's `GET /status`. The default trusts the server to
  report a faithful snapshot, which is appropriate when running against
  your own server. For third-party deployments, pass `--trusted-root` to
  verify the proof-bearing facts against an independently-obtained
  anchor — the verifier then earns its keep.
- **Cage blueprint** — `--cage-config FILE`, or, by default, the path in
  `$MPFS_BLUEPRINT`. The blueprint carries the validator scripts; the CLI
  parses it locally and derives the script hash, so the client owns
  validator-script provenance and never trusts the server for it. If
  neither the flag nor the env var is set, write commands fail with a
  clear message.

Network and timing default to a testnet/devnet profile; a mainnet flag
is a future addition.

## Subcommands

Write commands (need `--owner-key`; resolve both anchors above):

```
mpfs-cli register-token --server URL --owner-key KEYFILE [--cage-config FILE] [--trusted-root HEX]
mpfs-cli fact insert    --server URL --token TOKEN --key HEX --value HEX --owner-key KEYFILE [...]
mpfs-cli fact update    --server URL --token TOKEN --key HEX --old-value HEX --new-value HEX --owner-key KEYFILE [...]
mpfs-cli fact delete    --server URL --token TOKEN --key HEX --value HEX --owner-key KEYFILE [...]
mpfs-cli fact retract   --server URL --token TOKEN --request-id TXHASH#IX --owner-key KEYFILE [...]
mpfs-cli fact reject    --server URL --token TOKEN --owner-key KEYFILE [...]
mpfs-cli token end       --server URL --token TOKEN --owner-key KEYFILE [...]
```

Read-only commands (no key, no anchors):

```
mpfs-cli token list      --server URL
mpfs-cli fact get        --server URL --token TOKEN --key HEX
```

Notes:

- `fact delete` requires `--value`: the deletion proves the existing
  key→value leaf, so the current value must be supplied.
- `fact retract --request-id` is the pending request's UTxO reference in
  `txhash#ix` form.
- Each write command prints `{"command":…,"status":"submitted","txId":…}`
  on success.

Every subcommand has `--help` (e.g. `mpfs-cli register-token --help`).

## Output contract

- **stdout**: exactly one JSON object per successful invocation.
- **stderr**: all diagnostics.
- **exit code**: non-zero on any failure, with stdout left empty so a
  caller never parses a half-result.

```bash
mpfs-cli token list --server http://localhost:3000 | jq '.[]'
```

## End-to-end walkthrough

`e2e/walkthrough.sh` runs a full session against a local devnet:
register a token, insert a fact, and end the cage, asserting each step
exits 0 and emits JSON. See the script header for what it launches and
the environment it expects.

```bash
nix develop -c cardano-mpfs-cli/e2e/walkthrough.sh
```

## Key format

Bech32 ed25519 signing keys only (CIP-5 `ed25519_sk1…`). No hardware
wallet, no encrypted keystore, no TextEnvelope JSON.
