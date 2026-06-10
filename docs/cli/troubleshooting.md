# Troubleshooting

## `fact get` returns an absent result right after `fact insert`

Expected. `fact insert` creates a pending request UTxO. The fact only
materializes after the token owner runs:

```bash
mpfs-cli token process TOKEN
```

Use `requests list TOKEN` before processing to see the pending request,
and `fact get TOKEN KEY` after processing to verify the materialized
value.

## `fact retract` returns HTTP 409 `request_not_retractable`

The request is still inside its process window. Retraction is only valid
after `processDeadline` and before `retractDeadline`.

Check the current window:

```bash
mpfs-cli --json requests list TOKEN | jq '.result.requests[]'
```

For devnet demos, register the token with a retract window that is
larger than the process window, for example:

```bash
mpfs-cli register-token --process-time-ms 5000 --retract-time-ms 15000
```

## `fact reject` does not clear a pending request

Reject is for expired pending requests. Wait until the relevant request
is past its deadline, then run `fact reject` again. Use
`requests list TOKEN` to inspect the request ids and deadlines. Pass
`--request-id TXHASH#IX` if you want to reject specific requests instead
of all expired requests.

## `token end` returns 409 "pending request UTxOs exist"

Expected. A token cannot be ended while requests are pending. Clear them
with `token process`, `fact reject`, or `fact retract`, then run
`token end` again.

## "no --cage-config FILE supplied and MPFS_BLUEPRINT is unset"

Write commands need the cage blueprint to build transactions.
`token list` and `requests list` also need it to derive verifier
addresses locally. Set `$MPFS_BLUEPRINT` (the `nix develop` shell does
this) or pass `--cage-config FILE`.

## "invalid server URL" / connection refused

Check `--server` or `$MPFS_SERVER` (for example,
`http://localhost:3000`) and confirm the MPFS server is reachable. For a
local devnet, start it with:

```bash
nix run .#mpfs-devnet-server -- --port 3000
```

## "owner-key: KeyBech32Error ..."

The `--owner-key` file, or `$MPFS_SIGNER_WALLET`, must contain a Bech32
ed25519 signing key (`ed25519_sk1...`). Cardano CLI TextEnvelope JSON
keys, hardware wallets, and encrypted keystores are not accepted by
`mpfs-cli`.
