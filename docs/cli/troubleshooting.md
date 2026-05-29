# Troubleshooting

## `fact get` returns 404 right after `fact insert`

Expected. `fact insert` creates a *pending request*; the fact only
materializes once an oracle applies it via the server's `applyRequests`
path (which advances the trie root). The CLI is requester-facing and
does not expose an oracle "apply" command — see [Scope](index.md#scope).

## `token end` returns 409 "pending request UTxOs exist"

Expected. A cage cannot be ended while requests are pending. Clear them
first (retract or reject), then end.

## `fact retract` returns a 500

A known server-side bug in the `/facts/retract` handler, tracked as
[#299](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/299).
It reproduces with a direct request to the endpoint (no CLI involved),
so it is not a CLI defect — the CLI's `fact retract` command is correct
and will work once #299 lands.

## "no --cage-config FILE supplied and MPFS_BLUEPRINT is unset"

Write commands need the cage blueprint to build the transaction. Set
`$MPFS_BLUEPRINT` (the `nix develop` shell does this) or pass
`--cage-config FILE`.

## "invalid server URL" / connection refused

Check `--server` (e.g. `http://localhost:3000`) and that the MPFS server
is reachable.

## "owner-key: KeyBech32Error …"

The `--owner-key` file must contain a Bech32 ed25519 signing key
(`ed25519_sk1…`). TextEnvelope JSON keys and hardware/encrypted keystores
are not supported.
