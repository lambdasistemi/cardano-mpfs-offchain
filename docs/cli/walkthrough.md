# Walkthrough

A recorded session of the requester write path against a local MPFS
devnet: boot a token, list it, submit a fact request, and watch
`token end` cleanly refuse while a request is still pending. Recorded
from the live binary against `mpfs-devnet-server` — `register-token` and
`fact insert` each go through the full workflow → sign → `POST /submit` →
await chain.

```asciinema-player
{
    "file": "cli/assets/walkthrough.cast"
    , "cols": 100
    , "rows": 28
    , "mkap_theme": "none"
}
```

What the session shows:

- **`register-token`** — boots a new cage; prints the submitted txId.
- **`token list`** — returns the new token id.
- **`fact insert`** — submits a pending insert request for a key/value.
- **`token end`** — cleanly returns HTTP 409 because a request is still
  pending (you must clear pending requests before ending a cage).

Two steps are intentionally not shown:

- `fact get` after an insert returns 404 — the fact only materializes
  once an oracle applies the pending request (see
  [Scope](index.md#scope)).
- `fact retract` currently hits a server-side 500 in `/facts/retract`,
  tracked as
  [#299](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/299);
  the CLI command is correct and will work once #299 lands.

## Reproduce it

```bash
# 1. boot the devnet + server (one shell)
nix run .#mpfs-devnet-server -- --port 3000

# 2. run the asserted write-path smoke (another shell)
OWNER_KEY=<funded ed25519_sk1… file> MPFS_SERVER=http://localhost:3000 \
  nix develop -c cardano-mpfs-cli/e2e/walkthrough.sh
```

`e2e/walkthrough.sh` asserts each step exits 0 and emits JSON, and is the
loud-failing live-boundary smoke for the write path.
