# Walkthrough

This recording shows the complete `mpfs-cli` lifecycle against a local
MPFS devnet. It uses a throwaway funded devnet signer, points the CLI at
`http://localhost:3000`, and keeps the output focused on verified
results instead of proof payloads.

```asciinema-player
{
    "file": "cli/assets/devnet-lifecycle.cast"
    , "cols": 100
    , "rows": 28
    , "mkap_theme": "none"
}
```

What the session shows:

- **Environment and connection** — prints safe values for
  `MPFS_SERVER`, `MPFS_BLUEPRINT`, and the signer key path, then runs
  `token list` against the fresh devnet.
- **Token boot** — runs `register-token` with short devnet timing,
  waits for submission/indexing, then confirms the token with
  `token list` and `token get`.
- **Insert and materialize** — submits `fact insert`, shows the pending
  request with `requests list`, runs owner-side `token process`, then
  proves the value with `fact get` and `fact list`.
- **Update** — submits `fact update`, processes it, and shows
  `fact get` returning the new value.
- **Delete** — submits `fact delete`, processes it, and shows
  `fact get` returning a verified absence proof.
- **Reject** — submits a pending request, waits past the short devnet
  deadlines, runs `fact reject`, and confirms `requests list` is empty.
- **Retract** — submits another request, reads its request id from
  `requests list`, waits until the process window has elapsed, runs
  `fact retract`, and confirms no requests remain.
- **End** — runs `token end` after clearing pending requests and shows
  the final `token list` is empty.

The same devnet signer is used for requester and owner actions in the
recording. On a shared deployment those roles can be operated by
different funded keys when the token policy and request ownership allow
it.

## Reproduce it

Start a local devnet server:

```bash
nix run .#mpfs-devnet-server -- --port 3000
```

In another shell, use the dev shell so `MPFS_BLUEPRINT` and the CLI
tooling are available:

```bash
nix develop
export MPFS_SERVER=http://localhost:3000
export MPFS_SIGNER_WALLET=/path/to/funded-devnet.ed25519_sk
mpfs-cli --json token list | jq '{verified,result}'
```

The signer file must contain a Bech32 `ed25519_sk...` key whose
enterprise address is funded on the devnet. The E2E helpers use a
throwaway devnet genesis key; avoid printing key contents in terminal
recordings.

For a non-recorded smoke test of the main write path:

```bash
MPFS_SERVER=http://localhost:3000 \
MPFS_SIGNER_WALLET=/path/to/funded-devnet.ed25519_sk \
cardano-mpfs-cli/e2e/walkthrough.sh
```

The smoke script asserts register, insert/process/get,
update/process/get, delete/process/absence, reject, and end. The cast
adds the commented terminal narrative and the retract path.
