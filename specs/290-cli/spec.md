# Spec — #290 `mpfs-cli` Haskell command-line front-end

**Issue:** lambdasistemi/cardano-mpfs-offchain#290
**Epic:** #287 (child 3, after #288 POST /submit, #289 cardano-mpfs-workflows)

## P1 user story

As a user holding a Bech32 `.skey` file and pointing at a running MPFS
server, I can register a token and manage its facts end-to-end from a
single command-line tool — without writing Haskell or hand-rolling HTTP
calls — and pipe the JSON result into other tooling.

## User-visible surface

Nine subcommands:

```
mpfs-cli register-token --server URL --owner-key KEYFILE [--cage-config FILE]
mpfs-cli fact insert    --server URL --token TOKEN --key HEX --value HEX --owner-key KEYFILE
mpfs-cli fact update    --server URL --token TOKEN --key HEX --old-value HEX --new-value HEX --owner-key KEYFILE
mpfs-cli fact delete    --server URL --token TOKEN --key HEX --owner-key KEYFILE
mpfs-cli fact retract   --server URL --token TOKEN --request-id REQ_ID --owner-key KEYFILE
mpfs-cli fact reject    --server URL --token TOKEN --owner-key KEYFILE
mpfs-cli token end       --server URL --token TOKEN --owner-key KEYFILE
mpfs-cli token list      --server URL                                    # read-only
mpfs-cli fact get        --server URL --token TOKEN --key HEX            # read-only
```

## Behavior

Each **write** subcommand:

1. Calls the matching `cardano-mpfs-workflows` function to fetch facts,
   verify the proof-bearing response, and build the unsigned tx.
2. Loads the Bech32 `.skey` and signs the unsigned tx body locally.
3. POSTs the signed tx CBOR to `POST /submit` (#288).
4. Awaits confirmation via `GET /tx/:txId?timeout=…` (#288).
5. Prints a structured JSON result to **stdout**; all diagnostics go to
   **stderr**.

Each **read-only** subcommand (`token list`, `fact get`) calls the
corresponding existing read endpoint and prints JSON to stdout. No key
required.

## Functional requirements

- **FR1** — `cabal build mpfs-cli` succeeds; executable lives in a new
  `cardano-mpfs-cli` package, added to `cabal.project`.
- **FR2** — All nine subcommands parse with `optparse-applicative` and
  expose `--help` text (`mpfs-cli register-token --help`, etc.).
- **FR3** — Arg validation: hex args (`--key`, `--value`, `--old-value`,
  `--new-value`) reject non-hex input with a clear stderr error and a
  non-zero exit code before any network call.
- **FR4** — Bech32 `.skey` loading: a `bech32`-encoded ed25519 signing
  key (`ed25519_sk1…`) is read from the keyfile and turned into a
  `SignKeyDSIGN Ed25519DSIGN`. No other key formats (no hardware wallet,
  no encrypted keystore, no TextEnvelope JSON) in this ticket.
- **FR5** — Local signing: an unsigned tx CBOR is deserialized, witnessed
  with the loaded key, and reserialized to submission-ready CBOR — the
  same witness shape the #288 e2e test uses (`addKeyWitness`).
- **FR6** — Submission glue: POST signed CBOR to `/submit`, parse the
  `SubmitResponse` (txId) or surface the server error, then await the tx.
- **FR7** — **JSON on stdout, logs on stderr.** stdout is a single JSON
  object per invocation; the CLI is scriptable (`mpfs-cli … | jq`).
- **FR8** — No MPFS protocol logic in the CLI: fetch/verify/build comes
  only from `cardano-mpfs-workflows`. The CLI owns args, keys, signing,
  submission, and output formatting.
- **FR9** — No interactive mode / REPL. One `--owner-key` per invocation.
- **FR10** — E2E walkthrough script runs register-token → insert →
  update → get → retract → end against the local cluster and exits 0.
- **FR11** — `mpfs-cli/README.md` demonstrates each subcommand.

## Success criteria

- [ ] `cabal build mpfs-cli` succeeds (FR1).
- [ ] All nine subcommands documented with `--help` (FR2).
- [ ] Hex/arg validation rejects bad input pre-network (FR3).
- [ ] Bech32 `.skey` round-trips to a usable signing key (FR4/FR5).
- [ ] Signed tx posts to `/submit` and is awaited (FR6).
- [ ] stdout is JSON, stderr is logs (FR7).
- [ ] E2E walkthrough exits 0 (FR10).
- [ ] README walkthrough present (FR11).

## Non-goals

- No interactive mode / REPL.
- No multi-key wallet management — one `--owner-key` per invocation.
- No key formats beyond Bech32 (no hardware wallet, no encrypted keystore).
- No config beyond optional `--cage-config`; everything else is flags.

## Coordination note (#289)

`cardano-mpfs-workflows` (#289) is built in parallel and only S1
(`serializeCageTx`) has landed. Until its package surface exists, the
write subcommands' workflow step is a **stub** that prints the call it
*would* make. Auxiliary code (args, keys, signing, submission glue,
read-only commands) is real and lands now. The stub → real-call flip is
a small per-workflow commit once #289 publishes each function.
