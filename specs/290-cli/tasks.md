# Tasks — #290 `mpfs-cli`

One `## Slice` section per bisect-safe commit. Items get `[X]` when the
slice is accepted (commit reviewed, gate green, pushed).

## Slice S1 — package skeleton + arg parsing

- [X] T290-S1 Create `cardano-mpfs-cli` package (cabal file, common
      warnings stanza, `werror` flag) and add to `cabal.project`.
- [X] T290-S1 `Options.hs`: `Command` ADT + `optparse-applicative`
      parsers for all 9 subcommands with `--help` text.
- [X] T290-S1 `Hex.hs`: hex arg reader rejecting non-hex pre-network.
- [X] T290-S1 `Run.hs` + `app/Main.hs`: handlers route writes to stub
      emitting `{"stub":"Workflows.<fn>","args":{…}}` on stdout, log on
      stderr.
- [X] T290-S1 Gate: `cabal build mpfs-cli` + `--help` smoke per
      subcommand recorded in WIP.md.

## Slice S2 — Bech32 .skey loading + signing

- [X] T290-S2 `Key.hs`: decode `ed25519_sk1…` → `SignKeyDSIGN
      Ed25519DSIGN`; clear error on malformed key.
- [X] T290-S2 `Sign.hs`: unsigned CBOR → witnessed CBOR (addKeyWitness
      shape + `serialize'`).
- [X] T290-S2 Unit test: known key decodes; signed tx carries a vkey
      witness for the derived key hash.
- [X] T290-S2 Gate: unit test + build green.

## Slice S3 — submission glue + read-only commands

- [X] T290-S3 `Submit.hs`: POST `/submit`, parse txId/error, await via
      `GET /tx/:txId`.
- [X] T290-S3 `Output.hs`: JSON-on-stdout / log-on-stderr helpers.
- [X] T290-S3 Wire `token list` (`GET /tokens`) and `fact get`
      (`GET /tokens/:id/facts/:key`) to real read endpoints.
- [X] T290-S3 Gate: build green; live read/submit proof deferred to S5
      e2e (documented live-boundary follow-up).

## Slice S4 — flip write subcommands to real workflows

- [X] T290-S4 Replace each stub with `Cardano.MPFS.Workflows.<fn>` →
      sign → submit → await → JSON, as #289 publishes each function.
- [X] T290-S4 (per-workflow sub-commits allowed; depends on #289.)

## Slice S5 — E2E walkthrough + README

- [X] T290-S5 E2E script `cardano-mpfs-cli/e2e/walkthrough.sh`:
      register-token → token list → fact insert → fact get → token end,
      asserting each exits 0 and emits JSON (loud-failing live-boundary
      smoke). Live execution against the devnet with a funded key is a
      named operator follow-up (needs live data); see PR body.
- [X] T290-S5 `cardano-mpfs-cli/README.md` walkthrough per subcommand +
      trust model.

## Docs — MkDocs CLI section + screencast

- [X] T290-docs Integrate the CLI into the MkDocs site: `docs/cli/`
      (index overview + cheat sheet, walkthrough with the asciinema-player
      cast, troubleshooting incl. #299); `mkdocs.yml` gains the
      `asciinema-player` plugin, a light/dark palette toggle, and a CLI
      nav section. The recorded `docs/cli/assets/walkthrough.cast` is the
      source; `cardano-mpfs-cli/README.md` is trimmed to a short package
      README linking the docs site. `mkdocs build --strict` passes.
