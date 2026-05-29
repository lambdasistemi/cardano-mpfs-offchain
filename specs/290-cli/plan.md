# Plan — #290 `mpfs-cli`

## Tech stack

- Haskell GHC 9.10.1, `GHC2021`, project common-warnings stanza
  (`-Wall -Werror …`, `werror` flag pattern as in sibling packages).
- New package `cardano-mpfs-cli/` with executable `mpfs-cli`.
- `optparse-applicative` for parsing (new dependency for this package).
- `aeson` for JSON output, `bytestring`/`base16-bytestring` for hex,
  `text`.
- `bech32` for `.skey` decoding.
- `cardano-crypto-class` for `SignKeyDSIGN Ed25519DSIGN`,
  `cardano-ledger-conway`/`cardano-ledger-api` for tx (de)serialization
  + witness (signing is native-only auxiliary code, not the
  WASM-constrained verifier path).
- `cardano-mpfs-api` (wire types: `SubmitRequest`/`SubmitResponse`,
  `TokenIdJSON`, `FactResponse`), `cardano-mpfs-client` (HTTP client
  `MpfsHttp` + servant clients), and `cardano-mpfs-workflows` (#289,
  wired once available).

## Module layout

```
cardano-mpfs-cli/
├── cardano-mpfs-cli.cabal
├── app/Main.hs                       -- dispatch parsed command
└── lib/Cardano/MPFS/CLI/
    ├── Options.hs                    -- optparse-applicative: Command + parsers + --help
    ├── Hex.hs                        -- hex arg readers / validation
    ├── Key.hs                        -- Bech32 .skey -> SignKeyDSIGN Ed25519DSIGN
    ├── Sign.hs                       -- unsigned CBOR -> witnessed CBOR
    ├── Submit.hs                     -- POST /submit + await; read-only token/fact reads
    ├── Output.hs                     -- JSON-on-stdout / logs-on-stderr helpers
    └── Run.hs                        -- per-command handlers (stub workflow calls today)
```

## Reused existing surface (confirmed in repo)

- `POST /submit`: `SubmitRequest { srSignedTxCbor :: Hex }` →
  `SubmitResponse { srTxId :: Hex }`
  (`cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs`).
- Await: `GET /tx/:txId?timeout` → 204/408
  (`cardano-mpfs-api/.../API.hs` `TxAwaitAPI`).
- HTTP client scaffold: `MpfsHttp { manager, baseUrl, verifier }`,
  `runWriteEndpoint`, servant `client` derivations
  (`cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs`).
- Read endpoints: `GET /tokens` → `[TokenIdJSON]` (token list);
  `GET /tokens/:id/facts/:key` → `FactResponse` (fact get).
- Signing reference: `addKeyWitness :: SignKeyDSIGN Ed25519DSIGN ->
  ConwayTx -> ConwayTx`, `serialize' (natVersion @11)`
  (`cardano-node-clients .../E2E/Setup.hs`) — re-expressed in `Sign.hs`.
- Workflows intended surface (#289 spec): `registerToken`, `insertFact`,
  `updateFact`, `deleteFact`, `retractRequest`, `rejectExpired`,
  `endCage`, each `… -> IO (Either WorkflowError UnsignedTx)` with
  `UnsignedTx { unsignedTxCbor :: ByteString }`.

## Slices (one bisect-safe commit each)

- **S1 — package skeleton + arg parsing.** New `cardano-mpfs-cli`
  package + `mpfs-cli` exe; `Options.hs` parsers for all 9 subcommands
  with `--help`; `Hex.hs` validation; handlers route to **stubs** that
  emit a JSON `{"stub": "Workflows.<fn>", "args": {…}}` on stdout and a
  log line on stderr. Added to `cabal.project`. Gate: `cabal build
  mpfs-cli` + a `--help` smoke per subcommand.
- **S2 — Bech32 `.skey` + signing.** `Key.hs` + `Sign.hs`: decode
  `ed25519_sk1…` → `SignKeyDSIGN Ed25519DSIGN`; witness unsigned CBOR →
  signed CBOR. Unit test: known key decodes; signed tx contains a vkey
  witness for the derived key hash. Gate: unit test + build.
- **S3 — submission glue + read-only commands.** `Submit.hs` +
  `Output.hs`: POST `/submit`, parse txId/error, await; wire `token
  list` and `fact get` to the real read endpoints (these exist today).
  Gate: build; read-only commands proven against a live server in the
  E2E slice (live-boundary smoke deferred to S5 — documented).
- **S4 — flip write subcommands to real workflows.** Replace S1 stubs
  with `Cardano.MPFS.Workflows.<fn>` → `Sign` → `Submit` → JSON, as each
  #289 function publishes. May land as several small commits (one per
  workflow) tracked under this slice.
- **S5 — E2E walkthrough + README.** Shell script register-token →
  insert → update → get → retract → end against the local cluster,
  exit 0; `cardano-mpfs-cli/README.md` walkthrough per subcommand.

S1–S3 are unblocked and land this session (auxiliary + structural).
S4 depends on #289 surface; S5 depends on S4.

## Constitution check

- **Fact-provider boundary** — CLI never builds protocol txs itself;
  fetch/verify/build comes from `cardano-mpfs-workflows`. CLI only signs
  + submits + formats. ✅
- **Ledger-native types** — signing reuses `cardano-ledger-conway`
  `ConwayTx` + `cardano-crypto-class`; no shadow ledger types. ✅
- **Records of functions, not typeclasses** — HTTP via `MpfsHttp`
  record; workflows via their `HttpClient` record. ✅
- **Verifier portability** — unaffected: signing/ledger deps live only
  in the native CLI, never in the verifier or workflows path. ✅

## Risks

- **#289 lag** — mitigated by stub-first; S4 is a mechanical flip.
- **Bech32 key shape** — assumes raw `ed25519_sk1…` (32-byte ed25519
  seed). If the operator's keys are CIP-1852 extended or TextEnvelope
  JSON, S2 widens; flagged in spec non-goals and revisited if needed.
- **Live-boundary** — read-only + submit paths only truly exercise at
  the node boundary; S5 e2e is the boundary proof, not the unit suite.
