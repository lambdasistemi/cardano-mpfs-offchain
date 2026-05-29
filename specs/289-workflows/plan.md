# Plan — #289 `cardano-mpfs-workflows`

## Tech stack

- Haskell GHC 9.8.4 (haskell.nix `cabalProject'`, auto-discovers
  packages listed in `cabal.project`).
- New package `cardano-mpfs-workflows/` (library + `hspec` test-suite).
- Library deps (the whole point — keep this list minimal):
  `base`, `aeson`, `bytestring`, `text`, `cardano-mpfs-api`,
  `cardano-mpfs-client`. **Forbidden**: `http-client`, `servant-*`,
  `cardano-ledger-*`, `template-haskell`.

## Constitution check

- *Ledger-native types, no shadow representations* — we add zero new
  ledger representations; `Tx ConwayEra` is built and serialized inside
  `cardano-mpfs-client`.
- *Service boundaries are records of functions, not typeclasses* —
  `HttpClient` is a newtype over a function, matching the convention.
- *Server is a fact-provider; never returns unsigned tx* — unchanged;
  the **client** builds the unsigned tx locally from facts, which is
  exactly the fact-provider model. No server code is touched.
- *Verifiers are pure, no IO/网络/time* — we call them unchanged; the
  only `IO` in this package is the caller-supplied transport.
- *Verifier deps must cross-compile to WASM/JS* — the workflows package
  imports no ledger/servant/http-client module; #258 compiles it to
  WASM+JS. The ledger dependency closure is already present via
  `cardano-mpfs-client` (which #258 must cross-compile regardless); we
  add nothing new to it.

Re-check after design: PASS (no deviations introduced during design).

## The seam (verified by reading the client)

| Workflow | Path (POST) | Wire request | Facts | Verifier | Builder |
|---|---|---|---|---|---|
| `registerToken` | `/facts/boot` | `BootRequest` | `BootFacts` | `verifyBootFacts root` | `bootCageTx cfg pol` |
| `insertFact` | `/facts/request/insert` | `InsertRequest` | `RequestInsertFacts` | `verifyRequestInsertFacts root` | `requestInsertCageTx cfg pol` |
| `updateFact` | `/facts/request/update` | `UpdateValueRequest` | `RequestUpdateFacts` | `verifyRequestUpdateFacts root` | `requestUpdateCageTx cfg pol` |
| `deleteFact` | `/facts/request/delete` | `DeleteRequest` | `RequestDeleteFacts` | `verifyRequestDeleteFacts root` | `requestDeleteCageTx cfg pol` |
| `applyRequests` | `/facts/update` | `UpdateRequest` | `UpdateFacts` | `verifyUpdateFacts root` | `updateCageTx cfg pol` |
| `retractRequest` | `/facts/retract` | `RetractRequest` | `RetractFacts` | `verifyRetractFacts root` | `retractCageTx cfg pol` |
| `rejectExpired` | `/facts/reject` | `RejectRequest` | `RejectFacts` | `verifyRejectFacts root` | `rejectCageTx cfg pol` |
| `endCage` | `/facts/end` | `EndRequest` | `EndFacts` | `verifyEndFacts cfg root` | `endCageTx cfg pol` |

`endCage`'s verifier additionally takes the `CageConfig` (request-set
prefix derivation). All wire request types and `{Op}Facts` types come
from `cardano-mpfs-api`; all verifiers/builders from
`cardano-mpfs-client`.

## Module design

`Cardano.MPFS.Workflows.Internal` (core, not the public surface):

```haskell
-- transport abstraction (record of functions)
data HttpError = HttpStatus !Int !ByteString | HttpTransport !Text
newtype HttpClient = HttpClient
  { runHttpPost :: Text -> ByteString -> IO (Either HttpError ByteString) }
  -- relative path -> JSON body -> JSON response. The impl closes over
  -- base URL + manager (CLI) or fetch (SPA).

newtype UnsignedTx = UnsignedTx { unsignedTxCbor :: ByteString }
  deriving (Eq, Show)        -- submission-ready Conway CBOR

data WorkflowsConfig = WorkflowsConfig
  { wcCage        :: CageConfig
  , wcPolicy      :: WalletPolicy
  , wcTrustedRoot :: TrustedRoot
  }

data WorkflowError
  = WorkflowHttpError   !HttpError
  | WorkflowDecodeError !String
  | WorkflowVerifyError !VerifyError
  | WorkflowBuildError  !BuildError
  deriving (Show)

-- the one helper every workflow shares. Note: the builder argument is
-- pre-composed with serializeCageTx at the call site, so this signature
-- never names `Tx ConwayEra` -> no ledger import in this package.
runFactsWorkflow
  :: (ToJSON req, FromJSON facts)
  => HttpClient -> Text -> req
  -> (facts    -> Either VerifyError verified)
  -> (verified -> Either BuildError ByteString)   -- builder >>> serialize
  -> IO (Either WorkflowError UnsignedTx)
```

`Cardano.MPFS.Workflows` (public): the 8 workflows + re-exports of
`WorkflowError`, `UnsignedTx`, `WorkflowsConfig`, `HttpClient`, the
wire request types, `CageConfig`, `WalletPolicy`, `TrustedRoot`. Each
workflow is a one-liner over `runFactsWorkflow`, e.g.

```haskell
registerToken :: HttpClient -> WorkflowsConfig -> BootRequest
              -> IO (Either WorkflowError UnsignedTx)
registerToken http WorkflowsConfig{..} req =
  runFactsWorkflow http "/facts/boot" req
    (verifyBootFacts wcTrustedRoot)
    (\v -> serializeCageTx <$> bootCageTx wcCage wcPolicy v)
```

`serializeCageTx :: Tx ConwayEra -> ByteString` is the **new**
`cardano-mpfs-client` export (`= serialize' (natVersion @11)`); the
intermediate `Tx ConwayEra` is inferred, never written in this package.

## Test strategy

Unit tests use a **stub `HttpClient`** that records the
`(path, body)` it is handed and returns canned JSON. They assert,
per workflow:

1. **Routing** — correct path + the expected JSON request body.
2. **HTTP error** → `WorkflowHttpError`.
3. **Decode error** (malformed JSON) → `WorkflowDecodeError`.
4. **Verify error** — a well-formed `{Op}Facts` whose snapshot root ≠
   `wcTrustedRoot` → `WorkflowVerifyError (TrustedRootMismatch …)`,
   proving HTTP→decode→verify are wired.
5. Where reachable deterministically (e.g. `registerToken` with a
   root-matching, empty-wallet `BootFacts`): verify **passes** and the
   build stage is reached, propagating its `BuildError`
   (`EmptyFunding`) → `WorkflowBuildError`, proving verify→build→error
   wiring.

The **happy path** (verify passes on real proofs → real `Tx` → CBOR)
needs cryptographically valid fact fixtures and a live `/submit`; it is
an **integration test gated on #288** — documented in tasks.md, not
faked. The crypto itself is already covered by the client's own suite.

## Build wiring

- `cabal.project`: add `cardano-mpfs-workflows/`.
- `nix/project.nix`: `workflows-unit-tests` component,
  `workflows-unit-tests-runner` (writeShellApplication, no
  `MPFS_BLUEPRINT` needed unless a builder requires it — verify during
  S2), `packages.workflows-tests`, `apps.workflows-unit-tests`.
- `flake.nix`: add `workflows-tests` to the inherited `packages` set.
- `justfile`: `unit-workflows` recipe; add `just unit-workflows` to
  `ci`.

## Slices (one bisect-safe commit each)

- **S1** — `cardano-mpfs-client`: add `serializeCageTx` export
  (new module `Cardano.MPFS.Client.Cage.Serialize`) + client unit test.
  Additive, independently useful.
- **S2** — Stand up `cardano-mpfs-workflows`: cabal + build wiring +
  `Internal` core types + `runFactsWorkflow` + public module with
  `registerToken` + tests (routing, HTTP/decode/verify/build errors).
- **S3** — Requester request workflows `insertFact`, `updateFact`,
  `deleteFact` (one commit; identical shape) + tests.
- **S4** — `applyRequests` (oracle update) + tests.
- **S5** — `retractRequest` + `rejectExpired` + tests.
- **S6** — `endCage` (verifier takes `CageConfig`) + tests.
- **S7 (deferred, not implemented)** — live integration test +
  `/submit` round-trip; follows #288 merge. Recorded only.
