# Phase 0 Research: Atomic POST /tx/request/{insert,delete,update}

This research log resolves the design questions raised by the spec.
The slice carries no NEEDS CLARIFICATION markers — every decision
below is grounded in code already in the repo (post-PR #253) or in
the existing `requestImpl` body.

## Q0-1 — Share `Boot/Inputs.hs` or duplicate?

**Decision**: Rename
`cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Inputs.hs`
to
`cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Wallet/Inputs.hs`.
The module's exported types (`InputRow`, `decodeAll`, `ledgerPair`,
`rowToWitness`) are domain-neutral — they describe how the indexer's
raw bytes become ledger-typed pairs and witnessed-input rows.
Importers update from
`Cardano.MPFS.TxBuilder.Real.Boot.Inputs` to
`Cardano.MPFS.TxBuilder.Real.Wallet.Inputs`. No behaviour change.

**Rationale**: Both Boot and Request consume identical data. Two
copies would drift; a shared module under a name that doesn't imply
"boot-only" is the right home.

**Alternatives considered**:

- *Leave under Boot, import across handlers*: rejected. The name
  lies — readers seeing
  `Cardano.MPFS.TxBuilder.Real.Boot.Inputs.InputRow` in a Request
  module would assume a Boot-specific dependency.
- *Duplicate in Request*: rejected. Drift is guaranteed; the
  decoder either matches the indexer's encoding or the response
  is wrong, and we don't want two answers to that question.
- *Defer until update/reject also need it*: rejected. Three
  callers (boot, insert, delete, update — actually four including
  the boot case) is enough to justify the shared home now.

## Q0-2 — One `requestCore` parameterised, or three?

**Decision**: One pure `requestCore` taking the operation kind as
an argument; three thin public wrappers (`requestInsertCore`,
`requestDeleteCore`, `requestUpdateCore`) parameterise the
operation and call the shared core.

**Rationale**: This matches today's source — `requestImpl` already
takes an `Operation` parameter and is shared by the three impls. The
DSL migration doesn't change that structure; it only replaces the
imperative tx assembly inside `requestImpl` with a `TxBuild`
program. Keeping three public wrappers preserves the
`TxBuilder.requestInsert` / `requestDelete` / `requestUpdate` field
shapes used by `mkRealTxBuilder` and the HTTP handlers.

**Alternatives considered**:

- *Three independent `*Core` functions, each ~60 lines*: rejected.
  Triplicates the shared assembly (output construction, redeemer
  derivation, etc.), opens room for divergence.
- *No public wrappers; HTTP handlers call `requestCore` directly
  with the operation tag*: rejected. The `TxBuilder` record-of-
  functions interface is the project's service boundary
  (Principle II); each endpoint has its own field there.

## Q0-3 — Where does `runRequestBuilder` live?

**Decision**: `Cardano.MPFS.TxBuilder.Real`, alongside the existing
`runBootBuilder`. The two functions share the same shape (decode →
fetch `pp` → run DSL `build` → assemble envelope) and both depend
on `Provider IO`.

**Rationale**: PR #253 established `Real.hs` as the IO surface for
boot. Adding `runRequestBuilder` next to it keeps the IO call sites
greppable in one file. The HTTP handler imports neither
`Real.runRequestBuilder` nor `Real.runBootBuilder` directly — it
calls through `txBuilder ctx`.

**Alternatives considered**:

- *Inline in HTTP handler*: rejected. Mixes wire-shape concerns
  with tx-build IO and breaks the IO/handler separation the boot
  slice established.
- *New module
  `Cardano.MPFS.TxBuilder.Real.Request.Build`*: rejected. The IO
  surface is small (~40 lines per handler family); fragmenting
  across modules costs more than co-location.

## Q0-4 — DSL combinator coverage

**Decision**: The three request transactions need:

- `spend` per consumed wallet input,
- `payTo'` to construct the pending-request output at the
  per-token request address with the request datum inline,
- `collateral` to designate the script-witness slot,

… and nothing else. No `mint` (request txs don't mint), no
`spendScript` (they don't consume any script UTxO; the state UTxO
is consulted out-of-band via tip-checking), no `attachScript`
(the script-witness is on the consumed wallet input only — no
Plutus script attaches in the request tx body).

**Rationale**: Read `requestImpl`'s body in
`cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Request.hs`.
It builds a tx body with one input (the seed wallet UTxO), one
output (the pending request), one collateral. No mint, no script
witnesses on the redeemer side. The DSL combinators above cover
this exactly.

**Alternatives considered**:

- *Use `output`, not `payTo'`, and inline the datum manually*:
  rejected. `payTo'` is the DSL idiom for "value + inline datum at
  address"; matches the boot slice's `bootStateOutput`.

## Q0-5 — Where does the per-token request address come from?

**Decision**: Looked up via `State.tokens` (an existing `State`
field on `Context`) inside the IO orchestrator
`runRequestBuilder`. The pure `requestCore` receives the resolved
`requestAddr` as an argument rather than the `TokenId`; it does
not depend on `State`.

**Rationale**: Today `requestImpl` does:

```haskell
requestImpl cfg prov st proofFn snap tid key op addr = do
    pp <- queryProtocolParams prov
    utxos <- queryUTxOs prov addr            -- ← removed
    -- … look up the token's request address via cfg + tid …
    -- … build the tx …
```

The "look up the token's request address" step is `pure` and uses
only `cfg` + `tid` (no `State`). Looking at
`requestAddrFromCfg cfg tid (network cfg)` in
`TxBuilder/Real/Internal.hs` confirms this. So `requestCore` can
either:

a) take `tid` and call `requestAddrFromCfg` itself (still pure), or
b) take the resolved `requestAddr` from the orchestrator.

**Going with (a)** — the function takes `tid`. Keeps the
orchestrator simple; `requestAddrFromCfg` is pure so this doesn't
add IO to the core.

## Q0-6 — Does `requestImpl` use anything else from `Provider`?

**Decision** (by source reading): `requestImpl` calls:

- `queryUTxOs prov addr` — FORBIDDEN, removed.
- `queryProtocolParams prov` — kept; called from the IO
  orchestrator (same as boot).
- `evaluateTx prov` — kept; called from the IO orchestrator inside
  the DSL `build` evaluator (same as boot).

No `posixMsToSlot` / `posixMsCeilSlot` calls (request txs don't
set validity intervals). Provider stays in `runRequestBuilder`,
not in `requestCore`.

## Q0-7 — Reuse boot's `noCtxInterpretIO` or define another?

**Decision**: Promote `noCtxInterpretIO` from the local helper in
`Cardano.MPFS.TxBuilder.Real` (where it lives after PR #253) to a
shared helper in the same module — already there. Both boot and
request orchestrators use it.

## Pinned facts

- `Cardano.MPFS.Indexer.Reads` exposes `readSnapshot` and
  `readWalletInputsAt`. No new primitives are needed.
- `Cardano.MPFS.TxBuilder.Real.Boot.Inputs` will be renamed to
  `Cardano.MPFS.TxBuilder.Real.Wallet.Inputs` (only an import-path
  change, no semantics).
- `Cardano.MPFS.TxBuilder.Real.Internal` exposes
  `requestAddrFromCfg`, `mkRequestDatum`, `toPlcData`,
  `mkInlineDatum`, `requestLockedAda` — all the helpers we need.
  No edits there.
- The wire contracts in
  `Cardano.MPFS.HTTP.Types` (`InsertRequest`, `DeleteRequest`,
  `UpdateRequest`) are unchanged.
- `Provider.queryUTxOs` is kept as a Provider field (test
  fixtures still use it on the wallet side); after this slice
  there are zero call sites of `queryUTxOs` in
  `cardano-mpfs-offchain/lib/`.

## Output

`research.md` — this file.
