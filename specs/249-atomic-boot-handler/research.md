# Phase 0 Research: Atomic POST /tx/boot

This research log resolves the design questions raised by the spec.
The slice carries no NEEDS CLARIFICATION markers — every decision below
is grounded in code already in the repo or one of its pinned deps.

## Q0-1 — Where does the atomic transaction live?

**Decision**: The atomic read closure is constructed in
`Cardano.MPFS.Application.withApplication`, alongside the existing
`exists`, `resolve`, `root`, and `proof` closures. It uses the same
`utxoRt :: CSMT.RunTransaction` value that those closures already use.
The closure body is a single `CSMT.transact utxoRt $ do { … }` block
that performs every read needed for a boot response.

**Rationale**:

- `withApplication` is the only place in the application that holds
  `RunTransaction` over `UnifiedColumns` and the projections to
  `InUtxo` / `InCage` / `InRollbacks`. Any other location would have
  to re-derive these from the open `DB`, which would mean either
  duplicating wiring or threading the run-transaction value through
  the `Context`.
- Co-locating the new reader with `exists`, `resolve`, `root`, and
  `proof` makes the change reviewable as a local diff — the reviewer
  can compare the four existing single-concern closures to the new
  multi-concern one and see by inspection that all reads happen in
  one block (SC-005).
- The reader is then exposed on `Context` as a record-of-functions
  field so `bootTokenImpl` can call it without taking another
  dependency on `Application`.

**Alternatives considered**:

- *Build the closure inside `mkRealTxBuilder`*: rejected. The builder
  factory is parameterized on `UtxoProofFn` today; making it depend
  on `RunTransaction` and `UnifiedColumns` would push wiring
  responsibility down the stack and defeat the layering. Tests would
  also need to construct a real `UnifiedColumns`, which is overkill.
- *Add a generic `WithIndexerTransaction :: forall a. (… -> IO a) -> IO a`
  bracket on `Context`*: rejected. That generalises to every handler
  but, for the boot slice, makes the atomicity claim invisible:
  every caller would have to be inspected to confirm it does its work
  inside the bracket, which violates SC-005's "single function reveals
  the atomicity" criterion.
- *Push the closure into `cardano-utxo-csmt`*: rejected. The MPFS
  reader needs the cage checkpoint for the `BundleSnapshot`, which is
  a `chain-follower`/MPFS concept, not a `cardano-utxo-csmt` one.

## Q0-2 — How do we walk the address subtree?

**Decision**: Reuse `cardano-utxo-csmt`'s
`Application.Database.Implementation.Transaction.queryByAddress` shape:
inside the boot reader transaction, call
`collectValues CSMTCol [] addressKey`, then for each `Indirect{jump}`:

1. `query KVCol jump` to get the `TxOut` CBOR bytes,
2. `generateInclusionProof fromKV KVCol CSMTCol jump` to get the
   inclusion proof against the same CSMT root,

and bundle the (`TxIn`, `TxOut bytes`, `proof`) triples plus the
checkpoint into the result.

**Rationale**:

- `collectValues` walks the CSMT subtree at a prefix in
  `O(M)` where `M` is the number of leaves under the prefix. That's
  the cost surface FR-005/SC-003 demand.
- After the absolute-jump migration in `haskell-mts`
  (`Indirect.jump` is now the full key, no longer prefix-relative),
  the lookup `query KVCol jump` is a direct point lookup. No
  reconstruction step.
- Using exactly the same primitives the `cardano-utxo-csmt`
  `queryByAddress` uses keeps both call sites coherent — if a future
  change to `collectValues` semantics ships, both this reader and
  `queryByAddress` move together. (Memory: "Check downstream
  primitives".)

**Alternatives considered**:

- *Rebuild the address-prefix walk over `KVCol` directly*: rejected.
  `KVCol` is keyed by `TxIn`-encoded keys, not by address prefix; the
  CSMT is the structure that gives us address prefixing in `O(M)`.
- *Call `Provider.queryUTxOs` (the existing path)*: rejected — see
  Q0-5. It is `O(total UTxOs in ledger)`.
- *Call `cardano-utxo-csmt`'s public `queryByAddress` directly*:
  considered. The shape is right, but the primitive returns
  `[(key, value)]` and not the proof — we need the proof inside the
  same transaction. We inline its body, plus the proof generation,
  into the boot reader's transaction so the entire response is
  composed from one snapshot.

## Q0-3 — What is the exact return shape?

**Decision**: The reader is a record-of-functions field with type:

```haskell
type AtomicCageReader m =
    Addr
    -> m (Either AtomicReaderError AtomicCageRead)

data AtomicCageRead = AtomicCageRead
    { acrSnapshot :: BundleSnapshot
    , acrInputs   :: [(TxIn, ByteString, ByteString)]
        -- ^ (input ref, TxOut CBOR bytes, CSMT inclusion proof)
    }

data AtomicReaderError
    = AtomicReaderNoCheckpoint
    | AtomicReaderNoUtxos
    | AtomicReaderKvMissing TxIn
    | AtomicReaderRootMissing
```

**Rationale**:

- The triple `(TxIn, TxOut CBOR, proof)` matches one-to-one the
  `WitnessedInput` record `bootTokenImpl` already emits, so the
  builder's transformation from "what the reader returned" to "what
  the response carries" is a single `map` with no extra IO.
- Returning `Either` rather than throwing forces the HTTP layer to
  map each variant to a deterministic response (FR-004). The four
  constructors enumerate the four spec edge cases.
- The reader returns *raw* `TxOut` CBOR bytes (not a deserialized
  `TxOut ConwayEra`). The deserialization happens at `bootTokenImpl`,
  matching the pattern the existing `resolveUtxo` already follows.
  Avoids putting ledger decoding inside the indexer reader.

**Alternatives considered**:

- *Have the reader return already-decoded `TxOut ConwayEra`*:
  rejected. Forces ledger-version coupling deeper into the indexer
  layer; today `KVCol` simply stores opaque bytes.
- *Bundle the proof inside `WitnessedInput` directly*: rejected.
  `WitnessedInput` lives in `Cardano.MPFS.TxBuilder` and is part of
  the response shape. The reader is one layer below — it shouldn't
  know about response shapes.
- *Use exceptions for the four edge cases*: rejected. Servant
  handlers convert `Either` cleanly into 4xx/5xx responses; throwing
  pushes the mapping into a catch-all and loses precision.

## Q0-4 — How does the test seam look?

**Decision**: Add an optional override field to `AppConfig`:

```haskell
data AppConfig = AppConfig
    { … existing fields …
    , atomicCageReaderOverride
        :: !(Maybe (AtomicCageReader IO))
    }
```

`Serve.hs` (production entry point) sets it to `Nothing`. Test
fixtures with `followerEnabled = False` set it to `Just …` where the
override is constructed from a wallet-side `LocalStateQuery` (i.e. the
test acts as a wallet, on its own connection, using
`Provider.queryUTxOs` — which is acceptable on the wallet side because
each test queries infrequently against a tiny devnet UTxO set).

In `withApplication`, the production `AtomicCageReader` is replaced by
the override when it is `Just`. In `Context`, only the chosen reader
is exposed.

**Rationale**:

- `AppConfig.followerEnabled = False` already exists for tests that
  drive the indexer manually. Without the seam these tests cannot
  produce a `BundleSnapshot` at all (no checkpoint is ever written
  by the manual driver), which is exactly the failure mode we hit
  during the work that produced this spec.
- Configuring at startup (not at request time) means the seam cannot
  be reached by a misrouted HTTP request in production: the override
  field on `AppConfig` is `Nothing` in `Serve.hs` and there is no CLI
  flag for it.
- A field on `AppConfig` is type-safe and discoverable — it appears
  in every grep for `AppConfig`. An environment variable or a side
  channel would not.

**Alternatives considered**:

- *Skip the test seam; force tests to enable the follower*: rejected.
  The fixtures the seam unblocks are the existing harness used by
  the #243 family of work; rewriting them all to spin a chain
  follower for every test substantially expands scope.
- *Make the reader a `forall m. Monad m`-polymorphic effect and let
  tests pick `Identity` / `StateT`*: rejected. `mkMockContext` already
  exists for that purpose. The override seam is for tests that are
  otherwise running a *real* `Context IO` (devnet-backed) but with
  the chain follower disabled.

## Q0-5 — Why is removing `queryUTxOs` from boot the right reason?

**Decision**: It is removed because **`GetUTxOByAddress` in
`cardano-node` is implemented as a linear scan over the entire ledger
UTxO set**. The cost is `O(total UTxOs on chain)`, not `O(K)`. At
mainnet scale (millions of UTxOs) the call is unusable in a hot path
and a busy server effectively DoS's its own node.

This is the *only* sufficient reason. Earlier drafts of this slice
attempted to justify removal by appealing to torn reads (#250), to
Principle IV (External Signing), or to the proof-bearing contract.
Those reasons are real but secondary — the atomicity bug could in
principle be fixed without removing the node query, and Principle IV
applies to keys not to query shapes. The cost-of-the-query argument is
what makes removal mandatory.

**Rationale**:

- Memory recorded:
  `feedback_no_queryutxosbyaddress.md`. The lesson is to trace why a
  primitive is forbidden to its operational cost, not to a policy
  abstraction.
- Once the cost argument is admitted, no caller on a hot path can
  invoke `queryUTxOs`. The boot path is the first one to be cleaned
  up; tokens-list, retract, reject, update, end follow on subsequent
  slices (#250 / #252).
- The `Provider.queryUTxOs` field is *not* deleted in this slice —
  test seams and one-off CLI uses still call it on the wallet side,
  which is acceptable. A Haddock warning is added to the field
  documenting the cost.

**Alternatives considered**:

- *Delete `Provider.queryUTxOs` outright in this slice*: rejected.
  The boot test seam needs the wallet-side query. Deletion is
  scheduled for after every server-side caller has been migrated.
- *Keep `queryUTxOs` and "fix" the racy boot read*: rejected. The
  cost surface is unsolvable behind the node query.

## Q0-6 — What about the `requireBundleSnapshot` handler-level read?

**Decision**: Remove it from `txBootHandler`. The boot reader returns
the snapshot as part of its result, so the handler no longer needs a
separate snapshot read before invoking the builder. The handler shape
becomes:

```haskell
txBootHandler ctx (BootRequest addrHex) = do
    addr <- requireAddr addrHex
    er   <- liftIO (atomicCageReader ctx addr)
    case er of
        Left err -> throwError (mapAtomicError err)
        Right rd -> do
            bundle <-
                liftIO
                    $ Tx.bootToken
                        (txBuilder ctx) rd addr
            pure (mkBootTxResponse bundle)
```

(`Tx.bootToken`'s signature changes from
`BundleSnapshot -> Addr -> …` to `AtomicCageRead -> Addr -> …`, and
its body uses `acrSnapshot` and `acrInputs` directly instead of
calling `queryUTxOs`.)

**Rationale**:

- A separate `requireBundleSnapshot` call before the builder is
  exactly the torn-read source we are eliminating. Even if the
  builder's reads are atomic, a handler that reads the snapshot
  *first* and then reads inputs *later* re-opens the race window.
- The other handlers (`requestInsert`, `requestDelete`,
  `requestUpdate`, `updateToken`, `retractRequest`,
  `rejectRequests`, `endToken`) keep `requireBundleSnapshot` until
  their own slices replace it with a per-handler atomic reader.
  This slice intentionally fixes one endpoint at a time.

**Alternatives considered**:

- *Replace `requireBundleSnapshot` for every handler in this slice*:
  rejected. Spec is scoped to boot; doing every handler at once
  multiplies risk and review burden. Each remaining handler will get
  its own slice (per #250 acceptance criteria).
- *Leave `requireBundleSnapshot` in place and re-read inside the
  builder*: rejected — the second read is the bug.

## Pinned facts

- `cardano-utxo-csmt`'s `queryByAddress` already exists at
  `cardano-utxo-csmt/lib/Cardano/UTxOCSMT/Application/Database/Implementation/Transaction.hs`,
  using `collectValues CSMTCol [] addressKey` and returning
  `[(key, value)]`. We inline the body and add proof generation.
- `haskell-mts` `Indirect.jump` is absolute (post-2026-04 contract
  change), so `query KVCol jump` is a direct lookup.
- `cardano-mpfs-offchain` `Application.hs` already uses
  `mapColumns InUtxo` to project `UnifiedColumns` to the UTxO
  columns; we share that projection.
- `Provider.queryUTxOs` exists at
  `cardano-mpfs-offchain/lib/Cardano/MPFS/Provider.hs:55–58` and is
  used by tests; staying as a Provider field, gaining a Haddock
  warning, but with zero call sites in `cardano-mpfs-offchain/lib/`
  after this slice.

## Output

`research.md` — this file.
