# Phase 0 Research: Fact-provider pivot

This research log resolves the design questions raised by the spec
and Phase 1 of the plan. The pivot's foundation (WASM viability) was
already settled in the spike result on issue #257; that result is
referenced rather than re-derived here.

## Q0-1 — DSL helpers home

**Decision**: Cage-protocol DSL helpers (boot, request × 3, retract,
end, update, reject) live in `cardano-mpfs-client` (the in-repo
client library that already ships the verifier) under
`Cardano.MPFS.Client.Cage.{Boot,Request,Retract,End,Update,Reject}`.
The helpers compose the generic `Cardano.Node.Client.TxBuild`
operational-monad primitives (`spend`, `payTo'`, `attachScript`,
`mint`, `collateral`) imported from upstream `cardano-node-clients`.
The MPFS-specific cage protocol logic (datums, redeemers, asset-name
derivation, MPF fold) is MPFS-domain and belongs here, not upstream.

**Rationale**:

- `cardano-mpfs-client` is the existing in-repo client library;
  MOOG and any future client already pull it for the verifier.
  Adding cage builders gives clients one package for the full
  "verify the facts, build the tx" journey.
- Cage helpers are MPFS-protocol semantics (cage NFT mint logic,
  state datum, request datum, MPF inclusion fold). That is
  domain-specific to MPFS and does not belong upstream in
  `cardano-node-clients` — that repo's scope is the generic
  operational-monad TxBuild DSL, with no MPFS knowledge.
- Cross-target portability (Principle IX: GHC-native + GHC-WASM +
  GHC-JS) is already mandated for `cardano-mpfs-client`. The cage
  helpers must satisfy the same constraint (browser wallets build
  txs locally), so co-locating them keeps the cross-target
  discipline in one cabal package.
- The `Real.Boot` module on `main` is already a pure cage builder
  of this shape (`bootTokenCore` returns `BootCore { bcProgram ::
  TxBuild ... }`); the relocation is rename + repackage from
  `cardano-mpfs-offchain/lib/.../Real/Boot.hs` to
  `cardano-mpfs-client/lib/.../Cage/Boot.hs`, not a rewrite.

**Alternatives considered**:

- *Keep helpers in `cardano-mpfs-offchain`*: rejected. The helpers
  must live in a place a non-server client can consume. The server
  package's deps (RocksDB, chain-follower, indexer) don't belong in
  a wallet's transitive closure.
- *Push helpers upstream into `cardano-node-clients`*: rejected.
  The cage protocol is MPFS-specific; upstream `cardano-node-clients`
  is generic and must stay generic. Pushing cage logic upstream would
  pollute it with downstream protocol semantics and force every
  consumer of the DSL to drag MPFS code along.
- *Host helpers in `cardano-ledger-inspector`*: rejected. That
  repo's narrow scope is "Conway tx operations the WASI artifact
  exposes". Adding cage-protocol shape muddies it.
- *New in-repo package `cardano-mpfs-builder`*: rejected. One more
  cabal package to maintain without enough payload to justify it.
  `cardano-mpfs-client` already exists and already ships a
  cross-target client library; the cage helpers fit there.
  is the right home.

## Q0-2 — Cage helpers signature shape

**Decision**: Each helper is a pure function

```haskell
bootCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Either BuildError (Tx ConwayEra)
```

(and equivalents for the other seven operations). The helper
encapsulates the DSL run-loop internally; callers see a pure
function from facts to a balanced unsigned tx.

**Rationale**:

- Wallets shouldn't have to know about the operational-monad
  Program-of-TxInstr execution machinery. Encapsulating it makes
  the wallet seam a one-line call.
- Returning `Tx ConwayEra` directly aligns with what
  `cardano-cli`-style signers consume.
- `WalletPolicy` enforcement happens inside the helper, before
  the helper returns success. Wallet seam: build → check
  WalletPolicy in same call → sign.
- `VerifiedXFacts` carries the proof-token evidence the verifier
  produced; the helper trusts it without re-validating.

**Alternatives considered**:

- *Return a `TxBuild` program for the caller to run*: rejected.
  Forces every wallet to know about the DSL run-loop and the
  evaluator's IO type. The pivot's value is "wallet doesn't know
  about server-shaped tx logic"; making the wallet know about
  DSL run-loops contradicts that.
- *Return `Tx` together with a `WalletPolicy` violation if any*:
  rejected. Conflates success and the policy-check responsibility.
  Cleaner: helper enforces; if violated, returns `Left
  PolicyViolation`. Caller never receives a tx that violates its
  own policy.

## Q0-3 — IndexerTx primitives needed

**Decision**: Four new primitives in `Cardano.MPFS.Indexer.Reads`:

```haskell
readStateUtxoAt
    :: TokenId
    -> IndexerTx (Maybe ResolvedStateUtxo)

readRequestUtxosAt
    :: TokenId
    -> IndexerTx [ResolvedRequestUtxo]

readNamedRequestUtxo
    :: TxIn
    -> IndexerTx (Maybe ResolvedRequestUtxo)

readTrieFact
    :: TokenId
    -> ByteString  -- key
    -> IndexerTx (Maybe TrieFact)
```

Plus the existing `readSnapshot` and `readWalletInputsAt` from #249.

**Rationale**:

- Each handler composes the primitives it needs inside one
  `runIndexerTx ctx $ do { … }` block — same atomicity discipline
  PR #253 introduced.
- The primitives mirror what the legacy `*Impl` modules read
  imperatively today. After the pivot, those `*Impl` modules are
  deleted; the primitives carry the read responsibilities.
- All four primitives are cheap shape-equivalent ops over the
  CSMT (state UTxO, request UTxOs at the per-token request
  address, named UTxO lookup, trie-key inclusion/exclusion).

**Alternatives considered**:

- *Single bulk primitive `readEverythingFor TokenId`*: rejected.
  Different endpoints want different subsets; a bulk read either
  over-fetches (cost) or has too many fields (`Maybe`-y mess).
- *Push these into `cardano-utxo-csmt` so they're shared with
  potential future consumers*: deferred. Today MPFS is the only
  consumer; adding cross-repo abstractions is premature.

## Q0-4 — MPF fold home

**Decision**: The server returns MPF facts as raw data (key, value-
or-absence, proof against the snapshot's trie root). The wallet
runs the fold *during transaction building* — inside the
`Cage.Update.updateCageTx` helper — to compute the new
`stateRoot` that goes into the new state UTxO's datum.

**Rationale**:

- The fold result is a single hash (the new trie root) that lives
  in the consumed state UTxO's datum. The wallet has to know this
  hash to build the new state UTxO, so it must run the fold.
- The server *could* compute and return the new root, but that's
  redundant data (the wallet derives it deterministically from
  inputs the wallet already has) and adds a "trust the server's
  fold" surface the wallet would have to verify anyway.
- Running the fold inside the helper is byte-equivalent to the
  current server-side behavior in
  `Cardano.MPFS.TxBuilder.Real.Update.updateTokenImpl` — modulo
  module location.

**Alternatives considered**:

- *Server returns the new root pre-computed*: rejected. Either
  redundant (wallet recomputes) or adds an unverified-pre-computed-
  state field (server lies → wrong root → tx fails on-chain). Net
  loss either way.
- *Server returns the post-fold trie state in full (all leaves)*:
  rejected. Bandwidth cost and structural redundancy.

## Q0-5 — Protocol-parameter shape on the wire

**Decision**: Each `POST /facts/{op}` response carries:

```json
{
  "snapshot": { "utxo_root": "…", "slot": 1234, "block_id": "…" },
  "wallet_utxos": [ … ],
  // … other per-endpoint fields …
  "protocol_parameters": {
    "verified": false,
    "cbor": "<hex>"
  }
}
```

The CBOR is the full Conway `PParams ConwayEra` byte string. The
wallet decodes it via `cardano-ledger-binary` and uses it for fee
and ExUnits calculation. The `"verified": false` field is explicit
documentation that the server cannot prove these parameters; the
wallet's `WalletPolicy` is the documented mitigation.

**Rationale**:

- CBOR round-trip preserves fidelity (every Conway pp field, no
  silently-elided fields if the wire schema lags).
- `"verified": false` makes the gap visible in any tool that pretty-
  prints the response (debuggers, swagger UI, MOOG logs).
- Future signed-pp protocols (Mithril, etc.) can introduce a
  `"verified": true` shape with a signature payload without a
  wire-contract migration.

**Alternatives considered**:

- *Return pp as JSON with explicit fields*: rejected. JSON
  representations of Conway pp tend to lag behind the ledger's
  field-set; CBOR is fewer surprises.
- *Don't return pp at all; wallet is on its own*: rejected.
  Disenfranchises wallets without their own node connection
  (browser wallets in the future, MOOG when run against a remote
  server). The "unverified pp" path is honest.
- *Server fetches pp from cardano-node fresh per request and
  includes a slot anchor*: this is what we already do. The
  freshness comes from the IndexerTx's checkpoint slot; the
  request-time fetch is via `Provider.queryProtocolParams` (the
  one Provider call we keep in `runIndexerTx`-adjacent code).

## Q0-6 — Verifier signature shape

**Decision**: One `verifyXFacts` function per operation:

```haskell
verifyBootFacts        :: TrustedRoot -> BootFacts        -> Either VerifyError VerifiedBootFacts
verifyRequestFacts     :: TrustedRoot -> RequestFacts     -> Either VerifyError VerifiedRequestFacts
verifyRetractFacts     :: TrustedRoot -> RetractFacts     -> Either VerifyError VerifiedRetractFacts
verifyEndFacts         :: TrustedRoot -> EndFacts         -> Either VerifyError VerifiedEndFacts
verifyUpdateFacts      :: TrustedRoot -> UpdateFacts      -> Either VerifyError VerifiedUpdateFacts
verifyRejectFacts      :: TrustedRoot -> RejectFacts      -> Either VerifyError VerifiedRejectFacts
```

`Verified*Facts` is a newtype wrapper around the input `*Facts`
record — the proof-token evidence that the proofs have been
validated. The cage-protocol DSL helpers consume `Verified*Facts`
and refuse to operate on bare `*Facts`.

**Rationale**:

- Per-endpoint signatures keep the per-shape differences (single
  UTxO vs batch + MPF facts) clear at the call site.
- Newtype wrappers make "I have verified this" reflectable at
  the type level — the cage-protocol DSL helpers literally
  cannot be called with unverified facts.
- Cross-target byte-identity (Principle IX) is straightforward
  to assert via QuickCheck because each function is a pure
  fold.

**Alternatives considered**:

- *One polymorphic verifier with a typeclass dispatch*: rejected
  for Principle II (no typeclasses for service interfaces; not
  load-bearing here, but polymorphic verifier under a typeclass
  is also harder to cross-compile).
- *No newtype wrappers; just trust the caller did the verify
  step*: rejected. The point of separating verify from build is
  to make the trust boundary visible. A newtype is the cheapest
  way to make "I verified this" type-checkable.

## Q0-7 — Cross-repo sequencing

**Decision**: Land in this order across two repos:

1. `cardano-mpfs-offchain` PR (cage DSL helpers added to
   `cardano-mpfs-client` + server cutover + verifier rewrite). The
   cage helpers and the server cutover land in the same PR because
   they live in the same monorepo (cage helpers in
   `cardano-mpfs-client`, server cutover in `cardano-mpfs-offchain`,
   both packages in this repo). Hard cutover commit removes the
   legacy server surface and the legacy verifier in one merge.
2. `lambdasistemi/moog` PR (client migration). Depends on (1) being
   on main; pulls the new endpoints + cage helpers via the bumped
   `cardano-mpfs-offchain` pin. MOOG main is broken between (1)
   and (2) — that is the narrow cutover window.

The original three-repo sequencing rejected here was an artefact of
the wrong Q0-1 decision (helpers upstream in `cardano-node-clients`).
With helpers in `cardano-mpfs-client`, only two repos move.

**Rationale**:

- Each repo's CI runs independently; the dependency direction is
  unambiguous (clients depend on libraries, not vice versa).
- Clients (MOOG) cannot land before the server because the
  client's HTTP-call expectations would not match the server's
  endpoints.
- The "broken MOOG main" window must not contain any production
  deploy; this is an operational discipline, not a technical
  guarantee.

**Alternatives considered**:

- *Synchronised merge across two repos*: rejected as operationally
  fragile. Two separate CI pipelines + two independent reviews + a
  coordinator is more failure-prone than the staged sequence.
- *MOOG ships parallel "old + new" implementations and toggles
  via a feature flag*: rejected. The whole pivot is a hard
  cutover; a feature-flag toggle defeats the point.

## Pinned facts

- The IndexerTx primitives library introduced in #249 (PR #253) is
  the foundation; this slice extends it with four new primitives.
- The upstream `cardano-node-clients` TxBuild DSL is at the pinned
  commit used by `cardano-mpfs-offchain` today; no bump needed for
  this pivot (the cage helpers are added to in-repo
  `cardano-mpfs-client`, which already imports the pinned DSL).
- `cardano-mpfs-client` exists already; this slice rewrites its
  internals (verifier from tx-shape grammar to pure proof folds) and
  adds the cage helper module tree, but keeps the package name.
- MOOG's `MPFS.API` module currently exposes `requestInsert`,
  `requestDelete`, `requestUpdate`, `bootToken`, `endToken`,
  `retractChange`, `updateToken`, plus reads (`getToken`,
  `getTokenFacts`, `submitTransaction`, `waitNBlocks`,
  `getTransaction`). The reads remain (they're not the pivot's
  scope); the seven write-shaped ones are replaced.
- `cardano-mpfs-onchain`'s validators are unchanged. The pivot
  is purely client-side.

## Output

`research.md` — this file.
