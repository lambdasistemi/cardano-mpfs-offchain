# Verifier Contract: cardano-mpfs-client.Verify

The verifier surface in `cardano-mpfs-client` after the pivot. Pure
proof-validity functions over facts bundles.

## Signatures

```haskell
verifyBootFacts        :: TrustedRoot -> BootFacts        -> Either VerifyError VerifiedBootFacts
verifyRequestFacts     :: TrustedRoot -> RequestFacts     -> Either VerifyError VerifiedRequestFacts
verifyRetractFacts     :: TrustedRoot -> RetractFacts     -> Either VerifyError VerifiedRetractFacts
verifyEndFacts         :: TrustedRoot -> EndFacts         -> Either VerifyError VerifiedEndFacts
verifyUpdateFacts      :: TrustedRoot -> UpdateFacts      -> Either VerifyError VerifiedUpdateFacts
verifyRejectFacts      :: TrustedRoot -> RejectFacts      -> Either VerifyError VerifiedRejectFacts
```

## Semantics

Each function MUST:

1. Assert `xfSnapshot.utxoRoot == trustedRoot`.
   - Failure: `Left SnapshotMismatch`.
2. For every `(TxIn, txOutBytes, csmtProof)` in the bundle:
   - Recompute the CSMT leaf hash from `(TxIn, txOutBytes)`.
   - Run `verifyCsmtInclusion xfSnapshot.utxoRoot leafHash csmtProof`.
   - Failure: `Left (CsmtProofInvalid <TxIn>)`.
3. (Tier-3 only — `UpdateFacts` and `RejectFacts`)
   For every `TrieFact { tfKey, tfValue, tfMpfProof }`:
   - Decode the trie root from the `state_utxo`'s datum.
   - For inclusion (`tfValue = Just v`): run
     `verifyMpfInclusion trieRoot tfKey v tfMpfProof`.
   - For exclusion (`tfValue = Nothing`): run
     `verifyMpfExclusion trieRoot tfKey tfMpfProof`.
   - Failure: `Left (MpfProofInvalid <key>)`.
4. On all checks passing: return `Right (VerifiedXFacts xfBundle)`.

`VerifyError` ADT:

```haskell
data VerifyError
    = SnapshotMismatch
    | CsmtProofInvalid TxIn
    | MpfProofInvalid ByteString
    | MalformedFactsBundle Text
    deriving (Show, Eq)
```

## Pure-fold property (Principle VIII)

Each `verifyXFacts` is a pure function with no `IO`, no
networking, no filesystem, no time, no non-determinism. The
`TrustedRoot` and the input `XFacts` are the only inputs; the
output is a deterministic `Either`. This is the original
formulation of Principle VIII; the pivot's verifier matches it
exactly (the previous tx-shape grammar is gone).

## Cross-target byte identity (Principle IX)

For every input pair `(TrustedRoot, XFacts)`, the output of
`verifyXFacts` MUST be byte-identical across:

- GHC native (Linux x86_64).
- GHC-WASM (wasm32-wasi).
- GHC-JS.

Asserted by a QuickCheck property in
`cardano-mpfs-client/test/...`. The property generates random
`(root, facts)` pairs, runs each backend, and compares the
output `Either VerifyError ()` (after stripping the
`VerifiedXFacts` newtype to allow byte-comparison).

## Newtype invariant

`VerifiedXFacts` constructors are NOT exported from
`Cardano.MPFS.Client.Verify`. The only legitimate way to obtain a
`VerifiedXFacts` is to call `verifyXFacts` and unwrap the `Right`.
This makes "I have verified this bundle" type-checkable at the
cage-protocol DSL helper boundary; the helpers refuse to operate
on bare `XFacts`.

## What's gone

Removed from the pivot:

- `verifyBootTxResponse`, `verifyRequestTxResponse`, etc. — the
  per-endpoint tx-shape validators.
- `Cardano.MPFS.Client.Verify.Conservation` — the value-conservation
  helper sketched in the brainstorm. Obsolete because the wallet
  builds the tx; conservation is structural.
- Any import of `Cardano.Ledger.Api.Tx`, `Tx ConwayEra`, or any
  transaction-grammar type. The verifier deals only in bytes
  (txOutCbor) and proof structures (CsmtProof, MpfProof).

A reviewer searching for these names after the pivot returns zero
matches. SC-002 / SC-003 of the spec are the acceptance criteria.

## Forbidden patterns

These patterns MUST NOT appear in
`cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify*`:

- Any `IO` in a function signature.
- Any `import Cardano.Ledger.Api.Tx`.
- Any `import Cardano.Ledger.Conway.Tx`.
- Any inspection of a `Tx` value.
- Any HTTP / network / filesystem operation.

## Test plan

Per-endpoint, three buckets:

1. **Happy path**: a known-good facts bundle anchored to a known
   root verifies with `Right VerifiedXFacts ...`.
2. **Snapshot tamper**: flip a byte in the response's
   `xfSnapshot.utxoRoot`; verifier returns `Left SnapshotMismatch`.
3. **Proof tamper**: flip a byte in any included CSMT or MPF
   proof; verifier returns `Left (CsmtProofInvalid ...)` or
   `Left (MpfProofInvalid ...)` accordingly.

Tier-3 (Update / Reject) adds a fourth:

4. **Trie fact tamper**: flip a byte in `tfValue`; verifier
   returns `Left (MpfProofInvalid ...)`.

All tests run under all three target backends (native, WASM, JS)
and compare outputs.
