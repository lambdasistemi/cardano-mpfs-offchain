# Data Model: Cryptographic CSMT + MPF proof replay

**Feature**: 178-crypto-proof-replay
**Date**: 2026-04-23

This feature does not introduce new wire types. It adds a new
*verification layer* that consumes existing response types and emits
a richer `VerifyError`. The entities below are either new
(client-internal) or extended (existing) types.

## Entity: `VerifyError` (extended)

**Status**: extended — two new constructors.

**Existing constructors** (preserved byte-for-byte):

- `MalformedHex Text Text` — hex decode failure.
- `WrongHexLength Text Int Int` — wrong byte length for a 32-byte hash.
- `EmptyBlockId` — snapshot chainpoint block id decoded to zero bytes.
- `MalformedTxCbor Text` — top-level tx CBOR failed hex decode.

**New constructors**:

- `CsmtReplayFailed Text Text`
  - Field 1 (`path`): dotted field path rooted at the endpoint name,
    e.g. `"retract.state_ref.utxo_proof"`,
    `"boot.funding[3].utxo_proof"`.
  - Field 2 (`reason`): human-readable one-liner — one of
    `"root mismatch"`, `"key binding mismatch"`,
    `"value binding mismatch"`, `"malformed proof CBOR"`.
- `MpfReplayFailed Text Text`
  - Same shape as above, for MPF proofs rooted against an
    `UpdateProof.trie_root`, e.g.
    `"update.trie_read[0].mpf_proof"`.

**Invariants**:

- `MalformedHex` / `WrongHexLength` / `EmptyBlockId` /
  `MalformedTxCbor` are emitted *before* any replay is attempted.
- Binding-mismatch errors (`"key binding mismatch"`,
  `"value binding mismatch"`) are emitted before root-mismatch
  errors — a key-binding violation is more informative.
- `CsmtReplayFailed` and `MpfReplayFailed` are the *only* errors the
  cryptographic layer produces.

## Entity: `WitnessedUtxo` (unchanged wire, extended semantic contract)

**Status**: unchanged on the wire; semantically now the subject of a
cryptographic replay.

**Fields**:

- `txIn   : TxIn`
- `txOut  : Hex` — CBOR-encoded `TxOut` bytes.
- `utxoProof : Hex` — CSMT inclusion-proof CBOR.

**Replay contract**:

1. Decode `utxoProof` → `InclusionProof Hash`.
2. `proofKey == blake2b_256(cbor(txIn))` (key binding).
3. `proofValue == bytes(txOut)` (value binding).
4. `verifyInclusionProof utxoRoot utxoProofBytes == True` (root
   replay).

## Entity: `TrieFact` (unchanged wire, extended semantic contract)

**Status**: unchanged on the wire.

**Fields**:

- `key      : Hex`
- `value    : Maybe Hex`
- `mpfProof : Hex` — Aiken-parity MPF proof CBOR.

**Replay contract** (dispatched by `value`):

- `value = Just v`:
  1. Decode `mpfProof` as an inclusion proof.
  2. In-proof key path == `aikenKeyPath(key)`.
  3. In-proof value == `v`.
  4. `MPF.Verify.verifyInclusionProof trieRoot … == True`.
- `value = Nothing`:
  1. Decode `mpfProof` as an exclusion proof.
  2. In-proof key path == `aikenKeyPath(key)`.
  3. `MPF.Verify.verifyExclusionProof trieRoot … == True`.

## Entity: `VerificationSnapshot` (unchanged)

**Status**: unchanged. Still the carrier of the advertised
`utxo_root` (+ indexed chainpoint). Trusting `utxo_root` itself
remains out of scope (separate anchor ticket).

## Entity: `ReplayPrimitives` (NEW — internal)

**Status**: new internal module — not part of the public API beyond
re-exports through `Cardano.MPFS.Client.Verify`.

**Module**: `Cardano.MPFS.Client.Verify.Replay`.

**Purpose**: the only place in the client that knows how to turn
`ByteString` proof bytes and advertised fields into
`Either VerifyError ()`. All six per-endpoint response verifiers call
into it.

**Surface** (see `contracts/replay-primitives.md` for detail):

```haskell
replayWitnessedUtxo
    :: Text           -- field path prefix
    -> ByteString     -- snapshot utxo_root
    -> WitnessedUtxo
    -> Either VerifyError ()

replayTrieFact
    :: Text           -- field path prefix
    -> ByteString     -- UpdateProof.trie_root
    -> TrieFact
    -> Either VerifyError ()
```

## Entity: `Client.Verify.DSL` (NEW — public)

**Status**: new public module.

**Module**: `Cardano.MPFS.Client.Verify.DSL`.

**Purpose**: make the E2E and unit tests read as tutorial code and
give downstream wallets a vocabulary of the same shape.

**Surface**:

```haskell
-- Assertions
shouldAccept     :: (HasCallStack, Show a)
                 => a -> (a -> Either VerifyError ()) -> Expectation
shouldRejectWith :: (HasCallStack, Show a)
                 => a -> (a -> Either VerifyError ())
                      -> ErrorMatcher
                      -> Expectation

-- Matchers
csmtReplayFailedAt :: Text -> ErrorMatcher
mpfReplayFailedAt  :: Text -> ErrorMatcher

-- Forgery helpers (deterministic by default; IO variant draws random bytes)
forgingRandomUtxoProofAt :: Text -> a -> IO a
forgingWrongRootAt       :: Text -> a -> IO a
tamperingTxOutAt         :: Text -> a -> IO a
tamperingTrieValueAt     :: Int  -> a -> IO a
dropToExclusionAt        :: Int  -> a -> IO a
promoteToInclusionAt     :: Int  -> a -> IO a
```

**Invariants**:

- Every helper is exported via `Cardano.MPFS.Client`.
- Every helper has Haddock that links back to the spec scenario
  that introduces it.
- The forgery helpers are the *only* documented way to construct a
  rejected response in tests — hand-editing response records is
  discouraged and not shown in any scenario.
