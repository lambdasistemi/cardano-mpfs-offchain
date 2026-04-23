# Contract: `Cardano.MPFS.Client.Verify.Replay`

**Status**: new internal module in `cardano-mpfs-client`.

**Purpose**: the single place that turns advertised fields + proof
bytes into `Either VerifyError ()`. All per-endpoint verifiers
delegate to it; test code reaches into it through the DSL, never
directly.

## Surface

```haskell
module Cardano.MPFS.Client.Verify.Replay
    ( replayWitnessedUtxo
    , replayTrieFact
    ) where

replayWitnessedUtxo
    :: Text
       -- ^ Field path prefix (e.g. @"retract.state_ref"@). The
       --   primitive appends @.utxo_proof@, @.tx_out@, etc. as it
       --   narrows the failure.
    -> ByteString
       -- ^ Trusted CSMT root (raw 32 bytes) — the caller decodes
       --   @snapshot.utxo_root@ once and passes the bytes.
    -> WitnessedUtxo
    -> Either VerifyError ()

replayTrieFact
    :: Text
       -- ^ Field path prefix (e.g. @"update.trie_read[0]"@).
    -> ByteString
       -- ^ Trusted MPF root (raw 32 bytes).
    -> TrieFact
    -> Either VerifyError ()
```

## Behavioural contract: `replayWitnessedUtxo`

Given `(prefix, root, WitnessedUtxo{txIn, txOut, utxoProof})`:

1. Hex-decode `txOut` → `tout :: ByteString`. If decode fails,
   return `Left (MalformedHex (prefix <> ".tx_out") _)` — matches
   the existing structural layer.
2. Hex-decode `utxoProof` → `proofBytes`. On failure return
   `Left (MalformedHex (prefix <> ".utxo_proof") _)`.
3. Parse `proofBytes` via `CSMT.Core.CBOR.parseProof`. On failure
   return
   `Left (CsmtReplayFailed (prefix <> ".utxo_proof") "malformed proof CBOR")`.
4. Compute `expectedKey = blake2b_256 (cbor txIn)` using the
   `mts:csmt-verify` pure Blake2b; compare against `proofKey`. On
   mismatch return
   `Left (CsmtReplayFailed (prefix <> ".utxo_proof") "key binding mismatch")`.
5. Compare `proofValue` against `tout`. On mismatch return
   `Left (CsmtReplayFailed (prefix <> ".utxo_proof") "value binding mismatch")`.
6. Call `verifyInclusionProof root proofBytes :: Bool`. On `False`
   return
   `Left (CsmtReplayFailed (prefix <> ".utxo_proof") "root mismatch")`.
7. Return `Right ()`.

## Behavioural contract: `replayTrieFact`

Given `(prefix, trieRoot, TrieFact{key, value, mpfProof})`:

1. Hex-decode `key` → `kbytes`, `mpfProof` → `pbytes`, and (when
   present) `value` → `vbytes`. Hex decode failures surface as
   `MalformedHex` at the exact subfield.
2. Inspect `value`:
   - `Just _` → **inclusion claim**:
     1. Parse `pbytes` as an MPF inclusion proof via the Aiken-parity
        decoder from `mts:mpf-write`. On failure:
        `MpfReplayFailed (prefix <> ".mpf_proof") "malformed proof CBOR"`.
     2. Confirm the in-proof shape is "inclusion"; otherwise emit
        `"exclusion proof for inclusion claim"`.
     3. In-proof key-path == `aikenKeyPath kbytes`; mismatch →
        `"key binding mismatch"`.
     4. In-proof value == `vbytes`; mismatch →
        `"value binding mismatch"`.
     5. `MPF.Verify.verifyInclusionProof trieRoot … == True`;
        `False` → `"root mismatch"`.
   - `Nothing` → **exclusion claim**:
     1. Parse `pbytes` as an MPF exclusion proof. On failure:
        `"malformed proof CBOR"`.
     2. Confirm the shape is "exclusion"; otherwise emit
        `"inclusion proof for absence claim"`.
     3. In-proof key-path == `aikenKeyPath kbytes`.
     4. `MPF.Verify.verifyExclusionProof trieRoot … == True`.
3. Return `Right ()`.

## Integration points

- `Cardano.MPFS.Client.Verify.checkWitnessedUtxo` calls
  `replayWitnessedUtxo` after the existing structural checks pass.
  The trusted `utxoRoot` is decoded from the caller's
  `VerificationSnapshot` once and threaded through.
- `Cardano.MPFS.Client.Verify.checkTrieFact` is rewritten to call
  `replayTrieFact` after structural checks; the trusted `trieRoot`
  is decoded from `UpdateProof.trie_root` once.
- No caller outside `Cardano.MPFS.Client.Verify*` imports
  `Cardano.MPFS.Client.Verify.Replay` — the DSL combinators call
  the per-endpoint verifiers, not the primitives.

## Purity and cross-target contract

- Pure Haskell: no `IO`, no mutable state, no `unsafePerformIO`.
- Only dependencies new to `cardano-mpfs-client`:
  - `mts:csmt-verify` (already WASM-safe).
  - `mts:mpf-write` (WASM-safe since PR #147).
  - `cborg` (already WASM-safe in the `haskell-mts` toolchain).
- The module compiles under GHC-native, `wasm32-wasi`, and GHC-JS
  with byte-identical `Either VerifyError ()` for the same inputs.
