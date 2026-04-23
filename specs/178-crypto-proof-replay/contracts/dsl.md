# Contract: `Cardano.MPFS.Client.Verify.DSL`

**Status**: new public module in `cardano-mpfs-client`.

**Purpose**: make the E2E and unit tests read as tutorial code, and
expose the same vocabulary to downstream wallet integrators so they
can reuse it inside their own test suites.

## Re-exports

Every symbol in this contract is re-exported from
`Cardano.MPFS.Client` so downstream users get a single import:

```haskell
import Cardano.MPFS.Client
    ( shouldAccept
    , shouldRejectWith
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , forgingRandomUtxoProofAt
    , forgingWrongRootAt
    , tamperingTxOutAt
    , tamperingTrieValueAt
    , dropToExclusionAt
    , promoteToInclusionAt
    )
```

## Assertions

```haskell
shouldAccept
    :: (HasCallStack, Show a)
    => a
    -> (a -> Either VerifyError ())
    -> Expectation
```

Succeeds when the verifier returns `Right ()`. On failure, the error
and the full response are reported via `expectationFailure` so the
scenario author sees the structured `VerifyError` without needing a
debugger.

```haskell
shouldRejectWith
    :: (HasCallStack, Show a)
    => a
    -> (a -> Either VerifyError ())
    -> ErrorMatcher
    -> Expectation
```

Succeeds when the verifier returns `Left err` and `match err ==
True`. The matcher's `toString` explains *why* it matched when the
assertion fails, so a regression reads like:

```
expected  : CsmtReplayFailed "boot.funding[0].utxo_proof" "root mismatch"
but got   : CsmtReplayFailed "boot.funding[0].utxo_proof" "value binding mismatch"
```

## Matchers

```haskell
csmtReplayFailedAt :: Text -> ErrorMatcher
mpfReplayFailedAt  :: Text -> ErrorMatcher
```

These match on the `VerifyError` *path* only. The reason is asserted
separately via chainable combinators (`withReason`):

```haskell
csmtReplayFailedAt "retract.state_ref.utxo_proof"
  `withReason` "root mismatch"
```

The default (no `withReason`) matches any reason — used when a
scenario only cares that *some* replay error fires at that path.

## Forgery helpers

All forgery helpers share the signature shape
`Text -> a -> IO a` (or `Int -> a -> IO a` for indexed roles); they
return a deep-copied response with the named field tampered. The
rest of the response is unchanged, so a single scenario can exercise
one-field-at-a-time regressions.

| Helper | What it does | Targets |
|--------|-------------|---------|
| `forgingRandomUtxoProofAt path` | Replaces `utxo_proof` at `path` with random bytes of the same length. | CSMT |
| `forgingWrongRootAt path` | Replaces the *advertised* `utxo_root` (in the envelope's snapshot) with a random root while leaving proofs untouched — exercises "proof correct, root lies". | CSMT |
| `tamperingTxOutAt path` | Flips a byte in the advertised `tx_out` bytes at `path`. | CSMT |
| `tamperingTrieValueAt i` | Flips a byte inside `UpdateProof.trie_read[i].value`. | MPF |
| `dropToExclusionAt i` | Sets `UpdateProof.trie_read[i].value` to `Nothing` while leaving the inclusion proof intact. Models "present claim → absence claim with wrong proof". | MPF |
| `promoteToInclusionAt i` | Inverse: inject a `Just v` and a leftover exclusion proof. | MPF |

Deterministic seeds are available via the `IO`-free variants:

```haskell
forgingRandomUtxoProofAt' :: StdGen -> Text -> a -> a
forgingWrongRootAt'       :: StdGen -> Text -> a -> a
```

## Tutorial shape

A complete scenario using the DSL reads as:

```haskell
spec :: Spec
spec = describe "cryptographic CSMT replay at /tx/boot" $ do
    it "accepts an honest response" $ do
        response <- server `postsBoot` ownerAddress
        response `shouldAccept` verifyBootTxResponse

    it "rejects a funding proof tampered to random bytes" $ do
        response <- server `postsBoot` ownerAddress
        forged   <- response
                      `forgingRandomUtxoProofAt` "boot.funding[0]"
        forged `shouldRejectWith` verifyBootTxResponse
                  $ csmtReplayFailedAt "boot.funding[0].utxo_proof"
```

A reviewer who has never seen `cardano-mpfs-client` internals can,
from reading a single scenario, enumerate:

- What endpoint is exercised.
- What the "honest" expectation is.
- What field is tampered.
- What kind of verifier-level error the client emits.

Every other scenario in the suite follows the same shape, so the
E2E spec *is* the client-library manual.
