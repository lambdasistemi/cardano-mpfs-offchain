# Contract: `Cardano.MPFS.Client.Verify.DSL`

**Status**: public module in `cardano-mpfs-client`. Shipped in
[#228](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/228);
the forgery DSL landed in
[#229](https://github.com/lambdasistemi/cardano-mpfs-offchain/pull/229).

**Purpose**: let the E2E and unit tests read as tutorial code, and
expose the same vocabulary to downstream wallet integrators so they
can reuse it inside their own test suites.

## Shape

Three layers, each re-exported from `Cardano.MPFS.Client` so downstream
users get a single import:

1. **Assertions** — `shouldAccept`, `shouldRejectWith`.
2. **Error matchers** — one smart constructor per `VerifyError` case
   plus a `withReason` combinator.
3. **Forgery DSL** — an `operational` free monad with two program
   types (`CsmtForge`, `TrieForge`) and one runner per response
   envelope.

```haskell
import Cardano.MPFS.Client
    ( -- Assertions
      shouldAccept
    , shouldRejectWith
      -- Matchers
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , malformedHexAt
    , wrongHexLengthAt
    , withReason
      -- Forgery instructions
    , CsmtForge
    , TrieForge
    , flipProof
    , flipTxOut
    , flipSnapshotRoot
    , flipTrieValue
    , dropToExclusion
    , flipTrieRoot
      -- Per-endpoint runners
    , runForgeBoot
    , runForgeRequest
    , runForgeRetract
    , runForgeReject
    , runForgeEnd
    , runForgeUpdate
    , runForgeUpdateTrie
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

Succeeds when the verifier returns `Right ()`. On failure, the
structured `VerifyError` and the full response are reported via
`expectationFailure`, so the scenario author sees the error without
needing a debugger.

```haskell
shouldRejectWith
    :: (HasCallStack, Show a)
    => a
    -> (a -> Either VerifyError ())
    -> ErrorMatcher
    -> Expectation
```

Succeeds when the verifier returns `Left err` and `matcher err ==
True`. The matcher's description is rendered in the failure message:

```
expected  : CsmtReplayFailed "boot.funding[0].utxo_proof" "root mismatch"
but got   : CsmtReplayFailed "boot.funding[0].utxo_proof" "value binding mismatch"
```

## Matchers

```haskell
csmtReplayFailedAt :: Text -> ErrorMatcher
mpfReplayFailedAt  :: Text -> ErrorMatcher
malformedHexAt     :: Text -> ErrorMatcher
wrongHexLengthAt   :: Text -> ErrorMatcher
```

Each matches on the `VerifyError` path only. Chain `withReason` to
pin the reason string:

```haskell
csmtReplayFailedAt "retract.state_ref.utxo_proof"
    `withReason` "root mismatch"
```

The fixed reason vocabulary is documented in
[`verify-error.md`](verify-error.md).

## Forgery DSL — design

The forgery surface is an `operational` free monad split into two
instruction types:

```haskell
data CsmtForgeI a where
    FlipProof        :: Text -> CsmtForgeI ()
    FlipTxOut        :: Text -> CsmtForgeI ()
    FlipSnapshotRoot :: CsmtForgeI ()

data TrieForgeI a where
    FlipTrieValue   :: Int -> TrieForgeI ()
    DropToExclusion :: Int -> TrieForgeI ()
    FlipTrieRoot    :: TrieForgeI ()

type CsmtForge = Program CsmtForgeI
type TrieForge = Program TrieForgeI
```

Each constructor is **one deterministic one-field tampering**. No
`StdGen`, no `IO`; scenarios are byte-for-byte reproducible. The
split into two programs is load-bearing: only an `UpdateTxResponse`
carries an MPF trie, so the type checker rejects trie tamperings
on other envelopes at compile time.

### Smart constructors

One per instruction, lifting into the corresponding program:

```haskell
flipProof        :: Text -> CsmtForge ()
flipTxOut        :: Text -> CsmtForge ()
flipSnapshotRoot :: CsmtForge ()

flipTrieValue    :: Int -> TrieForge ()
dropToExclusion  :: Int -> TrieForge ()
flipTrieRoot     :: TrieForge ()
```

Programs compose with do-notation:

```haskell
twoSpots :: CsmtForge ()
twoSpots = do
    flipTxOut "state"
    flipProof "requests[0]"
```

### Path grammar

`FlipProof` and `FlipTxOut` take a role path. The supported shapes
differ by endpoint:

| Endpoint               | Roles                                                          |
|------------------------|----------------------------------------------------------------|
| `BootTxResponse`       | `"funding[<i>]"`                                               |
| `RequestTxResponse`    | `"funding[<i>]"`                                               |
| `RetractTxResponse`    | `"request_in"`, `"state_ref"`, `"funding[<i>]"`                |
| `RejectTxResponse`     | `"state"`, `"request_ins[<i>]"`, `"funding[<i>]"`              |
| `EndTxResponse`        | `"state"`, `"funding[<i>]"`                                    |
| `UpdateTxResponse`     | `"state"`, `"requests[<i>]"`, `"funding[<i>]"`                 |

`FlipTrieValue` and `DropToExclusion` take an `Int` index into
`UpdateProof.trie_read`; `FlipTrieRoot` takes no argument.

Invalid or out-of-range paths are a test-author bug: the runner
fails immediately via `error` rather than silently passing through.

### Runners

One runner per response envelope. `UpdateTxResponse` gets two —
one for each program type:

```haskell
runForgeBoot       :: CsmtForge () -> BootTxResponse    -> BootTxResponse
runForgeRequest    :: CsmtForge () -> RequestTxResponse -> RequestTxResponse
runForgeRetract    :: CsmtForge () -> RetractTxResponse -> RetractTxResponse
runForgeReject     :: CsmtForge () -> RejectTxResponse  -> RejectTxResponse
runForgeEnd        :: CsmtForge () -> EndTxResponse     -> EndTxResponse
runForgeUpdate     :: CsmtForge () -> UpdateTxResponse  -> UpdateTxResponse
runForgeUpdateTrie :: TrieForge () -> UpdateTxResponse  -> UpdateTxResponse
```

A runner is a plain function — no type-class dispatch (per
Principle II, records of functions rather than type classes).

### Expected rejection reason

Each instruction tampers one field and surfaces a specific replay
error on a known field path:

| Instruction                       | Rejection path suffix                       | Reason                                    |
|-----------------------------------|---------------------------------------------|-------------------------------------------|
| `flipProof "funding[0]"`          | `funding[0].utxo_proof`                     | `"malformed proof CBOR"` or `"root mismatch"` |
| `flipTxOut "state"`               | `state.utxo_proof`                          | `"value binding mismatch"`                |
| `flipSnapshotRoot`                | first `.utxo_proof` reached by the verifier | `"root mismatch"`                         |
| `flipTrieValue i`                 | `trie_read[i].mpf_proof`                    | `"root mismatch"`                         |
| `dropToExclusion i`               | `trie_read[i].mpf_proof`                    | `"root mismatch"`                         |
| `flipTrieRoot`                    | first `trie_read[i].mpf_proof`              | `"root mismatch"`                         |

The path prefix the verifier prepends (`boot.`, `update.`, …) is
fixed by the endpoint; scenarios assert the full dotted path:

```haskell
csmtReplayFailedAt "boot.funding[0].utxo_proof"
```

> **Note**: the MPF shape-mismatch reasons
> (`"inclusion proof for absence claim"`,
> `"exclusion proof for inclusion claim"`) are not structurally
> determinable on small tries and are collapsed to `"root mismatch"`
> in the current implementation — see the note on T033 in
> [`tasks.md`](../tasks.md).

## Tutorial shape

A complete scenario using the DSL reads as:

```haskell
spec :: Spec
spec = describe "cryptographic CSMT replay at /tx/boot" $ do
    it "accepts an honest response" $
        honestBoot `shouldAccept` verifyBootTxResponse

    it "rejects a funding proof tampered to random bytes" $
        runForgeBoot (flipProof "funding[0]") honestBoot
            `shouldRejectWith` verifyBootTxResponse
            $ csmtReplayFailedAt
                "boot.funding[0].utxo_proof"
```

A reviewer who has never seen `cardano-mpfs-client` internals can,
from reading a single scenario, enumerate:

- What endpoint is exercised (by the runner name).
- What the "honest" expectation is.
- What field is tampered (by the forgery instruction).
- What kind of verifier-level error the client emits.

Every other scenario in the suite follows the same shape, so the
E2E spec *is* the client-library manual.
