# Quickstart: consuming the proof-replaying `Cardano.MPFS.Client`

**Audience**: wallet or signer integrators who want to enforce
"verify before sign" on every MPFS response without running any
Cardano ledger code on the client.

This quickstart mirrors the E2E spec — reading either one top-to-bottom
is enough to wire up a proof-replaying client.

## 1. Add the dependency

```cabal
build-depends:
    , cardano-mpfs-client  ^>= 0.1
```

No `cardano-ledger-*`, no `crypton`, no C-FFI packages are required.
The library builds on GHC-native, `wasm32-wasi`, and GHC-JS with
byte-identical outputs.

## 2. Verify an honest response

```haskell
import Cardano.MPFS.Client
    ( BootTxResponse
    , shouldAccept
    , verifyBootTxResponse
    )

verifyHonestBoot :: BootTxResponse -> IO ()
verifyHonestBoot response =
    response `shouldAccept` verifyBootTxResponse
```

Every per-endpoint response type has a matching verifier:

| Endpoint | Response type | Verifier |
|----------|---------------|----------|
| `POST /tx/boot` | `BootTxResponse` | `verifyBootTxResponse` |
| `POST /tx/request/{insert,delete,update}` | `RequestTxResponse` | `verifyRequestTxResponse` |
| `POST /tx/retract` | `RetractTxResponse` | `verifyRetractTxResponse` |
| `POST /tx/reject` | `RejectTxResponse` | `verifyRejectTxResponse` |
| `POST /tx/end` | `EndTxResponse` | `verifyEndTxResponse` |
| `POST /tx/update` | `UpdateTxResponse` | `verifyUpdateTxResponse` |

## 3. Enforce on every received response

The canonical client loop before presenting an unsigned tx to a
human signer:

```haskell
signOrReject
    :: UpdateTxResponse
    -> IO (Either VerifyError SignedTx)
signOrReject response =
    case verifyUpdateTxResponse response of
        Left err ->
            pure (Left err)
        Right () -> do
            signed <- signWithHardwareWallet
                        (envTx response)
            pure (Right signed)
```

What `verifyUpdateTxResponse` guarantees, given that it returns
`Right ()`:

- Every `WitnessedUtxo` the response carries (state input, each
  contributing request, each funding input) has a CSMT inclusion
  proof that cryptographically replays against the advertised
  `snapshot.utxo_root` *and* whose in-proof `(key, value)` equals
  the advertised `(tx_in, tx_out)`.
- Every `TrieFact` in `UpdateProof.trie_read` has an Aiken-parity
  MPF proof that cryptographically replays against the advertised
  `UpdateProof.trie_root` *and* whose in-proof `(key, value)`
  equals the advertised `(key, value)`. Absence claims replay as
  exclusion proofs; presence claims as inclusion.
- The envelope is structurally well-formed: 32-byte hashes where
  they should be, non-empty hex where it should be, tx CBOR
  decodes.

What it does **not** guarantee (covered by other tickets):

- That `snapshot.utxo_root` is the *right* root — the signer must
  still anchor this against an independent source (e.g. a chain
  follower). Tracked by a separate anchor ticket.
- That the proofs describe the same transaction as the `tx` field
  in the response. Tracked by issue
  [#227](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/227).

## 4. Reading a rejection

The `VerifyError` returned by a rejected response is deliberately
rich enough to skip a debugger:

```haskell
reportError :: VerifyError -> Text
reportError = \case
    MalformedHex path value ->
        "malformed hex at " <> path <> ": " <> value
    WrongHexLength path want got ->
        "wrong hash length at " <> path <>
            ": want " <> tshow want <>
            " bytes, got " <> tshow got <> " bytes"
    EmptyBlockId ->
        "snapshot chainpoint has empty block id"
    MalformedTxCbor path ->
        "tx CBOR at " <> path <> " failed to decode"
    CsmtReplayFailed path reason ->
        "CSMT replay at " <> path <> " failed: " <> reason
    MpfReplayFailed path reason ->
        "MPF replay at " <> path <> " failed: " <> reason
```

The `path` field is a dotted reference to the exact field in the
response envelope that failed verification, e.g.
`retract.state_ref.utxo_proof`. The `reason` is one of a small
fixed vocabulary: `"root mismatch"`, `"key binding mismatch"`,
`"value binding mismatch"`, `"malformed proof CBOR"`,
`"inclusion proof for absence claim"`,
`"exclusion proof for inclusion claim"`.

## 5. Writing tests that read as prose

Downstream test code gets the same DSL the MPFS E2E suite uses.
Pair every positive scenario with at least one negative scenario:

```haskell
import Cardano.MPFS.Client
    ( shouldAccept
    , shouldRejectWith
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , forgingRandomUtxoProofAt
    , tamperingTrieValueAt
    , verifyBootTxResponse
    , verifyUpdateTxResponse
    )

spec :: Spec
spec = describe "my wallet's MPFS client" $ do
    it "accepts honest boot responses" $ do
        response <- server `postsBoot` ownerAddress
        response `shouldAccept` verifyBootTxResponse

    it "rejects a forged boot funding proof" $ do
        response <- server `postsBoot` ownerAddress
        forged   <- response
                      `forgingRandomUtxoProofAt` "boot.funding[0]"
        forged
            `shouldRejectWith` verifyBootTxResponse
            $ csmtReplayFailedAt "boot.funding[0].utxo_proof"

    it "rejects a tampered trie value in an update batch" $ do
        response <- server `postsUpdate` (tokenId, ownerAddress)
        forged   <- response `tamperingTrieValueAt` 0
        forged
            `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt "update.trie_read[0].mpf_proof"
```

A new reader who opens only this test file can list what the
verifier accepts, what it rejects, and at what field granularity.
That is the manual.
