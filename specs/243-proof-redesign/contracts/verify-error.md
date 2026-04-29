# Contract: VerifyError vocabulary

The verifier's failure type is a closed sum. Each constructor names a distinct, machine-classifiable failure mode and carries enough information to point a human reviewer at the exact field where verification broke. The convention from #226 is preserved: every constructor includes a dotted field path of the form `<endpoint>.<role>[<index>]?.<leaf>`.

This document lists the additions for the new shapes introduced by feature #243. Existing constructors from #226 (`CsmtReplayFailed`, `MpfReplayFailed`, etc.) are preserved verbatim; the additions are non-overlapping.

## New constructors

```haskell
data VerifyError
    = ...  -- existing constructors kept

    -- snapshot agreement / freshness
    | SnapshotMismatch FieldPath FieldPath   -- two snapshot fields disagree
    | TrustedRootMismatch FieldPath          -- snapshot.utxo_root != supplied trusted root

    -- address derivation
    | StateAddressMismatch FieldPath         -- decoded txout_cbor.address != local global state address
    | RequestAddressMismatch FieldPath       -- decoded txout_cbor.address != local per-cage request address

    -- NFT classification (state UTxO only)
    | StateNftPolicyMismatch FieldPath       -- value.policy != Blueprint.bpStatePolicyId
    | StateNftNameMismatch FieldPath         -- value.asset != requested token id
    | StateNftNotUnique FieldPath            -- value contains zero or more than one NFT under bpStatePolicyId

    -- datum decoding
    | StateDatumMalformed FieldPath          -- couldn't decode datum into the State shape

    -- completeness witness
    | CompletenessProofInvalid FieldPath        -- the witness fails to verify under the trusted root
    | CompletenessExtraLeaf FieldPath UtxoRef   -- claimed leaf not actually under the prefix
    | CompletenessMissingLeaf FieldPath UtxoRef -- on-chain leaf under the prefix not in the witness (only detectable by cross-checking against an independent enumeration)

    -- MPF inclusion / exclusion (extending #226's vocabulary if needed)
    | MpfInclusionInvalid FieldPath          -- proof does not validate (key, value) under the trie root
    | MpfExclusionInvalid FieldPath          -- proof does not validate absence of key under the trie root

    -- write side (uniform UnsignedTxResponse)
    | UnsignedTxDecodeFailed FieldPath        -- unsigned_tx_cbor did not decode as a Conway Tx
    | UnsignedTxInputNotCovered FieldPath UtxoRef
                                              -- a tx input has no entry in inputs[]
    | UnsignedTxInputExtra FieldPath UtxoRef  -- an inputs[] entry doesn't correspond to any tx input
    | UnsignedTxInputCborMismatch FieldPath UtxoRef
                                              -- the inputs[i].txout_cbor disagrees with what the resolver expects

    -- write side extras
    | RequestsCompletenessMissing FieldPath   -- endpoint requires this field but it's absent
    | RequestsCompletenessForbidden FieldPath -- endpoint forbids this field but it's present
    | RequestsCompletenessNotEmpty FieldPath  -- end endpoint received a non-empty witness

    -- confirm
    | ConfirmRefMismatch FieldPath UtxoRef    -- ref != (txId, 0)

    -- token unknown (used as a soft signal, not a verification failure per se)
    | TokenUnknown FieldPath                  -- HTTP 404 no body — caller should fall back to /tokens
```

`FieldPath` is the existing dotted-string newtype from #226's verifier vocabulary.

## Forgery corpus mapping

Every new constructor MUST be triggered by at least one fixture in the forgery corpus shipped with `cardano-mpfs-client/test/.../`. The mapping is recorded explicitly:

| Constructor | Forgery fixture |
|---|---|
| `SnapshotMismatch` | response with two distinct snapshots in nested fields |
| `TrustedRootMismatch` | honest response, wrong trusted root supplied to verifier |
| `StateAddressMismatch` | TxOut CBOR rebuilt with a different script address |
| `RequestAddressMismatch` | request UTxO entry with a wrong address |
| `StateNftPolicyMismatch` | TxOut CBOR with NFT under a wrong policy |
| `StateNftNameMismatch` | TxOut CBOR with NFT under a wrong asset name |
| `StateNftNotUnique` | TxOut CBOR with two NFTs under the trusted state policy |
| `StateDatumMalformed` | TxOut CBOR with a datum that doesn't decode |
| `CompletenessProofInvalid` | flipped byte in the completeness proof |
| `CompletenessExtraLeaf` | extra leaf inserted into the entries list |
| `CompletenessMissingLeaf` | leaf removed from the entries list (validated only when a second source attests the same prefix; documented in the verifier as detectable only with side-channel) |
| `MpfInclusionInvalid` | flipped byte in the MPF proof |
| `MpfExclusionInvalid` | exclusion proof for a key that is actually present |
| `UnsignedTxDecodeFailed` | non-CBOR garbage in `unsigned_tx_cbor` |
| `UnsignedTxInputNotCovered` | tx body references a UTxO not present in `inputs[]` |
| `UnsignedTxInputExtra` | `inputs[]` carries an entry not referenced in the tx |
| `UnsignedTxInputCborMismatch` | `inputs[i].txout_cbor` differs from a separately-resolved truth |
| `RequestsCompletenessMissing` | response from `/tx/oracle/update` with the field absent |
| `RequestsCompletenessForbidden` | response from `/tx/boot` with the field present |
| `RequestsCompletenessNotEmpty` | response from `/tx/oracle/end` whose witness has at least one leaf |
| `ConfirmRefMismatch` | confirm response with `ref.tx_ix != 0` |

Each fixture is a small JSON file under `test/fixtures/forgery/` together with a Haskell unit test asserting the verifier returns the expected constructor.
