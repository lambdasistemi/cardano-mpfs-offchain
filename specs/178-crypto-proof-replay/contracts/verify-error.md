# Contract: `VerifyError` (extended)

**Module**: `Cardano.MPFS.Client.Verify`

**Compatibility**: additive. Existing downstream pattern matches on
`MalformedHex | WrongHexLength | EmptyBlockId | MalformedTxCbor`
continue to compile once a fall-through wildcard is added; `Eq` and
`Show` derivations are preserved.

```haskell
data VerifyError
    = MalformedHex Text Text
    | WrongHexLength Text Int Int
    | EmptyBlockId
    | MalformedTxCbor Text
    | CsmtReplayFailed Text Text
    | MpfReplayFailed Text Text
    deriving stock (Eq, Show)
```

## Field-path conventions

`Text` paths are **dotted and deterministic**. The grammar is:

```
<endpoint> "." <role> ( "[" <index> "]" )? "." <leaf-field>
```

- `<endpoint>` ∈ { `boot`, `request`, `retract`, `reject`, `end`,
  `update` }
- `<role>` ∈ { `funding`, `state`, `state_ref`, `request_in`,
  `request_ins`, `requests`, `trie_read` }
- `<leaf-field>` ∈ { `utxo_proof`, `mpf_proof`, `tx_in`, `tx_out`,
  `value`, `key` }
- `<index>` appears only for list-valued roles (`funding`,
  `request_ins`, `requests`, `trie_read`).

Example paths that the new constructors may carry:

- `boot.funding[0].utxo_proof`
- `retract.state_ref.utxo_proof`
- `retract.request_in.utxo_proof`
- `reject.state.utxo_proof`
- `reject.request_ins[2].utxo_proof`
- `end.state.utxo_proof`
- `end.funding[1].utxo_proof`
- `update.state.utxo_proof`
- `update.requests[0].utxo_proof`
- `update.trie_read[0].mpf_proof`

## Reason vocabulary (`Text`)

The second `Text` is one of the following fixed strings; tests match
on it directly via `csmtReplayFailedAt` / `mpfReplayFailedAt`:

- `"root mismatch"` — the cryptographic replay returned `False`
  against the advertised root.
- `"key binding mismatch"` — the in-proof key does not equal the
  hashed representation of the advertised `tx_in` (CSMT) or `key`
  (MPF).
- `"value binding mismatch"` — the in-proof value does not equal the
  advertised `tx_out` (CSMT) or `value` (MPF inclusion).
- `"malformed proof CBOR"` — the proof bytes failed CBOR decode
  after passing structural hex decode. Distinct from
  `MalformedHex` because structural hex decode already succeeded.
- `"inclusion proof for absence claim"` — MPF only; `TrieFact.value`
  is `Nothing` but the proof decodes as an inclusion proof.
- `"exclusion proof for inclusion claim"` — MPF only; inverse of
  the above.

## Determinism

Given the same envelope bytes, `verify*TxResponse` returns the same
`Either VerifyError ()` across GHC-native / GHC-WASM / GHC-JS. The
cross-target CI check enforces this.
