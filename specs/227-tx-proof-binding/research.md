# Research: Bind proof bundles to unsigned transactions

## Decision: targeted client-side CBOR reader

Use a small pure CBOR reader in `cardano-mpfs-client` to extract the
transaction body input set and reference-input set from the unsigned tx
CBOR. Do not use a server-authored `tx_summary` as the authority for this
slice.

## Rationale

Issue #227 proposed two options:

- A: decode tx CBOR in the client
- B: have the server emit a pre-decoded summary

B is faster, but it cannot prove the summary describes the unsigned tx
unless the client can independently check the tx body. A malicious server
could include a summary that matches the proof and a tx that consumes
something else. Cryptographic CSMT replay catches false witnesses, but it
does not catch omitted tx inputs unless the verifier reads the tx inputs.

Therefore this slice chooses A for the input/reference-input layer. The
decoder is deliberately narrow and only reads fields needed for the first
binding invariant.

## Local CDDL facts

From `cardano-ledger/eras/conway/impl/cddl-files/conway.cddl`:

```text
transaction = [transaction_body, transaction_witness_set, bool, auxiliary_data / nil]
transaction_body = {0 : set<transaction_input>, ..., ? 18 : nonempty_set<transaction_input>, ...}
transaction_input = [transaction_id : $hash32, index : uint .size 2]
set<a0> = #6.258([* a0]) / [* a0]
nonempty_set<a0> = #6.258([+ a0]) / [+ a0]
```

The client decoder must therefore accept both plain-list and tag-258 set
encodings for fields `0` and `18`.

## Binding matrix for this slice

| Endpoint | Tx inputs must equal | Tx reference inputs must equal |
|----------|----------------------|--------------------------------|
| boot | `funding[*]` | empty |
| request insert/delete/update | `funding[*]` | empty |
| retract | `request_in + funding[*]` | `state_ref` |
| reject | `state + request_ins[*] + funding[*]` | empty |
| end | `state + funding[*]` | empty |
| update | `state + requests[*] + funding[*]` | empty |

Collateral inputs are left for a later slice. They are not regular tx
inputs or reference inputs in the issue's first acceptance surface.

## Second slice: mint and state-output binding

Extend the same targeted CBOR reader to decode:

- tx-body field `9` (`mint = multiasset<nonZeroInt64>`)
- tx-body field `1` transaction outputs
- Shelley/Babbage output `value` assets
- Babbage inline-datum markers
- witnessed state `tx_out` values from proof payloads

The verifier can then enforce:

| Endpoint | Mint/burn rule | State-output rule |
|----------|----------------|-------------------|
| boot | exactly one `+1` asset | exactly one inline-datum output carries the minted asset |
| request | empty mint | no state-output assertion |
| retract | empty mint | no state-output assertion |
| reject | empty mint | exactly one inline-datum output carries the witnessed state token |
| end | exactly one burn matching the witnessed state token | no output carries the state token |
| update | empty mint | exactly one inline-datum output carries the witnessed state token |

This still deliberately avoids `cardano-ledger-*` in the client. It does
not attempt to derive the boot asset name or decode full Aiken datum
contents; those are covered by the on-chain script and will be tightened
further when redeemer binding lands.

## Rejected alternative: server summary as authoritative

Rejected for this slice. It leaves the client dependent on a
server-authored interpretation of the transaction and cannot detect a tx
whose body omits or adds inputs relative to the summary.

## Third slice: redeemer and MPF proof binding

Extend the targeted reader to decode transaction witness-set field `5`
for the redeemer shapes produced by this repository. The verifier should
fail closed for unsupported redeemer encodings instead of accepting an
unknown script payload.

The binding target is:

| Endpoint | Redeemer rule |
|----------|---------------|
| boot | exactly the expected minting redeemer tag for token creation |
| request | no script redeemer binding required in this issue slice |
| retract | spending redeemer tag must be `Retract` and refer to the same state reference role |
| reject | spending redeemer tags must be `Rejected` for the request inputs being rejected |
| end | spending redeemer tag must end the state and minting redeemer tag must burn the same state token |
| update | spending redeemer tag must be `Update`; embedded trie root and MPF proof facts must match `UpdateProof.trie_root` and `UpdateProof.trie_read` exactly |

This is the last client-side binding layer called out by issue #227.
If implementation confirms that real update responses still publish an
empty `trie_read` while the unsigned tx redeemer embeds MPF steps, the
server-side response proof must be fixed before exact update redeemer
binding can pass real fixtures.
