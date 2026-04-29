# Contract: API Shapes

Every endpoint that survives or is added by this feature, with its path, HTTP method, request body, response status codes, response body types per status, and verifier obligations.

Removed endpoints are listed at the end with a brief reason.

---

## `GET /status`

- **Method**: GET
- **Request**: no body, no params
- **Response 200**: `StatusResponse` — `{ tip_slot, tip_block_id }`
- **Verifier obligation**: none (server is not authoritative for any of these fields; they are advisory liveness signals)

## `GET /tokens`

- **Method**: GET
- **Request**: no body, no params
- **Response 200**: `TokensListResponse` — `{ snapshot, tokens : UtxoSetWitness }`
- **Verifier obligation** (`verifyTokensListResponse :: TrustedRoot -> Blueprint -> TokensListResponse -> Either VerifyError ()`):
  - all snapshots within the response agree
  - `snapshot.utxo_root == unTrustedRoot trustedRoot` (or, if the verifier accepts a chainpoint-pinned root, the trusted-root tuple matches `snapshot.chainpoint`)
  - the completeness proof verifies against the trusted root and the locally-derived global state script address
  - each entry's `txout_cbor` decodes successfully
  - (the verifier does *not* classify legitimate vs garbage; that is the wrapping application's concern)

## `GET /tokens/:id`

- **Method**: GET
- **Request**: path captures `id : TokenIdJSON`, no body
- **Response 200**: `TokenResponse` — `{ snapshot, state_utxo : UtxoEntry, requests : UtxoSetWitness }`
- **Response 404**: no body — token unknown to the indexer (unverified by design, fall back to `/tokens`)
- **Verifier obligation** (`verifyTokenResponse :: TrustedRoot -> Blueprint -> TokenIdJSON -> TokenResponse -> Either VerifyError ()`):
  - all snapshots within the response agree
  - `snapshot.utxo_root` matches the trusted root
  - `state_utxo.inclusion_proof` verifies against the trusted root and `state_utxo.ref`
  - decoded `state_utxo.txout_cbor` has its address equal to the locally-derived global state script address
  - decoded `state_utxo.txout_cbor`'s value contains exactly one NFT whose policy id equals `Blueprint.bpStatePolicyId` and whose asset name equals `id`
  - decoded `state_utxo.txout_cbor`'s datum decodes to the expected State shape
  - `requests.completeness_proof` verifies against the trusted root and the locally-derived per-cage request script address derived from `(bpStatePolicyId, id)` via `Blueprint.bpRequestScriptAddress`
  - every entry in `requests.entries` has a decodable `txout_cbor`

## `GET /tokens/:id/facts/:key`

- **Method**: GET
- **Request**: path captures `id : TokenIdJSON, key : Hex`, no body
- **Response 200**: `FactPresentResponse` — `{ snapshot, state_utxo, value, mpf_inclusion_proof }`
- **Response 404 with body**: `FactAbsentResponse` — `{ snapshot, state_utxo, mpf_exclusion_proof }`
- **Response 404 no body**: token unknown to the indexer (unverified)
- **Servant pattern**: `UVerb 'GET '[JSON] '[ WithStatus 200 FactPresentResponse, WithStatus 404 FactAbsentResponse, WithStatus 404 NoContent ]` — see `research.md` §3
- **Verifier obligation** (two sibling verifiers):
  - `verifyFactPresentResponse :: TrustedRoot -> Blueprint -> TokenIdJSON -> Hex -> FactPresentResponse -> Either VerifyError ()` — checks the state UTxO as in `verifyTokenResponse`, then verifies `mpf_inclusion_proof` proves `(key, value)` is in the trie at the root recovered from the state UTxO datum
  - `verifyFactAbsentResponse :: TrustedRoot -> Blueprint -> TokenIdJSON -> Hex -> FactAbsentResponse -> Either VerifyError ()` — checks the state UTxO, then verifies `mpf_exclusion_proof` proves `key` is not in the trie at the recovered trie root

## `GET /tx/:txId?timeout=N`

- **Method**: GET
- **Request**: path captures `txId : Hex`, optional query `timeout : Word64` (default 30s)
- **Response 200**: `ConfirmResponse` — `{ snapshot, ref : { txId, 0 }, txout_cbor, inclusion_proof }`
- **Response 408**: no body — timeout
- **Verifier obligation** (`verifyConfirmResponse :: TrustedRoot -> Hex -> ConfirmResponse -> Either VerifyError ()`):
  - `inclusion_proof` verifies against the trusted root and `ref`
  - `ref` is exactly `(txId, 0)`
  - decoded `txout_cbor` is consistent with the on-chain output the client expected from their submitted tx (out of scope for the verifier; checked by the wrapping application)

## `POST /tx/boot`

- **Method**: POST (top-level — no signer-role prefix)
- **Request**: `BootRequest` (existing shape)
- **Response 200**: `UnsignedTxResponse` (no `requests_completeness_proof`)
- **Verifier obligation**: see "Uniform write verifier" below

## `POST /tx/requester/{insert,delete,update,retract}`

- **Method**: POST
- **Request**: existing per-action shapes — `InsertRequest`, `DeleteRequest`, `UpdateValueRequest`, `RetractRequest`
- **Response 200**: `UnsignedTxResponse` (no `requests_completeness_proof`)
- **Verifier obligation**: see "Uniform write verifier" below

## `POST /tx/oracle/reject`

- **Method**: POST
- **Request**: `RejectRequest` (existing shape)
- **Response 200**: `UnsignedTxResponse` (no `requests_completeness_proof`)
- **Verifier obligation**: see "Uniform write verifier" below

## `POST /tx/oracle/update`

- **Method**: POST
- **Request**: `UpdateRequest` (existing shape)
- **Response 200**: `UnsignedTxResponse` **with** `requests_completeness_proof` populated
- **Verifier obligation**: see "Uniform write verifier" below, additionally:
  - `requests_completeness_proof` verifies against the trusted root and the locally-derived per-cage request script address
  - every consumed input in the unsigned tx whose decoded address equals the per-cage request script address appears in the attested set

## `POST /tx/oracle/sweep`

- **Method**: POST (per-cage owner-signed sweep)
- **Request**: `SweepRequest` (existing shape, includes `token_id`)
- **Response 200**: `UnsignedTxResponse` (no `requests_completeness_proof`)
- **Verifier obligation**: uniform write verifier; additionally the wrapping application checks the targeted UTxO is non-legitimate (datum doesn't decode as Request) before signing

## `POST /tx/sweep`

- **Method**: POST (top-level — public global sweep)
- **Request**: a new shape `GlobalSweepRequest` with `{ utxo_ref, oracle_address /* refund destination */ }`. Distinct from `SweepRequest` because the request body does not carry a `token_id`.
- **Response 200**: `UnsignedTxResponse` (no `requests_completeness_proof`)
- **Verifier obligation**: uniform write verifier; additionally the wrapping application checks the targeted UTxO has its decoded address equal to the global state script address but is non-legitimate (no NFT under the trusted state policy or malformed datum) before signing

## `POST /tx/oracle/end`

- **Method**: POST
- **Request**: `EndRequest` (existing shape)
- **Response 200**: `UnsignedTxResponse` **with** `requests_completeness_proof` populated, attesting an empty leaf set
- **Verifier obligation**: uniform write verifier; additionally:
  - `requests_completeness_proof` verifies as an empty-set proof at the locally-derived per-cage request script address against the trusted root

## `POST /tx/submit`

- **Method**: POST (top-level)
- **Request**: `SubmitRequest` (existing shape)
- **Response 200**: bare hex (existing shape — txId)
- **Verifier obligation**: none (this is a relay; the chain becomes the source of truth)

## Uniform write verifier

`verifyUnsignedTxResponse :: TrustedRoot -> Blueprint -> UnsignedTxResponse -> Either VerifyError ()` performs:

- all snapshots agree
- `snapshot.utxo_root` matches the trusted root
- `unsigned_tx_cbor` decodes to a valid `Tx Conway`
- the set of `inputs` covers every input (consumed + reference) the decoded tx mentions; no extra entries
- each entry's `inclusion_proof` verifies against the trusted root and the entry's `ref`
- each entry's decoded `txout_cbor` matches the expected on-chain TxOut for that ref (idempotent, simple shape comparison)

The endpoint-specific extras (per-cage requests completeness for `update`/`end`) are layered on top via `verifyUnsignedTxResponseWithCompleteness` or equivalent.

---

## Removed endpoints

| Endpoint | Reason for removal |
|---|---|
| `GET /tokens/:id/root` | scalar derivable from the state UTxO datum returned by `/tokens/:id` |
| `GET /tokens/:id/proofs/:key` | strict subset of `/facts/:key`; never queried standalone in the new model |
| `GET /tokens/:id/requests` | folded into `/tokens/:id` so the state UTxO and the request set share one snapshot |
| `GET /utxo/:txId/:txIx` | proofs ride with data; raw resolution is not part of the trust-minimised surface |
| `GET /utxo/:txId/:txIx/proof` | proofs ride with data; never queried standalone |
| `GET /utxo/root` | duplicates the (now-removed) authoritative root output |

No alias or compatibility shim is provided. Downstream consumers must migrate to the new shapes in lockstep.
