# Quickstart: Verifying MPFS Responses Under the Post-Split Proof Model

This walkthrough shows two end-to-end client flows: a read-side verification and a write-side verification including the per-cage requests completeness check. Both flows are pure once the trusted UTxO-CSMT root has been obtained from a separate CSMT service. All Haskell code below is shippable in `cardano-mpfs-client` and compiles to native, GHC-WASM, and GHC-JS targets per constitution principle IX.

## Prerequisites

- A trusted Aiken blueprint, providing:
  - `bpStatePolicyId` — the policy id of the state-token NFT minting policy
  - `bpStateScriptAddress` — the address of the global state validator
  - `bpRequestScriptAddress :: AssetName -> Address` — derives the per-cage request validator address from a token's asset name
- A trusted CSMT service that publishes the UTxO-CSMT root for a given chain point
- An HTTP client for the MPFS offchain service (any HTTP library; the verifier itself is decoupled from transport)

## Read flow — verifying `GET /tokens/:id`

1. **Pin the chain point you want to read at.** Either ask the trusted CSMT service for its current root + chainpoint, or accept the chainpoint reported by the offchain service's `GET /status` and re-ask the trusted CSMT service for the root *at that chainpoint*. Either way the result is a `TrustedRoot` value.

2. **Call the offchain service:**

   ```haskell
   resp :: TokenResponse <- httpGet (mpfsBase <> "/tokens/" <> tokenId)
   ```

3. **Verify offline:**

   ```haskell
   case verifyTokenResponse trustedRoot blueprint tokenId resp of
       Left err -> -- inspect err.fieldPath; refuse to proceed
       Right () -> -- everything checks out
           let stateDatum = decodeStateDatum (txout_cbor (state_utxo resp))
               trieRoot   = sdTrieRoot stateDatum
               pendingReqs = entries (requests resp)
            in continueWithVerifiedState trieRoot pendingReqs
   ```

4. **Use the verified state.** `trieRoot` is now the cryptographically-attested current trie root for this cage; `pendingReqs` is the cryptographically-attested full set of pending request UTxOs at the per-cage request address. Any further check against either (e.g. an MPF inclusion proof for a specific key, classification of pending requests as legitimate vs garbage) is a pure operation on these values.

What the verifier checked, in plain language:
- the snapshot the response is anchored to is the one you trusted;
- the state UTxO's address matches the global state validator from the blueprint;
- the state UTxO's value contains exactly one NFT under the trusted policy id with the requested asset name;
- the state UTxO's CSMT inclusion proof is valid against the trusted root;
- the per-cage requests completeness proof is valid against the trusted root and the per-cage request validator address derived locally.

## Write flow — verifying `POST /tx/oracle/update`

The oracle's signing flow is the load-bearing case for this whole feature: the verifier must protect the oracle from signing a transaction that consumes only a server-curated subset of pending requests.

1. **Pin a trusted root and call the offchain service:**

   ```haskell
   req  :: UpdateRequest = UpdateRequest { tokenId, selectedRequestRefs, ... }
   resp :: UnsignedTxResponse <- httpPost
       (mpfsBase <> "/tx/oracle/update") (toJSON req)
   ```

2. **Verify the response:**

   ```haskell
   case verifyUnsignedTxResponse trustedRoot blueprint resp of
       Left err -> -- refuse to sign
       Right () -> verifyOracleUpdateExtras trustedRoot blueprint tokenId resp
   ```

   The first call covers everything common to write endpoints: snapshot consistency, every input's CSMT inclusion proof, every tx-side input is covered. The second call does the update-specific extras.

3. **Verify update-specific extras:**

   ```haskell
   verifyOracleUpdateExtras
     :: TrustedRoot -> Blueprint -> TokenIdJSON
     -> UnsignedTxResponse -> Either VerifyError ()
   verifyOracleUpdateExtras root bp tid resp = do
       proof <- maybeToErr (RequestsCompletenessMissing "tx.oracle.update.requests_completeness_proof")
                          (requests_completeness_proof resp)
       let perCageAddr = bpRequestScriptAddress bp tid
       verifyCompleteness root perCageAddr proof  -- attests entries to be exactly the per-cage set
       -- now: every consumed input in the tx whose decoded address is perCageAddr
       -- must be in the attested set
       checkConsumedRequestsInAttested resp perCageAddr proof
   ```

4. **Apply the operations and re-derive the new root.**

   The unsigned tx's `Modify` redeemer carries the MPF proofs the on-chain validator will use. The same proofs let the client re-derive the new trie root and compare it to the new state UTxO's datum:

   ```haskell
   let oldRoot   = sdTrieRoot (decodeStateDatum (consumed state_utxo))
       ops       = decodeOpsFromConsumedRequests resp
       proofs    = decodeProofsFromModifyRedeemer (unsigned_tx_cbor resp)
       newRoot'  = applyOperations oldRoot (zip ops proofs)
       newRoot   = sdTrieRoot (decodeStateDatum (produced state_utxo))
   when (newRoot /= newRoot') $
       Left (StateRootDisagreement "tx.oracle.update.produced_state_utxo")
   ```

   (Step 4 is conventionally the wrapping application's responsibility — operating on values the verifier has already attested as cryptographically authentic — but is shown here for completeness.)

5. **Sign and submit.** Only after every check above succeeds.

## End flow — verifying `POST /tx/oracle/end`

Identical to the write flow above, with one extra invariant: the completeness witness must attest an empty leaf set. The verifier rejects the response with `RequestsCompletenessNotEmpty` if any entry survives.

```haskell
verifyOracleEndExtras
    :: TrustedRoot -> Blueprint -> TokenIdJSON
    -> UnsignedTxResponse -> Either VerifyError ()
verifyOracleEndExtras root bp tid resp = do
    proof <- maybeToErr (RequestsCompletenessMissing "tx.oracle.end.requests_completeness_proof")
                       (requests_completeness_proof resp)
    let perCageAddr = bpRequestScriptAddress bp tid
    verifyCompletenessEmpty root perCageAddr proof
```

## Trust boundaries (one-page recap)

| Value | Authority |
|---|---|
| `TrustedRoot` | provided by the wrapping application, fetched from a trusted CSMT service |
| `Blueprint` | provided by the wrapping application, distributed out of band |
| `unsigned_tx_cbor` | proposed by the offchain service; verified against the above two |
| `txout_cbor` for any input or state UTxO | proposed by the offchain service; verified against the trusted root via CSMT inclusion |
| `requests_completeness_proof` | proposed by the offchain service; verified against the trusted root and the per-cage address |
| MPF proofs (inside the unsigned tx redeemer) | proposed by the offchain service; verified against the trie root recovered from the state UTxO datum |

The offchain service is *not* trusted to provide any of these — it provides the bytes, and the verifier checks them. Substitute any byte in any of the above and the verifier returns a named `VerifyError` with the failing field path.
