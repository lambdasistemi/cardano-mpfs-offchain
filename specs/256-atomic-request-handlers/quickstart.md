# Quickstart: Exercise the atomic POST /tx/request/{insert,delete,update}

This walkthrough drives the three request endpoints end-to-end and
verifies the atomic-read property. Boots from a fresh devnet plus a
booted token.

## 1. Enter dev shell + run the boot scenario first

```bash
cd /code/cardano-mpfs-offchain-atomic-requests
nix develop
```

Run the boot quickstart from the previous slice
(`specs/249-atomic-boot-handler/quickstart.md`) up to "boot a
token", or use the e2e harness directly. The result: a
`tokenId` whose state UTxO is indexed.

## 2. Drive POST /tx/request/insert

```bash
TOKEN_ID=…                # from boot step
ADDR=…                    # requester address (hex)
KEY=$(printf 'hello' | xxd -p)
VALUE=$(printf 'world' | xxd -p)

curl -sS http://localhost:3000/tx/request/insert \
    -H 'Content-Type: application/json' \
    -d "{\"token\":\"$TOKEN_ID\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"address\":\"$ADDR\"}" \
    | jq
```

Expected: an `UnsignedTxResponse` with the same shape as boot — a
`transaction` field plus a `snapshot` and a `proof.request_funding`
list of `WitnessedInput` triples (ref, txout, csmt_proof).

Sign + submit (same script the boot quickstart uses) and observe
the chain follower indexing the resulting pending-request UTxO at
the per-token request address.

## 3. Drive POST /tx/request/delete

```bash
curl -sS http://localhost:3000/tx/request/delete \
    -H 'Content-Type: application/json' \
    -d "{\"token\":\"$TOKEN_ID\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"address\":\"$ADDR\"}" \
    | jq
```

Same response shape; the on-chain validators discriminate
insert/delete/update via the request datum.

## 4. Drive POST /tx/request/update

```bash
NEW_VALUE=$(printf 'monde' | xxd -p)
curl -sS http://localhost:3000/tx/request/update \
    -H 'Content-Type: application/json' \
    -d "{\"token\":\"$TOKEN_ID\",\"key\":\"$KEY\",\"old_value\":\"$VALUE\",\"new_value\":\"$NEW_VALUE\",\"address\":\"$ADDR\"}" \
    | jq
```

## 5. Verify each response offline

```bash
just verify-request-response /tmp/insert-response.json --trusted-root <utxo_root>
just verify-request-response /tmp/delete-response.json --trusted-root <utxo_root>
just verify-request-response /tmp/update-response.json --trusted-root <utxo_root>
```

All three MUST exit 0 (`accepted`). Tamper with `--trusted-root`
and the verifier MUST exit non-zero with a snapshot mismatch.

## 6. Atomicity under chain churn (SC-001)

```bash
just chain-churn-load &      # background: 1+ block per second
sleep 60                     # let the chain churn

# Hit each endpoint 50 times back-to-back; every response must
# verify cleanly.
for i in $(seq 1 50); do
    just request-insert-and-verify "$TOKEN_ID" "$ADDR" "key$i" "val$i"
    just request-delete-and-verify "$TOKEN_ID" "$ADDR" "key$i" "val$i"
    just request-update-and-verify "$TOKEN_ID" "$ADDR" "key$i" "old$i" "new$i"
done
```

All 150 invocations MUST exit 0.

## 7. followerEnabled = False fixture path

The harness used by `CageFlowSpec` already calls the three request
builders directly via `Tx.requestX (txBuilder ctx) …`. After this
slice, the call site adds `bootInputs` (or an analogous
`requestInputs`) before the `tokenId`:

```haskell
inputs <- walletBootInputs (provider ctx) genesisAddr
bundle <- Tx.requestInsert (txBuilder ctx) emptySnap inputs tid key value addr
```

Run with:

```bash
just unit-offchain --match "CageFlow"
just e2e --match "Request"
```
