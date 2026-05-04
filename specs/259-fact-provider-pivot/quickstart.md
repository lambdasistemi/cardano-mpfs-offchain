# Quickstart: Exercise the fact-provider pivot end-to-end

Walks through all eight cage-protocol operations using the new
`POST /facts/*` API + native MOOG client + cage-protocol DSL +
verifier.

## 1. Bring up devnet + MPFS server

```bash
cd /code/cardano-mpfs-offchain-fact-provider
nix develop
just devnet-up         # cardano-node subprocess
just serve             # MPFS server against the devnet socket
```

In a second terminal, fund a wallet address from the faucet and
note its address (`$ADDR`).

## 2. Boot a token

```bash
TRUSTED_ROOT=$(curl -sS http://localhost:3000/utxo/root | jq -r '.root')

curl -sS http://localhost:3000/facts/boot \
    -H 'Content-Type: application/json' \
    -d "{\"address\":\"$ADDR\"}" \
    > /tmp/boot-facts.json

# Verify the response client-side via the verifier CLI
just verify-facts boot --trusted-root $TRUSTED_ROOT \
    --input /tmp/boot-facts.json
# Expect: "verified"

# Build the unsigned tx locally via the cage DSL
just build-cage-tx boot --policy /etc/moog/wallet-policy.json \
    --facts /tmp/boot-facts.json \
    > /tmp/boot-unsigned.cbor

# Sign and submit
just sign-and-submit /tmp/boot-unsigned.cbor /etc/moog/keys/sign.skey
```

The chain follower indexes the new state UTxO; note the resulting
`tokenId` from the chain-follower trace.

## 3. Insert a request

```bash
TOKEN_ID=…
KEY=$(printf 'hello' | xxd -p)
VALUE=$(printf 'world' | xxd -p)

curl -sS http://localhost:3000/facts/request/insert \
    -H 'Content-Type: application/json' \
    -d "{\"token\":\"$TOKEN_ID\",\"key\":\"$KEY\",\"value\":\"$VALUE\",\"address\":\"$ADDR\"}" \
    > /tmp/insert-facts.json

just verify-facts request-insert --trusted-root $TRUSTED_ROOT \
    --input /tmp/insert-facts.json

just build-cage-tx request-insert --policy ... --facts /tmp/insert-facts.json \
    --token "$TOKEN_ID" --key "$KEY" --value "$VALUE" \
    > /tmp/insert-unsigned.cbor

just sign-and-submit /tmp/insert-unsigned.cbor /etc/moog/keys/sign.skey
```

## 4. Update the token (tier-3)

```bash
curl -sS http://localhost:3000/facts/update \
    -H 'Content-Type: application/json' \
    -d "{\"token\":\"$TOKEN_ID\",\"address\":\"$ADDR\"}" \
    > /tmp/update-facts.json

# Verifier checks both CSMT proofs (state UTxO + request UTxOs +
# wallet UTxOs) AND MPF facts against the snapshot's roots
just verify-facts update --trusted-root $TRUSTED_ROOT \
    --input /tmp/update-facts.json

# The DSL helper runs the MPF fold internally, computes the new
# stateRoot, builds the unsigned tx
just build-cage-tx update --policy ... --facts /tmp/update-facts.json \
    --token "$TOKEN_ID" \
    > /tmp/update-unsigned.cbor

just sign-and-submit /tmp/update-unsigned.cbor /etc/moog/keys/sign.skey
```

## 5. Retract / End / Delete / Update-request / Reject

Same shape; replace the endpoint and the per-op parameters.

## 6. WalletPolicy enforcement check

```bash
# Test seam: server returns artificially-inflated pp
just serve --override-pp \
    --max-fee-multiplier 100  # multiplies pp.minFeeA × 100

curl -sS http://localhost:3000/facts/boot \
    -H 'Content-Type: application/json' \
    -d "{\"address\":\"$ADDR\"}" \
    > /tmp/boot-facts-inflated.json

just build-cage-tx boot --policy /etc/moog/wallet-policy.json \
    --facts /tmp/boot-facts-inflated.json
# Expect: error PolicyViolation FeeBoundExceeded
# The DSL refuses to build (let alone sign) over-budget txs
```

## 7. Cross-target verifier check (Principle IX)

```bash
just check-verifier-cross-target
# Generates random facts bundles, runs verifier under
# native + GHC-WASM + GHC-JS, asserts all three produce
# byte-identical Either VerifyError outputs
```

## 8. Migration check (Principle IV amendment & SC-002 / SC-003)

```bash
# After the pivot lands:
grep -rn 'transaction/' cardano-mpfs-offchain/lib/ cardano-mpfs-offchain/exe/
# Expect: zero matches

grep -rn 'Cardano.Ledger.Api.Tx\|Tx ConwayEra' \
    cardano-mpfs-offchain/cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify*
# Expect: zero matches

cd /code/moog
grep -rn 'MPFS.API\b' src/
# Expect: zero matches; everything imports MPFS.Facts
```

## 9. End-to-end MOOG flow (SC-001)

```bash
cd /code/moog
just integration-test
# Boots, inserts, updates, retracts, ends a token end-to-end
# against the new server. Every facts response verified;
# every tx built locally; every tx accepted on-chain.
```

If all of (1)–(9) pass, the pivot has shipped.
