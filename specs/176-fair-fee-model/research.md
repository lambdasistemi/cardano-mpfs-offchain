# Research: Fair Fee Model

## Upstream APIs

### cardano-node-clients (a37cbd6)

```haskell
balanceFeeLoop
    :: PParams ConwayEra
    -> (Coin -> Either String (StrictSeq (TxOut ConwayEra)))
    -> Int               -- max iterations
    -> Tx ConwayEra
    -> Either FeeLoopError (Tx ConwayEra)

evaluateAndBalance
    :: Language
    -> Provider IO
    -> PParams ConwayEra
    -> [(TxIn, TxOut ConwayEra)]
    -> Addr
    -> Tx ConwayEra
    -> IO (Tx ConwayEra)

computeScriptIntegrity
    :: Language -> PParams ConwayEra -> Redeemers ConwayEra
    -> StrictMaybe ScriptIntegrityHash

spendingIndex :: TxIn -> Set TxIn -> Word32
placeholderExUnits :: ExUnits  -- ExUnits 0 0
```

Key: `balanceFeeLoop` takes a function `Coin -> outputs` that
rebuilds outputs given the fee. It iterates until fee converges.
This is what we need for the conservation equation where refunds
depend on tx.fee.

### cardano-mpfs-onchain (001-fair-fee-model)

Aiken types:
```
State { owner, root, tip, process_time, retract_time }
Request { requestToken, requestOwner, requestKey, requestValue, tip, submitted_at }
```

Conservation equation (both update and reject):
```
totalRefunded == totalInputLovelace - tx_fee - n * tip
```

Where tx_fee is the Plutus V3 `tx.fee` from the transaction context.

### Haskell library (cardano-mpfs-onchain, 9a5ddbe)

Canonical types with `stateTip` and `requestTip` fields.
Can import directly instead of maintaining our own copies.

## Current offchain flow (update tx)

1. Query UTxOs, find state + requests
2. Compute proofs speculatively
3. Build refunds: `refund = reqValue - defaultMaxFee`
4. Build tx body with outputs
5. `evaluateAndBalance` → evaluate scripts → patch ExUnits → balance

## New flow (update tx with fair fee)

1. Query UTxOs, find state + requests (same)
2. Compute proofs speculatively (same)
3. Build tx skeleton with placeholder outputs
4. Evaluate scripts → patch ExUnits
5. `balanceFeeLoop`: given fee → compute refunds → rebuild outputs → recompute fee → iterate
6. Final tx with converged fee and correct refunds

The key difference: refunds are now a function of fee, so we
can't compute them before knowing the fee. `balanceFeeLoop`
solves this circular dependency.
