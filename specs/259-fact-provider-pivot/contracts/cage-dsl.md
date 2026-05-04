# Client-Library Contract: Cage-protocol DSL helpers

The cage-protocol-aware transaction builders that wallets call after
verifying a facts bundle. Hosted in `cardano-node-clients` under
`Cardano.Node.Client.TxBuild.Cage.{Boot,Request,Retract,End,Update,
Reject}`.

## Signatures

```haskell
-- Cage.Boot
bootCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Either BuildError (Tx ConwayEra)

-- Cage.Request (three exports)
requestInsertCageTx
    :: CageConfig
    -> WalletPolicy
    -> RequestPayload  -- the (token, key, value, address) the wallet asked for
    -> VerifiedRequestFacts
    -> Either BuildError (Tx ConwayEra)
requestDeleteCageTx :: ...same shape, OpDelete...
requestUpdateCageTx :: ...same shape, OpUpdate...

-- Cage.Retract
retractCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRetractFacts
    -> Either BuildError (Tx ConwayEra)

-- Cage.End
endCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedEndFacts
    -> Either BuildError (Tx ConwayEra)

-- Cage.Update
updateCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedUpdateFacts
    -> Either BuildError (Tx ConwayEra)

-- Cage.Reject
rejectCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRejectFacts
    -> Either BuildError (Tx ConwayEra)
```

## Semantics

Each helper MUST:

1. **Decode bytes from facts**: the verified facts bundle carries
   `(TxIn, ByteString txOutCbor, ByteString proof)` triples; the
   helper decodes the `txOutCbor` to `TxOut ConwayEra` for the
   underlying TxBuild DSL, and uses the proof bytes verbatim where
   the cage-protocol's on-chain contract expects them.

2. **Decode protocol parameters**: the `UnverifiedPParams.uppCbor`
   is decoded to `PParams ConwayEra` via
   `Cardano.Ledger.Binary.decodeFull`. The helper enforces
   `WalletPolicy` bounds against this `PParams` *before* running
   the DSL `build` loop:
   - estimated fee ≤ `wpMaxFee`
   - exUnits price ≤ `wpMaxExUnitsPrice` field-wise
   - `pp.minFeeRefScriptCostPerByte` ≤ `wpMaxMinUtxoCoinPerByte`
   - validity-interval window ≤ `wpMaxValidityWindow`

   Any violation returns
   `Left (PolicyViolation PolicyViolationDetail)` BEFORE the DSL
   runs.

3. **Run the cage-protocol DSL program**: each helper composes a
   `TxBuild` program describing what the cage requires (boot:
   `spend` + `attachScript` + `mint` + `payTo'` + `collateral`;
   request: `spend` + `payTo'` + `collateral`; retract / end /
   update / reject: their respective shapes). The program is
   discharged via `build pp interpret evalAdapter inputs addr
   program`. The evaluator uses pure Plutus evaluation
   (Conway-evaluation function from `cardano-ledger-conway`) — no
   IO, no Provider. Pure CPU work.

4. **Return**: the balanced unsigned `Tx ConwayEra` on success;
   `BuildError` on failure.

The byte-equality property: for the same `(cfg, walletPolicy,
verifiedFacts)`, the helper MUST produce the same `Tx ConwayEra`
CBOR as the legacy server-side `Cardano.MPFS.TxBuilder.Real.*Core`
produces for the equivalent `(cfg, snap, inputs, op-params)`. This
is asserted by a property test in the DSL host's test suite (and
is the FR-008 / SC-001 acceptance criterion for Principle V).

## WalletPolicy enforcement points

Pre-build:

- Fee bound: `pp.minFeeA × <estimated tx size>` ≤ `wpMaxFee`. The
  estimate uses `cardano-ledger-api`'s `estimateMinFeeTx` after
  drafting the unbalanced tx.
- ExUnits prices: pp's `prices` field-wise ≤ `wpMaxExUnitsPrice`.
- Min UTxO: pp's `minFeeRefScriptCostPerByte` ≤
  `wpMaxMinUtxoCoinPerByte`.
- Validity window: every helper's validity interval ≤
  `wpMaxValidityWindow`.

Post-build (sanity check):

- The actual fee in the balanced tx ≤ `wpMaxFee`. If the DSL's
  bisection drove the fee above the bound during balancing, the
  helper returns `Left (PolicyViolation FeeBoundExceeded)`.

## Pure operation

Every helper is pure (`Either BuildError ...`, no `IO`). Plutus
script evaluation uses the cardano-ledger Conway-era pure
evaluator. The DSL's `interpret` field is `noCtxInterpretIO`
unwrapped to a pure `noCtxInterpret` (cage helpers don't need any
domain-query context).

This is what enables eventual cross-target compilation per
cardano-node-clients#123 — pure functions cross-compile cleanly
once the dep closure (already cross-compiled by
`cardano-ledger-inspector`) is set up.

## Forbidden patterns

These patterns MUST NOT appear in `Cardano.Node.Client.TxBuild.Cage.*`:

- Any `IO` in a function signature.
- Any import of `Cardano.MPFS.Indexer.*` or other server-side
  modules.
- Any HTTP client call.
- Any reliance on the `Provider` record (the cage helpers don't
  need it; they have facts in hand).

## Byte-equality property

```haskell
prop_bootCageTx_matchesLegacy
    :: Property
prop_bootCageTx_matchesLegacy = property $ do
    cfg     <- forAll genCageConfig
    addr    <- forAll genAddr
    inputs  <- forAll genResolvedWalletInputs
    snap    <- forAll genBundleSnapshot
    pp      <- forAll genPParams

    let policy = mainnetDefaultWalletPolicy
        verified = unsafeAssumeVerifiedBootFacts (BootFacts snap inputs (UnverifiedPParams (serialize pp) False))
        -- hypothesis: Cage.Boot.bootCageTx == Real.Boot.bootTokenCore
        new = bootCageTx cfg policy verified
        old = Real.Boot.bootTokenCore cfg snap inputs addr

    case (new, old) of
      (Right newTx, Right oldEnv) ->
        serialize newTx === serialize (envTx oldEnv)
      _ -> failure
```

(Using `cardano-mpfs-offchain` from a vendored snapshot until the
old `Real.Boot` tree is removed — at which point the property is
captured as fixed bytes in a golden file.)
