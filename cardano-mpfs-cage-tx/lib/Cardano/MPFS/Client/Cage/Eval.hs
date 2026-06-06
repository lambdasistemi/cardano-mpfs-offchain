{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Eval
-- Description : Pure ledger ex-unit evaluation for cage builders.
module Cardano.MPFS.Client.Cage.Eval
    ( DecodedEvalContext (..)
    , decodeEvalContext
    , evaluateAndBalancePure
    , evaluateAndBalancePureAtFee
    , placeholderRedeemers
    , patchEvaluatedRedeemers
    ) where

import Codec.Serialise
    ( Serialise
    , deserialiseOrFail
    )
import Data.ByteString.Lazy qualified as BSL
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address
    ( Addr
    )
import Cardano.Ledger.Alonzo.Plutus.Evaluate
    ( TransactionScriptFailure
    )
import Cardano.Ledger.Alonzo.Scripts
    ( AsIx
    , PlutusPurpose
    )
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( ppMaxTxExUnitsL
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , evalTxExUnits
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , inputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , datsTxWitsL
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( StrictMaybe (..)
    )
import Cardano.Ledger.Binary
    ( decodeFull
    , natVersion
    )
import Cardano.Ledger.Coin
    ( Coin (..)
    )
import Cardano.Ledger.Core
    ( PParams
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    )
import Cardano.Ledger.State
    ( UTxO (..)
    )
import Cardano.Ledger.TxIn
    ( TxIn
    )
import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common qualified as Wire
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.Slotting.EpochInfo
    ( EpochInfo
    , fixedEpochInfo
    )
import Cardano.Slotting.Slot
    ( EpochSize (..)
    )
import Cardano.Slotting.Time
    ( SystemStart
    , slotLengthFromMillisec
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    , evalBudgetExUnits
    , languagesUsedInTx
    )
import Cardano.Tx.Ledger
    ( ConwayTx
    )

data DecodedEvalContext = DecodedEvalContext
    { evalProtocolParameters :: !(PParams ConwayEra)
    , evalSystemStart :: !SystemStart
    , evalEpochInfo :: !(EpochInfo (Either Text))
    }

decodeEvalContext
    :: Wire.EvalContext
    -> Either BuildError DecodedEvalContext
decodeEvalContext Wire.EvalContext{..} = do
    pp <- decodePParams ecProtocolParameters
    systemStart <-
        decodeSerialise
            "eval_context.system_start_cbor"
            ecSystemStartCbor
    pure
        DecodedEvalContext
            { evalProtocolParameters = pp
            , evalSystemStart = systemStart
            , evalEpochInfo =
                fixedEpochInfo
                    (EpochSize ecEpochSize)
                    ( slotLengthFromMillisec
                        (fromIntegral ecSlotLengthMs)
                    )
            }

decodePParams
    :: Wire.UnverifiedPParams
    -> Either BuildError (PParams ConwayEra)
decodePParams Wire.UnverifiedPParams{Wire.uppCbor = Hex ppBytes} =
    case decodeFull (natVersion @11) (BSL.fromStrict ppBytes) of
        Left err ->
            Left
                $ MalformedPParams
                $ T.pack
                $ show err
        Right pp -> Right pp

decodeSerialise
    :: Serialise a
    => Text
    -> Hex
    -> Either BuildError a
decodeSerialise label (Hex bytes) =
    case deserialiseOrFail (BSL.fromStrict bytes) of
        Left err ->
            Left
                $ EvaluationFailed
                $ label <> ": " <> T.pack (show err)
        Right value -> Right value

evaluateAndBalancePure
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> Addr
    -> ConwayTx
    -> Either BuildError ConwayTx
evaluateAndBalancePure ctx inputUtxos refUtxos changeAddr tx =
    go (0 :: Int) (Coin 0) Nothing
  where
    pp = evalProtocolParameters ctx
    existingInputs =
        tx ^. bodyTxL . inputsTxBodyL
    allInputs =
        foldl
            ( \acc (txIn, _) ->
                Set.insert txIn acc
            )
            existingInputs
            inputUtxos
    baseTx =
        tx
            & bodyTxL . inputsTxBodyL .~ allInputs

    go n previousFee previousExUnits
        | n > (10 :: Int) =
            Left
                $ EvaluationFailed
                    "ex-unit/fee convergence failed"
        | otherwise = do
            let txForEval =
                    baseTx
                        & bodyTxL . feeTxBodyL .~ previousFee
            preBalanceReport <-
                evaluateRedeemers ctx inputUtxos refUtxos txForEval
            preBalancePatched <-
                patchEvaluatedRedeemers
                    ctx
                    refUtxos
                    txForEval
                    preBalanceReport
            preBalanced <- balance preBalancePatched
            balancedReport <-
                evaluateRedeemers ctx inputUtxos refUtxos preBalanced
            finalPatched <-
                patchEvaluatedRedeemers
                    ctx
                    refUtxos
                    txForEval
                    balancedReport
            balanced <- balance finalPatched
            let finalFee =
                    balanced ^. bodyTxL . feeTxBodyL
                finalExUnits =
                    redeemerExUnits balanced
            if finalFee == previousFee
                && maybe True (== finalExUnits) previousExUnits
                then Right balanced
                else go (n + 1) finalFee (Just finalExUnits)

    balance candidate =
        case balanceTx pp inputUtxos refUtxos changeAddr candidate of
            Left err ->
                Left
                    $ DSLBuildFailed
                    $ T.pack
                    $ show err
            Right BalanceResult{balancedTx} ->
                Right balancedTx

-- | Evaluate and balance a transaction whose outputs are already
-- parameterized by the supplied fee.
--
-- Update and reject transactions refund request UTxOs using the final tx
-- fee. Their enclosing builders own that convergence because they must
-- rebuild outputs whenever the fee changes. This helper evaluates the
-- candidate at the caller's current fee guess, balances it, and, once the
-- balanced fee already matches the guess, evaluates the balanced shape as
-- well so the returned redeemer budgets are measured on the final tx.
evaluateAndBalancePureAtFee
    :: DecodedEvalContext
    -> Coin
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> Addr
    -> ConwayTx
    -> Either BuildError ConwayTx
evaluateAndBalancePureAtFee ctx expectedFee inputUtxos refUtxos changeAddr tx = do
    let txForEval =
            baseTx
                & bodyTxL . feeTxBodyL .~ expectedFee
    preBalanceReport <-
        evaluateRedeemers ctx inputUtxos refUtxos txForEval
    preBalancePatched <-
        patchEvaluatedRedeemers
            ctx
            refUtxos
            txForEval
            preBalanceReport
    preBalanced <- balance preBalancePatched
    let balancedFee =
            preBalanced ^. bodyTxL . feeTxBodyL
    if balancedFee /= expectedFee
        then Right preBalanced
        else do
            balancedReport <-
                evaluateRedeemers ctx inputUtxos refUtxos preBalanced
            finalPatched <-
                patchEvaluatedRedeemers
                    ctx
                    refUtxos
                    txForEval
                    balancedReport
            balance finalPatched
  where
    pp = evalProtocolParameters ctx
    existingInputs =
        tx ^. bodyTxL . inputsTxBodyL
    allInputs =
        foldl
            ( \acc (txIn, _) ->
                Set.insert txIn acc
            )
            existingInputs
            inputUtxos
    baseTx =
        tx
            & bodyTxL . inputsTxBodyL .~ allInputs

    balance candidate =
        case balanceTx pp inputUtxos refUtxos changeAddr candidate of
            Left err ->
                Left
                    $ DSLBuildFailed
                    $ T.pack
                    $ show err
            Right BalanceResult{balancedTx} ->
                Right balancedTx

evaluateRedeemers
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> Either BuildError (RedeemerReport ConwayEra)
evaluateRedeemers ctx inputUtxos refUtxos tx = do
    let report =
            evalTxExUnits
                (evalProtocolParameters ctx)
                (placeholderRedeemers ctx refUtxos tx)
                (UTxO $ Map.fromList $ inputUtxos <> refUtxos)
                (evalEpochInfo ctx)
                (evalSystemStart ctx)
        failures =
            [ (purpose, err)
            | (purpose, Left err) <- Map.toList report
            ]
    case failures of
        [] -> Right report
        _ ->
            Left
                $ EvaluationFailed
                $ "script evaluation failed: "
                    <> T.pack (show failures)
type RedeemerReport era =
    Map.Map
        (PlutusPurpose AsIx era)
        (Either (TransactionScriptFailure era) ExUnits)

placeholderRedeemers
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> ConwayTx
placeholderRedeemers ctx refUtxos tx =
    tx
        & witsTxL . rdmrsTxWitsL .~ inflated
        & bodyTxL . scriptIntegrityHashTxBodyL .~ integrity
  where
    pp = evalProtocolParameters ctx
    Redeemers rdmrs =
        tx ^. witsTxL . rdmrsTxWitsL
    inflated =
        Redeemers
            $ fmap
                ( \(dat, _) ->
                    (dat, evalBudgetExUnits)
                )
                rdmrs
    integrity =
        if Map.null rdmrs
            then SNothing
            else
                computeScriptIntegrity
                    (languagesUsedInTx tx refUtxos)
                    pp
                    inflated
                    (tx ^. witsTxL . datsTxWitsL)

patchEvaluatedRedeemers
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> RedeemerReport ConwayEra
    -> Either BuildError ConwayTx
patchEvaluatedRedeemers ctx refUtxos tx report = do
    let Redeemers rdmrs =
            tx ^. witsTxL . rdmrsTxWitsL
        missing =
            Map.keysSet rdmrs `Set.difference` Map.keysSet report
    if Set.null missing
        then do
            patchedMap <-
                traversePatchRedeemers rdmrs
            let patched = Redeemers patchedMap
                integrity =
                    if Map.null patchedMap
                        then SNothing
                        else
                            computeScriptIntegrity
                                (languagesUsedInTx tx refUtxos)
                                (evalProtocolParameters ctx)
                                patched
                                (tx ^. witsTxL . datsTxWitsL)
            Right
                $ tx
                & witsTxL . rdmrsTxWitsL .~ patched
                & bodyTxL . scriptIntegrityHashTxBodyL .~ integrity
        else
            Left
                $ EvaluationFailed
                $ "script evaluation missing redeemers: "
                    <> T.pack (show missing)
  where
    pp = evalProtocolParameters ctx
    traversePatchRedeemers =
        Map.traverseWithKey
            ( \purpose (dat, oldExUnits) ->
                case Map.lookup purpose report of
                    Just (Right actual) ->
                        Right (dat, marginExUnits pp actual)
                    Just (Left err) ->
                        Left
                            $ EvaluationFailed
                            $ T.pack (show err)
                    Nothing -> Right (dat, oldExUnits)
            )

marginExUnits :: PParams ConwayEra -> ExUnits -> ExUnits
marginExUnits pp (ExUnits mem steps) =
    capExUnits
        (ExUnits (mem + max 1 (mem `div` 20)) (steps + max 1 (steps `div` 20)))
        (pp ^. ppMaxTxExUnitsL)

capExUnits :: ExUnits -> ExUnits -> ExUnits
capExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    ExUnits (min mem maxMem) (min steps maxSteps)

redeemerExUnits
    :: ConwayTx
    -> Map.Map (PlutusPurpose AsIx ConwayEra) ExUnits
redeemerExUnits tx =
    let Redeemers rdmrs =
            tx ^. witsTxL . rdmrsTxWitsL
    in  fmap snd rdmrs
