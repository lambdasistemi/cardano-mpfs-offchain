{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
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

import Codec.CBOR.Decoding
    ( Decoder
    , TokenType (..)
    , decodeBreakOr
    , decodeListLen
    , decodeListLenOrIndef
    , decodeNull
    , decodeWord8
    , peekTokenType
    )
import Codec.CBOR.Read
    ( deserialiseFromBytes
    )
import Codec.Serialise
    ( Serialise
    , deserialiseOrFail
    )
import Codec.Serialise qualified as Serialise
import Control.Monad
    ( replicateM
    , unless
    , void
    , when
    )
import Data.ByteString.Lazy qualified as BSL
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word
    ( Word64
    )
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
    , txIdTx
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
    )
import Cardano.Slotting.EpochInfo qualified as EpochInfo
import Cardano.Slotting.Slot
    ( EpochNo (..)
    , EpochSize (..)
    , SlotNo (..)
    )
import Cardano.Slotting.Time
    ( RelativeTime (..)
    , SlotLength
    , SystemStart
    , addRelativeTime
    , getSlotLength
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

data EraSummaryLite = EraSummaryLite
    { esStart :: !EraBoundLite
    , esEnd :: !(Maybe EraBoundLite)
    , esEpochSize :: !EpochSize
    , esSlotLength :: !SlotLength
    }

data EraBoundLite = EraBoundLite
    { ebRelativeTime :: !RelativeTime
    , ebSlot :: !SlotNo
    , ebEpoch :: !EpochNo
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
    epochInfo <- decodeEraHistoryEpochInfo ecEraHistoryCbor
    pure
        DecodedEvalContext
            { evalProtocolParameters = pp
            , evalSystemStart = systemStart
            , evalEpochInfo = epochInfo
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

decodeEraHistoryEpochInfo
    :: Hex
    -> Either BuildError (EpochInfo (Either Text))
decodeEraHistoryEpochInfo (Hex bytes) = do
    summaries <-
        case deserialiseFromBytes
            decodeEraSummaries
            (BSL.fromStrict bytes) of
            Left err ->
                Left
                    $ EvaluationFailed
                    $ "eval_context.era_history_cbor: "
                        <> T.pack (show err)
            Right (trailing, value)
                | BSL.null trailing -> Right value
                | otherwise ->
                    Left
                        $ EvaluationFailed
                            "eval_context.era_history_cbor: trailing bytes"
    case summaries of
        [] ->
            Left
                $ EvaluationFailed
                    "eval_context.era_history_cbor: empty era history"
        _ -> Right $ eraSummariesEpochInfo summaries

decodeEraSummaries :: Decoder s [EraSummaryLite]
decodeEraSummaries =
    decodeListLenOrIndef >>= \case
        Just n -> replicateM n decodeEraSummaryLite
        Nothing -> go []
  where
    go acc = do
        done <- decodeBreakOr
        if done
            then pure $ reverse acc
            else do
                summary <- decodeEraSummaryLite
                go (summary : acc)

decodeEraSummaryLite :: Decoder s EraSummaryLite
decodeEraSummaryLite = do
    enforceListLen "EraSummary" 3
    esStart <- decodeEraBoundLite
    esEnd <- decodeEraEndLite
    (esEpochSize, esSlotLength) <- decodeEraParamsLite
    pure EraSummaryLite{..}

decodeEraBoundLite :: Decoder s EraBoundLite
decodeEraBoundLite = do
    len <- decodeListLen
    unless (len == 3 || len == 4)
        $ fail "Bound: expected list length 3 or 4"
    ebRelativeTime <- Serialise.decode
    ebSlot <- Serialise.decode
    ebEpoch <- Serialise.decode
    when (len == 4)
        $ void (Serialise.decode @Word64)
    pure EraBoundLite{..}

decodeEraEndLite :: Decoder s (Maybe EraBoundLite)
decodeEraEndLite =
    peekTokenType >>= \case
        TypeNull -> do
            decodeNull
            pure Nothing
        _ -> Just <$> decodeEraBoundLite

decodeEraParamsLite :: Decoder s (EpochSize, SlotLength)
decodeEraParamsLite = do
    len <- decodeListLen
    unless (len == 4 || len == 5)
        $ fail "EraParams: expected list length 4 or 5"
    epochSize <- EpochSize <$> Serialise.decode @Word64
    slotLength <- Serialise.decode
    decodeSafeZoneLite
    void (Serialise.decode @Word64)
    when (len == 5)
        $ void (Serialise.decode @Word64)
    pure (epochSize, slotLength)

decodeSafeZoneLite :: Decoder s ()
decodeSafeZoneLite = do
    size <- decodeListLen
    tag <- decodeWord8
    case (size, tag) of
        (3, 0) -> do
            void (Serialise.decode @Word64)
            decodeSafeBeforeEpochLite
        (1, 1) ->
            pure ()
        _ ->
            fail
                $ "SafeZone: invalid size and tag "
                    <> show (size, tag)

decodeSafeBeforeEpochLite :: Decoder s ()
decodeSafeBeforeEpochLite = do
    size <- decodeListLen
    tag <- decodeWord8
    case (size, tag) of
        (1, 0) ->
            pure ()
        (2, 1) ->
            void (Serialise.decode @EpochNo)
        _ ->
            fail
                $ "SafeBeforeEpoch: invalid size and tag "
                    <> show (size, tag)

enforceListLen :: String -> Int -> Decoder s ()
enforceListLen label expected = do
    actual <- decodeListLen
    unless (actual == expected)
        $ fail
        $ label
            <> ": expected list length "
            <> show expected
            <> ", got "
            <> show actual

eraSummariesEpochInfo
    :: [EraSummaryLite]
    -> EpochInfo (Either Text)
eraSummariesEpochInfo summaries =
    EpochInfo.EpochInfo
        { EpochInfo.epochInfoSize_ =
            fmap esEpochSize . findEraByEpoch
        , EpochInfo.epochInfoFirst_ = epochToSlot
        , EpochInfo.epochInfoEpoch_ = slotToEpoch
        , EpochInfo.epochInfoSlotToRelativeTime_ =
            slotToRelativeTime
        , EpochInfo.epochInfoSlotLength_ =
            fmap esSlotLength . findEraBySlot
        }
  where
    findEraBySlot slot =
        maybe
            ( Left
                $ "slot "
                    <> T.pack (show slot)
                    <> " outside eval-context era history"
            )
            Right
            $ findFirst (eraContainsSlot slot) summaries

    findEraByEpoch epoch =
        maybe
            ( Left
                $ "epoch "
                    <> T.pack (show epoch)
                    <> " outside eval-context era history"
            )
            Right
            $ findFirst (eraContainsEpoch epoch) summaries

    slotToRelativeTime slot = do
        EraSummaryLite{esStart, esSlotLength} <- findEraBySlot slot
        slotDelta <- countSlotDelta slot (ebSlot esStart)
        let timeDelta =
                fromIntegral slotDelta * getSlotLength esSlotLength
        Right
            $ addRelativeTime
                timeDelta
                (ebRelativeTime esStart)

    slotToEpoch slot = do
        EraSummaryLite{esStart, esEpochSize = EpochSize epochSize} <-
            findEraBySlot slot
        slotDelta <- countSlotDelta slot (ebSlot esStart)
        Right
            $ addEpochs
                (slotDelta `div` epochSize)
                (ebEpoch esStart)

    epochToSlot epoch = do
        EraSummaryLite{esStart, esEpochSize = EpochSize epochSize} <-
            findEraByEpoch epoch
        epochDelta <- countEpochDelta epoch (ebEpoch esStart)
        Right
            $ addSlots
                (epochDelta * epochSize)
                (ebSlot esStart)

eraContainsSlot :: SlotNo -> EraSummaryLite -> Bool
eraContainsSlot slot EraSummaryLite{esStart, esEnd} =
    ebSlot esStart <= slot
        && maybe True ((slot <) . ebSlot) esEnd

eraContainsEpoch :: EpochNo -> EraSummaryLite -> Bool
eraContainsEpoch epoch EraSummaryLite{esStart, esEnd} =
    ebEpoch esStart <= epoch
        && maybe True ((epoch <) . ebEpoch) esEnd

countSlotDelta
    :: SlotNo
    -> SlotNo
    -> Either Text Word64
countSlotDelta (SlotNo hi) (SlotNo lo)
    | hi >= lo = Right $ hi - lo
    | otherwise =
        Left
            $ "slot "
                <> T.pack (show hi)
                <> " precedes era start slot "
                <> T.pack (show lo)

countEpochDelta
    :: EpochNo
    -> EpochNo
    -> Either Text Word64
countEpochDelta (EpochNo hi) (EpochNo lo)
    | hi >= lo = Right $ hi - lo
    | otherwise =
        Left
            $ "epoch "
                <> T.pack (show hi)
                <> " precedes era start epoch "
                <> T.pack (show lo)

addSlots :: Word64 -> SlotNo -> SlotNo
addSlots delta (SlotNo slot) =
    SlotNo $ slot + delta

addEpochs :: Word64 -> EpochNo -> EpochNo
addEpochs delta (EpochNo epoch) =
    EpochNo $ epoch + delta

findFirst :: (a -> Bool) -> [a] -> Maybe a
findFirst _ [] = Nothing
findFirst p (x : xs)
    | p x = Just x
    | otherwise = findFirst p xs

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
            finalPatched <-
                stabilizeSubmittedRedeemers
                    ctx
                    inputUtxos
                    refUtxos
                    preBalanced
            let finalFee =
                    finalPatched ^. bodyTxL . feeTxBodyL
                finalExUnits =
                    redeemerExUnits finalPatched
            if finalFee == previousFee
                && maybe True (== finalExUnits) previousExUnits
                then Right finalPatched
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
            finalPatched <-
                stabilizeSubmittedRedeemers
                    ctx
                    inputUtxos
                    refUtxos
                    preBalanced
            Right finalPatched
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
    evaluateRedeemersWith
        ctx
        inputUtxos
        refUtxos
        (placeholderRedeemers ctx refUtxos tx)

evaluateSubmittedRedeemers
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> Either BuildError (RedeemerReport ConwayEra)
evaluateSubmittedRedeemers = evaluateRedeemersWith

evaluateRedeemersWith
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> Either BuildError (RedeemerReport ConwayEra)
evaluateRedeemersWith ctx inputUtxos refUtxos tx = do
    let report =
            evalTxExUnits
                (evalProtocolParameters ctx)
                tx
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

stabilizeSubmittedRedeemers
    :: DecodedEvalContext
    -> [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> ConwayTx
    -> Either BuildError ConwayTx
stabilizeSubmittedRedeemers ctx inputUtxos refUtxos =
    go (0 :: Int)
  where
    go n tx
        | n > 10 =
            Left
                $ EvaluationFailed
                    "submitted ex-unit convergence failed"
        | otherwise = do
            report <-
                evaluateSubmittedRedeemers
                    ctx
                    inputUtxos
                    refUtxos
                    tx
            patched <-
                patchEvaluatedRedeemers
                    ctx
                    refUtxos
                    tx
                    report
            if txIdTx patched == txIdTx tx
                then Right patched
                else go (n + 1) patched

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
