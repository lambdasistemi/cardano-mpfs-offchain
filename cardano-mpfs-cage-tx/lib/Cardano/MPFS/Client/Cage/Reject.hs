{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Reject
-- Description : Client-side reject cage transaction builder.
--
-- Builds an unsigned Phase 3 reject transaction from already
-- verified 'VerifiedRejectFacts'. Mirrors the legacy
-- server-side @buildRejectProgram@ from
-- "Cardano.MPFS.TxBuilder.Real.Reject" but consumes
-- already-decoded ledger facts (no node round-trips) and
-- pins both validity bounds — the legacy server reject
-- leaves @validTo@ unset, whereas reject facts now ship an
-- explicit upper slot.
--
-- Per Q-S2-001 the state root is unchanged across the
-- reject step; the new state output carries the same datum
-- as the consumed state UTxO.
module Cardano.MPFS.Client.Cage.Reject
    ( RefundPlan (..)
    , preflightLegacyExactRefund
    , rejectCageTx
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.Coerce (coerce)
import Data.List (find, sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word16, Word32, Word64)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromBytes
    , hashToBytes
    )
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.Scripts
    ( AsIx (..)
    , fromPlutusScript
    , mkPlutusScript
    )
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( CoinPerByte (..)
    , ppCoinsPerUTxOByteL
    , ppMaxTxExUnitsL
    , ppPricesL
    )
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , binaryDataToData
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , inputsTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( ConwayPlutusPurpose (..)
    , Redeemers (..)
    , TxDats (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network
    , StrictMaybe (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Core
    ( PParams
    , Script
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes
    ( ScriptHash (..)
    , extractHash
    , unsafeMakeSafeHash
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    )
import Cardano.Ledger.Plutus.Language
    ( Language (..)
    , Plutus (..)
    , PlutusBinary (..)
    )
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import Cardano.Tx.Ledger (ConwayTx)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )

import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common
    ( UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RejectFacts (..)
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    , TokenId
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cageAddrFromCfg
    , mkCageScript
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestScriptBytesFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRejectFacts
    , verifiedRejectFacts
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    )
import Cardano.Tx.Build qualified as TxBuild

data NoCtx a

data RefundPlan = RefundPlan
    { refundRawCoin :: !Coin
    , refundMinCoin :: !Coin
    , refundFinalCoin :: !Coin
    }
    deriving stock (Eq, Show)

-- | Build an unsigned reject transaction from already
-- verified reject facts. The function decodes the supplied
-- ledger facts, enforces wallet caps, applies the
-- server-derived Phase 3 validity interval (both bounds),
-- preserves the state root, and emits per-request refunds
-- back to each requester. The returned transaction is
-- ready for the cage owner to sign.
rejectCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRejectFacts
    -> Either BuildError ConwayTx
rejectCageTx cfg policy verified = do
    let facts = verifiedRejectFacts verified
        token = tokenIdFromJSON (rfToken facts)
    pp <- decodePParams (rfProtocolParameters facts)
    enforcePParamsPolicy policy pp
    stateRow <- decodeUtxo "reject.state_utxo" (rfStateUtxo facts)
    requestRows <-
        traverse
            (uncurry decodeRequestUtxo)
            (zip [0 :: Int ..] (rfRequestUtxos facts))
    case requestRows of
        [] ->
            Left
                $ MalformedTxOut
                    "reject.request_utxos must not be empty"
        _ -> Right ()
    fundingRows <- decodeWalletUtxos (rfWalletUtxos facts)
    feeRow <- selectFeeRow fundingRows
    oldState <- stateDatum stateRow
    ownerSigner <-
        ownerWitnessKeyHash
            $ let BuiltinByteString ownerBytes = stateOwner oldState
              in  ownerBytes
    let changeAddr = rowAddress feeRow
        validity =
            ValidityInterval
                (SJust (slot (rfValidityLowerSlot facts)))
                (SJust (slot (rfValidityUpperSlot facts)))
    tx <-
        buildRejectTx
            cfg
            pp
            token
            stateRow
            requestRows
            feeRow
            changeAddr
            oldState
            ownerSigner
            validity
    enforceTxPolicy policy tx
    pure tx
  where
    slot = SlotNo . fromIntegral

-- ---------------------------------------------------------------
-- Decoding helpers (mirror Cage/Update.hs)
-- ---------------------------------------------------------------

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

rowAddress :: InputRow -> Addr
rowAddress row = rowOut row ^. addrTxOutL

selectFeeRow :: [InputRow] -> Either BuildError InputRow
selectFeeRow [] = Left EmptyFunding
selectFeeRow rows =
    case sortOn (Down . (^. coinTxOutL) . rowOut) rows of
        row : _ -> Right row
        [] -> Left EmptyFunding

decodePParams
    :: UnverifiedPParams -> Either BuildError (PParams ConwayEra)
decodePParams UnverifiedPParams{uppCbor = Hex ppBytes} =
    case decodeFull (natVersion @11) (BSL.fromStrict ppBytes) of
        Left err ->
            Left
                $ MalformedPParams
                $ T.pack
                $ show err
        Right pp -> Right pp

decodeUtxo :: Text -> UtxoEntry -> Either BuildError InputRow
decodeUtxo path UtxoEntry{ueRef, ueTxOutCbor = Hex outBytes} =
    InputRow
        <$> decodeRef (path <> ".ref") ueRef
        <*> decodeTxOut (path <> ".tx_out_cbor") outBytes

decodeRequestUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeRequestUtxo ix =
    decodeUtxo
        ("reject.request_utxos[" <> T.pack (show ix) <> "]")

decodeWalletUtxos
    :: [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos =
    traverse (uncurry decodeWalletUtxo) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo ix =
    decodeUtxo
        ("reject.wallet_utxos[" <> T.pack (show ix) <> "]")

decodeRef :: Text -> UtxoRef -> Either BuildError TxIn
decodeRef path UtxoRef{urTxId = Hex txIdBytes, urTxIx} = do
    txId <- decodeTxId (path <> ".tx_id") txIdBytes
    txIx <- decodeTxIx (path <> ".tx_ix") urTxIx
    Right (TxIn txId txIx)

decodeTxId :: Text -> ByteString -> Either BuildError TxId
decodeTxId path txIdBytes =
    case hashFromBytes @Blake2b_256 txIdBytes of
        Just hash ->
            Right (TxId $ unsafeMakeSafeHash hash)
        Nothing ->
            Left
                $ MalformedTxOut
                $ path <> " must be 32 bytes"

decodeTxIx :: Text -> Word64 -> Either BuildError TxIx
decodeTxIx path txIx
    | txIx <= fromIntegral (maxBound :: Word16) =
        Right (TxIx $ fromIntegral txIx)
    | otherwise =
        Left
            $ MalformedTxOut
            $ path <> " exceeds Word16"

decodeTxOut
    :: Text -> ByteString -> Either BuildError (TxOut ConwayEra)
decodeTxOut path outBytes =
    case decodeFull (natVersion @11) (BSL.fromStrict outBytes) of
        Left err ->
            Left
                $ MalformedTxOut
                $ path <> ": " <> showDecoder err
        Right out -> Right out

showDecoder :: DecoderError -> Text
showDecoder = T.pack . show

stateDatum :: InputRow -> Either BuildError OnChainTokenState
stateDatum row =
    case extractCageDatum (rowOut row) of
        Just (StateDatum state) -> Right state
        _ ->
            Left
                $ MalformedTxOut
                    "reject.state_utxo.tx_out_cbor missing \
                    \state datum"

requestDatum :: InputRow -> Either BuildError OnChainRequest
requestDatum row =
    case extractCageDatum (rowOut row) of
        Just (RequestDatum request) -> Right request
        _ ->
            Left
                $ MalformedTxOut
                    "reject.request_utxos[].tx_out_cbor missing \
                    \request datum"

extractCageDatum :: TxOut ConwayEra -> Maybe CageDatum
extractCageDatum txOut =
    case txOut ^. datumTxOutL of
        Datum bd ->
            let Data plcData = binaryDataToData bd
            in  fromBuiltinData (BuiltinData plcData)
        _ -> Nothing

ownerWitnessKeyHash
    :: ByteString -> Either BuildError (KeyHash Witness)
ownerWitnessKeyHash ownerBytes =
    coerce <$> ownerPaymentKeyHash ownerBytes

witnessKeyHashToGuard :: KeyHash Witness -> KeyHash Guard
witnessKeyHashToGuard (KeyHash h) = KeyHash h

ownerPaymentKeyHash
    :: ByteString -> Either BuildError (KeyHash Payment)
ownerPaymentKeyHash ownerBytes =
    case hashFromBytes @Blake2b_224 ownerBytes of
        Just hash -> Right (KeyHash hash)
        Nothing ->
            Left
                $ MalformedTxOut
                    "reject.state_utxo.datum.owner must be 28 bytes"

-- ---------------------------------------------------------------
-- Builder (mirrors Real/Reject.buildRejectProgram +
-- Cage/Update.buildUpdateTx convergence)
-- ---------------------------------------------------------------

buildRejectTx
    :: CageConfig
    -> PParams ConwayEra
    -> TokenId
    -> InputRow
    -> [InputRow]
    -> InputRow
    -> Addr
    -> OnChainTokenState
    -> KeyHash Witness
    -> ValidityInterval
    -> Either BuildError ConwayTx
buildRejectTx
    cfg
    pp
    token
    stateRow
    requestRows
    feeRow
    changeAddr
    oldState
    ownerSigner
    validity =
        converge Set.empty (Coin 0)
      where
        converge seenFees previousFee = do
            tx <- buildWithFee previousFee
            let finalFee = tx ^. bodyTxL . feeTxBodyL
            if finalFee == previousFee
                then finalize finalFee tx
                else
                    if Set.member finalFee seenFees
                        then do
                            let fallbackFee = max finalFee previousFee
                            tx' <- buildWithFee fallbackFee
                            finalize fallbackFee tx'
                        else converge (Set.insert finalFee seenFees) finalFee

        finalize fee tx = do
            preflightLegacyExactRefund
                (usesLegacyExactRefundValidator cfg)
                (map snd (refundPlans fee))
            Right tx

        buildWithFee feeForRefunds =
            case balanceTx pp ledgerPairs [] changeAddr draft of
                Left err ->
                    Left (DSLBuildFailed $ T.pack $ show err)
                Right BalanceResult{balancedTx} ->
                    Right
                        $ patchRejectRedeemers
                            pp
                            budget
                            stateRef
                            (map fst requestRefs)
                            balancedTx
          where
            draft =
                patchRedeemerBudgets pp budget
                    $ TxBuild.draft
                        pp
                        ( program
                            :: TxBuild.TxBuild NoCtx BuildError ()
                        )
            program = do
                _ <-
                    TxBuild.spendScript
                        stateRef
                        ( Modify
                            $ replicate
                                (length requestRows)
                                Rejected
                        )
                mapM_
                    ( \(ref, _) ->
                        TxBuild.spendScript
                            ref
                            ( Contribute
                                $ txInToOnChainRef stateRef
                            )
                    )
                    requestRefs
                mapM_ TxBuild.output outputs
                TxBuild.attachScript stateScript
                TxBuild.attachScript requestScript
                TxBuild.collateral feeRef
                TxBuild.requireSignature
                    (witnessKeyHashToGuard ownerSigner)
                applyValidity validity
            outputs =
                newStateOut
                    : [ refundOutput addr plan
                      | (addr, plan) <- refundPlans feeForRefunds
                      ]

        stateRef = rowRef stateRow
        feeRef = rowRef feeRow
        allRows = stateRow : requestRows <> [feeRow]
        requestRefs =
            [(rowRef row, rowOut row) | row <- requestRows]
        ledgerPairs =
            [(rowRef row, rowOut row) | row <- allRows]
        stateScript = mkCageScript cfg
        requestScript = mkRequestScript cfg token
        budget = rejectRedeemerBudget pp
        newStateOut =
            mkBasicTxOut
                (cageAddrFromCfg cfg (network cfg))
                (rowOut stateRow ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum (StateDatum oldState)
        refundPlans (Coin fee) =
            let nReqs =
                    fromIntegral (length requestRows) :: Integer
                perReqFee = fee `div` nReqs
                remainder = fee - perReqFee * nReqs
                OnChainTokenState
                    { stateMaxFee = tipAmount
                    } = oldState
            in  [ (addr, refundPlan pp addr raw)
                | (ix, row) <- zip [0 :: Int ..] requestRows
                , let request = unsafeRequestDatum row
                      addr = refundAddress (network cfg) request
                      Coin reqValue = rowOut row ^. coinTxOutL
                      extra = if ix == 0 then remainder else 0
                      raw = Coin (reqValue - tipAmount - perReqFee - extra)
                ]

applyValidity :: ValidityInterval -> TxBuild.TxBuild q e ()
applyValidity ValidityInterval{invalidBefore, invalidHereafter} = do
    case invalidBefore of
        SNothing -> pure ()
        SJust slot -> TxBuild.validFrom slot
    case invalidHereafter of
        SNothing -> pure ()
        SJust slot -> TxBuild.validTo slot

patchRedeemerBudgets
    :: PParams ConwayEra
    -> ExUnits
    -> ConwayTx
    -> ConwayTx
patchRedeemerBudgets pp budget tx =
    tx
        & witsTxL . rdmrsTxWitsL .~ budgetedRedeemers
        & bodyTxL . scriptIntegrityHashTxBodyL
            .~ computeScriptIntegrity
                (Set.singleton PlutusV3)
                pp
                budgetedRedeemers
                (TxDats mempty)
  where
    Redeemers rdmrs =
        tx ^. witsTxL . rdmrsTxWitsL
    budgetedRedeemers =
        Redeemers
            $ fmap
                ( \(dat, _) ->
                    (dat, budget)
                )
                rdmrs

patchRejectRedeemers
    :: PParams ConwayEra
    -> ExUnits
    -> TxIn
    -> [TxIn]
    -> ConwayTx
    -> ConwayTx
patchRejectRedeemers pp budget stateRef requestRefs tx =
    tx
        & witsTxL . rdmrsTxWitsL .~ redeemers
        & bodyTxL . scriptIntegrityHashTxBodyL
            .~ computeScriptIntegrity
                (Set.singleton PlutusV3)
                pp
                redeemers
                (TxDats mempty)
  where
    allInputs =
        tx ^. bodyTxL . inputsTxBodyL
    redeemers =
        Redeemers
            $ Map.fromList
            $ stateRedeemer
                : map requestRedeemer requestRefs
    stateRedeemer =
        ( spendingPurpose stateRef
        ,
            ( toLedgerData
                (Modify $ replicate (length requestRefs) Rejected)
            , budget
            )
        )
    requestRedeemer ref =
        ( spendingPurpose ref
        ,
            ( toLedgerData
                (Contribute $ txInToOnChainRef stateRef)
            , budget
            )
        )
    spendingPurpose ref =
        ConwaySpending
            (AsIx $ spendingIndex ref allInputs)

spendingIndex :: TxIn -> Set.Set TxIn -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "rejectCageTx: script input missing from balanced tx"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

refundPlan :: PParams ConwayEra -> Addr -> Coin -> RefundPlan
refundPlan pp addr raw =
    let draft = mkBasicTxOut addr (inject raw)
        minCoin = getMinCoinTxOut pp draft
        finalCoin = max raw minCoin
    in  RefundPlan raw minCoin finalCoin

-- | Build a refund output, bumping the coin to satisfy the
-- minUTxO floor. Fixed validators allow this bounded owner-funded
-- top-up; legacy exact-refund validators are preflight-refused below.
refundOutput :: Addr -> RefundPlan -> TxOut ConwayEra
refundOutput addr RefundPlan{refundFinalCoin} =
    mkBasicTxOut addr (inject refundFinalCoin)

preflightLegacyExactRefund
    :: Bool -> [RefundPlan] -> Either BuildError ()
preflightLegacyExactRefund False _ = Right ()
preflightLegacyExactRefund True plans =
    case find requiresTopUp plans of
        Nothing -> Right ()
        Just RefundPlan{refundRawCoin, refundMinCoin, refundFinalCoin} ->
            Left
                $ LegacyRejectRefundRequiresTopUp
                $ "legacy exact-refund validator cannot accept \
                  \min-UTxO refund top-up: raw refund "
                    <> coinText refundRawCoin
                    <> ", min refund "
                    <> coinText refundMinCoin
                    <> ", final refund "
                    <> coinText refundFinalCoin
  where
    requiresTopUp RefundPlan{refundRawCoin, refundFinalCoin} =
        refundFinalCoin > refundRawCoin

usesLegacyExactRefundValidator :: CageConfig -> Bool
usesLegacyExactRefundValidator CageConfig{cfgScriptHash = ScriptHash h} =
    hashToBytes h == legacyExactRefundStateHashBytes

legacyExactRefundStateHashBytes :: ByteString
legacyExactRefundStateHashBytes =
    case B16.decode
        "c0f05a30f5210d6009ec69923a3969eef40a62429e7d620b66b66e06" of
        Right bytes -> bytes
        Left err ->
            error
                $ "invalid legacy exact-refund validator hash: "
                    <> err

coinText :: Coin -> Text
coinText (Coin coin) = T.pack (show coin)

refundAddress :: Network -> OnChainRequest -> Addr
refundAddress net OnChainRequest{requestOwner = BuiltinByteString ownerBytes} =
    Addr
        net
        (KeyHashObj $ unsafeOwnerPaymentKeyHash ownerBytes)
        StakeRefNull

unsafeOwnerPaymentKeyHash :: ByteString -> KeyHash Payment
unsafeOwnerPaymentKeyHash ownerBytes =
    case hashFromBytes @Blake2b_224 ownerBytes of
        Just hash -> KeyHash hash
        Nothing -> error "rejectCageTx: invalid request owner hash"

unsafeRequestDatum :: InputRow -> OnChainRequest
unsafeRequestDatum row =
    case requestDatum row of
        Right request -> request
        Left _ -> error "rejectCageTx: request datum disappeared"

mkRequestScript
    :: CageConfig -> TokenId -> Script ConwayEra
mkRequestScript cfg token =
    let plutus =
            Plutus @PlutusV3
                $ PlutusBinary
                $ requestScriptBytesFromCfg cfg token
    in  case mkPlutusScript plutus of
            Just ps -> fromPlutusScript ps
            Nothing ->
                error "rejectCageTx: invalid PlutusV3 request script"

txInToOnChainRef :: TxIn -> OnChainTxOutRef
txInToOnChainRef (TxIn (TxId h) (TxIx ix)) =
    OnChainTxOutRef
        { txOutRefId =
            BuiltinByteString
                (hashToBytes (extractHash h))
        , txOutRefIdx = fromIntegral ix
        }

mkInlineDatum :: (ToData a) => a -> Datum ConwayEra
mkInlineDatum datum =
    Datum $ dataToBinaryData (toLedgerData datum)

toLedgerData :: (ToData a) => a -> Data ConwayEra
toLedgerData value =
    let BuiltinData d = toBuiltinData value
    in  Data d

rejectRedeemerBudget :: PParams ConwayEra -> ExUnits
rejectRedeemerBudget pp =
    capExUnits (ExUnits 900_000 250_000_000)
        $ pp ^. ppMaxTxExUnitsL

capExUnits :: ExUnits -> ExUnits -> ExUnits
capExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    ExUnits (min mem maxMem) (min steps maxSteps)

enforcePParamsPolicy
    :: WalletPolicy
    -> PParams ConwayEra
    -> Either BuildError ()
enforcePParamsPolicy WalletPolicy{..} pp = do
    let CoinPerByte minUtxoCompact = pp ^. ppCoinsPerUTxOByteL
        minUtxo = fromCompact minUtxoCompact
        prices = pp ^. ppPricesL
    if minUtxo <= wpMaxMinUtxoCoinPerByte
        then Right ()
        else
            Left
                $ PolicyViolation
                $ MinUtxoCoinPerByteTooHigh
                    minUtxo
                    wpMaxMinUtxoCoinPerByte
    if prices <= wpMaxExUnitPrices
        then Right ()
        else
            Left
                $ PolicyViolation
                $ ExUnitPricesTooHigh prices wpMaxExUnitPrices

enforceTxPolicy
    :: WalletPolicy -> ConwayTx -> Either BuildError ()
enforceTxPolicy WalletPolicy{..} tx = do
    let fee = tx ^. bodyTxL . feeTxBodyL
        width = validityWindow (tx ^. bodyTxL . vldtTxBodyL)
    if fee <= wpMaxFee
        then Right ()
        else
            Left
                $ PolicyViolation
                $ FeeTooHigh fee wpMaxFee
    if width <= wpMaxValidityWindow
        then Right ()
        else
            Left
                $ PolicyViolation
                $ ValidityWindowTooWide width wpMaxValidityWindow

validityWindow :: ValidityInterval -> SlotNo
validityWindow ValidityInterval{invalidBefore, invalidHereafter} =
    case (invalidBefore, invalidHereafter) of
        (SJust lo, SJust hi)
            | hi >= lo ->
                SlotNo (unSlotNo hi - unSlotNo lo)
        _ -> SlotNo maxBound
