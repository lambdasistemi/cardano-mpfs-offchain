{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Retract
-- Description : Client-side retract cage transaction builder.
module Cardano.MPFS.Client.Cage.Retract
    ( retractCageTx
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Coerce (coerce)
import Data.List (sortOn)
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
import Cardano.Ledger.Address (Addr)
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
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralReturnTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , totalCollateralTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , coinTxOutL
    , datumTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( ConwayPlutusPurpose (..)
    , Redeemers (..)
    , TxDats (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( StrictMaybe (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    )
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Core
    ( PParams
    , Script
    )
import Cardano.Ledger.Hashes
    ( extractHash
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
    ( RetractFacts (..)
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    , TokenId
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , OnChainRequest (..)
    , OnChainTxOutRef (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
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
    ( VerifiedRetractFacts
    , verifiedRetractFacts
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    )
import Cardano.Tx.Build qualified as TxBuild

data NoCtx a

-- | Build an unsigned retract transaction from already-verified
-- retract facts. The function decodes the supplied ledger facts,
-- enforces wallet caps, applies the server-derived Phase 2
-- validity interval, and returns a transaction ready for the
-- request owner to sign.
retractCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRetractFacts
    -> Either BuildError ConwayTx
retractCageTx cfg policy verified = do
    let facts = verifiedRetractFacts verified
    pp <- decodePParams (rfProtocolParameters facts)
    enforcePParamsPolicy policy pp
    requestRow <-
        decodeUtxo "retract.request_utxo" (rfRequestUtxo facts)
    stateRow <-
        decodeUtxo "retract.state_utxo" (rfStateUtxo facts)
    fundingRows <- decodeWalletUtxos (rfWalletUtxos facts)
    feeRow <- selectFeeRow fundingRows
    ownerBytes <- requestOwnerBytes requestRow
    ownerSigner <- ownerWitnessKeyHash ownerBytes
    let token = tokenIdFromJSON (rfToken facts)
        changeAddr = rowAddress feeRow
        validity =
            ValidityInterval
                (SJust (slot (rfValidityStartSlot facts)))
                (SJust (slot (rfValidityEndSlot facts)))
    tx <-
        buildRetractTx
            cfg
            pp
            token
            requestRow
            stateRow
            feeRow
            changeAddr
            ownerSigner
            validity
    enforceTxPolicy policy tx
    pure tx
  where
    slot = SlotNo . fromIntegral

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

rowAddress :: InputRow -> Addr
rowAddress row =
    rowOut row ^. addrTxOutL

-- | Pick the wallet UTxO carrying the largest lovelace
-- balance. The selected row pays the script fee and the
-- Conway collateral on retract, which requires a larger
-- balance than the smallest funding entry can guarantee.
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

decodeWalletUtxos
    :: [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos =
    traverse (uncurry decodeWalletUtxo) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo ix =
    decodeUtxo (walletField ix)

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

walletField :: Int -> Text
walletField ix =
    "retract.wallet_utxos[" <> T.pack (show ix) <> "]"

requestOwnerBytes :: InputRow -> Either BuildError ByteString
requestOwnerBytes row =
    case extractCageDatum (rowOut row) of
        Just
            ( RequestDatum
                    OnChainRequest
                        { requestOwner =
                            BuiltinByteString ownerBytes
                        }
                ) ->
                Right ownerBytes
        _ ->
            Left
                $ MalformedTxOut
                    "retract.request_utxo.tx_out_cbor missing \
                    \request datum"

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
                    "retract.request_utxo.datum.owner must \
                    \be 28 bytes"

extractCageDatum :: TxOut ConwayEra -> Maybe CageDatum
extractCageDatum txOut =
    case txOut ^. datumTxOutL of
        Datum bd ->
            let Data plcData =
                    binaryDataToData bd
            in  fromBuiltinData (BuiltinData plcData)
        _ -> Nothing

buildRetractTx
    :: CageConfig
    -> PParams ConwayEra
    -> TokenId
    -> InputRow
    -> InputRow
    -> InputRow
    -> Addr
    -> KeyHash Witness
    -> ValidityInterval
    -> Either BuildError ConwayTx
buildRetractTx
    cfg
    pp
    token
    requestRow
    stateRow
    feeRow
    changeAddr
    ownerSigner
    validity =
        case balanceTx pp ledgerPairs [] changeAddr draft of
            Left err ->
                Left (DSLBuildFailed $ T.pack $ show err)
            Right BalanceResult{balancedTx} ->
                Right
                    $ stripBalancingCollateralFields
                    $ patchRetractRedeemers
                        pp
                        budget
                        requestRef
                        redeemer
                        balancedTx
      where
        requestRef = rowRef requestRow
        stateRef = rowRef stateRow
        feeRef = rowRef feeRow
        script = mkRequestScript cfg token
        budget = retractRedeemerBudget pp
        redeemer = Retract (txInToOnChainRef stateRef)
        program = do
            _ <- TxBuild.spendScript requestRef redeemer
            TxBuild.reference stateRef
            TxBuild.attachScript script
            TxBuild.collateral feeRef
            TxBuild.requireSignature
                (witnessKeyHashToGuard ownerSigner)
            applyValidity validity
        draft =
            patchRedeemerBudgets pp budget
                $ TxBuild.draft
                    pp
                    ( program
                        :: TxBuild.TxBuild NoCtx BuildError ()
                    )
        ledgerPairs =
            [ (rowRef requestRow, rowOut requestRow)
            , (rowRef feeRow, rowOut feeRow)
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

patchRetractRedeemers
    :: PParams ConwayEra
    -> ExUnits
    -> TxIn
    -> UpdateRedeemer
    -> ConwayTx
    -> ConwayTx
patchRetractRedeemers pp budget requestRef redeemer tx =
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
            $ Map.singleton
                ( ConwaySpending
                    (AsIx $ spendingIndex requestRef allInputs)
                )
                (toLedgerData redeemer, budget)

spendingIndex :: TxIn -> Set.Set TxIn -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "retractCageTx: script input missing from balanced tx"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

stripBalancingCollateralFields :: ConwayTx -> ConwayTx
stripBalancingCollateralFields tx =
    tx
        & bodyTxL . totalCollateralTxBodyL .~ SNothing
        & bodyTxL . collateralReturnTxBodyL .~ SNothing

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
                error "retractCageTx: invalid PlutusV3 script"

txInToOnChainRef :: TxIn -> OnChainTxOutRef
txInToOnChainRef (TxIn (TxId h) (TxIx ix)) =
    OnChainTxOutRef
        { txOutRefId =
            BuiltinByteString
                (hashToBytes (extractHash h))
        , txOutRefIdx = fromIntegral ix
        }

toLedgerData :: (ToData a) => a -> Data ConwayEra
toLedgerData value =
    let BuiltinData d = toBuiltinData value
    in  Data d

retractRedeemerBudget :: PParams ConwayEra -> ExUnits
retractRedeemerBudget pp =
    capExUnits legacyEvalBudgetExUnits
        $ halfExUnits
        $ pp ^. ppMaxTxExUnitsL

legacyEvalBudgetExUnits :: ExUnits
legacyEvalBudgetExUnits = ExUnits 14_000_000 10_000_000_000

capExUnits :: ExUnits -> ExUnits -> ExUnits
capExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    ExUnits (min mem maxMem) (min steps maxSteps)

halfExUnits :: ExUnits -> ExUnits
halfExUnits (ExUnits mem steps) =
    ExUnits (mem `div` 2) (steps `div` 2)

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
