{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.End
-- Description : Client-side end cage transaction builder.
module Cardano.MPFS.Client.Cage.End
    ( endCageTx
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
    )
import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.Scripts
    ( AsIx (..)
    )
import Cardano.Ledger.Alonzo.TxBody
    ( reqSignerHashesTxBodyL
    , scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Alonzo.TxWits
    ( TxDats (..)
    )
import Cardano.Ledger.Api.PParams
    ( ppCoinsPerUTxOByteL
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
    , mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , mkBasicTxBody
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , coinTxOutL
    , datumTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
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
import Cardano.Ledger.Coin
    ( CoinPerByte (..)
    )
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core
    ( PParams
    , hashScript
    )
import Cardano.Ledger.Hashes
    ( unsafeMakeSafeHash
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.Ledger.Mary.Value
    ( MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    )
import Cardano.Ledger.Plutus.Language
    ( Language (..)
    )
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
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
    ( EndFacts (..)
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    , TokenId (..)
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cagePolicyIdFromCfg
    , mkCageScript
    )
import Cardano.MPFS.Client.Cage.Identity
    ( onChainTokenId
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedEndFacts
    , verifiedEndFacts
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    , evalBudgetExUnits
    )
import Cardano.Tx.Ledger
    ( ConwayTx
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )

-- | Build an unsigned end transaction from already-verified end
-- facts. The function decodes the supplied ledger facts, enforces
-- wallet caps, and returns a transaction ready for owner signing.
endCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedEndFacts
    -> Either BuildError (ConwayTx)
endCageTx cfg policy verified = do
    let facts = verifiedEndFacts verified
    pp <- decodePParams (efProtocolParameters facts)
    enforcePParamsPolicy policy pp
    stateRow <- decodeStateUtxo (efStateUtxo facts)
    fundingRows <- decodeWalletUtxos (efWalletUtxos facts)
    collateralRow <- selectFeeRow fundingRows
    ownerBytes <- stateOwnerBytes stateRow
    ownerSigner <- ownerWitnessKeyHash ownerBytes
    let changeAddr = rowAddress collateralRow
    tx <-
        buildEndTx
            cfg
            pp
            stateRow
            fundingRows
            collateralRow
            changeAddr
            ownerSigner
            (tokenIdFromJSON $ efToken facts)
    enforceTxPolicy policy tx
    pure tx

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

rowAddress :: InputRow -> Addr
rowAddress row =
    rowOut row ^. addrTxOutL

-- | Pick the wallet UTxO carrying the largest lovelace
-- balance. The selected row pays the script fee and the
-- Conway collateral on end, which requires a larger
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

decodeStateUtxo :: UtxoEntry -> Either BuildError InputRow
decodeStateUtxo UtxoEntry{ueRef, ueTxOutCbor = Hex outBytes} =
    InputRow
        <$> decodeRef "end.state_utxo.ref" ueRef
        <*> decodeTxOut "end.state_utxo.tx_out_cbor" outBytes

decodeWalletUtxos
    :: [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos =
    traverse (uncurry decodeWalletUtxo) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo ix UtxoEntry{ueRef, ueTxOutCbor = Hex outBytes} =
    InputRow
        <$> decodeRef (walletField ix "ref") ueRef
        <*> decodeTxOut (walletField ix "tx_out_cbor") outBytes

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
showDecoder =
    T.pack . show

walletField :: Int -> Text -> Text
walletField ix name =
    "end.wallet_utxos[" <> T.pack (show ix) <> "]." <> name

stateOwnerBytes :: InputRow -> Either BuildError ByteString
stateOwnerBytes row =
    case extractCageDatum (rowOut row) of
        Just
            ( StateDatum
                    OnChainTokenState
                        { stateOwner =
                            BuiltinByteString ownerBytes
                        }
                ) ->
                Right ownerBytes
        _ ->
            Left
                $ MalformedTxOut
                    "end.state_utxo.tx_out_cbor missing state datum"

ownerWitnessKeyHash
    :: ByteString -> Either BuildError (KeyHash Guard)
ownerWitnessKeyHash ownerBytes =
    coerce <$> ownerPaymentKeyHash ownerBytes

ownerPaymentKeyHash
    :: ByteString -> Either BuildError (KeyHash Payment)
ownerPaymentKeyHash ownerBytes =
    case hashFromBytes @Blake2b_224 ownerBytes of
        Just hash -> Right (KeyHash hash)
        Nothing ->
            Left
                $ MalformedTxOut
                    "end.state_utxo.datum.owner must be 28 bytes"

extractCageDatum :: TxOut ConwayEra -> Maybe CageDatum
extractCageDatum txOut =
    case txOut ^. datumTxOutL of
        Datum bd ->
            let Data plcData =
                    binaryDataToData bd
            in  fromBuiltinData (BuiltinData plcData)
        _ -> Nothing

buildEndTx
    :: CageConfig
    -> PParams ConwayEra
    -> InputRow
    -> [InputRow]
    -> InputRow
    -> Addr
    -> KeyHash Guard
    -> TokenId
    -> Either BuildError (ConwayTx)
buildEndTx
    cfg
    pp
    stateRow
    fundingRows
    collateralRow
    ownerAddr
    ownerSigner
    token =
        case balanceTx pp ledgerPairs [] ownerAddr draft of
            Left err ->
                Left (DSLBuildFailed $ T.pack $ show err)
            Right BalanceResult{balancedTx} ->
                Right balancedTx
      where
        stateRef = rowRef stateRow
        collateralRef = rowRef collateralRow
        policyId = cagePolicyIdFromCfg cfg
        script = mkCageScript cfg
        scriptHash = hashScript script
        tokenAsset = unTokenId token
        burnValue =
            MultiAsset
                $ Map.singleton policyId
                $ Map.singleton tokenAsset (-1)
        allRows = stateRow : fundingRows
        allInputs =
            Set.fromList (map rowRef allRows)
        stateIx =
            spendingIndex stateRef allInputs
        endBudget =
            endRedeemerBudget pp
        redeemers =
            Redeemers
                $ Map.fromList
                    [
                        ( ConwaySpending (AsIx stateIx)
                        ,
                            ( toLedgerData End
                            , endBudget
                            )
                        )
                    ,
                        ( ConwayMinting (AsIx 0)
                        ,
                            ( toLedgerData
                                (Burning $ onChainTokenId token)
                            , endBudget
                            )
                        )
                    ]
        body =
            mkBasicTxBody @ConwayEra
                & inputsTxBodyL .~ allInputs
                & mintTxBodyL .~ burnValue
                & collateralInputsTxBodyL
                    .~ Set.singleton collateralRef
                & reqSignerHashesTxBodyL
                    .~ Set.singleton ownerSigner
                & scriptIntegrityHashTxBodyL
                    .~ computeScriptIntegrity
                        (Set.singleton PlutusV3)
                        pp
                        redeemers
                        (TxDats mempty)
        draft :: ConwayTx
        draft =
            mkBasicTx body
                & witsTxL . scriptTxWitsL
                    .~ Map.singleton scriptHash script
                & witsTxL . rdmrsTxWitsL .~ redeemers
        ledgerPairs =
            [ (rowRef row, rowOut row)
            | row <- allRows
            ]

toLedgerData :: (ToData a) => a -> Data ConwayEra
toLedgerData value =
    let BuiltinData d = toBuiltinData value
    in  Data d

endRedeemerBudget :: PParams ConwayEra -> ExUnits
endRedeemerBudget pp =
    capExUnits evalBudgetExUnits
        $ halfExUnits
        $ pp ^. ppMaxTxExUnitsL

capExUnits :: ExUnits -> ExUnits -> ExUnits
capExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    ExUnits (min mem maxMem) (min steps maxSteps)

halfExUnits :: ExUnits -> ExUnits
halfExUnits (ExUnits mem steps) =
    ExUnits (mem `div` 2) (steps `div` 2)

spendingIndex :: TxIn -> Set.Set TxIn -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "spendingIndex: TxIn not in input set"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

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
