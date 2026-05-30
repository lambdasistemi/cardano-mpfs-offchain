{-# LANGUAGE DataKinds #-}
{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Boot
-- Description : Client-side boot cage transaction builder.
module Cardano.MPFS.Client.Cage.Boot
    ( bootCageTx
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word16, Word64)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Crypto.Hash
    ( Blake2b_256
    , hashFromBytes
    , hashToBytes
    )
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( CoinPerByte (..)
    , ppCoinsPerUTxOByteL
    , ppPricesL
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
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
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
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Core (PParams)
import Cardano.Ledger.Credential (Credential (..))
import Cardano.Ledger.Hashes
    ( extractHash
    , unsafeMakeSafeHash
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
    )
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MaryValue (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.Language
    ( Language (..)
    )
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Common
    ( UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    )
import Cardano.MPFS.API.Types.Facts (BootFacts (..))
import Cardano.MPFS.Cage.AssetName (deriveAssetName)
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cageAddrFromCfg
    , cagePolicyIdFromCfg
    , mkCageScript
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedBootFacts
    , verifiedBootFacts
    )
import Cardano.Slotting.Slot (SlotNo (..))
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    , evalBudgetExUnits
    )
import Cardano.Tx.Build
    ( Interpret (..)
    , TxBuild
    )
import Cardano.Tx.Build qualified as TxBuild
import Cardano.Tx.Ledger (ConwayTx)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

-- | Build an unsigned boot transaction from already-verified boot
-- facts. The function performs all decoding and wallet policy checks
-- before returning a transaction for signing.
bootCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedBootFacts
    -> Either BuildError ConwayTx
bootCageTx cfg policy verified = do
    let facts = verifiedBootFacts verified
    pp <- decodePParams (bfProtocolParameters facts)
    enforcePParamsPolicy policy pp
    rows <- decodeWalletUtxos (bfWalletUtxos facts)
    (pickedRows, seedRow, collateralRow) <- selectBootRows rows
    let ownerAddr = rowAddress seedRow
    tx <-
        buildBootTx
            cfg
            pp
            seedRow
            collateralRow
            pickedRows
            ownerAddr
    enforceTxPolicy policy tx
    pure tx

-- | Pick the wallet rows that fund the boot transaction.
-- Conway requires the collateral row to back the script
-- fee, which is larger than the smallest funding entry can
-- guarantee when the wallet holds several UTxOs of mixed
-- size. Select by largest lovelace balance (matching
-- 'requestInsertCageTx' and 'retractCageTx'): the largest
-- row is the collateral, the second-largest is the spent
-- seed input. When only one row is available it serves
-- both roles, as before.
selectBootRows
    :: [InputRow]
    -> Either BuildError ([InputRow], InputRow, InputRow)
selectBootRows rows =
    case sortOn (Down . (^. coinTxOutL) . rowOut) rows of
        [] -> Left EmptyFunding
        [only] -> Right ([only], only, only)
        largest : second : _ ->
            Right ([largest, second], second, largest)

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

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

decodeWalletUtxos
    :: [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos =
    traverse (uncurry decodeWalletUtxo) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo ix UtxoEntry{ueRef, ueTxOutCbor = Hex outBytes} =
    InputRow
        <$> decodeRef ix ueRef
        <*> decodeTxOut ix outBytes

decodeRef :: Int -> UtxoRef -> Either BuildError TxIn
decodeRef ix UtxoRef{urTxId = Hex txIdBytes, urTxIx} = do
    txId <- decodeTxId ix txIdBytes
    txIx <- decodeTxIx ix urTxIx
    Right (TxIn txId txIx)

decodeTxId :: Int -> ByteString -> Either BuildError TxId
decodeTxId ix txIdBytes =
    case hashFromBytes @Blake2b_256 txIdBytes of
        Just hash ->
            Right (TxId $ unsafeMakeSafeHash hash)
        Nothing ->
            Left
                $ MalformedTxOut
                $ field ix "ref.tx_id must be 32 bytes"

decodeTxIx :: Int -> Word64 -> Either BuildError TxIx
decodeTxIx ix txIx
    | txIx <= fromIntegral (maxBound :: Word16) =
        Right (TxIx $ fromIntegral txIx)
    | otherwise =
        Left
            $ MalformedTxOut
            $ field ix "ref.tx_ix exceeds Word16"

decodeTxOut
    :: Int -> ByteString -> Either BuildError (TxOut ConwayEra)
decodeTxOut ix outBytes =
    case decodeFull (natVersion @11) (BSL.fromStrict outBytes) of
        Left err ->
            Left
                $ MalformedTxOut
                $ field ix (showDecoder err)
        Right out -> Right out

showDecoder :: DecoderError -> Text
showDecoder =
    T.pack . show

field :: Int -> Text -> Text
field ix msg =
    "boot.wallet_utxos[" <> T.pack (show ix) <> "]." <> msg

rowAddress :: InputRow -> Addr
rowAddress row =
    rowOut row ^. addrTxOutL

buildBootTx
    :: CageConfig
    -> PParams ConwayEra
    -> InputRow
    -> InputRow
    -> [InputRow]
    -> Addr
    -> Either BuildError ConwayTx
buildBootTx cfg pp seedRow collateralRow rows ownerAddr =
    case balanceTx pp ledgerPairs [] ownerAddr withSubmitBudget of
        Left err ->
            Left (DSLBuildFailed $ T.pack $ show err)
        Right BalanceResult{balancedTx} ->
            Right balancedTx
  where
    seedRef = rowRef seedRow
    collateralRef = rowRef collateralRow
    program =
        bootProgram cfg seedRef collateralRef ownerAddr
    draft =
        TxBuild.draftWith pp noCtxInterpret program
    allInputs =
        Set.fromList (map rowRef rows)
    withAllInputs =
        draft
            & bodyTxL . inputsTxBodyL
                .~ Set.union
                    allInputs
                    (draft ^. bodyTxL . inputsTxBodyL)
    withSubmitBudget =
        patchRedeemerBudgets pp withAllInputs
    ledgerPairs =
        [ (rowRef row, rowOut row)
        | row <- rows
        ]

patchRedeemerBudgets
    :: PParams ConwayEra
    -> ConwayTx
    -> ConwayTx
patchRedeemerBudgets pp tx =
    tx
        & witsTxL . rdmrsTxWitsL
            .~ budgetedRedeemers
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
                    (dat, evalBudgetExUnits)
                )
                rdmrs

bootProgram
    :: CageConfig
    -> TxIn
    -> TxIn
    -> Addr
    -> TxBuild (ConstQuery ()) BuildError ()
bootProgram cfg seedRef collateralRef ownerAddr = do
    _ <- TxBuild.spend seedRef
    TxBuild.attachScript script
    TxBuild.mint policyId mintAssets redeemer
    _ <-
        TxBuild.payTo'
            scriptAddr
            stateValue
            (bootStateDatum cfg ownerAddr)
    TxBuild.collateral collateralRef
  where
    policyId = cagePolicyIdFromCfg cfg
    script = mkCageScript cfg
    scriptAddr = cageAddrFromCfg cfg (network cfg)
    assetName = bootAssetName seedRef
    mintAssets = Map.singleton assetName 1
    stateValue =
        MaryValue
            (Coin 2_000_000)
            (MultiAsset $ Map.singleton policyId mintAssets)
    redeemer = Minting (txInToRef seedRef)

data ConstQuery a b

noCtxInterpret :: Interpret (ConstQuery ())
noCtxInterpret =
    Interpret $ \case {}

bootAssetName :: TxIn -> AssetName
bootAssetName seedRef =
    AssetName
        $ SBS.toShort
        $ deriveAssetName (txInToRef seedRef)

bootStateDatum :: CageConfig -> Addr -> CageDatum
bootStateDatum cfg ownerAddr =
    StateDatum
        OnChainTokenState
            { stateOwner =
                BuiltinByteString (addrKeyHashBytes ownerAddr)
            , stateRoot = OnChainRoot emptyRoot
            , stateMaxFee =
                let Coin c = defaultTip cfg
                in  c
            , stateProcessTime = defaultProcessTime cfg
            , stateRetractTime = defaultRetractTime cfg
            }

txInToRef :: TxIn -> OnChainTxOutRef
txInToRef (TxIn (TxId hash) (TxIx ix)) =
    OnChainTxOutRef
        { txOutRefId =
            BuiltinByteString
                (hashToBytes (extractHash hash))
        , txOutRefIdx = fromIntegral ix
        }

addrKeyHashBytes :: Addr -> ByteString
addrKeyHashBytes
    (Addr _ (KeyHashObj (KeyHash hash)) _) =
        hashToBytes hash
addrKeyHashBytes _ = BS.empty

emptyRoot :: ByteString
emptyRoot = BS.replicate 32 0

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
