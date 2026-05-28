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
    ( rejectCageTx
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Coerce (coerce)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Sequence.Strict qualified as StrictSeq
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
    ( reqSignerHashesTxBodyL
    , scriptIntegrityHashTxBodyL
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
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
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
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Babbage.PParams
    ( CoinPerByte (..)
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
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core
    ( PParams
    , Script
    , hashScript
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
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
import Cardano.Node.Client.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    , evalBudgetExUnits
    )

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
    -> Either BuildError (Tx ConwayEra)
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
    :: ByteString -> Either BuildError (KeyHash 'Witness)
ownerWitnessKeyHash ownerBytes =
    coerce <$> ownerPaymentKeyHash ownerBytes

ownerPaymentKeyHash
    :: ByteString -> Either BuildError (KeyHash 'Payment)
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
    -> KeyHash 'Witness
    -> ValidityInterval
    -> Either BuildError (Tx ConwayEra)
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
                then Right tx
                else
                    if Set.member finalFee seenFees
                        then buildWithFee (max finalFee previousFee)
                        else converge (Set.insert finalFee seenFees) finalFee

        buildWithFee feeForRefunds =
            case balanceTx pp ledgerPairs changeAddr draft of
                Left err ->
                    Left (DSLBuildFailed $ T.pack $ show err)
                Right BalanceResult{balancedTx} ->
                    Right balancedTx
          where
            draft =
                mkBasicTx body
                    & witsTxL . scriptTxWitsL .~ scripts
                    & witsTxL . rdmrsTxWitsL .~ redeemers
            body =
                mkBasicTxBody
                    & inputsTxBodyL .~ allInputs
                    & collateralInputsTxBodyL
                        .~ Set.singleton feeRef
                    & outputsTxBodyL
                        .~ StrictSeq.fromList outputs
                    & reqSignerHashesTxBodyL
                        .~ Set.singleton ownerSigner
                    & vldtTxBodyL .~ validity
                    & scriptIntegrityHashTxBodyL
                        .~ computeScriptIntegrity PlutusV3 pp redeemers
            outputs =
                newStateOut : refundOutputs feeForRefunds

        stateRef = rowRef stateRow
        feeRef = rowRef feeRow
        allRows = stateRow : requestRows <> [feeRow]
        allInputs = Set.fromList (map rowRef allRows)
        ledgerPairs =
            [(rowRef row, rowOut row) | row <- allRows]
        stateScript = mkCageScript cfg
        requestScript = mkRequestScript cfg token
        scripts =
            Map.fromList
                [ (hashScript stateScript, stateScript)
                , (hashScript requestScript, requestScript)
                ]
        budget = rejectRedeemerBudget pp
        redeemers =
            Redeemers
                $ Map.fromList
                $ stateRedeemer : requestRedeemers
        stateRedeemer =
            ( ConwaySpending (AsIx $ spendingIndex stateRef allInputs)
            ,
                ( toLedgerData
                    $ Modify
                    $ replicate (length requestRows) Rejected
                , budget
                )
            )
        requestRedeemers =
            [ ( ConwaySpending (AsIx $ spendingIndex ref allInputs)
              ,
                  ( toLedgerData
                        $ Contribute
                        $ txInToOnChainRef stateRef
                  , budget
                  )
              )
            | ref <- map rowRef requestRows
            ]
        newStateOut =
            mkBasicTxOut
                (cageAddrFromCfg cfg (network cfg))
                (rowOut stateRow ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum (StateDatum oldState)
        refundOutputs (Coin fee) =
            let nReqs =
                    fromIntegral (length requestRows) :: Integer
                perReqFee = fee `div` nReqs
                remainder = fee - perReqFee * nReqs
                OnChainTokenState
                    { stateMaxFee = tipAmount
                    } = oldState
            in  [ refundOutput
                    pp
                    (refundAddress (network cfg) request)
                    (Coin (reqValue - tipAmount - perReqFee - extra))
                | (ix, row) <- zip [0 :: Int ..] requestRows
                , let request = unsafeRequestDatum row
                      Coin reqValue = rowOut row ^. coinTxOutL
                      extra = if ix == 0 then remainder else 0
                ]

-- | Build a refund output, bumping the coin to satisfy the
-- minUTxO floor when the raw refund would underpay it.
refundOutput :: PParams ConwayEra -> Addr -> Coin -> TxOut ConwayEra
refundOutput pp addr raw =
    let draft = mkBasicTxOut addr (inject raw)
        minCoin = getMinCoinTxOut pp draft
    in  mkBasicTxOut addr (inject (max raw minCoin))

refundAddress :: Network -> OnChainRequest -> Addr
refundAddress net OnChainRequest{requestOwner = BuiltinByteString ownerBytes} =
    Addr
        net
        (KeyHashObj $ unsafeOwnerPaymentKeyHash ownerBytes)
        StakeRefNull

unsafeOwnerPaymentKeyHash :: ByteString -> KeyHash 'Payment
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
    let CoinPerByte minUtxo = pp ^. ppCoinsPerUTxOByteL
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
    :: WalletPolicy -> Tx ConwayEra -> Either BuildError ()
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
