{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Request
-- Description : Client-side request cage transaction builders.
module Cardano.MPFS.Client.Cage.Request
    ( requestInsertCageTx
    , requestDeleteCageTx
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.List (sortOn)
import Data.Ord (Down (..))
import Data.Sequence.Strict qualified as StrictSeq
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word16, Word64)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Crypto.Hash
    ( Blake2b_256
    , hashFromBytes
    , hashToBytes
    )
import Cardano.Ledger.Address
    ( Addr (..)
    , decodeAddrEither
    )
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Api.PParams
    ( ppCoinsPerUTxOByteL
    , ppPricesL
    )
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , mkBasicTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.Babbage.PParams
    ( CoinPerByte (..)
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , StrictMaybe (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    )
import Cardano.Ledger.Coin
    ( Coin (..)
    )
import Cardano.Ledger.Core
    ( PParams
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    )
import Cardano.Ledger.Hashes
    ( unsafeMakeSafeHash
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
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
    ( RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    , TokenId
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.Client.Cage.Identity
    ( onChainTokenId
    , requestAddrFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRequestDeleteFacts
    , VerifiedRequestInsertFacts
    , verifiedRequestDeleteFacts
    , verifiedRequestInsertFacts
    )
import Cardano.Node.Client.Balance
    ( BalanceResult (..)
    , balanceTx
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( ToData (..)
    )

-- | Build an unsigned request-insert transaction from already-verified
-- request-insert facts. The function decodes ledger facts, enforces
-- wallet caps, and returns a transaction ready for requester signing.
requestInsertCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRequestInsertFacts
    -> Either BuildError (Tx ConwayEra)
requestInsertCageTx cfg policy verified =
    let facts = verifiedRequestInsertFacts verified
    in  buildRequestCageTx
            "request_insert"
            cfg
            policy
            (rifProtocolParameters facts)
            (rifWalletUtxos facts)
            (rifAddress facts)
            (tokenIdFromJSON $ rifToken facts)
            (unHex $ rifKey facts)
            (OpInsert (unHex $ rifValue facts))
            (rifSubmittedAt facts)

-- | Build an unsigned request-delete transaction from already-verified
-- request-delete facts. The function decodes ledger facts, enforces
-- wallet caps, and returns a transaction ready for requester signing.
requestDeleteCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedRequestDeleteFacts
    -> Either BuildError (Tx ConwayEra)
requestDeleteCageTx cfg policy verified =
    let facts = verifiedRequestDeleteFacts verified
    in  buildRequestCageTx
            "request_delete"
            cfg
            policy
            (rdfProtocolParameters facts)
            (rdfWalletUtxos facts)
            (rdfAddress facts)
            (tokenIdFromJSON $ rdfToken facts)
            (unHex $ rdfKey facts)
            (OpDelete (unHex $ rdfValue facts))
            (rdfSubmittedAt facts)

buildRequestCageTx
    :: Text
    -> CageConfig
    -> WalletPolicy
    -> UnverifiedPParams
    -> [UtxoEntry]
    -> Hex
    -> TokenId
    -> ByteString
    -> OnChainOperation
    -> Integer
    -> Either BuildError (Tx ConwayEra)
buildRequestCageTx
    label
    cfg
    policy
    ppEnc
    walletEntries
    addrHex
    token
    key
    op
    submittedAt = do
        pp <- decodePParams ppEnc
        enforcePParamsPolicy policy pp
        fundingRows <- decodeWalletUtxos label walletEntries
        fundingRow <- selectFundingRow fundingRows
        requesterAddr <- decodeAddress label addrHex
        tx <-
            buildRequestTx
                cfg
                pp
                fundingRow
                requesterAddr
                token
                key
                op
                submittedAt
        enforceTxPolicy policy tx
        pure tx

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

selectFundingRow :: [InputRow] -> Either BuildError InputRow
selectFundingRow [] = Left EmptyFunding
selectFundingRow rows =
    case sortOn (Down . (^. coinTxOutL) . rowOut) rows of
        row : _ -> Right row
        [] -> Left EmptyFunding

decodeAddress :: Text -> Hex -> Either BuildError Addr
decodeAddress label (Hex addrBytes) =
    case decodeAddrEither addrBytes of
        Left err ->
            Left
                $ MalformedTxOut
                $ label <> ".address: " <> T.pack err
        Right addr -> Right addr

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
    :: Text -> [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos label =
    traverse (uncurry (decodeWalletUtxo label)) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Text -> Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo label ix UtxoEntry{ueRef, ueTxOutCbor = Hex outBytes} =
    InputRow
        <$> decodeRef (walletField label ix "ref") ueRef
        <*> decodeTxOut (walletField label ix "tx_out_cbor") outBytes

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

walletField :: Text -> Int -> Text -> Text
walletField label ix name =
    label
        <> ".wallet_utxos["
        <> T.pack (show ix)
        <> "]."
        <> name

buildRequestTx
    :: CageConfig
    -> PParams ConwayEra
    -> InputRow
    -> Addr
    -> TokenId
    -> ByteString
    -> OnChainOperation
    -> Integer
    -> Either BuildError (Tx ConwayEra)
buildRequestTx
    cfg
    pp
    fundingRow
    requesterAddr
    token
    key
    op
    submittedAt =
        case balanceTx
            pp
            [(rowRef fundingRow, rowOut fundingRow)]
            requesterAddr
            draft of
            Left err ->
                Left (DSLBuildFailed $ T.pack $ show err)
            Right BalanceResult{balancedTx} ->
                Right balancedTx
      where
        Coin tip = defaultTip cfg
        scriptAddr =
            requestAddrFromCfg cfg token (network cfg)
        datum =
            requestDatum
                token
                requesterAddr
                key
                op
                tip
                submittedAt
        draftOut =
            mkBasicTxOut
                scriptAddr
                (inject (Coin 0))
                & datumTxOutL .~ datum
        refundDraft =
            mkBasicTxOut requesterAddr (inject (Coin 0))
        lockedAda =
            requestLockedAda pp draftOut refundDraft tip
        txOut =
            mkBasicTxOut
                scriptAddr
                (inject lockedAda)
                & datumTxOutL .~ datum
        body =
            mkBasicTxBody
                & outputsTxBodyL
                    .~ StrictSeq.singleton txOut
        draft = mkBasicTx body

requestDatum
    :: TokenId
    -> Addr
    -> ByteString
    -> OnChainOperation
    -> Integer
    -> Integer
    -> Datum ConwayEra
requestDatum token addr key op fee submittedAt =
    mkInlineDatum
        $ RequestDatum
        $ OnChainRequest
            { requestToken = onChainTokenId token
            , requestOwner =
                BuiltinByteString (addrKeyHashBytes addr)
            , requestKey = key
            , requestValue = op
            , requestFee = fee
            , requestSubmittedAt = submittedAt
            }

mkInlineDatum :: (ToData a) => a -> Datum ConwayEra
mkInlineDatum datum =
    Datum $ dataToBinaryData (toLedgerData datum)

toLedgerData :: (ToData a) => a -> Data ConwayEra
toLedgerData value =
    let BuiltinData d = toBuiltinData value
    in  Data d

addrKeyHashBytes :: Addr -> ByteString
addrKeyHashBytes
    (Addr _ (KeyHashObj (KeyHash hash)) _) =
        hashToBytes hash
addrKeyHashBytes _ = ""

requestLockedAda
    :: PParams ConwayEra
    -> TxOut ConwayEra
    -> TxOut ConwayEra
    -> Integer
    -> Coin
requestLockedAda pp reqDraft refDraft tip =
    let Coin refMin =
            getMinCoinTxOut pp refDraft
        feeBuffer = 1_000_000
        locked = tip + feeBuffer + refMin
        adjusted =
            getMinCoinTxOut
                pp
                ( reqDraft
                    & valueTxOutL
                        .~ inject (Coin locked)
                )
    in  max adjusted (Coin locked)

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
