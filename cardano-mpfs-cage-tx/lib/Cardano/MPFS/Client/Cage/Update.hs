{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Update
-- Description : Client-side update cage transaction builder.
module Cardano.MPFS.Client.Cage.Update
    ( updateCageTx
    , foldUpdateTrieFacts
    ) where

import Control.Monad (guard, when)
import Data.Bits (testBit)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Coerce (coerce)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust)
import Data.Ord (Down (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word16, Word32, Word64, Word8)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromBytes
    , hashToBytes
    )
import Cardano.Ledger.Address
    ( Addr (..)
    )
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
import Cardano.Ledger.Coin
    ( Coin (..)
    )
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
import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common
    ( UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( TrieFact (..)
    , UpdateFacts (..)
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    , TokenId
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , Neighbor (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , ProofStep (..)
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
    ( VerifiedUpdateFacts
    , verifiedUpdateFacts
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    , computeScriptIntegrity
    , evalBudgetExUnits
    )
import Cardano.Tx.Build qualified as TxBuild
import Cardano.Tx.Ledger (ConwayTx)
import MPF.Hashes
    ( MPFHash (..)
    , MPFHashing (..)
    , aikenKeyPath
    , byteStringToHexKey'
    , mkMPFHash
    , mpfHashing
    , nullHash
    , renderMPFHash
    )
import MPF.Interface
    ( HexDigit (..)
    , HexKey
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )

data NoCtx a

-- | Build an unsigned update transaction from verified update facts.
updateCageTx
    :: CageConfig
    -> WalletPolicy
    -> VerifiedUpdateFacts
    -> Either BuildError ConwayTx
updateCageTx cfg policy verified = do
    let facts = verifiedUpdateFacts verified
        token = tokenIdFromJSON (ufToken facts)
    pp <- decodePParams (ufProtocolParameters facts)
    enforcePParamsPolicy policy pp
    stateRow <- decodeUtxo "update.state_utxo" (ufStateUtxo facts)
    requestRows <-
        traverse
            (uncurry decodeRequestUtxo)
            (zip [0 :: Int ..] (ufRequestUtxos facts))
    fundingRows <- decodeWalletUtxos (ufWalletUtxos facts)
    feeRow <- selectFeeRow fundingRows
    oldState <- stateDatum stateRow
    requestDatums <- traverse requestDatum requestRows
    ensureRequestTrieFactCount requestRows (ufTrieFacts facts)
    let Hex oldRoot = ufTrieRoot facts
    newRoot <-
        foldUpdateTrieFacts
            oldRoot
            (zip requestDatums (ufTrieFacts facts))
    proofs <-
        traverse
            (trieFactProofSteps . snd)
            (zip requestDatums (ufTrieFacts facts))
    ownerSigner <-
        ownerWitnessKeyHash
            $ let BuiltinByteString ownerBytes = stateOwner oldState
              in  ownerBytes
    let changeAddr = rowAddress feeRow
        upperSlot =
            SlotNo
                $ fromIntegral
                $ ufValidityUpperSlot facts
    tx <-
        buildUpdateTx
            cfg
            pp
            token
            stateRow
            requestRows
            feeRow
            changeAddr
            oldState
            newRoot
            ownerSigner
            proofs
            upperSlot
    enforceTxPolicy policy tx
    pure tx

-- | Fold request datums and their MPF fact bytes from an old root
-- to the root produced by applying the request operations.
foldUpdateTrieFacts
    :: ByteString
    -> [(OnChainRequest, TrieFact)]
    -> Either BuildError ByteString
foldUpdateTrieFacts root =
    foldl' step (Right root)
  where
    step acc pair = do
        currentRoot <- acc
        foldOneUpdateTrieFact currentRoot pair

foldOneUpdateTrieFact
    :: ByteString
    -> (OnChainRequest, TrieFact)
    -> Either BuildError ByteString
foldOneUpdateTrieFact oldRoot (request, fact) = do
    steps <- parseAikenProofBytes (unHex $ tfMpfProof fact)
    let key = requestKey request
    checkTrieFactBinding request fact
    case requestValue request of
        OpInsert value -> do
            checkFoldedRoot oldRoot
                =<< computeProofRoot False key Nothing steps
            computeProofRoot True key (Just value) steps
        OpDelete oldValue -> do
            checkFoldedRoot oldRoot
                =<< computeProofRoot True key (Just oldValue) steps
            computeProofRoot False key Nothing steps
        OpUpdate oldValue newValue -> do
            checkFoldedRoot oldRoot
                =<< computeProofRoot True key (Just oldValue) steps
            computeProofRoot True key (Just newValue) steps

checkFoldedRoot :: ByteString -> ByteString -> Either BuildError ()
checkFoldedRoot expected actual
    | actual == expected = Right ()
    | otherwise =
        Left
            $ MalformedTxOut
                "update.trie_facts[].mpf_proof root mismatch"

checkTrieFactBinding
    :: OnChainRequest -> TrieFact -> Either BuildError ()
checkTrieFactBinding request TrieFact{tfKey = Hex key, tfValue} = do
    when (key /= requestKey request)
        $ Left
        $ MalformedTxOut "update.trie_facts[].key does not match request"
    case requestValue request of
        OpInsert{} ->
            when (isJust tfValue)
                $ Left
                $ MalformedTxOut
                    "update.trie_facts[].value must be null for insert"
        OpDelete oldValue ->
            expectOldValue oldValue
        OpUpdate oldValue _ ->
            expectOldValue oldValue
  where
    expectOldValue oldValue =
        case tfValue of
            Just (Hex factValue)
                | factValue == oldValue -> Right ()
            _ ->
                Left
                    $ MalformedTxOut
                        "update.trie_facts[].value does not match request"

data InputRow = InputRow
    { rowRef :: !TxIn
    , rowOut :: !(TxOut ConwayEra)
    }

rowAddress :: InputRow -> Addr
rowAddress row =
    rowOut row ^. addrTxOutL

ensureRequestTrieFactCount
    :: [InputRow] -> [TrieFact] -> Either BuildError ()
ensureRequestTrieFactCount requestRows trieFacts
    | length requestRows == length trieFacts = Right ()
    | otherwise =
        Left
            $ MalformedTxOut
                "update.trie_facts length must match \
                \update.request_utxos"

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

decodeRequestUtxo :: Int -> UtxoEntry -> Either BuildError InputRow
decodeRequestUtxo ix =
    decodeUtxo
        ("update.request_utxos[" <> T.pack (show ix) <> "]")

decodeWalletUtxos
    :: [UtxoEntry] -> Either BuildError [InputRow]
decodeWalletUtxos =
    traverse (uncurry decodeWalletUtxo) . zip [0 :: Int ..]

decodeWalletUtxo
    :: Int -> UtxoEntry -> Either BuildError InputRow
decodeWalletUtxo ix =
    decodeUtxo
        ("update.wallet_utxos[" <> T.pack (show ix) <> "]")

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
        Just (StateDatum state) ->
            Right state
        _ ->
            Left
                $ MalformedTxOut
                    "update.state_utxo.tx_out_cbor missing state datum"

requestDatum :: InputRow -> Either BuildError OnChainRequest
requestDatum row =
    case extractCageDatum (rowOut row) of
        Just (RequestDatum request) ->
            Right request
        _ ->
            Left
                $ MalformedTxOut
                    "update.request_utxos[].tx_out_cbor missing \
                    \request datum"

extractCageDatum :: TxOut ConwayEra -> Maybe CageDatum
extractCageDatum txOut =
    case txOut ^. datumTxOutL of
        Datum bd ->
            let Data plcData =
                    binaryDataToData bd
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
                    "update.state_utxo.datum.owner must be 28 bytes"

buildUpdateTx
    :: CageConfig
    -> PParams ConwayEra
    -> TokenId
    -> InputRow
    -> [InputRow]
    -> InputRow
    -> Addr
    -> OnChainTokenState
    -> ByteString
    -> KeyHash Witness
    -> [[ProofStep]]
    -> SlotNo
    -> Either BuildError ConwayTx
buildUpdateTx
    cfg
    pp
    token
    stateRow
    requestRows
    feeRow
    changeAddr
    oldState
    newRoot
    ownerSigner
    proofs
    upperSlot =
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
            case balanceTx pp ledgerPairs [] changeAddr draft of
                Left err ->
                    Left (DSLBuildFailed $ T.pack $ show err)
                Right BalanceResult{balancedTx} ->
                    Right
                        $ patchUpdateRedeemers
                            pp
                            budget
                            stateRef
                            (map fst requestRefs)
                            proofs
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
                        (Modify $ map Update proofs)
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
                TxBuild.validTo upperSlot
            outputs =
                newStateOut : refundOutputs feeForRefunds

        stateRef = rowRef stateRow
        feeRef = rowRef feeRow
        allRows = stateRow : requestRows <> [feeRow]
        requestRefs =
            [(rowRef row, rowOut row) | row <- requestRows]
        ledgerPairs =
            [ (rowRef row, rowOut row)
            | row <- allRows
            ]
        stateScript = mkCageScript cfg
        requestScript = mkRequestScript cfg token
        budget = updateRedeemerBudget pp
        newStateOut =
            mkBasicTxOut
                (cageAddrFromCfg cfg (network cfg))
                (rowOut stateRow ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum
                        ( StateDatum
                            oldState
                                { stateRoot = OnChainRoot newRoot
                                }
                        )
        refundOutputs (Coin fee) =
            let nReqs = fromIntegral (length requestRows) :: Integer
                perReqFee = fee `div` nReqs
                remainder = fee - perReqFee * nReqs
                OnChainTokenState{stateMaxFee = tipAmount} = oldState
            in  [ mkBasicTxOut
                    (refundAddress (network cfg) request)
                    (inject refundCoin)
                | (ix, row) <- zip [0 :: Int ..] requestRows
                , let request = unsafeRequestDatum row
                      Coin reqValue = rowOut row ^. coinTxOutL
                      extra =
                        if ix == 0
                            then remainder
                            else 0
                      refundCoin =
                        Coin (reqValue - tipAmount - perReqFee - extra)
                ]

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

patchUpdateRedeemers
    :: PParams ConwayEra
    -> ExUnits
    -> TxIn
    -> [TxIn]
    -> [[ProofStep]]
    -> ConwayTx
    -> ConwayTx
patchUpdateRedeemers pp budget stateRef requestRefs proofs tx =
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
                (Modify $ map Update proofs)
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
        Nothing -> error "updateCageTx: invalid request owner hash"

unsafeRequestDatum :: InputRow -> OnChainRequest
unsafeRequestDatum row =
    case requestDatum row of
        Right request -> request
        Left _ -> error "updateCageTx: request datum disappeared"

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
                error "updateCageTx: invalid PlutusV3 request script"

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

updateRedeemerBudget :: PParams ConwayEra -> ExUnits
updateRedeemerBudget pp =
    capExUnits evalBudgetExUnits
        $ halfExUnits
        $ pp ^. ppMaxTxExUnitsL

capExUnits :: ExUnits -> ExUnits -> ExUnits
capExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    ExUnits (min mem maxMem) (min steps maxSteps)

spendingIndex :: TxIn -> Set.Set TxIn -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "updateCageTx: script input missing from balanced tx"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

halfExUnits :: ExUnits -> ExUnits
halfExUnits (ExUnits mem steps) =
    ExUnits (mem `div` 2) (steps `div` 2)

trieFactProofSteps :: TrieFact -> Either BuildError [ProofStep]
trieFactProofSteps TrieFact{tfMpfProof = Hex proofBytes} =
    map aikenToProofStep <$> parseAikenProofBytes proofBytes

data AikenNeighbor = AikenNeighbor
    { anNibble :: !HexDigit
    , anPrefix :: !HexKey
    , anRoot :: !MPFHash
    }

data AikenProofStep
    = AikenBranch
        { apsSkip :: !Int
        , apsNeighbors :: ![MPFHash]
        }
    | AikenFork
        { apsSkip :: !Int
        , apsNeighbor :: !AikenNeighbor
        }
    | AikenLeaf
        { apsSkip :: !Int
        , apsKey :: !HexKey
        , apsValue :: !MPFHash
        }

aikenToProofStep :: AikenProofStep -> ProofStep
aikenToProofStep = \case
    AikenBranch skip neighbors ->
        Branch (fromIntegral skip) (mconcat $ map renderMPFHash neighbors)
    AikenFork skip AikenNeighbor{anNibble, anPrefix, anRoot} ->
        Fork
            (fromIntegral skip)
            Neighbor
                { neighborNibble =
                    fromIntegral (unHexDigit anNibble)
                , neighborPrefix = BS.pack (map unHexDigit anPrefix)
                , neighborRoot = renderMPFHash anRoot
                }
    AikenLeaf skip key value ->
        Leaf
            (fromIntegral skip)
            (packHexKey key)
            (renderMPFHash value)

computeProofRoot
    :: Bool
    -> ByteString
    -> Maybe ByteString
    -> [AikenProofStep]
    -> Either BuildError ByteString
computeProofRoot including key value proofSteps =
    case folded of
        Just (Just computedRoot) ->
            Right (renderMPFHash computedRoot)
        Just Nothing ->
            Right (renderMPFHash nullHash)
        _ ->
            Left
                $ MalformedTxOut
                    "update.trie_facts[].mpf_proof cannot be folded"
  where
    folded =
        foldAikenProof
            including
            (aikenKeyPath key)
            (mkMPFHash <$> value)
            proofSteps

foldAikenProof
    :: Bool
    -> HexKey
    -> Maybe MPFHash
    -> [AikenProofStep]
    -> Maybe (Maybe MPFHash)
foldAikenProof including path valueDigest =
    go 0
  where
    go cursor [] =
        if including
            then
                Just . leafHash mpfHashing (drop cursor path)
                    <$> valueDigest
            else pure Nothing
    go cursor (proofStep : rest) = do
        let skip = apsSkip proofStep
            nibbleIx = cursor + skip
            isLast = null rest
        prefix <- sliceHex cursor nibbleIx path
        acc <- go (nibbleIx + 1) rest
        case proofStep of
            AikenBranch{apsNeighbors} -> do
                ourNibble <- indexHex nibbleIx path
                let merkle =
                        rebuildMerkleRoot
                            (hexDigitToInt ourNibble)
                            acc
                            apsNeighbors
                pure
                    $ Just
                    $ branchHash mpfHashing prefix merkle
            AikenFork{apsNeighbor = AikenNeighbor{anNibble, anPrefix, anRoot}}
                | not including && isLast ->
                    pure
                        $ Just
                        $ branchHash
                            mpfHashing
                            (prefix <> [anNibble] <> anPrefix)
                            anRoot
                | otherwise -> do
                    ourNibble <- indexHex nibbleIx path
                    guard (ourNibble /= anNibble)
                    let neighborHash =
                            branchHash mpfHashing anPrefix anRoot
                        sparseChildren =
                            [ if HexDigit n == ourNibble
                                then acc
                                else
                                    if HexDigit n == anNibble
                                        then Just neighborHash
                                        else Nothing
                            | n <- [0 .. 15]
                            ]
                    pure
                        $ Just
                        $ branchHash
                            mpfHashing
                            prefix
                            (merkleRoot mpfHashing sparseChildren)
            AikenLeaf{apsKey, apsValue} -> do
                witnessPrefix <- sliceHex 0 cursor apsKey
                guard (witnessPrefix == take cursor path)
                targetNibble <- indexHex nibbleIx path
                neighborNibble <- indexHex nibbleIx apsKey
                guard (neighborNibble /= targetNibble)
                if not including && isLast
                    then
                        pure
                            $ Just
                            $ leafHash
                                mpfHashing
                                (drop cursor apsKey)
                                apsValue
                    else do
                        let neighborSuffix = drop (nibbleIx + 1) apsKey
                            neighborHash =
                                leafHash
                                    mpfHashing
                                    neighborSuffix
                                    apsValue
                            sparseChildren =
                                [ if HexDigit n == targetNibble
                                    then acc
                                    else
                                        if HexDigit n == neighborNibble
                                            then Just neighborHash
                                            else Nothing
                                | n <- [0 .. 15]
                                ]
                        pure
                            $ Just
                            $ branchHash
                                mpfHashing
                                prefix
                                (merkleRoot mpfHashing sparseChildren)

hexDigitToInt :: HexDigit -> Int
hexDigitToInt (HexDigit w) = fromIntegral w

rebuildMerkleRoot
    :: Int -> Maybe MPFHash -> [MPFHash] -> MPFHash
rebuildMerkleRoot position acc =
    foldl' step (fromMaybe nullHash acc) . zip [0 :: Int ..] . reverse
  where
    step current (depth, siblingHash)
        | testBit position depth =
            mkMPFHash (renderMPFHash siblingHash <> renderMPFHash current)
        | otherwise =
            mkMPFHash (renderMPFHash current <> renderMPFHash siblingHash)

sliceHex :: Int -> Int -> HexKey -> Maybe HexKey
sliceHex start end key
    | start < 0 || end < start = Nothing
    | otherwise =
        let prefix = take (end - start) (drop start key)
        in  if length prefix == end - start then Just prefix else Nothing

indexHex :: Int -> HexKey -> Maybe HexDigit
indexHex ix key
    | ix < 0 = Nothing
    | otherwise = case drop ix key of
        d : _ -> Just d
        [] -> Nothing

parseAikenProofBytes
    :: ByteString -> Either BuildError [AikenProofStep]
parseAikenProofBytes proofBytes =
    case parseBytes proofBytes of
        Just (steps, rest)
            | BS.null rest -> Right steps
        _ ->
            Left
                $ MalformedTxOut
                    "update.trie_facts[].mpf_proof malformed CBOR"

type Parser a = ByteString -> Maybe (a, ByteString)

parseByte :: Parser Word8
parseByte bs = case BS.uncons bs of
    Just (w, rest) -> Just (w, rest)
    Nothing -> Nothing

expectByte :: Word8 -> Parser ()
expectByte expected bs = case parseByte bs of
    Just (w, rest) | w == expected -> Just ((), rest)
    _ -> Nothing

parseUInt :: Parser Int
parseUInt bs = case parseByte bs of
    Just (w, rest)
        | w < 24 -> Just (fromIntegral w, rest)
        | w == 0x18 -> case parseByte rest of
            Just (v, rest') -> Just (fromIntegral v, rest')
            Nothing -> Nothing
        | w == 0x19 -> do
            (bytes, rest') <- takeN 2 rest
            let hi = BS.index bytes 0
                lo = BS.index bytes 1
            Just (fromIntegral hi * 256 + fromIntegral lo, rest')
    _ -> Nothing

parseDefBytes :: Parser ByteString
parseDefBytes bs = case parseByte bs of
    Just (w, rest)
        | w >= 0x40 && w <= 0x57 ->
            takeN (fromIntegral (w - 0x40)) rest
        | w == 0x58 -> case parseByte rest of
            Just (len, rest') -> takeN (fromIntegral len) rest'
            Nothing -> Nothing
        | w == 0x59 -> do
            (bytes, rest') <- takeN 2 rest
            let hi = BS.index bytes 0
                lo = BS.index bytes 1
                len = fromIntegral hi * 256 + fromIntegral lo
            takeN len rest'
    _ -> Nothing

parseIndefBytes :: Parser ByteString
parseIndefBytes bs = case expectByte 0x5f bs of
    Just ((), rest) -> collectChunks [] rest
    Nothing -> Nothing
  where
    collectChunks acc bs' = case parseByte bs' of
        Just (0xff, rest) -> Just (BS.concat (reverse acc), rest)
        _ -> do
            (chunk, rest) <- parseDefBytes bs'
            collectChunks (chunk : acc) rest

parseCBORBytes :: Parser ByteString
parseCBORBytes bs = case parseDefBytes bs of
    Just out -> Just out
    Nothing -> parseIndefBytes bs

parseTag :: Parser Int
parseTag bs = case parseByte bs of
    Just (0xd8, rest) -> case parseByte rest of
        Just (v, rest') -> Just (fromIntegral v, rest')
        Nothing -> Nothing
    Just (0xd9, rest) -> do
        (bytes, rest') <- takeN 2 rest
        let hi = BS.index bytes 0
            lo = BS.index bytes 1
        Just (fromIntegral hi * 256 + fromIntegral lo, rest')
    _ -> Nothing

parseListBegin :: Parser ()
parseListBegin = expectByte 0x9f

parseBreak :: Parser ()
parseBreak = expectByte 0xff

takeN :: Int -> Parser ByteString
takeN n bs
    | BS.length bs >= n = Just (BS.take n bs, BS.drop n bs)
    | otherwise = Nothing

parseBranchStep :: Parser AikenProofStep
parseBranchStep bs = do
    (skip, bs1) <- parseUInt bs
    (neighborsBS, bs2) <- parseCBORBytes bs1
    ((), bs3) <- parseBreak bs2
    guard (BS.length neighborsBS == 128)
    let proofHashes = splitHashes neighborsBS
    pure (AikenBranch skip proofHashes, bs3)

parseForkStep :: Parser AikenProofStep
parseForkStep bs = do
    (skip, bs1) <- parseUInt bs
    (tag, bs2) <- parseTag bs1
    when (tag /= 121) Nothing
    ((), bs3) <- parseListBegin bs2
    (nibble, bs4) <- parseUInt bs3
    (prefixBS, bs5) <- parseCBORBytes bs4
    (rootBS, bs6) <- parseCBORBytes bs5
    ((), bs7) <- parseBreak bs6
    ((), bs8) <- parseBreak bs7
    guard (nibble >= 0 && nibble < 16)
    neighborPrefix <- unpackNibblePrefix prefixBS
    neighborRoot <- MPFHash <$> takeExact 32 rootBS
    pure
        ( AikenFork
            { apsSkip = skip
            , apsNeighbor =
                AikenNeighbor
                    { anNibble = HexDigit (fromIntegral nibble)
                    , anPrefix = neighborPrefix
                    , anRoot = neighborRoot
                    }
            }
        , bs8
        )

parseLeafStep :: Parser AikenProofStep
parseLeafStep bs = do
    (skip, bs1) <- parseUInt bs
    (keyBS, bs2) <- parseCBORBytes bs1
    (valueBS, bs3) <- parseCBORBytes bs2
    ((), bs4) <- parseBreak bs3
    keyBytes <- takeExact 32 keyBS
    let keyPath = byteStringToHexKey' keyBytes
    valueHash <- MPFHash <$> takeExact 32 valueBS
    pure
        ( AikenLeaf
            { apsSkip = skip
            , apsKey = keyPath
            , apsValue = valueHash
            }
        , bs4
        )

parseStep :: Parser AikenProofStep
parseStep bs = do
    (tag, bs1) <- parseTag bs
    ((), bs2) <- parseListBegin bs1
    case tag of
        121 -> parseBranchStep bs2
        122 -> parseForkStep bs2
        123 -> parseLeafStep bs2
        _ -> Nothing

parseBytes :: Parser [AikenProofStep]
parseBytes bs = do
    ((), bs1) <- parseListBegin bs
    collectSteps [] bs1
  where
    collectSteps acc bs' = case parseByte bs' of
        Just (0xff, rest) -> Just (reverse acc, rest)
        _ -> do
            (step, rest) <- parseStep bs'
            collectSteps (step : acc) rest

splitHashes :: ByteString -> [MPFHash]
splitHashes bs =
    [ MPFHash (BS.take 32 bs)
    , MPFHash (BS.take 32 (BS.drop 32 bs))
    , MPFHash (BS.take 32 (BS.drop 64 bs))
    , MPFHash (BS.take 32 (BS.drop 96 bs))
    ]

takeExact :: Int -> ByteString -> Maybe ByteString
takeExact n bs
    | BS.length bs == n = Just bs
    | otherwise = Nothing

unpackNibblePrefix :: ByteString -> Maybe HexKey
unpackNibblePrefix =
    mapM toNibble . BS.unpack
  where
    toNibble w
        | w < 16 = Just (HexDigit w)
        | otherwise = Nothing

packHexKey :: HexKey -> ByteString
packHexKey = BS.pack . pairNibbles
  where
    pairNibbles [] = []
    pairNibbles [HexDigit lo] = [lo]
    pairNibbles (HexDigit hi : HexDigit lo : rest) =
        (hi * 16 + lo) : pairNibbles rest

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
