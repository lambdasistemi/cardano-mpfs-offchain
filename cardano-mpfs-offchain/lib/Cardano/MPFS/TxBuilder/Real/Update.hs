{-# LANGUAGE DataKinds #-}
{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Update
-- Description : Update token transaction
-- License     : Apache-2.0
--
-- Builds the oracle update transaction that processes
-- all pending requests for a token. Consumes the State
-- UTxO and all request UTxOs, applies each operation
-- speculatively through the trie to generate proofs,
-- then outputs a new State UTxO with the updated root
-- and per-request refund outputs.
module Cardano.MPFS.TxBuilder.Real.Update
    ( RequestTrieRead (..)
    , UpdateBuildError (..)
    , UpdateBuildException (..)
    , computeProofs
    , computeUpperSlot
    , updateTokenImpl
    ) where

import Control.Exception (Exception, SomeException, throwIO, try)
import Data.Foldable (fold)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Void (Void)
import Data.Word (Word64)
import Lens.Micro ((&), (.~), (^.))
import System.Environment (lookupEnv)
import Text.Read (readMaybe)

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Alonzo.Scripts (AsIx)
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose
    )
import Cardano.Ledger.Core (Script)
import Cardano.Ledger.Keys
    ( KeyHash
    , KeyRole (..)
    )
import Cardano.Ledger.Plutus.ExUnits (ExUnits)
import Cardano.Tx.Ledger (ConwayTx)
import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , Neighbor (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , ProofStep (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , PParams
    , Root (..)
    , TokenId
    , TxIn
    )
import Cardano.MPFS.Provider
    ( Provider (..)
    )
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , TrieFact (..)
    , UpdateProof (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Slotting.Slot (SlotNo (..))
import Cardano.Tx.Build qualified as Tx

-- | Empty query GADT (no context needed).
data NoCtx a

-- | Typed update-construction failure.
newtype UpdateBuildError = UpdateBuildError
    { updateBuildErrorMessage :: Text
    }
    deriving (Eq, Show)

newtype UpdateBuildException = UpdateBuildException UpdateBuildError
    deriving (Show)

instance Exception UpdateBuildException

-- | Build an update-token transaction (fair fee).
--
-- Uses the TxBuild DSL for convergent fee/refund
-- balancing. Conservation equation:
-- @sum(refunds) = sum(inputs) - fee - N * tip@
updateTokenImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> TrieManager IO
    -> UtxoProofFn
    -> BundleSnapshot
    -> TokenId
    -> Addr
    -> IO (ProofEnvelope UpdateProof)
updateTokenImpl cfg prov _st tm proofFn snap tid addr = do
    -- 1. Query on-chain context
    eCtx <- queryContext cfg prov tid addr
    (stateUtxo, reqUtxos, feeUtxo, pp) <-
        either throwUpdateBuildError pure eCtx
    let (stateIn, stateOut) = stateUtxo
    oldRootBytes <-
        either throwUpdateBuildError pure
            $ stateRootBytes
                "updateToken.state_utxo(root)"
                stateOut
    -- 2. Compute proofs via speculative trie
    eProofs <-
        computeProofs tm tid reqUtxos
    (trieReads, newRoot) <-
        either throwUpdateBuildError pure eProofs
    -- 3. Extract state and build new datum
    ( oldState
        , newStateOut
        , stateScript
        , requestScript
        , ownerKh
        ) <-
        either throwUpdateBuildError pure
            $ prepareState
                cfg
                tid
                stateOut
                newRoot
    -- 4. Compute validity upper slot
    upperSlot <-
        computeUpperSlot prov (snapshotSlot snap) oldState reqUtxos
    -- 5. Build DSL program and execute
    let evalTx = mkEvalTx prov
        prog =
            buildProgram
                cfg
                pp
                stateIn
                stateOut
                reqUtxos
                feeUtxo
                oldState
                newStateOut
                stateScript
                requestScript
                ownerKh
                (map readProofSteps trieReads)
                upperSlot
    result <-
        Tx.build
            (Tx.mkPParamsBound pp)
            (Tx.InterpretIO $ \case {})
            evalTx
            (feeUtxo : stateUtxo : reqUtxos)
            []
            addr
            (prog :: Tx.TxBuild NoCtx Void ())
    case result of
        Right tx -> do
            stateWitness <- witness proofFn stateUtxo
            reqWitnesses <- witnesses proofFn reqUtxos
            fundingWitnesses <-
                witnesses proofFn [feeUtxo]
            pure
                ProofEnvelope
                    { envTx = tx
                    , envSnapshot = snap
                    , envProof =
                        UpdateProof
                            { updateState = stateWitness
                            , updateRequests = reqWitnesses
                            , updateFunding = fundingWitnesses
                            , updateTrieRoot =
                                oldRootBytes
                            , updateTrieRead =
                                map readTrieFact trieReads
                            }
                    }
        Left err ->
            throwUpdateBuildError
                $ updateBuildError
                $ "updateToken: build failed: "
                    <> T.pack (show err)

-- | Query cage UTxOs, find the state and request
-- UTxOs, pick a fee-paying wallet UTxO.
queryContext
    :: CageConfig
    -> Provider IO
    -> TokenId
    -> Addr
    -> IO
        ( Either
            UpdateBuildError
            ( (TxIn, TxOut ConwayEra)
            , [(TxIn, TxOut ConwayEra)]
            , (TxIn, TxOut ConwayEra)
            , PParams ConwayEra
            )
        )
queryContext cfg prov tid addr = do
    let stateAddr =
            cageAddrFromCfg cfg (network cfg)
        reqAddr =
            requestAddrFromCfg
                cfg
                tid
                (network cfg)
    stateUtxos <- queryUTxOs prov stateAddr
    requestUtxos <- queryUTxOs prov reqAddr
    let policyId = cagePolicyIdFromCfg cfg
    let mStateUtxo =
            findStateUtxo
                policyId
                tid
                stateUtxos
    let reqUtxos =
            sortOn fst
                $ findRequestUtxos tid requestUtxos
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    let mFeeUtxo =
            case sortOn
                (Down . (^. coinTxOutL) . snd)
                walletUtxos of
                [] -> Nothing
                u : _ -> Just u
    pure $ do
        stateUtxo <- case mStateUtxo of
            Nothing ->
                Left
                    $ updateBuildError
                        "updateToken: state UTxO not found"
            Just x -> Right x
        if null reqUtxos
            then
                Left
                    $ updateBuildError
                        "updateToken: no pending requests"
            else Right ()
        feeUtxo <- case mFeeUtxo of
            Nothing ->
                Left
                    $ updateBuildError
                        "updateToken: no wallet UTxOs"
            Just u -> Right u
        pure (stateUtxo, reqUtxos, feeUtxo, pp)

-- | Run speculative trie operations to compute
-- proofs and the new root hash.
computeProofs
    :: TrieManager IO
    -> TokenId
    -> [(TxIn, TxOut ConwayEra)]
    -> IO (Either UpdateBuildError ([RequestTrieRead], Root))
computeProofs tm tid reqUtxos =
    withSpeculativeTrie tm tid $ \trie -> do
        eReads <- sequence <$> mapM (processRequest trie) reqUtxos
        case eReads of
            Left err ->
                pure (Left err)
            Right ps -> do
                r <- getRoot trie
                pure (Right (ps, r))

data RequestTrieRead = RequestTrieRead
    { readProofSteps :: [ProofStep]
    , readTrieFact :: TrieFact
    }

-- | Extract old state, build new state output,
-- cage script, and owner key hash.
prepareState
    :: CageConfig
    -> TokenId
    -> TxOut ConwayEra
    -> Root
    -> Either
        UpdateBuildError
        ( OnChainTokenState
        , TxOut ConwayEra
        , Script ConwayEra
        , Script ConwayEra
        , KeyHash Witness
        )
prepareState cfg tid stateOut newRoot = do
    oldState <- stateDatum "updateToken.state_utxo" stateOut
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
        OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = oldState
        newStateDatum =
            StateDatum
                oldState
                    { stateRoot =
                        OnChainRoot (unRoot newRoot)
                    }
        newStateOut =
            mkBasicTxOut
                scriptAddr
                (stateOut ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum
                        (toPlcData newStateDatum)
        stateScript = mkCageScript cfg
        requestScript = mkRequestScript cfg tid
        ownerKh = addrWitnessKeyHash ownerBs
    pure
        ( oldState
        , newStateOut
        , stateScript
        , requestScript
        , ownerKh
        )

-- | Compute the validity upper slot from the
-- earliest request deadline. If that future-time
-- conversion is past the Ouroboros forecast horizon,
-- fall back to a fixed slot offset above the indexed
-- snapshot slot. The fallback avoids a second
-- POSIX→slot conversion for another future time.
computeUpperSlot
    :: Provider IO
    -> SlotNo
    -> OnChainTokenState
    -> [(TxIn, TxOut ConwayEra)]
    -> IO SlotNo
computeUpperSlot prov snapshotSlotNo oldState reqUtxos = do
    let extractSubmittedAt (_, rOut) =
            case extractCageDatum rOut of
                Just (RequestDatum r) ->
                    requestSubmittedAt r
                _ -> 0
        earliestDeadline =
            minimum
                $ map
                    ( \u ->
                        extractSubmittedAt u
                            + stateProcessTime
                                oldState
                    )
                    reqUtxos
    mUpperSlot <-
        try @SomeException
            (posixMsToSlot prov earliestDeadline)
    case mUpperSlot of
        Right s -> pure s
        Left _ ->
            updateUpperSlotFallback snapshotSlotNo

updateUpperSlotFallback :: SlotNo -> IO SlotNo
updateUpperSlotFallback (SlotNo lower) = do
    ttl <- updateTtlSlotsIO
    pure (SlotNo (lower + ttl))

-- | Slot TTL added to the in-horizon snapshot slot
-- when update deadline conversion is past the
-- forecast horizon. Kept small for devnet's short
-- safe zone and within the client verifier's
-- update validity buffer.
updateTtlSlots :: Word64
updateTtlSlots = 20

-- | Update TTL resolved at request time. The
-- @MPFS_UPDATE_TTL_SLOTS@ environment variable
-- overrides 'updateTtlSlots' when set to an integer
-- in @[1, 600]@.
updateTtlSlotsIO :: IO Word64
updateTtlSlotsIO = do
    mv <- lookupEnv "MPFS_UPDATE_TTL_SLOTS"
    pure $ case mv >>= readMaybe of
        Just n | n >= 1 && n <= 600 -> n
        _ -> updateTtlSlots

-- | Wrap the Provider's evaluateTx for the DSL.
mkEvalTx
    :: Provider IO
    -> ConwayTx
    -> IO
        ( Map.Map
            ( ConwayPlutusPurpose
                AsIx
                ConwayEra
            )
            (Either String ExUnits)
        )
mkEvalTx prov tx = do
    r <- evaluateTx prov tx
    pure
        $ Map.map
            ( \case
                Left e -> Left (show e)
                Right eu -> Right eu
            )
            r

-- | The TxBuild DSL program for an update tx.
--
-- Spends: state UTxO (Modify) + all request UTxOs
-- (Contribute). Outputs: new state + refunds.
-- Peeks at the fee to compute per-request refund.
buildProgram
    :: CageConfig
    -> PParams ConwayEra
    -> TxIn
    -> TxOut ConwayEra
    -> [(TxIn, TxOut ConwayEra)]
    -> (TxIn, TxOut ConwayEra)
    -> OnChainTokenState
    -> TxOut ConwayEra
    -> Script ConwayEra
    -> Script ConwayEra
    -> KeyHash Witness
    -> [[ProofStep]]
    -> SlotNo
    -> Tx.TxBuild NoCtx Void ()
buildProgram
    cfg
    _pp
    stateIn
    _stateOut
    reqUtxos
    feeUtxo
    oldState
    newStateOut
    stateScript
    requestScript
    ownerKh
    proofs
    upperSlot = do
        let stateRef = txInToRef stateIn
            OnChainTokenState
                { stateMaxFee = tipAmount
                } = oldState
            nReqs =
                fromIntegral (length reqUtxos)
                    :: Integer
        -- Spend state UTxO
        let actions = map Update proofs
        _ <- Tx.spendScript stateIn (Modify actions)
        -- Spend request UTxOs
        mapM_
            ( \(rIn, _) ->
                Tx.spendScript
                    rIn
                    (Contribute stateRef)
            )
            reqUtxos
        -- State output (unchanged value)
        _ <- Tx.output newStateOut
        -- Fee-dependent refund outputs
        Coin fee <- Tx.peek $ \tx ->
            let f = tx ^. bodyTxL . feeTxBodyL
            in  if f > Coin 0
                    then Tx.Ok f
                    else Tx.Iterate f
        let perReqFee = fee `div` nReqs
            remainder = fee - perReqFee * nReqs
        mapM_
            ( \(i, (_, reqOut)) -> do
                let Coin reqVal =
                        reqOut ^. coinTxOutL
                    -- First request absorbs the
                    -- integer division remainder
                    -- so conservation holds exactly.
                    extra =
                        if i == (0 :: Int)
                            then remainder
                            else 0
                    rawRefund =
                        Coin
                            ( reqVal
                                - tipAmount
                                - perReqFee
                                - extra
                            )
                    refundAddr =
                        addrFromKeyHashBytes
                            (network cfg)
                            ( extractOwnerBytes
                                reqOut
                            )
                Tx.output
                    $ mkBasicTxOut
                        refundAddr
                        (inject rawRefund)
            )
            (zip [0 ..] reqUtxos)
        -- Constraints — both validators witness this tx
        Tx.attachScript stateScript
        Tx.attachScript requestScript
        Tx.requireSignature (witnessKeyHashToGuard ownerKh)
        Tx.collateral (fst feeUtxo)
        Tx.validTo upperSlot

-- | Process a single request: apply the operation
-- to the trie and get proof steps.
--
-- Proof timing depends on the operation:
--
-- * __Insert__: proof obtained /after/ the insert.
--   The on-chain @mpf.insert@ checks
--   @excluding(key, proof) == old_root@ and computes
--   @including(key, value, proof) == new_root@.
--   A membership proof of the key in the trie /with/
--   the key satisfies both: @excluding@ strips the
--   key to recover the old root, and @including@
--   recomputes the new root.
--
-- * __Delete__: proof obtained /before/ the delete.
--   The on-chain @mpf.delete@ checks
--   @including(key, value, proof) == old_root@.
--   The key must still be in the trie when the proof
--   is generated (inclusion proofs require the key
--   to exist).
--
-- * __Update__: proof obtained /before/ the update.
--   The on-chain @mpf.update@ checks
--   @including(key, old_value, proof) == old_root@
--   and computes @including(key, new_value, proof)@.
--   The key must exist when the proof is generated.
processRequest
    :: Monad m
    => Trie m
    -> (TxIn, TxOut ConwayEra)
    -> m (Either UpdateBuildError RequestTrieRead)
processRequest trie (txIn, txOut) =
    case extractCageDatum txOut of
        Nothing ->
            pure
                $ Left
                $ updateBuildError
                $ "processRequest: invalid request datum \
                  \(input: "
                    <> txInText txIn
                    <> ")"
        Just (StateDatum _) ->
            pure
                $ Left
                $ updateBuildError
                $ "processRequest: invalid request datum; \
                  \found state datum (input: "
                    <> txInText txIn
                    <> ")"
        Just
            ( RequestDatum
                    OnChainRequest
                        { requestKey = key
                        , requestValue = op
                        }
                ) -> do
                case op of
                    OpInsert v -> do
                        _ <- insert trie key v
                        mSteps <- getProofSteps trie key
                        pure $ do
                            steps <-
                                requireProofSteps
                                    "processRequest.insert"
                                    txIn
                                    key
                                    mSteps
                            pure
                                ( mkRequestTrieRead
                                    key
                                    Nothing
                                    steps
                                )
                    OpDelete old -> do
                        eRead <-
                            requireExistingTrieRead
                                trie
                                txIn
                                key
                                old
                        case eRead of
                            Left err -> pure (Left err)
                            Right (current, steps) -> do
                                _ <-
                                    Cardano.MPFS.Trie.delete
                                        trie
                                        key
                                pure
                                    $ Right
                                    $ mkRequestTrieRead
                                        key
                                        (Just current)
                                        steps
                    OpUpdate old v -> do
                        eRead <-
                            requireExistingTrieRead
                                trie
                                txIn
                                key
                                old
                        case eRead of
                            Left err -> pure (Left err)
                            Right (current, steps) -> do
                                _ <-
                                    Cardano.MPFS.Trie.delete
                                        trie
                                        key
                                _ <- insert trie key v
                                pure
                                    $ Right
                                    $ mkRequestTrieRead
                                        key
                                        (Just current)
                                        steps

mkRequestTrieRead
    :: BS.ByteString -> Maybe BS.ByteString -> [ProofStep] -> RequestTrieRead
mkRequestTrieRead key value steps =
    RequestTrieRead
        { readProofSteps = steps
        , readTrieFact =
            TrieFact
                { factKey = key
                , factValue = value
                , factMpfProof = renderProofSteps steps
                }
        }

requireExistingTrieRead
    :: Monad m
    => Trie m
    -> TxIn
    -> BS.ByteString
    -> BS.ByteString
    -> m (Either UpdateBuildError (BS.ByteString, [ProofStep]))
requireExistingTrieRead trie txIn key expected = do
    mSteps <- getProofSteps trie key
    mCurrent <- Cardano.MPFS.Trie.lookup trie key
    pure $ do
        steps <-
            requireProofSteps
                "processRequest.existing"
                txIn
                key
                mSteps
        current <- case mCurrent of
            Nothing ->
                Left
                    $ updateBuildError
                    $ "processRequest: no value in \
                      \TrieRawValues for key "
                        <> strictHex key
                        <> " (input: "
                        <> txInText txIn
                        <> ")"
            Just value -> Right value
        if current == expected
            then Right (current, steps)
            else
                Left
                    $ updateBuildError
                    $ "processRequest: request old value \
                      \mismatch for key "
                        <> strictHex key
                        <> " (input: "
                        <> txInText txIn
                        <> ", request-old: "
                        <> strictHex expected
                        <> ", trie-current: "
                        <> strictHex current
                        <> ")"

requireProofSteps
    :: Text
    -> TxIn
    -> BS.ByteString
    -> Maybe [ProofStep]
    -> Either UpdateBuildError [ProofStep]
requireProofSteps path txIn key =
    maybe
        ( Left
            $ updateBuildError
            $ path
                <> ": no MPF inclusion proof for key "
                <> strictHex key
                <> " (input: "
                <> txInText txIn
                <> ")"
        )
        Right

stateRootBytes
    :: Text -> TxOut ConwayEra -> Either UpdateBuildError BS.ByteString
stateRootBytes path out =
    unOnChainRoot . stateRoot <$> stateDatum path out

stateDatum
    :: Text -> TxOut ConwayEra -> Either UpdateBuildError OnChainTokenState
stateDatum path out =
    case extractCageDatum out of
        Just (StateDatum s) -> Right s
        Just (RequestDatum _) ->
            Left
                $ updateBuildError
                $ path
                    <> ": expected state datum, found \
                       \request datum"
        Nothing ->
            Left
                $ updateBuildError
                $ path
                    <> ": missing or invalid state datum"

throwUpdateBuildError :: UpdateBuildError -> IO a
throwUpdateBuildError =
    throwIO . UpdateBuildException

updateBuildError :: Text -> UpdateBuildError
updateBuildError = UpdateBuildError

txInText :: TxIn -> Text
txInText = T.pack . show

strictHex :: BS.ByteString -> Text
strictHex =
    TE.decodeUtf8 . B16.encode

renderProofSteps :: [ProofStep] -> BS.ByteString
renderProofSteps steps =
    CBOR.toStrictByteString
        $ CBOR.encodeListLenIndef
            <> foldMap encodeProofStep steps
            <> CBOR.encodeBreak

encodeProofStep :: ProofStep -> CBOR.Encoding
encodeProofStep = \case
    Branch skip neighbors ->
        encodeConstr
            0
            [ CBOR.encodeInteger skip
            , encodeIndefBytes (split64 neighbors)
            ]
    Fork skip Neighbor{..} ->
        encodeConstr
            1
            [ CBOR.encodeInteger skip
            , encodeConstr
                0
                [ CBOR.encodeInteger neighborNibble
                , CBOR.encodeBytes neighborPrefix
                , CBOR.encodeBytes neighborRoot
                ]
            ]
    Leaf skip key value ->
        encodeConstr
            2
            [ CBOR.encodeInteger skip
            , CBOR.encodeBytes key
            , CBOR.encodeBytes value
            ]

encodeConstr :: Word -> [CBOR.Encoding] -> CBOR.Encoding
encodeConstr tag fields =
    CBOR.encodeTag (121 + tag)
        <> CBOR.encodeListLenIndef
        <> fold fields
        <> CBOR.encodeBreak

encodeIndefBytes :: [BS.ByteString] -> CBOR.Encoding
encodeIndefBytes chunks =
    CBOR.encodeBytesIndef
        <> foldMap CBOR.encodeBytes chunks
        <> CBOR.encodeBreak

split64 :: BS.ByteString -> [BS.ByteString]
split64 bs
    | BS.null bs = []
    | otherwise =
        let (chunk, rest) = BS.splitAt 64 bs
        in  chunk : split64 rest
