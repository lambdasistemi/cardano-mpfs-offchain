{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}

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
    ( updateTokenImpl
    ) where

import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.Foldable (fold)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Ord (Down (..))
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.POSIX
    ( utcTimeToPOSIXSeconds
    )
import Data.Void (Void)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Alonzo.Scripts (AsIx)
import Cardano.Ledger.Alonzo.TxWits (Redeemers (..))
import Cardano.Ledger.Api.PParams (ppProtocolVersionL)
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , estimateMinFeeTx
    , sizeTxF
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , inputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits (rdmrsTxWitsL)
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , pvMajor
    )
import Cardano.Ledger.Binary (serialize)
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose
    )
import Cardano.Ledger.Core (Script, getMinFeeTx)
import Cardano.Ledger.Keys
    ( KeyHash
    , KeyRole (..)
    )
import Cardano.Ledger.Plutus.ExUnits (ExUnits)
import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.Set qualified as Set
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
    ( BundleSnapshot
    , ProofEnvelope (..)
    , TrieFact (..)
    , UpdateProof (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Tx.Build qualified as Tx
import Cardano.Slotting.Slot (SlotNo)

-- | Empty query GADT (no context needed).
data NoCtx a

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
    (stateUtxo, reqUtxos, feeUtxo, pp) <-
        queryContext cfg prov tid addr
    let (stateIn, stateOut) = stateUtxo
        oldRootBytes =
            case extractCageDatum stateOut of
                Just (StateDatum s) ->
                    unOnChainRoot (stateRoot s)
                _ ->
                    error
                        "updateToken: invalid \
                        \state datum (root)"
    -- 2. Compute proofs via speculative trie
    (trieReads, newRoot) <-
        computeProofs tm tid reqUtxos
    -- 3. Extract state and build new datum
    let ( oldState
            , newStateOut
            , stateScript
            , requestScript
            , ownerKh
            ) =
                prepareState
                    cfg
                    tid
                    stateOut
                    newRoot
    -- 4. Compute validity upper slot
    upperSlot <-
        computeUpperSlot prov oldState reqUtxos
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
            pp
            (Tx.InterpretIO (const (pure undefined)))
            evalTx
            (feeUtxo : stateUtxo : reqUtxos)
            addr
            (prog :: Tx.TxBuild NoCtx Void ())
    case result of
        Right tx -> do
            stateWitness <- witness proofFn stateUtxo
            reqWitnesses <- witnesses proofFn reqUtxos
            fundingWitnesses <-
                witnesses proofFn [feeUtxo]
            let Coin fee =
                    tx ^. bodyTxL . feeTxBodyL
                unsignedSize =
                    tx ^. sizeTxF
                estFee =
                    estimateMinFeeTx pp tx 1 0 0
                getMin =
                    getMinFeeTx pp tx 0
            -- Also dump what estimateMinFeeTx
            -- sees inside balanceTx: the tx
            -- that was passed to balanceTx is
            -- the BUILD-RESULT tx. If it has
            -- ExUnits 0, patchExUnits failed.
            let Redeemers rdmrs =
                    tx ^. witsTxL . rdmrsTxWitsL
                rdmrEUs =
                    [ (show p, show eu)
                    | (p, (_, eu)) <-
                        Map.toList rdmrs
                    ]
            appendFile "/tmp/mpfs-dsl.log"
                $ "RESULT-EUS: "
                    <> show rdmrEUs
                    <> "\n"
            let ver = pvMajor (pp ^. ppProtocolVersionL)
                txHex =
                    B16.encode
                        ( BSL.toStrict
                            (serialize ver tx)
                        )
            BS.writeFile
                "/tmp/mpfs-unsigned.cbor.hex"
                txHex
            appendFile "/tmp/mpfs-dsl.log"
                $ "BUILD: fee="
                    <> show fee
                    <> " unsignedSize="
                    <> show unsignedSize
                    <> " estimateMinFee(1)="
                    <> show estFee
                    <> " getMinFee(0)="
                    <> show getMin
                    <> "\n"
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
            error
                $ "updateToken: build failed: "
                    <> show err

-- | Query cage UTxOs, find the state and request
-- UTxOs, pick a fee-paying wallet UTxO.
queryContext
    :: CageConfig
    -> Provider IO
    -> TokenId
    -> Addr
    -> IO
        ( (TxIn, TxOut ConwayEra)
        , [(TxIn, TxOut ConwayEra)]
        , (TxIn, TxOut ConwayEra)
        , PParams ConwayEra
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
    stateUtxo <- case findStateUtxo
        policyId
        tid
        stateUtxos of
        Nothing ->
            error
                "updateToken: state UTxO \
                \not found"
        Just x -> pure x
    let reqUtxos =
            sortOn fst
                $ findRequestUtxos tid requestUtxos
    when (null reqUtxos)
        $ error "updateToken: no pending requests"
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "updateToken: no UTxOs"
        (u : _) -> pure u
    pure (stateUtxo, reqUtxos, feeUtxo, pp)

-- | Run speculative trie operations to compute
-- proofs and the new root hash.
computeProofs
    :: TrieManager IO
    -> TokenId
    -> [(TxIn, TxOut ConwayEra)]
    -> IO ([RequestTrieRead], Root)
computeProofs tm tid reqUtxos =
    withSpeculativeTrie tm tid $ \trie -> do
        ps <- mapM (processRequest trie) reqUtxos
        r <- getRoot trie
        pure (ps, r)

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
    -> ( OnChainTokenState
       , TxOut ConwayEra
       , Script ConwayEra
       , Script ConwayEra
       , KeyHash 'Witness
       )
prepareState cfg tid stateOut newRoot =
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
        oldState =
            case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "updateToken: invalid \
                        \state datum"
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
    in  ( oldState
        , newStateOut
        , stateScript
        , requestScript
        , ownerKh
        )

-- | Compute the validity upper slot from the
-- earliest request deadline. Falls back to
-- now + 30s/5s/2s if the slot is past the
-- Ouroboros forecast horizon.
computeUpperSlot
    :: Provider IO
    -> OnChainTokenState
    -> [(TxIn, TxOut ConwayEra)]
    -> IO SlotNo
computeUpperSlot prov oldState reqUtxos = do
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
        Left _ -> do
            nowUtc <- getCurrentTime
            let posixSec =
                    utcTimeToPOSIXSeconds nowUtc
            trySlots prov
                $ map
                    ( \d ->
                        round
                            ((posixSec + d) * 1000)
                    )
                    [30, 5, 2]

-- | Wrap the Provider's evaluateTx for the DSL.
mkEvalTx
    :: Provider IO
    -> Tx ConwayEra
    -> IO
        ( Map.Map
            ( ConwayPlutusPurpose
                AsIx
                ConwayEra
            )
            (Either String ExUnits)
        )
mkEvalTx prov tx = do
    let ins = tx ^. bodyTxL . inputsTxBodyL
    r <- evaluateTx prov tx
    appendFile "/tmp/mpfs-dsl.log"
        $ "EVAL: ins="
            <> show (Set.size ins)
            <> " sorted="
            <> show (Set.toAscList ins)
            <> " result="
            <> show (Map.keys r)
            <> "\n"
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
    -> KeyHash 'Witness
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
        Tx.requireSignature ownerKh
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
    -> m RequestTrieRead
processRequest trie (_txIn, txOut) = do
    let OnChainRequest
            { requestKey = key
            , requestValue = op
            } = case extractCageDatum txOut of
                Just (RequestDatum r) -> r
                _ ->
                    error
                        "processRequest: \
                        \invalid request datum"
    case op of
        OpInsert v -> do
            _ <- insert trie key v
            mSteps <- getProofSteps trie key
            pure
                ( mkRequestTrieRead
                    key
                    Nothing
                    (fromMaybe [] mSteps)
                )
        OpDelete _ -> do
            mSteps <- getProofSteps trie key
            _ <- Cardano.MPFS.Trie.delete trie key
            pure
                ( mkRequestTrieRead
                    key
                    (Just (operationOldValue op))
                    (fromMaybe [] mSteps)
                )
        OpUpdate _ v -> do
            mSteps <- getProofSteps trie key
            _ <- Cardano.MPFS.Trie.delete trie key
            _ <- insert trie key v
            pure
                ( mkRequestTrieRead
                    key
                    (Just (operationOldValue op))
                    (fromMaybe [] mSteps)
                )

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

operationOldValue :: OnChainOperation -> BS.ByteString
operationOldValue = \case
    OpInsert v -> v
    OpDelete v -> v
    OpUpdate old _new -> old

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
