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
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , ProofStep
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
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
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Node.Client.TxBuild qualified as Tx

-- | Empty query GADT (no context needed).
data NoCtx a

-- | Build an update-token transaction (fair fee).
--
-- Uses the TxBuild DSL for convergent fee/refund
-- balancing. Conservation equation:
-- sum(refunds) = sum(inputs) - fee - N * tip
updateTokenImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> TrieManager IO
    -> TokenId
    -> Addr
    -> IO (Tx ConwayEra)
updateTokenImpl cfg prov _st tm tid addr = do
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    let policyId = cagePolicyIdFromCfg cfg
    stateUtxo <- case findStateUtxo
        policyId
        tid
        cageUtxos of
        Nothing ->
            error
                "updateToken: state UTxO \
                \not found"
        Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
        reqUtxos =
            sortOn fst
                $ findRequestUtxos tid cageUtxos
    when (null reqUtxos)
        $ error "updateToken: no pending requests"
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "updateToken: no UTxOs"
        (u : _) -> pure u
    -- Compute proofs
    (proofs, newRoot) <-
        withSpeculativeTrie tm tid $ \trie -> do
            ps <-
                mapM
                    (processRequest trie)
                    reqUtxos
            r <- getRoot trie
            pure (ps, r)
    let oldState =
            case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "updateToken: invalid \
                        \state datum"
        OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            , stateTip = tipAmount
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
        stateRef = txInToRef stateIn
        script = mkCageScript cfg
        ownerKh = addrWitnessKeyHash ownerBs
        nReqs =
            fromIntegral (length reqUtxos)
                :: Integer
    -- Validity interval
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
    upperSlot <- case mUpperSlot of
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
    -- Build with DSL
    let evalTx tx = do
            r <- evaluateTx prov tx
            pure
                $ Map.map
                    ( \case
                        Left e -> Left (show e)
                        Right eu -> Right eu
                    )
                    r
        prog = do
            -- Spend state UTxO
            _ <-
                Tx.spendScript
                    stateIn
                    (Modify proofs)
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
            mapM_
                ( \(_, reqOut) -> do
                    let Coin reqVal =
                            reqOut ^. coinTxOutL
                        rawRefund =
                            Coin
                                ( reqVal
                                    - tipAmount
                                    - perReqFee
                                )
                        refundAddr =
                            addrFromKeyHashBytes
                                (network cfg)
                                ( extractOwnerBytes
                                    reqOut
                                )
                        draft =
                            mkBasicTxOut
                                refundAddr
                                (inject rawRefund)
                        minCoin =
                            getMinCoinTxOut
                                pp
                                draft
                    Tx.output
                        $ mkBasicTxOut
                            refundAddr
                            ( inject
                                ( max
                                    rawRefund
                                    minCoin
                                )
                            )
                )
                reqUtxos
            -- Constraints
            Tx.attachScript script
            Tx.requireSignature ownerKh
            Tx.collateral (fst feeUtxo)
            Tx.validTo upperSlot
    result <-
        Tx.build
            pp
            (Tx.InterpretIO (const (pure undefined)))
            evalTx
            (feeUtxo : stateUtxo : reqUtxos)
            addr
            (prog :: Tx.TxBuild NoCtx Void ())
    case result of
        Right tx -> pure tx
        Left err ->
            error
                $ "updateToken: build failed: "
                    <> show err

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
    -> m [ProofStep]
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
            pure (fromMaybe [] mSteps)
        OpDelete _ -> do
            mSteps <- getProofSteps trie key
            _ <- Cardano.MPFS.Trie.delete trie key
            pure (fromMaybe [] mSteps)
        OpUpdate _ v -> do
            mSteps <- getProofSteps trie key
            _ <- Cardano.MPFS.Trie.delete trie key
            _ <- insert trie key v
            pure (fromMaybe [] mSteps)
