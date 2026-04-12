{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Reject
-- Description : Reject transaction for Phase 3 requests
-- License     : Apache-2.0
--
-- Builds a reject transaction that consumes expired
-- (Phase 3) requests. The oracle keeps the tip and
-- refunds remaining ADA to request owners. The trie
-- root does NOT change.
--
-- Phase 3 starts after @submitted_at + process_time
-- + retract_time@. The validity interval has a lower
-- bound (validFrom) to prove we are past the deadline.
module Cardano.MPFS.TxBuilder.Real.Reject
    ( rejectRequestsImpl
    ) where

import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.POSIX
    ( utcTimeToPOSIXSeconds
    )
import Data.Void (Void)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Alonzo.Scripts (AsIx)
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
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose
    )
import Cardano.Ledger.Core (Script)
import Cardano.Ledger.Keys
    ( KeyHash
    , KeyRole (..)
    )
import Cardano.Ledger.Plutus.ExUnits (ExUnits)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , PParams
    , TokenId
    , TxIn
    )
import Cardano.MPFS.Provider
    ( Provider (..)
    )
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Node.Client.TxBuild qualified as Tx
import Cardano.Slotting.Slot (SlotNo)

-- | Empty query GADT (no context needed).
data NoCtx a

-- | Build a reject transaction for Phase 3
-- requests.
--
-- Finds all pending requests past their
-- @process_time + retract_time@ deadline and
-- consumes them. The trie root does not change.
-- Uses the TxBuild DSL for convergent fee/refund
-- balancing.
rejectRequestsImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> TokenId
    -> Addr
    -> IO (Tx ConwayEra)
rejectRequestsImpl cfg prov _st tid addr = do
    -- 1. Query on-chain context
    (stateUtxo, reqUtxos, feeUtxo, pp) <-
        queryRejectContext cfg prov tid addr
    let (stateIn, stateOut) = stateUtxo
    -- 2. Extract state, build unchanged datum
    let (oldState, newStateOut, script, ownerKh) =
            prepareRejectState cfg stateOut
    -- 3. Compute validity lower slot
    lowerSlot <-
        computeLowerSlot prov oldState reqUtxos
    -- 4. Build DSL program and execute
    let evalTx = mkRejectEvalTx prov
        prog =
            buildRejectProgram
                cfg
                pp
                stateIn
                reqUtxos
                feeUtxo
                oldState
                newStateOut
                script
                ownerKh
                lowerSlot
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
                $ "rejectRequests: build failed: "
                    <> show err

-- | Query cage UTxOs, find state, filter
-- rejectable (Phase 3) requests, pick fee UTxO.
queryRejectContext
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
queryRejectContext cfg prov tid addr = do
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
                "rejectRequests: state UTxO \
                \not found"
        Just x -> pure x
    let (_, stateOut) = stateUtxo
    now <- currentPosixMs
    let allReqs =
            sortOn fst
                $ findRequestUtxos tid cageUtxos
        oldState =
            case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "rejectRequests: invalid \
                        \state datum"
        pt = stateProcessTime oldState
        rt = stateRetractTime oldState
        isRejectable (_, rOut) =
            case extractCageDatum rOut of
                Just (RequestDatum r) ->
                    let sa = requestSubmittedAt r
                        deadline = sa + pt + rt
                    in  now > deadline || sa > now
                _ -> False
        reqUtxos = filter isRejectable allReqs
    when (null reqUtxos)
        $ error
            "rejectRequests: no rejectable \
            \requests"
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "rejectRequests: no UTxOs"
        (u : _) -> pure u
    pure (stateUtxo, reqUtxos, feeUtxo, pp)

-- | Extract state (unchanged root), build new
-- state output, cage script, and owner key hash.
prepareRejectState
    :: CageConfig
    -> TxOut ConwayEra
    -> ( OnChainTokenState
       , TxOut ConwayEra
       , Script ConwayEra
       , KeyHash 'Witness
       )
prepareRejectState cfg stateOut =
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
        oldState =
            case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "rejectRequests: invalid \
                        \state datum"
        OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = oldState
        -- Root unchanged for reject
        newStateOut =
            mkBasicTxOut
                scriptAddr
                (stateOut ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum
                        ( toPlcData
                            (StateDatum oldState)
                        )
        script = mkCageScript cfg
        ownerKh = addrWitnessKeyHash ownerBs
    in  (oldState, newStateOut, script, ownerKh)

-- | Compute the validity lower slot: must be
-- AFTER the latest Phase 3 deadline among the
-- rejected requests. Falls back to now-0s/5s/30s
-- if the slot is past the forecast horizon.
computeLowerSlot
    :: Provider IO
    -> OnChainTokenState
    -> [(TxIn, TxOut ConwayEra)]
    -> IO SlotNo
computeLowerSlot prov oldState reqUtxos = do
    let pt = stateProcessTime oldState
        rt = stateRetractTime oldState
        latestDeadline =
            maximum
                $ map
                    ( \(_, rOut) ->
                        case extractCageDatum rOut of
                            Just (RequestDatum r) ->
                                requestSubmittedAt r
                                    + pt
                                    + rt
                            _ -> 0
                    )
                    reqUtxos
    mLowerSlot <-
        try @SomeException
            (posixMsCeilSlot prov latestDeadline)
    case mLowerSlot of
        Right s -> pure s
        Left _ -> do
            nowUtc <- getCurrentTime
            let posixSec =
                    utcTimeToPOSIXSeconds nowUtc
            trySlots prov
                $ map
                    ( \d ->
                        round
                            ((posixSec - d) * 1000)
                    )
                    [0, 5, 30]

-- | Wrap the Provider's evaluateTx for the DSL.
mkRejectEvalTx
    :: Provider IO
    -> Tx ConwayEra
    -> IO
        ( Map.Map
            (ConwayPlutusPurpose AsIx ConwayEra)
            (Either String ExUnits)
        )
mkRejectEvalTx prov tx = do
    r <- evaluateTx prov tx
    pure
        $ Map.map
            ( \case
                Left e -> Left (show e)
                Right eu -> Right eu
            )
            r

-- | The TxBuild DSL program for a reject tx.
--
-- Spends: state UTxO (Reject) + expired request
-- UTxOs (Contribute). Outputs: same state (root
-- unchanged) + refunds. Peeks at the fee to
-- compute per-request refund.
buildRejectProgram
    :: CageConfig
    -> PParams ConwayEra
    -> TxIn
    -> [(TxIn, TxOut ConwayEra)]
    -> (TxIn, TxOut ConwayEra)
    -> OnChainTokenState
    -> TxOut ConwayEra
    -> Script ConwayEra
    -> KeyHash 'Witness
    -> SlotNo
    -> Tx.TxBuild NoCtx Void ()
buildRejectProgram
    cfg
    pp
    stateIn
    reqUtxos
    feeUtxo
    oldState
    newStateOut
    script
    ownerKh
    lowerSlot = do
        let stateRef = txInToRef stateIn
            OnChainTokenState
                { stateTip = tipAmount
                } = oldState
            nReqs =
                fromIntegral (length reqUtxos)
                    :: Integer
        -- Spend state UTxO (Reject redeemer)
        _ <- Tx.spendScript stateIn Reject
        -- Spend request UTxOs
        mapM_
            ( \(rIn, _) ->
                Tx.spendScript
                    rIn
                    (Contribute stateRef)
            )
            reqUtxos
        -- State output (root unchanged)
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
                        getMinCoinTxOut pp draft
                Tx.output
                    $ mkBasicTxOut
                        refundAddr
                        ( inject
                            (max rawRefund minCoin)
                        )
            )
            reqUtxos
        -- Constraints
        Tx.attachScript script
        Tx.requireSignature ownerKh
        Tx.collateral (fst feeUtxo)
        Tx.validFrom lowerSlot
