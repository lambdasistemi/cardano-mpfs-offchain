-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Reject
-- Description : Reject transaction for Phase 3 requests
-- License     : Apache-2.0
--
-- Builds a reject transaction that consumes expired
-- (Phase 3) requests. The oracle keeps the fee and
-- refunds remaining ADA to request owners. The trie
-- root does NOT change.
module Cardano.MPFS.TxBuilder.Real.Reject
    ( rejectRequestsImpl
    ) where

import Control.Exception (SomeException, try)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Sequence.Strict qualified as StrictSeq
import Data.Set qualified as Set
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.POSIX
    ( utcTimeToPOSIXSeconds
    )
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.Scripts (AsIx (..))
import Cardano.Ledger.Alonzo.TxBody
    ( reqSignerHashesTxBodyL
    , scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , inputsTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( coinTxOutL
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
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , StrictMaybe (SJust, SNothing)
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core (hashScript)
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
    , TokenId
    )
import Cardano.MPFS.Provider
    ( Provider (..)
    , SlotNo
    )
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build a reject transaction for Phase 3
-- requests.
--
-- Finds all pending requests in Phase 3 (past
-- @submitted_at + process_time + retract_time@)
-- and consumes them. The oracle keeps the fee;
-- remaining ADA goes to request owners. The trie
-- root does not change.
rejectRequestsImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> TokenId
    -> Addr
    -> IO (Tx ConwayEra)
rejectRequestsImpl cfg prov _st tid addr = do
    -- 1. Query cage UTxOs
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    -- 2. Find state UTxO
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
    let (stateIn, stateOut) = stateUtxo
    -- 3. Find rejectable request UTxOs
    now <- currentPosixMs
    let allReqs =
            sortOn fst
                $ findRequestUtxos tid cageUtxos
        oldState = case extractCageDatum stateOut of
            Just (StateDatum s) -> s
            _ ->
                error
                    "rejectRequests: invalid \
                    \state datum"
        OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = oldState
        pt = stateProcessTime oldState
        rt = stateRetractTime oldState
        isRejectable (_, rOut) =
            case extractCageDatum rOut of
                Just (RequestDatum r) ->
                    let sa = requestSubmittedAt r
                        deadline = sa + pt + rt
                    in  now > deadline
                            || sa > now
                _ -> False
        reqUtxos = filter isRejectable allReqs
    when (null reqUtxos)
        $ error
            "rejectRequests: no rejectable \
            \requests"
    -- 4. Get wallet UTxO for fees
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "rejectRequests: no UTxOs"
        (u : _) -> pure u
    -- 5. Build new state output (root unchanged)
    let newStateOut =
            mkBasicTxOut
                scriptAddr
                (stateOut ^. valueTxOutL)
                & datumTxOutL
                    .~ mkInlineDatum
                        ( toPlcData
                            (StateDatum oldState)
                        )
    -- 6. Build refund outputs
    let mkRefund (_, reqOut) =
            let Coin reqVal =
                    reqOut ^. coinTxOutL
                Coin mf = defaultMaxFee cfg
                refundAddr =
                    addrFromKeyHashBytes
                        (network cfg)
                        (extractOwnerBytes reqOut)
                rawRefund = Coin (reqVal - mf)
                draft =
                    mkBasicTxOut
                        refundAddr
                        (inject rawRefund)
                minCoin = getMinCoinTxOut pp draft
            in  mkBasicTxOut
                    refundAddr
                    (inject (max rawRefund minCoin))
        extractOwnerBytes out =
            case extractCageDatum out of
                Just (RequestDatum req) ->
                    let OnChainRequest
                            { requestOwner =
                                BuiltinByteString bs
                            } = req
                    in  bs
                _ ->
                    error
                        "extractOwnerBytes: \
                        \not a request"
        refundOuts = map mkRefund reqUtxos
        allOuts =
            StrictSeq.fromList
                (newStateOut : refundOuts)
    -- 7. Build redeemers
    let script = mkCageScript cfg
        scriptHash = hashScript script
        reqIns = map fst reqUtxos
        allScriptIns =
            Set.fromList (stateIn : reqIns)
        allInputs =
            Set.insert (fst feeUtxo) allScriptIns
        stateRef = txInToRef stateIn
        stateIx =
            spendingIndex stateIn allInputs
        contributeEntries =
            map
                ( \rIn ->
                    let ix =
                            spendingIndex
                                rIn
                                allInputs
                        rdm = Contribute stateRef
                    in  ( ConwaySpending (AsIx ix)
                        ,
                            ( toLedgerData rdm
                            , placeholderExUnits
                            )
                        )
                )
                reqIns
        redeemers =
            Redeemers
                $ Map.fromList
                $ ( ConwaySpending
                        (AsIx stateIx)
                  ,
                      ( toLedgerData Reject
                      , placeholderExUnits
                      )
                  )
                    : contributeEntries
        integrity =
            computeScriptIntegrity pp redeemers
        ownerKh =
            addrWitnessKeyHash ownerBs
    -- 8. Validity: must be after Phase 3 start
    -- (latest deadline among rejected requests)
    let latestDeadline =
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
            ( posixMsCeilSlot
                prov
                latestDeadline
            )
    lowerSlot <- case mLowerSlot of
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
    let vldt =
            ValidityInterval
                (SJust lowerSlot)
                SNothing
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ allScriptIns
                & outputsTxBodyL .~ allOuts
                & collateralInputsTxBodyL
                    .~ Set.singleton
                        (fst feeUtxo)
                & reqSignerHashesTxBodyL
                    .~ Set.singleton ownerKh
                & vldtTxBodyL .~ vldt
                & scriptIntegrityHashTxBodyL
                    .~ integrity
        tx =
            mkBasicTx body
                & witsTxL . scriptTxWitsL
                    .~ Map.singleton
                        scriptHash
                        script
                & witsTxL . rdmrsTxWitsL
                    .~ redeemers
    evaluateAndBalance
        prov
        pp
        (feeUtxo : stateUtxo : reqUtxos)
        addr
        tx
  where
    when False _ = pure ()
    when True act = act
    currentPosixMs :: IO Integer
    currentPosixMs = do
        nowUtc <- getCurrentTime
        let posixSec =
                utcTimeToPOSIXSeconds nowUtc
        pure (round (posixSec * 1000))

-- | Try converting successive POSIX ms values
-- to slots, returning the first that succeeds.
trySlots
    :: Provider IO -> [Integer] -> IO SlotNo
trySlots _ [] =
    error
        "posixMsCeilSlot: all fallbacks \
        \past horizon"
trySlots p (ms : rest) = do
    r <-
        try @SomeException
            (posixMsCeilSlot p ms)
    case r of
        Right s -> pure s
        Left _ -> trySlots p rest
