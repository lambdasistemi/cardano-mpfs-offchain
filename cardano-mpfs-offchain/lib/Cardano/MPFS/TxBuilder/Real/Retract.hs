-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Retract
-- Description : Retract request transaction
-- License     : Apache-2.0
--
-- Builds the retract transaction that cancels a
-- pending request. The requester spends their request
-- UTxO (recovering locked ADA) while referencing the
-- State UTxO. Validity interval is Phase 2:
-- @entirely_after(submitted_at + process_time)@ and
-- @entirely_before(submitted_at + process_time + retract_time)@.
module Cardano.MPFS.TxBuilder.Real.Retract
    ( retractRequestImpl
    ) where

import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Set qualified as Set
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
    ( mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , inputsTxBodyL
    , mkBasicTxBody
    , referenceInputsTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( coinTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( SlotNo (..)
    , StrictMaybe (SJust)
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core (hashScript)
import Cardano.Ledger.TxIn (TxIn)

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( LocatedRequest (..)
    , Request (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , UnsignedTxBundle
    , bareTxBundle
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

-- | Build a retract-request transaction.
--
-- The requester spends their request UTxO
-- (returning locked ADA) while referencing the
-- state UTxO. Requires Phase 2 validity.
retractRequestImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Request state (to look up the request)
    -> BundleSnapshot
    -- ^ Snapshot this bundle will be anchored to
    -> TxIn
    -- ^ UTxO reference of the request to retract
    -> Addr
    -- ^ Requester's address (receives refund)
    -> IO UnsignedTxBundle
retractRequestImpl cfg prov st snap reqTxIn addr = do
    -- 1. Look up the request to find its token
    mReq <- getRequest (requests st) reqTxIn
    req <- case mReq of
        Nothing ->
            error "retractRequest: unknown request"
        Just LocatedRequest{request = r} -> pure r
    -- 2. Query cage UTxOs to find request + state
    let scriptAddr = cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    let reqUtxo = findUtxoByTxIn reqTxIn cageUtxos
    reqUtxoPair <- case reqUtxo of
        Nothing ->
            error
                "retractRequest: request UTxO \
                \not found on chain"
        Just x -> pure x
    let (reqIn, reqOut) = reqUtxoPair
    -- 3. Find state UTxO for this token
    let Request{requestToken = tid} = req
        policyId = cagePolicyIdFromCfg cfg
    stateUtxo <-
        case findStateUtxo policyId tid cageUtxos of
            Nothing ->
                error
                    "retractRequest: state UTxO \
                    \not found"
            Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
    -- 4. Get wallet UTxO for fees
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "retractRequest: no UTxOs"
        (u : _) -> pure u
    -- 5. Extract request owner for required signer
    -- 5. Extract request datum fields
    let reqDatum = case extractCageDatum reqOut of
            Just (RequestDatum r) -> r
            _ ->
                error
                    "retractRequest: invalid \
                    \request datum"
        OnChainRequest
            { requestOwner =
                BuiltinByteString ownerBs
            , requestSubmittedAt = submAt
            } = reqDatum
        ownerKh = addrWitnessKeyHash ownerBs
    -- 5b. Read process_time and retract_time
    -- from the state datum
    let stateDatum = case extractCageDatum stateOut of
            Just (StateDatum s) -> s
            _ ->
                error
                    "retractRequest: invalid \
                    \state datum"
        OnChainTokenState
            { stateProcessTime = procTime
            , stateRetractTime = retrTime
            } = stateDatum
    -- 6. Build tx with Retract redeemer
    --    (Retract takes the STATE UTxO reference)
    -- Phase 2: entirely_after(submitted_at +
    --   process_time) AND entirely_before(
    --   submitted_at + process_time + retract_time)
    let phase2Start = submAt + procTime
        phase2End = submAt + procTime + retrTime
    -- Ceil for lower bound: first slot that is
    -- at-or-after the Phase 2 start.
    lowerSlot <-
        posixMsCeilSlot prov phase2Start
    -- Floor for upper bound: last slot that is
    -- at-or-before Phase 2 end, minus 1 to
    -- stay strictly before the deadline.
    SlotNo s <-
        posixMsToSlot prov phase2End
    let upperSlot = SlotNo (max 0 (s - 1))
        script = mkCageScript cfg
        scriptHash = hashScript script
        allInputs =
            Set.fromList [reqIn, fst feeUtxo]
        reqIx = spendingIndex reqIn allInputs
        stateRef = txInToRef stateIn
        redeemer = Retract stateRef
        spendPurpose =
            ConwaySpending (AsIx reqIx)
        redeemers =
            Redeemers
                $ Map.singleton
                    spendPurpose
                    ( toLedgerData redeemer
                    , placeholderExUnits
                    )
        integrity =
            computeScriptIntegrity pp redeemers
        vldt =
            ValidityInterval
                (SJust lowerSlot)
                (SJust upperSlot)
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton reqIn
                & referenceInputsTxBodyL
                    .~ Set.singleton stateIn
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
    balanced <-
        evaluateAndBalance
            prov
            pp
            [feeUtxo, reqUtxoPair]
            addr
            tx
    pure (bareTxBundle snap balanced)
