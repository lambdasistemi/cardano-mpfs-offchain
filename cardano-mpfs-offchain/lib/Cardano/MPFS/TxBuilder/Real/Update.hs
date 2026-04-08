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

import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Ord (Down (..))
import Data.Sequence.Strict qualified as StrictSeq
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
    ( TxOut
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
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , StrictMaybe (SJust, SNothing)
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core (hashScript)
import Cardano.Ledger.TxIn (TxIn)
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
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build an update-token transaction.
--
-- Consumes the state UTxO and all pending request
-- UTxOs, processes each request through the trie,
-- and outputs a new state UTxO with updated root.
updateTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Token and request state
    -> TrieManager IO
    -- ^ Trie manager (for speculative proof generation)
    -> TokenId
    -- ^ Token to process requests for
    -> Addr
    -- ^ Oracle's address (pays fee, receives fee income)
    -> IO (Tx ConwayEra)
updateTokenImpl cfg prov _st tm tid addr = do
    -- 1. Query cage UTxOs
    let scriptAddr = cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    -- 2. Find state UTxO
    let policyId = cagePolicyIdFromCfg cfg
    stateUtxo <- case findStateUtxo policyId tid cageUtxos of
        Nothing ->
            error
                "updateToken: state UTxO not found"
        Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
    -- 3. Find request UTxOs for this token
    let reqUtxos =
            sortOn fst
                $ findRequestUtxos tid cageUtxos
    when (null reqUtxos)
        $ error "updateToken: no pending requests"
    -- 4. Get wallet UTxO for fees
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "updateToken: no UTxOs"
        (u : _) -> pure u
    -- 5. Compute proofs speculatively (no mutation)
    (proofs, newRoot) <-
        withSpeculativeTrie tm tid $ \trie -> do
            ps <-
                mapM
                    (processRequest trie)
                    reqUtxos
            r <- getRoot trie
            pure (ps, r)
    -- 7. Build new state output
    let oldState = case extractCageDatum stateOut of
            Just (StateDatum s) -> s
            _ ->
                error
                    "updateToken: invalid state datum"
        OnChainTokenState
            { stateOwner = BuiltinByteString ownerBs
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
    -- 8. Build refund outputs (one per request)
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
    -- 9. Build redeemers
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
        modifyRedeemer = Modify proofs
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
                      ( toLedgerData modifyRedeemer
                      , placeholderExUnits
                      )
                  )
                    : contributeEntries
        integrity =
            computeScriptIntegrity pp redeemers
        ownerKh =
            addrWitnessKeyHash ownerBs
        -- Phase 1: upper bound must be before
        -- the earliest submitted_at + process_time.
        -- Extract submitted_at from each request
        -- and take the minimum deadline.
        extractSubmittedAt (_, rOut) =
            case extractCageDatum rOut of
                Just (RequestDatum r) ->
                    requestSubmittedAt r
                _ -> 0
        earliestDeadline =
            minimum
                $ map
                    ( \u ->
                        extractSubmittedAt u
                            + stateProcessTime oldState
                    )
                    reqUtxos
    upperSlot <-
        posixMsToSlot prov earliestDeadline
    let
        vldt =
            ValidityInterval
                SNothing
                (SJust upperSlot)
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
