-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Sweep
-- Description : Owner-sweep transaction
-- License     : Apache-2.0
--
-- Builds a sweep transaction. The cage owner spends a
-- non-legitimate UTxO at the per-cage request address
-- (no datum, a request datum targeting a different
-- token, or another malformed request UTxO) while
-- referencing the state UTxO from which the validator
-- reads the owner's public-key hash for the signature
-- check.
--
-- The sweep predicate is enforced on-chain: the
-- spent UTxO must NOT be the legitimate state UTxO
-- and must NOT be a legitimate request for this cage.
-- The redeemer points at the state UTxO so the
-- validator can locate it directly.
module Cardano.MPFS.TxBuilder.Real.Sweep
    ( sweepUtxoImpl
    ) where

import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Set qualified as Set
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
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
    , referenceInputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out (coinTxOutL)
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
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
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( ConwayEra
    , TokenId
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build a standalone sweep transaction.
--
-- Spends one non-legitimate UTxO at the cage's
-- per-cage request address. References the state
-- UTxO so the validator can read the owner's
-- verification-key hash. Requires the state owner's
-- signature.
sweepUtxoImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> TokenId
    -- ^ Token whose cage's address is being swept
    -> TxIn
    -- ^ UTxO reference of the garbage to sweep
    -> Addr
    -- ^ State owner's address (signs and balances)
    -> IO (Tx ConwayEra)
sweepUtxoImpl cfg prov tid garbTxIn addr = do
    let reqAddr =
            requestAddrFromCfg
                cfg
                tid
                (network cfg)
        stateAddr =
            cageAddrFromCfg cfg (network cfg)
    requestUtxos <- queryUTxOs prov reqAddr
    stateUtxos <- queryUTxOs prov stateAddr
    garbUtxoPair <-
        case findUtxoByTxIn
            garbTxIn
            requestUtxos of
            Nothing ->
                error
                    "sweepUtxo: garbage UTxO not \
                    \found at the request address"
            Just x -> pure x
    let (garbIn, _garbOut) = garbUtxoPair
    let policyId = cagePolicyIdFromCfg cfg
    stateUtxo <-
        case findStateUtxo
            policyId
            tid
            stateUtxos of
            Nothing ->
                error
                    "sweepUtxo: state UTxO not \
                    \found at the state address"
            Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "sweepUtxo: no UTxOs in wallet"
        (u : _) -> pure u
    let stateDatum =
            case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "sweepUtxo: invalid state \
                        \datum at state UTxO"
        OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = stateDatum
        ownerKh = addrWitnessKeyHash ownerBs
    let script = mkRequestScript cfg tid
        scriptHash = hashScript script
        allInputs =
            Set.fromList [garbIn, fst feeUtxo]
        garbIx = spendingIndex garbIn allInputs
        stateRef = txInToRef stateIn
        redeemer = Sweep stateRef
        spendPurpose =
            ConwaySpending (AsIx garbIx)
        redeemers =
            Redeemers
                $ Map.singleton
                    spendPurpose
                    ( toLedgerData redeemer
                    , placeholderExUnits
                    )
        integrity =
            computeScriptIntegrity pp redeemers
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton garbIn
                & referenceInputsTxBodyL
                    .~ Set.singleton stateIn
                & collateralInputsTxBodyL
                    .~ Set.singleton
                        (fst feeUtxo)
                & reqSignerHashesTxBodyL
                    .~ Set.singleton ownerKh
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
        [feeUtxo, garbUtxoPair]
        addr
        tx
