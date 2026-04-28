-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.End
-- Description : End token (burn) transaction
-- License     : Apache-2.0
--
-- Builds the burn transaction that retires an MPFS
-- cage token. Consumes the State UTxO with an @End@
-- spending redeemer, mints -1 with @Burning@, and
-- returns remaining ADA to the owner.
module Cardano.MPFS.TxBuilder.Real.End
    ( endTokenImpl
    ) where

import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
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
    , mintTxBodyL
    , mkBasicTxBody
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core (hashScript)
import Cardano.Ledger.Mary.Value
    ( MultiAsset (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Data.ByteString.Short qualified as SBS

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , TokenId (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , EndProof (..)
    , ProofEnvelope (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

import Data.List (sortOn)
import Data.Ord (Down (..))

import Cardano.Ledger.Api.Tx.Out (coinTxOutL)

-- | Build an end-token (burn) transaction.
--
-- Consumes the state UTxO with an @End@ spending
-- redeemer, mints -1 of the cage token with a
-- @Burning@ minting redeemer, and returns remaining
-- ADA to the owner. The token is permanently
-- destroyed.
endTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> UtxoProofFn
    -- ^ CSMT inclusion proof lookup
    -> BundleSnapshot
    -- ^ Snapshot this bundle will be anchored to
    -> TokenId
    -- ^ Token to retire
    -> Addr
    -- ^ Fee-paying address (receives remaining ADA)
    -> IO (ProofEnvelope EndProof)
endTokenImpl cfg prov proofFn snap tid addr = do
    -- 1. Query cage UTxOs
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    -- 2. Find state UTxO
    let policyId = cagePolicyIdFromCfg cfg
    stateUtxo <-
        case findStateUtxo policyId tid cageUtxos of
            Nothing ->
                error
                    "endToken: state UTxO not found"
            Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
    -- 3. Extract owner from state datum
    let OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = case extractCageDatum stateOut of
                Just (StateDatum s) -> s
                _ ->
                    error
                        "endToken: invalid state datum"
        ownerKh = addrWitnessKeyHash ownerBs
    -- 4. Get wallet UTxO for fees
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] -> error "endToken: no UTxOs"
        (u : _) -> pure u
    -- 5. Build mint value (-1 token)
    let assetName = unTokenId tid
        burnMA =
            MultiAsset
                $ Map.singleton policyId
                $ Map.singleton assetName (-1)
    -- 6. Build redeemers
    let script = mkCageScript cfg
        scriptHash = hashScript script
        allInputs =
            Set.fromList [stateIn, fst feeUtxo]
        stateIx =
            spendingIndex stateIn allInputs
        spendRedeemer = End
        onChainTid =
            OnChainTokenId
                $ BuiltinByteString
                $ SBS.fromShort
                $ let AssetName sbs = unTokenId tid
                  in  sbs
        mintRedeemer = Burning onChainTid
        redeemers =
            Redeemers
                $ Map.fromList
                    [
                        ( ConwaySpending
                            (AsIx stateIx)
                        ,
                            ( toLedgerData spendRedeemer
                            , placeholderExUnits
                            )
                        )
                    ,
                        ( ConwayMinting (AsIx 0)
                        ,
                            ( toLedgerData mintRedeemer
                            , placeholderExUnits
                            )
                        )
                    ]
        integrity =
            computeScriptIntegrity pp redeemers
    -- 7. Build tx body
    let body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton stateIn
                & mintTxBodyL .~ burnMA
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
    balanced <-
        evaluateAndBalance
            prov
            pp
            [feeUtxo, stateUtxo]
            addr
            tx
    stateWitness <- witness proofFn stateUtxo
    fundingWitnesses <- witnesses proofFn [feeUtxo]
    pure
        ProofEnvelope
            { envTx = balanced
            , envSnapshot = snap
            , envProof =
                EndProof
                    { endState = stateWitness
                    , endFunding = fundingWitnesses
                    }
            }
