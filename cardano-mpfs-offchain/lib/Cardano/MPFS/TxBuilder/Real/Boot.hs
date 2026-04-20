{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot
-- Description : Boot token minting transaction
-- License     : Apache-2.0
--
-- Builds the minting transaction for a new MPFS cage
-- token. Picks a wallet UTxO as seed for asset-name
-- derivation, mints +1 token at the cage policy, and
-- creates a State UTxO with empty root and configured
-- default parameters.
module Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    ) where

import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Sequence.Strict qualified as StrictSeq
import Data.Set qualified as Set
import Lens.Micro ((&), (.~))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Alonzo.Scripts (AsIx (..))
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
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
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( datumTxOutL
    , mkBasicTxOut
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
    ( MaryValue (..)
    , MultiAsset (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , Mint (..)
    , MintRedeemer (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , deriveAssetName
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , UnsignedTxBundle
    , bareTxBundle
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build a boot-token minting transaction.
--
-- Picks a wallet UTxO as seed for the asset name,
-- mints +1 token at the cage policy, and creates
-- a State UTxO with empty root and tip.
bootTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> BundleSnapshot
    -- ^ Snapshot this bundle will be anchored to
    -> Addr
    -- ^ Owner address (receives change, owns the token)
    -> IO UnsignedTxBundle
bootTokenImpl cfg prov snap addr = do
    pp <- queryProtocolParams prov
    utxos <- queryUTxOs prov addr
    case utxos of
        [] -> error "bootToken: no UTxOs"
        (seedUtxo : rest) -> do
            let (seedRef, _seedOut) = seedUtxo
                allInputUtxos = case rest of
                    [] -> [seedUtxo]
                    (u : _) -> [seedUtxo, u]
            -- 1. Derive asset name from seed
            let onChainRef = txInToRef seedRef
                assetNameBs =
                    deriveAssetName onChainRef
                assetName =
                    AssetName
                        (SBS.toShort assetNameBs)
            -- 2. Build mint value (+1 token)
            let policyId =
                    cagePolicyIdFromCfg cfg
                mintMA =
                    MultiAsset
                        $ Map.singleton
                            policyId
                        $ Map.singleton
                            assetName
                            1
            -- 3. Build output datum
            let stateDatum =
                    StateDatum
                        OnChainTokenState
                            { stateOwner =
                                BuiltinByteString
                                    (addrKeyHashBytes addr)
                            , stateRoot =
                                OnChainRoot emptyRoot
                            , stateMaxFee =
                                let Coin c =
                                        defaultTip cfg
                                in  c
                            , stateProcessTime =
                                defaultProcessTime cfg
                            , stateRetractTime =
                                defaultRetractTime cfg
                            }
                datumData = toPlcData stateDatum
            -- 4. Build output with ada + token
            let scriptAddr =
                    cageAddrFromCfg
                        cfg
                        (network cfg)
                outValue =
                    MaryValue
                        (Coin 2_000_000)
                        mintMA
                txOut =
                    mkBasicTxOut
                        scriptAddr
                        outValue
                        & datumTxOutL
                            .~ mkInlineDatum
                                datumData
            -- 5. Build script + redeemer
            let script = mkCageScript cfg
                scriptHash = hashScript script
                redeemer =
                    Minting (Mint onChainRef)
                mintPurpose =
                    ConwayMinting (AsIx 0)
                redeemers =
                    Redeemers
                        $ Map.singleton
                            mintPurpose
                            ( toLedgerData redeemer
                            , placeholderExUnits
                            )
            -- 6. Build tx body
            let integrity =
                    computeScriptIntegrity
                        pp
                        redeemers
                body =
                    mkBasicTxBody
                        & inputsTxBodyL
                            .~ Set.singleton
                                seedRef
                        & outputsTxBodyL
                            .~ StrictSeq.singleton
                                txOut
                        & mintTxBodyL .~ mintMA
                        & collateralInputsTxBodyL
                            .~ Set.singleton
                                ( fst
                                    $ last allInputUtxos
                                )
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
            -- 7. Evaluate and balance
            balanced <-
                evaluateAndBalance
                    prov
                    pp
                    allInputUtxos
                    addr
                    tx
            pure (bareTxBundle snap balanced)
