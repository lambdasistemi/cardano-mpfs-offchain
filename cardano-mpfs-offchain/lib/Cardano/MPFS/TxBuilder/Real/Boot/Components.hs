{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot.Components
-- Description : Per-step builders for POST /tx/boot
-- License     : Apache-2.0
--
-- Decomposes the boot transaction into named building
-- blocks: asset-name derivation, mint value, output
-- datum, output, script\/redeemer, body, and witnessed
-- assembly. Each block has a small, type-checked
-- signature so the orchestrator
-- ('Cardano.MPFS.TxBuilder.Real.Boot.bootTokenImpl')
-- reads as a sequence of named operations.
module Cardano.MPFS.TxBuilder.Real.Boot.Components
    ( -- * Mint
      bootAssetName
    , bootMintValue

      -- * Output
    , bootStateDatum
    , bootStateOutput

      -- * Script
    , bootScriptAndRedeemers

      -- * Body and tx
    , bootTxBody
    , bootAssembledTx
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
import Cardano.Ledger.Api.PParams
    ( PParams
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( TxBody
    , collateralInputsTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
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
    , PolicyID
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , deriveAssetName
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , ConwayEra
    , TxIn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( addrKeyHashBytes
    , cageAddrFromCfg
    , computeScriptIntegrity
    , emptyRoot
    , mkCageScript
    , mkInlineDatum
    , placeholderExUnits
    , toLedgerData
    , toPlcData
    , txInToRef
    )

import Cardano.Ledger.Api.Scripts
    ( Script
    , ScriptHash
    )

-- ---------------------------------------------------
-- Mint
-- ---------------------------------------------------

-- | Derive the cage's asset name from the seed input.
bootAssetName :: TxIn -> AssetName
bootAssetName seedRef =
    AssetName
        $ SBS.toShort
        $ deriveAssetName (txInToRef seedRef)

-- | Build the mint value: +1 of the cage policy's
-- token at the derived asset name.
bootMintValue
    :: PolicyID -> AssetName -> MultiAsset
bootMintValue policyId an =
    MultiAsset
        $ Map.singleton policyId
        $ Map.singleton an 1

-- ---------------------------------------------------
-- Output
-- ---------------------------------------------------

-- | Build the boot state's on-chain datum for the
-- given owner address.
bootStateDatum
    :: CageConfig -> Addr -> CageDatum
bootStateDatum cfg addr =
    StateDatum
        OnChainTokenState
            { stateOwner =
                BuiltinByteString
                    (addrKeyHashBytes addr)
            , stateRoot =
                OnChainRoot emptyRoot
            , stateMaxFee =
                let Coin c = defaultTip cfg
                in  c
            , stateProcessTime =
                defaultProcessTime cfg
            , stateRetractTime =
                defaultRetractTime cfg
            }

-- | Build the boot state UTxO at the cage script
-- address, carrying ada + the freshly-minted token
-- and the inline state datum.
bootStateOutput
    :: CageConfig
    -> MultiAsset
    -> CageDatum
    -> TxOut ConwayEra
bootStateOutput cfg mintMA datum =
    mkBasicTxOut
        (cageAddrFromCfg cfg (network cfg))
        (MaryValue (Coin 2_000_000) mintMA)
        & datumTxOutL
            .~ mkInlineDatum (toPlcData datum)

-- ---------------------------------------------------
-- Script
-- ---------------------------------------------------

-- | Build the cage script, its hash, and the
-- single-redeemer 'Redeemers' map for the boot mint
-- redeemer derived from the seed input.
bootScriptAndRedeemers
    :: CageConfig
    -> TxIn
    -> ( Script ConwayEra
       , ScriptHash
       , Redeemers ConwayEra
       )
bootScriptAndRedeemers cfg seedRef =
    let script = mkCageScript cfg
        redeemer = Minting (txInToRef seedRef)
        rdmrs =
            Redeemers
                $ Map.singleton
                    (ConwayMinting (AsIx 0))
                    ( toLedgerData redeemer
                    , placeholderExUnits
                    )
    in  (script, hashScript script, rdmrs)

-- ---------------------------------------------------
-- Body and tx
-- ---------------------------------------------------

-- | Build the unbalanced, unsigned tx body. The
-- collateral input is taken from the last picked
-- ledger pair so the orchestrator can keep its
-- "seed first, collateral last" layout.
bootTxBody
    :: PParams ConwayEra
    -> TxIn
    -- ^ Seed input (consumed for asset-name uniqueness)
    -> TxIn
    -- ^ Collateral input (last of the picked pairs)
    -> TxOut ConwayEra
    -- ^ Boot state output
    -> MultiAsset
    -- ^ Mint value
    -> Redeemers ConwayEra
    -- ^ Mint redeemer
    -> TxBody ConwayEra
bootTxBody pp seedRef collatRef stateOut mintMA rdmrs =
    mkBasicTxBody
        & inputsTxBodyL
            .~ Set.singleton seedRef
        & outputsTxBodyL
            .~ StrictSeq.singleton stateOut
        & mintTxBodyL .~ mintMA
        & collateralInputsTxBodyL
            .~ Set.singleton collatRef
        & scriptIntegrityHashTxBodyL
            .~ computeScriptIntegrity pp rdmrs

-- | Wrap the tx body with the script witness and
-- redeemers map. The result is unbalanced — the
-- orchestrator passes it to @evaluateAndBalance@.
bootAssembledTx
    :: TxBody ConwayEra
    -> ScriptHash
    -> Script ConwayEra
    -> Redeemers ConwayEra
    -> Tx ConwayEra
bootAssembledTx body scriptHash script rdmrs =
    mkBasicTx body
        & witsTxL . scriptTxWitsL
            .~ Map.singleton scriptHash script
        & witsTxL . rdmrsTxWitsL .~ rdmrs
