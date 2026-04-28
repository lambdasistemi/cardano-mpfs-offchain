-- |
-- Module      : Cardano.MPFS.Core.OnChain
-- Description : On-chain type encodings (re-exports from cage)
-- License     : Apache-2.0
--
-- Re-exports on-chain types from @cardano-mpfs-cage@.
-- The script hash, policy id, and address of the
-- global state validator are not constants here:
-- they live on the 'CageConfig' value built from the
-- upstream blueprint at startup, and every TxBuilder
-- consumes them through the @*FromCfg@ helpers in
-- "Cardano.MPFS.TxBuilder.Real.Internal".
module Cardano.MPFS.Core.OnChain
    ( -- * On-chain datum\/redeemer types (from cage)
      CageDatum (..)
    , MintRedeemer (..)
    , Migration (..)
    , UpdateRedeemer (..)
    , RequestAction (..)

      -- * On-chain domain types (from cage)
    , OnChainTokenId (..)
    , OnChainOperation (..)
    , OnChainRoot (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)

      -- * Proof steps (from cage)
    , ProofStep (..)
    , Neighbor (..)

      -- * Asset-name derivation (from cage)
    , deriveAssetName

      -- * Blueprint loading (from cage)
    , Blueprint (..)
    , loadCageScript
    ) where

import Cardano.MPFS.Cage.AssetName (deriveAssetName)
import Cardano.MPFS.Cage.Blueprint
    ( Blueprint (..)
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , Migration (..)
    , MintRedeemer (..)
    , Neighbor (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , ProofStep (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
    )

-- | Load and parse a CIP-57 blueprint.
loadCageScript
    :: FilePath
    -> IO (Either String Blueprint)
loadCageScript = loadBlueprint
