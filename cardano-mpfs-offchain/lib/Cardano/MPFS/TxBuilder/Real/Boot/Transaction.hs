-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot.Transaction
-- Description : Domain helpers for the boot transaction
-- License     : Apache-2.0
--
-- Two small helpers for the cage-protocol content of
-- the boot transaction:
--
-- * 'bootAssetName' — derive the cage's unique asset
--   name from the seed input (matches the Aiken
--   policy's @deriveAssetName@).
-- * 'bootStateDatum' — build the on-chain @StateDatum@
--   for the freshly-booted token, using the owner
--   address and the configured default tip / process /
--   retract times.
--
-- The mechanical parts of building the transaction
-- (mint asset assembly, output construction, script
-- integrity hashing, ExUnits, fee balancing) are
-- delegated to the 'Cardano.Node.Client.TxBuild' DSL
-- by 'Cardano.MPFS.TxBuilder.Real.Boot.bootTokenImpl'.
module Cardano.MPFS.TxBuilder.Real.Boot.Transaction
    ( bootAssetName
    , bootStateDatum
    ) where

import Data.ByteString.Short qualified as SBS

import Cardano.Ledger.Address (Addr)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , deriveAssetName
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , TxIn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( addrKeyHashBytes
    , emptyRoot
    , txInToRef
    )

-- | Derive the cage's asset name from the seed input.
bootAssetName :: TxIn -> AssetName
bootAssetName seedRef =
    AssetName
        $ SBS.toShort
        $ deriveAssetName (txInToRef seedRef)

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
