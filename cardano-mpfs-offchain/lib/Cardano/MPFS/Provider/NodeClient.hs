-- |
-- Module      : Cardano.MPFS.Provider.NodeClient
-- Description : N2C-backed Provider via LocalStateQuery
-- License     : Apache-2.0
--
-- Production implementation of the 'Provider'
-- interface. Delegates to the @cardano-node-clients@
-- library for N2C LocalStateQuery queries.
module Cardano.MPFS.Provider.NodeClient
    ( -- * Construction
      mkNodeClientProvider
    ) where

import Cardano.Node.Client.N2C.Provider qualified as Lib
import Cardano.Node.Client.N2C.Types (LSQChannel)
import Cardano.Node.Client.Provider qualified as Lib

import Cardano.MPFS.Provider (Provider (..))

-- | Create a 'Provider IO' backed by the N2C
-- LocalStateQuery protocol.
mkNodeClientProvider
    :: LSQChannel
    -- ^ LocalStateQuery channel to the Cardano node
    -> Provider IO
mkNodeClientProvider ch =
    let libProv = Lib.mkN2CProvider ch
    in  Provider
            { queryProtocolParams =
                Lib.queryProtocolParams libProv
            , queryUTxOs =
                Lib.queryUTxOs libProv
            , evaluateTx =
                Lib.evaluateTx libProv
            , posixMsToSlot =
                Lib.posixMsToSlot libProv
            , posixMsCeilSlot =
                Lib.posixMsCeilSlot libProv
            }
