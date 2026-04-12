-- |
-- Module      : Cardano.MPFS.Provider.NodeClient
-- Description : N2C-backed Provider via LocalStateQuery
-- License     : Apache-2.0
--
-- Production implementation of the 'Provider'
-- interface. Delegates to the @cardano-node-clients@
-- library for N2C LocalStateQuery queries.
-- Slot conversion uses the hard-fork interpreter
-- directly (not available in the upstream Provider).
module Cardano.MPFS.Provider.NodeClient
    ( -- * Construction
      mkNodeClientProvider
    ) where

import Data.Time.Clock (NominalDiffTime)
import Data.Time.Clock.POSIX
    ( posixSecondsToUTCTime
    )

import Cardano.Slotting.Slot (SlotNo)
import Cardano.Slotting.Time
    ( RelativeTime (..)
    , getRelativeTime
    , toRelativeTime
    )
import Ouroboros.Consensus.Cardano.Block
    ( BlockQuery (..)
    )
import Ouroboros.Consensus.HardFork.Combinator.Ledger.Query
    ( QueryHardFork (..)
    )
import Ouroboros.Consensus.HardFork.History.Qry
    ( interpretQuery
    , wallclockToSlot
    )
import Ouroboros.Consensus.Ledger.Query
    ( Query (BlockQuery, GetSystemStart)
    )

import Cardano.Node.Client.N2C.LocalStateQuery
    ( queryLSQ
    )
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
            , posixMsToSlot = \ms -> do
                (slot, _, _) <-
                    queryWallclockToSlot ch ms
                pure slot
            , posixMsCeilSlot = \ms -> do
                (slot, timeInSlot, _) <-
                    queryWallclockToSlot ch ms
                pure
                    $ if timeInSlot == 0
                        then slot
                        else slot + 1
            }

-- | Query SystemStart and HardFork interpreter,
-- then convert POSIX milliseconds to a slot.
queryWallclockToSlot
    :: LSQChannel
    -> Integer
    -> IO (SlotNo, NominalDiffTime, NominalDiffTime)
queryWallclockToSlot ch ms = do
    systemStart <-
        queryLSQ ch GetSystemStart
    interpreter <-
        queryLSQ ch
            $ BlockQuery
            $ QueryHardFork GetInterpreter
    let utcTime =
            posixSecondsToUTCTime
                $ fromIntegral ms / 1000
        relTime =
            toRelativeTime
                systemStart
                utcTime
        clamped =
            RelativeTime
                $ max 0
                $ getRelativeTime relTime
    case interpretQuery
        interpreter
        (wallclockToSlot clamped) of
        Right r -> pure r
        Left err ->
            error
                $ "posixMsToSlot: "
                    <> show err
