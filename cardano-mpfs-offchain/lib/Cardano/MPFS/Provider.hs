-- |
-- Module      : Cardano.MPFS.Provider
-- Description : Blockchain query interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for querying the Cardano
-- blockchain. Implementations live in
-- "Cardano.MPFS.Provider.NodeClient" (node-to-client
-- LocalStateQuery) and "Cardano.MPFS.Mock.Provider"
-- (in-memory stub for tests).
module Cardano.MPFS.Provider
    ( -- * Provider interface
      Provider (..)

      -- * Result types
    , EvaluateTxResult

      -- * Re-exports
    , SlotNo (..)
    ) where

import Data.Map.Strict (Map)

import Cardano.Ledger.Alonzo.Plutus.Evaluate
    ( TransactionScriptFailure
    )
import Cardano.Ledger.Alonzo.Scripts
    ( AsIx
    , PlutusPurpose
    )
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Plutus (ExUnits)
import Cardano.Slotting.Slot (SlotNo (..))
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.Core.Types
    ( Addr
    , ConwayEra
    , PParams
    , TxIn
    )

-- | Per-script evaluation result.
type EvaluateTxResult era =
    Map
        (PlutusPurpose AsIx era)
        ( Either
            (TransactionScriptFailure era)
            ExUnits
        )

-- | Interface for querying the blockchain.
-- All era-specific types are fixed to 'ConwayEra'.
data Provider m = Provider
    { queryUTxOs
        :: Addr
        -> m [(TxIn, TxOut ConwayEra)]
    -- ^ Look up UTxOs at an address.
    --
    -- __FORBIDDEN on tx-build paths.__ The
    -- underlying cardano-node @LocalStateQuery@
    -- @GetUTxOByAddress@ scans the entire ledger
    -- UTxO set; its cost is @O(total UTxOs on
    -- chain)@, not @O(K)@ at the queried address.
    -- A high-traffic server using this on the hot
    -- path effectively DoS's its own node.
    --
    -- Server-side tx builders MUST source UTxO
    -- state from the local indexer's CSMT (see
    -- 'Cardano.MPFS.Context.AtomicCageReader').
    -- Wallet-side test code may still call this on
    -- its own @LocalStateQuery@ connection because
    -- each test queries a tiny devnet UTxO set
    -- infrequently. See issue #252.
    , queryProtocolParams
        :: m (PParams ConwayEra)
    -- ^ Fetch current protocol parameters
    , evaluateTx
        :: ConwayTx
        -> m (EvaluateTxResult ConwayEra)
    -- ^ Evaluate script execution units
    , posixMsToSlot
        :: Integer
        -> m SlotNo
    -- ^ Convert POSIX time (ms) to slot (floor)
    , posixMsCeilSlot
        :: Integer
        -> m SlotNo
    -- ^ Convert POSIX time (ms) to slot (ceiling)
    }
