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
import Cardano.Ledger.Api.Tx (Tx)
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Plutus (ExUnits)
import Cardano.Slotting.Slot (SlotNo (..))

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
    -- ^ FORBIDDEN IN ANY TX-BUILD PATH.
    --
    -- This call routes to cardano-node's
    -- @LocalStateQuery@ @GetUTxOByAddress@, whose
    -- implementation IS A LINEAR SCAN OVER THE
    -- ENTIRE LEDGER UTXO SET. Cost is O(total
    -- UTxOs on chain), not O(UTxOs at the
    -- address). On mainnet that is millions of
    -- entries per call.
    --
    -- Use the local indexer's atomic
    -- @AtomicCageReader@ instead — it is O(M)
    -- where M = UTxOs at the address. This field
    -- is retained only for legacy callers that
    -- have not yet been migrated.
    , queryProtocolParams
        :: m (PParams ConwayEra)
    -- ^ Fetch current protocol parameters
    , evaluateTx
        :: Tx ConwayEra
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
