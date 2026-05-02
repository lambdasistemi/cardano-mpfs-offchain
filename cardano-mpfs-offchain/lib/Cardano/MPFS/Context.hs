-- |
-- Module      : Cardano.MPFS.Context
-- Description : Facade bundling all singleton interfaces
-- License     : Apache-2.0
--
-- Top-level product type that bundles every service
-- interface into a single value. Constructed once at
-- application startup (see "Cardano.MPFS.Application")
-- and threaded to all callers. Parametric in the
-- effect @m@ so both real ('IO') and mock
-- ('Control.Monad.State.Strict.StateT') code share
-- the same record shape.
module Cardano.MPFS.Context
    ( -- * Context
      Context (..)

      -- * Atomic cage reader
    , AtomicCageReader
    ) where

import Data.ByteString (ByteString)

import Cardano.MPFS.Core.Types (TxIn)
import Cardano.MPFS.Provider (Provider)
import Cardano.MPFS.State (State)
import Cardano.MPFS.Submitter (Submitter)
import Cardano.MPFS.Trie (TrieManager)
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , TxBuilder
    )
import Cardano.MPFS.TxBuilder.Config (CageConfig)
import Cardano.UTxOCSMT.Application.Metrics (Metrics)

-- | Read a 'BundleSnapshot' (CSMT root + chain
-- checkpoint), the resolved @TxOut@ bytes, and the
-- CSMT inclusion proof for each supplied 'TxIn' in
-- ONE database transaction. Proof-bearing handlers
-- MUST use this — never combine a separately-fetched
-- snapshot with separately-fetched proofs or
-- separately-fetched UTxO state.
--
-- Returning the @TxOut@ bytes here is what lets the
-- server avoid cardano-node's @LocalStateQuery@
-- @GetUTxOByAddress@ entirely on the tx-build hot
-- path. That node-side query is a linear scan over
-- the entire ledger UTxO set; calling it from a
-- proof-bearing endpoint is forbidden (#252) both
-- on cost and on torn-read grounds — the indexer is
-- the single source of truth, and one transaction
-- against it gives a coherent view.
--
-- Returns 'Nothing' if the indexer is not ready
-- (no checkpoint or no root yet) or any input is
-- not present in the current snapshot.
type AtomicCageReader m =
    [TxIn]
    -> m
        ( Maybe
            ( BundleSnapshot
            , [ ( TxIn
                , ByteString
                , ByteString
                )
              ]
            )
        )

-- | Top-level context bundling all service
-- interfaces. Parametric in the effect @m@.
data Context m = Context
    { provider :: Provider m
    -- ^ Blockchain query operations
    , trieManager :: TrieManager m
    -- ^ Per-token trie management
    , state :: State m
    -- ^ Token and request state tracking
    , submitter :: Submitter m
    -- ^ Transaction submission
    , txBuilder :: TxBuilder m
    -- ^ Transaction construction
    , cfgCage :: ~CageConfig
    -- ^ Static cage script config (used by sweep
    -- and any handler that needs raw script bytes
    -- or per-cage parameterisation at runtime).
    -- Distinct field name from 'AppConfig.cageConfig'
    -- to avoid ambiguous-field errors at use sites.
    -- Marked lazy with @~@ so that mock contexts
    -- which never reach the sweep handler can
    -- leave this field as @error "…"@ without
    -- crashing under @StrictData@.
    , utxoExists :: TxIn -> m Bool
    -- ^ Check if a UTxO exists in the indexed state
    , resolveUtxo
        :: TxIn -> m (Maybe ByteString)
    -- ^ Resolve a TxIn to its CBOR-encoded TxOut
    , awaitUtxo
        :: TxIn -> Maybe Int -> m (Maybe ByteString)
    -- ^ Block until a UTxO appears or timeout expires
    , utxoRoot :: m (Maybe ByteString)
    -- ^ Current CSMT Merkle root hash (raw bytes).
    -- Use only for non-proof-bearing /status reads.
    , utxoProof
        :: TxIn -> m (Maybe ByteString)
    -- ^ CSMT inclusion proof for a TxIn. NOT atomic
    -- with a separately-fetched snapshot — kept for
    -- handlers that have not yet been migrated to
    -- 'atomicCageReader' (#250). New code must use
    -- 'atomicCageReader'.
    , atomicCageReader :: AtomicCageReader m
    -- ^ Atomic snapshot + proofs reader for
    -- proof-bearing handlers (see 'AtomicCageReader').
    , readMetrics :: m (Maybe Metrics)
    -- ^ Current metrics snapshot (if available)
    }
