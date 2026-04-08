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
    ) where

import Data.ByteString (ByteString)

import Cardano.MPFS.Core.Types (TxIn)
import Cardano.MPFS.Provider (Provider)
import Cardano.MPFS.State (State)
import Cardano.MPFS.Submitter (Submitter)
import Cardano.MPFS.Trie (TrieManager)
import Cardano.MPFS.TxBuilder (TxBuilder)
import Cardano.UTxOCSMT.Application.Metrics (Metrics)

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
    , utxoExists :: TxIn -> m Bool
    -- ^ Check if a UTxO exists in the indexed state
    , resolveUtxo
        :: TxIn -> m (Maybe ByteString)
    -- ^ Resolve a TxIn to its CBOR-encoded TxOut
    , awaitUtxo
        :: TxIn -> Maybe Int -> m (Maybe ByteString)
    -- ^ Block until a UTxO appears or timeout expires
    , utxoRoot :: m (Maybe ByteString)
    -- ^ Current CSMT Merkle root hash (raw bytes)
    , utxoProof
        :: TxIn -> m (Maybe ByteString)
    -- ^ CSMT inclusion proof for a TxIn (raw bytes)
    , readMetrics :: m (Maybe Metrics)
    -- ^ Current metrics snapshot (if available)
    }
