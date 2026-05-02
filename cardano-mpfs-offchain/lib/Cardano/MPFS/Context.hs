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

      -- * Atomic indexer reader
    , AtomicCageReader
    , AtomicCageRead (..)
    , AtomicReaderError (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Address (Addr)

import Cardano.MPFS.Core.Types (TxIn)
import Cardano.MPFS.Provider (Provider)
import Cardano.MPFS.State (State)
import Cardano.MPFS.Submitter (Submitter)
import Cardano.MPFS.Trie (TrieManager)
import Cardano.MPFS.TxBuilder (BundleSnapshot, TxBuilder)
import Cardano.MPFS.TxBuilder.Config (CageConfig)
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
    -- ^ Current CSMT Merkle root hash (raw bytes)
    , utxoProof
        :: TxIn -> m (Maybe ByteString)
    -- ^ CSMT inclusion proof for a TxIn (raw bytes)
    , atomicCageReader :: AtomicCageReader m
    -- ^ Single-transaction reader over the indexer.
    -- Returns the @BundleSnapshot@ together with
    -- @(TxIn, TxOut bytes, CSMT proof)@ triples for
    -- every wallet UTxO at the requested address —
    -- all from one coherent read. Used by the
    -- proof-bearing tx builders to eliminate
    -- between-read race windows. See spec
    -- @specs/249-atomic-boot-handler@.
    , readMetrics :: m (Maybe Metrics)
    -- ^ Current metrics snapshot (if available)
    }

-- | Reader returning a coherent indexer view scoped
-- to one address. The implementation MUST perform
-- every read inside a single @RunTransaction@ call
-- over the unified column families projected to the
-- UTxO columns; the snapshot's CSMT root MUST be the
-- root every returned proof verifies against.
type AtomicCageReader m =
    Addr
    -> m (Either AtomicReaderError AtomicCageRead)

-- | Successful payload of an 'AtomicCageReader'
-- call. Each @acrInputs@ entry is the triple
-- @(input ref, TxOut CBOR bytes, CSMT inclusion
-- proof bytes)@.
data AtomicCageRead = AtomicCageRead
    { acrSnapshot :: BundleSnapshot
    -- ^ Snapshot the proofs are anchored to
    , acrInputs :: [(TxIn, ByteString, ByteString)]
    -- ^ Wallet inputs at the address with their
    -- resolved @TxOut@ bytes and CSMT proofs
    }
    deriving (Show)

-- | Failure modes of an 'AtomicCageReader' call.
-- Each constructor maps to a distinct, deterministic
-- HTTP error so clients can distinguish
-- "indexer not ready" from "no UTxOs at address"
-- from "indexer corruption".
data AtomicReaderError
    = -- | Indexer has no chain checkpoint yet
      -- (server just started, no block applied).
      AtomicReaderNoCheckpoint
    | -- | CSMT has no Merkle root yet
      -- (empty / un-bootstrapped CSMT).
      AtomicReaderRootMissing
    | -- | Address has zero UTxOs in the indexer
      -- (unfunded or fully-spent address).
      AtomicReaderNoUtxos
    | -- | A leaf was found in the CSMT but its
      -- resolved @TxOut@ bytes are absent from the
      -- KV column family (indexer corruption — fail
      -- loud).
      AtomicReaderKvMissing TxIn
    deriving (Show, Eq)
