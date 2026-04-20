-- |
-- Module      : Cardano.MPFS.TxBuilder
-- Description : Transaction construction interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for building MPFS
-- protocol transactions. Each method corresponds to a
-- cage-protocol action (boot, request insert\/delete,
-- update, retract, end). The real implementation lives
-- in "Cardano.MPFS.TxBuilder.Real"; the mock in
-- "Cardano.MPFS.Mock.TxBuilder". Returned transactions
-- are unsigned — callers add key witnesses before
-- submission.
module Cardano.MPFS.TxBuilder
    ( -- * Transaction builder interface
      TxBuilder (..)

      -- * Proof-bearing unsigned-transaction bundles
    , UnsignedTxBundle (..)
    , BundleSnapshot (..)
    , WitnessedInput (..)
    , TrieFact (..)
    , bareTxBundle
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Api.Tx (Tx)

import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId
    , ConwayEra
    , SlotNo
    , TokenId
    , TxIn
    )

-- | Interface for constructing proof-bearing
-- transactions for all MPFS protocol operations.
-- Every method takes the 'BundleSnapshot' that the
-- caller has already resolved so the returned
-- 'UnsignedTxBundle' bakes in a verified root and
-- indexed chain point.
data TxBuilder m = TxBuilder
    { bootToken
        :: BundleSnapshot
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Create a new MPFS token
    , requestInsert
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Request inserting a key-value pair
    , requestDelete
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Request deleting a key (key, old value)
    , requestUpdate
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> ByteString
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Request updating a key (key, old val, new val)
    , updateToken
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Process pending requests for a token
    , retractRequest
        :: BundleSnapshot
        -> TxIn
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Cancel a pending request
    , rejectRequests
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Reject Phase 3 requests for a token
    , endToken
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m UnsignedTxBundle
    -- ^ Retire an MPFS token
    }

-- | Proof-bearing unsigned-transaction bundle.
--
-- The output of a proof-carrying tx builder. A client
-- verifies the bundle offline by checking that every
-- consumed input's 'WitnessedInput' and every
-- 'TrieFact' resolves against the bundle's
-- 'BundleSnapshot' root, then re-derives that
-- 'bundleTx' is the unique transaction implied by
-- those witnesses.
--
-- Ledger-native: 'bundleTx' is a full Conway-era
-- 'Tx' and 'WitnessedInput' references ledger 'TxIn'
-- and serialized 'TxOut' CBOR. Serialization for the
-- HTTP boundary lives in "Cardano.MPFS.HTTP.Types".
data UnsignedTxBundle = UnsignedTxBundle
    { bundleTx :: Tx ConwayEra
    -- ^ Unsigned transaction ready for key witnesses
    , bundleSnapshot :: BundleSnapshot
    -- ^ UTxO-CSMT root and chain point the bundle
    -- was built against
    , bundleInputs :: [WitnessedInput]
    -- ^ Every consumed input with its UTxO-CSMT
    -- inclusion proof against 'bundleSnapshot'
    , bundleFacts :: [TrieFact]
    -- ^ Trie facts the builder relied on; empty for
    -- endpoints that consume no trie state
    }

-- | Indexed snapshot a bundle's proofs verify
-- against. Matches the shape of the HTTP-level
-- @VerificationSnapshot@ but carries raw bytes rather
-- than hex-wrapped JSON values.
data BundleSnapshot = BundleSnapshot
    { snapshotUtxoRoot :: ByteString
    -- ^ UTxO-CSMT Merkle root (raw bytes)
    , snapshotSlot :: SlotNo
    -- ^ Indexed chain point slot
    , snapshotBlockId :: BlockId
    -- ^ Indexed chain point block id
    }
    deriving (Eq, Show)

-- | A consumed input with its UTxO-CSMT inclusion
-- proof against 'snapshotUtxoRoot'.
data WitnessedInput = WitnessedInput
    { witnessedRef :: TxIn
    -- ^ The spent input's reference
    , witnessedTxOut :: ByteString
    -- ^ Serialized ledger CBOR of the consumed 'TxOut'
    , witnessedCsmtProof :: ByteString
    -- ^ CSMT inclusion proof against the root
    }
    deriving (Eq, Show)

-- | A trie fact the builder depended on. Surfaces
-- for trie-dependent endpoints so clients can verify
-- the MPF claim alongside the consumed UTxOs.
data TrieFact = TrieFact
    { factKey :: ByteString
    -- ^ Trie key the builder looked up
    , factValue :: Maybe ByteString
    -- ^ Value bound to 'factKey', or 'Nothing' for
    -- membership-of-absence facts
    , factMpfProof :: ByteString
    -- ^ MPF inclusion\/exclusion proof against the
    -- relevant trie root
    }
    deriving (Eq, Show)

-- | Wrap a bare unsigned 'Tx' as a proof-bearing
-- bundle with no bundled inputs or facts. Used by
-- migration steps that have not yet produced
-- witnesses, and by tests that only care about the
-- tx body.
bareTxBundle
    :: BundleSnapshot -> Tx ConwayEra -> UnsignedTxBundle
bareTxBundle snap tx =
    UnsignedTxBundle
        { bundleTx = tx
        , bundleSnapshot = snap
        , bundleInputs = []
        , bundleFacts = []
        }
