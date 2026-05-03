{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Reads
-- Description : Composable read primitives for the indexer
-- License     : Apache-2.0
--
-- A monadic action type ('IndexerTx') over the
-- @UnifiedColumns@ database transaction together with
-- a small library of read primitives. Handlers compose
-- these primitives inside a single 'IndexerTx' value
-- and discharge the whole composition through
-- 'Cardano.MPFS.Context.runIndexerTx', which opens
-- exactly one underlying RocksDB transaction.
--
-- This is the seam that satisfies the atomicity claim
-- of the proof-bearing API (see spec
-- @specs\/249-atomic-boot-handler@): each handler's
-- snapshot, KV reads, and proof generation observe a
-- single coherent indexer state.
--
-- Adding a new read shape (e.g. for the update or
-- reject handlers) means adding a primitive here and
-- composing it from the handler — never opening a
-- second transaction.
module Cardano.MPFS.Indexer.Reads
    ( -- * Indexer transaction
      IndexerTx (..)

      -- * Read primitives
    , readCheckpoint
    , readMerkleRoot
    , readSnapshot
    , readWalletInputsAt
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Maybe (catMaybes, fromMaybe)

import Control.Lens (review)

import Cardano.Ledger.Address (Addr, serialiseAddr)
import Cardano.Ledger.Binary
    ( decodeFull
    , natVersion
    )

import CSMT.Core.Types (Indirect (..))
import CSMT.Hashes
    ( generateInclusionProof
    , renderHash
    )
import CSMT.Hashes.Types (Hash)
import CSMT.Interface (FromKV (..))
import CSMT.Proof.Completeness (collectValues)

import Database.KV.Transaction
    ( mapColumns
    , query
    )
import Database.KV.Transaction qualified as L
    ( Transaction
    )

import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    , queryMerkleRoot
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( context
    , hashAddressKey
    )
import Cardano.UTxOCSMT.Ouroboros.Types (Point)
import ChainFollower.Rollbacks.Store qualified as CFStore
import ChainFollower.Rollbacks.Types (RollbackPoint (..))

import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    )

-- | An action over the unified-columns database
-- transaction. The @cf@ and @op@ existentials are
-- hidden so handlers can pass an 'IndexerTx' across
-- module boundaries; they are bound at use-site by
-- 'Cardano.MPFS.Context.runIndexerTx'.
--
-- Composing two 'IndexerTx' values via the 'Monad'
-- instance keeps the composition inside the same
-- single underlying transaction, which is what makes
-- atomicity hold.
newtype IndexerTx a = IndexerTx
    { unIndexerTx
        :: forall cf op
         . L.Transaction
            IO
            cf
            ( UnifiedColumns
                Point
                Hash
                BSL.ByteString
                BSL.ByteString
            )
            op
            a
    }

instance Functor IndexerTx where
    fmap f (IndexerTx m) = IndexerTx (fmap f m)

instance Applicative IndexerTx where
    pure x = IndexerTx (pure x)
    IndexerTx f <*> IndexerTx x =
        IndexerTx (f <*> x)

instance Monad IndexerTx where
    return = pure
    IndexerTx m >>= k =
        IndexerTx
            $ m
                >>= \a ->
                    case k a of
                        IndexerTx m' -> m'

-- | Read the indexer's chain checkpoint (the slot and
-- block id of the last block applied by the chain
-- follower). 'Nothing' when no block has been applied
-- yet — the indexer is not ready.
readCheckpoint
    :: IndexerTx (Maybe (SlotNo, BlockId))
readCheckpoint = IndexerTx $ do
    history <- CFStore.queryHistory InRollbacks
    pure $ case history of
        [] -> Nothing
        pts ->
            let (s, rp) = last pts
            in  Just
                    ( s
                    , fromMaybe
                        (BlockId mempty)
                        (rpMeta rp)
                    )

-- | Read the current UTxO-CSMT Merkle root.
-- 'Nothing' when the CSMT has not been bootstrapped
-- (genesis seeding has not run yet).
readMerkleRoot :: IndexerTx (Maybe ByteString)
readMerkleRoot =
    IndexerTx
        $ mapColumns InUtxo
        $ fmap renderHash
            <$> queryMerkleRoot (hashing context)

-- | Read a 'BundleSnapshot' anchoring all reads in
-- this transaction. 'Nothing' when either the
-- checkpoint or the root are missing — the handler
-- maps that to a 503.
readSnapshot :: IndexerTx (Maybe BundleSnapshot)
readSnapshot = do
    mCp <- readCheckpoint
    mRoot <- readMerkleRoot
    pure $ do
        (slot, blockId) <- mCp
        root <- mRoot
        pure
            BundleSnapshot
                { snapshotUtxoRoot = root
                , snapshotSlot = slot
                , snapshotBlockId = blockId
                }

-- | Walk the UTxO-CSMT subtree at the address prefix
-- and produce, for each wallet UTxO at that address,
-- the @(TxIn, TxOut bytes, CSMT inclusion proof)@
-- triple consumed by the tx builders.
--
-- Cost is proportional to the number of UTxOs at the
-- given address, NOT the total UTxO set on chain
-- (see issue #252).
--
-- Indexer corruption (a leaf in the CSMT whose KV
-- entry is missing, or a leaf whose proof generation
-- fails) is treated as an invariant violation rather
-- than a soft skip: 'readWalletInputsAt' throws a
-- descriptive 'error' identifying the offending TxIn.
-- This mirrors the original review-fix on the
-- pre-redesign code path: silently emitting an empty
-- proof would produce a response that fails offline
-- verification with no diagnostic; failing loud at
-- the read site lets ops surface the corruption
-- immediately.
readWalletInputsAt
    :: Addr -> IndexerTx [ResolvedWalletInput]
readWalletInputsAt addr =
    IndexerTx
        $ mapColumns InUtxo
        $ do
            let fkv = fromKV context
                addrKey =
                    hashAddressKey (serialiseAddr addr)
            indirects <-
                collectValues CSMTCol [] addrKey
            catMaybes <$> traverse (loadOne fkv) indirects
  where
    loadOne fkv Indirect{jump} = do
        let lazyKey = review (isoK fkv) jump
            txInDecoded =
                case decodeFull
                    (natVersion @11)
                    lazyKey of
                    Right t -> t
                    Left e ->
                        error
                            $ "readWalletInputsAt: \
                              \indexer KV column \
                              \produced a leaf whose \
                              \key did not decode as \
                              \a TxIn: "
                                <> show e
        mTxOut <- query KVCol lazyKey
        case mTxOut of
            Nothing ->
                error
                    $ "readWalletInputsAt: indexer \
                      \corruption — CSMT contains a \
                      \leaf at this address whose KV \
                      \column has no TxOut bytes \
                      \(input: "
                        <> show txInDecoded
                        <> ")"
            Just txOutBytes -> do
                mProof <-
                    generateInclusionProof
                        fkv
                        KVCol
                        CSMTCol
                        lazyKey
                let proofBytes = case mProof of
                        Just (_, p) -> p
                        Nothing ->
                            error
                                $ "readWalletInputsAt: \
                                  \indexer corruption — \
                                  \collectValues found \
                                  \a leaf at this \
                                  \address but \
                                  \generateInclusionProof \
                                  \returned Nothing \
                                  \(input: "
                                    <> show txInDecoded
                                    <> ")"
                pure
                    $ Just
                        ( txInDecoded
                        , BSL.toStrict txOutBytes
                        , proofBytes
                        )
