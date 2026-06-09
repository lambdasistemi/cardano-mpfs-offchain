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
    , readUtxoWitness
    , readStateUtxoAt
    , readNamedRequestUtxo
    , readRequestUtxosAt
    , readUtxoSetAt
    , readTrieFact
    , readTrieFacts
    , readWalletInputsAt
    , ResolvedUtxoSet

      -- * UTxO key helpers
    , addressScopedLeafKey
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.List (stripPrefix)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe)

import Control.Lens
    ( review
    , (^.)
    )

import Cardano.Ledger.Address (Addr, serialiseAddr)
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , valueTxOutL
    )
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    , serialize
    )
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )

import CSMT.Core.CBOR (renderCompletenessProof)
import CSMT.Core.Types
    ( Indirect (..)
    , Key
    )
import CSMT.Hashes
    ( generateInclusionProof
    , renderHash
    )
import CSMT.Hashes.Types (Hash)
import CSMT.Interface (FromKV (..))
import CSMT.Proof.Completeness
    ( CompletenessProof
    , collectValues
    , generateProof
    )

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
    , ConwayEra
    , PolicyID
    , SlotNo (..)
    , TokenId (..)
    , TxIn
    )
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.Trie.Persistent
    ( mkUnifiedTrie
    , tokenHexPrefix
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    )
import Cardano.MPFS.TxBuilder qualified as Tx

-- | Request-address UTxO set witness read from the indexer:
-- each entry carries only @(TxIn, TxOut CBOR)@ because the
-- enclosing completeness proof attests the whole address subtree.
type ResolvedUtxoSet =
    ([(TxIn, ByteString)], ByteString)

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
            catMaybes <$> traverse (loadOne fkv addrKey) indirects
  where
    loadOne fkv addrKey leaf = do
        let lazyKey =
                case addressScopedLeafKey fkv addrKey leaf of
                    Right key -> key
                    Left e ->
                        error
                            $ "readWalletInputsAt: "
                                <> e
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

-- | Resolve a 'TxIn' and its CSMT inclusion proof inside the
-- indexer transaction. Returns 'Nothing' when the UTxO is absent.
readUtxoWitness :: TxIn -> IndexerTx (Maybe ResolvedWalletInput)
readUtxoWitness txIn =
    IndexerTx
        $ mapColumns InUtxo
        $ do
            let fkv = fromKV context
                key = serialize (natVersion @11) txIn
            mTxOut <- query KVCol key
            case mTxOut of
                Nothing -> pure Nothing
                Just txOutBytes -> do
                    mProof <-
                        generateInclusionProof
                            fkv
                            KVCol
                            CSMTCol
                            key
                    proofBytes <- case mProof of
                        Just (_, proof) -> pure proof
                        Nothing ->
                            error
                                $ "readUtxoWitness: \
                                  \indexer corruption — \
                                  \KV column contains a \
                                  \TxOut but \
                                  \generateInclusionProof \
                                  \returned Nothing \
                                  \(input: "
                                    <> show txIn
                                    <> ")"
                    pure
                        $ Just
                            ( txIn
                            , BSL.toStrict txOutBytes
                            , proofBytes
                            )

-- | Read the current state UTxO for a token at the state
-- validator address and return it with a CSMT inclusion proof.
readStateUtxoAt
    :: Addr
    -> PolicyID
    -> TokenId
    -> IndexerTx (Maybe ResolvedWalletInput)
readStateUtxoAt stateAddr policyId tid =
    findState <$> readWalletInputsAt stateAddr
  where
    findState [] = Nothing
    findState (row@(_, txOutBytes, _) : rest)
        | carriesToken txOutBytes = Just row
        | otherwise = findState rest
    carriesToken txOutBytes =
        case decodeTxOut txOutBytes of
            Right txOut -> txOutCarriesToken policyId tid txOut
            Left err ->
                error
                    $ "readStateUtxoAt: indexer KV column \
                      \produced a state-address leaf whose \
                      \value did not decode as a TxOut: "
                        <> show err

-- | Read the named request UTxO at a given request address
-- by walking the CSMT subtree and selecting the entry whose
-- 'TxIn' matches the target. Returns 'Nothing' when the
-- address subtree does not carry the requested 'TxIn'.
readNamedRequestUtxo
    :: Addr -> TxIn -> IndexerTx (Maybe ResolvedWalletInput)
readNamedRequestUtxo reqAddr targetTxIn =
    findUtxo <$> readWalletInputsAt reqAddr
  where
    findUtxo [] = Nothing
    findUtxo (row@(txIn, _, _) : rest)
        | txIn == targetTxIn = Just row
        | otherwise = findUtxo rest

-- | Read all currently indexed request-address UTxOs with
-- individual CSMT inclusion proofs. The update facts route
-- consumes request UTxOs as transaction inputs rather than as
-- a completeness witness, so it needs the same resolved input
-- shape as wallet funding reads.
readRequestUtxosAt
    :: Addr -> IndexerTx [ResolvedWalletInput]
readRequestUtxosAt = readWalletInputsAt

-- | Walk the UTxO-CSMT subtree at an address and produce
-- the enumerated UTxOs plus a production
-- prefix-completeness proof for that exact subtree.
readUtxoSetAt :: Addr -> IndexerTx ResolvedUtxoSet
readUtxoSetAt addr =
    IndexerTx
        $ mapColumns InUtxo
        $ do
            let fkv = fromKV context
                addrKey =
                    hashAddressKey (serialiseAddr addr)
            indirects <-
                collectValues CSMTCol [] addrKey
            entries <- traverse (loadOne fkv addrKey) indirects
            mProof <- generateProof CSMTCol [] addrKey
            let proofBytes = case mProof of
                    Just proof ->
                        renderCompletenessProof
                            (proof :: CompletenessProof Hash)
                    Nothing ->
                        error
                            "readUtxoSetAt: indexer \
                            \corruption — \
                            \generateProof returned \
                            \Nothing for address \
                            \subtree"
            pure (entries, proofBytes)
  where
    loadOne fkv addrKey leaf = do
        let lazyKey =
                case addressScopedLeafKey fkv addrKey leaf of
                    Right key -> key
                    Left e ->
                        error
                            $ "readUtxoSetAt: "
                                <> e
            txInDecoded =
                case decodeFull
                    (natVersion @11)
                    lazyKey of
                    Right t -> t
                    Left e ->
                        error
                            $ "readUtxoSetAt: \
                              \indexer KV column \
                              \produced a leaf whose \
                              \key did not decode as \
                              \a TxIn: "
                                <> show e
        mTxOut <- query KVCol lazyKey
        case mTxOut of
            Nothing ->
                error
                    $ "readUtxoSetAt: indexer \
                      \corruption — CSMT contains a \
                      \leaf at this address whose KV \
                      \column has no TxOut bytes \
                      \(input: "
                        <> show txInDecoded
                        <> ")"
            Just txOutBytes ->
                pure (txInDecoded, BSL.toStrict txOutBytes)

-- | Read one MPF trie fact for a token/key inside the
-- indexer transaction.
--
-- The value comes from 'TrieRawValues' through the persistent
-- trie lookup path, so clients receive the original raw value
-- bytes rather than the internal MPF content hash.
readTrieFact
    :: TokenId -> ByteString -> IndexerTx Tx.TrieFact
readTrieFact tid key =
    IndexerTx
        $ mapColumns InCage
        $ do
            let trie = mkUnifiedTrie (tokenHexPrefix tid)
            mValue <- Trie.lookup trie key
            mProof <- Trie.getProof trie key
            case mProof of
                Just (Trie.Proof proofBytes) ->
                    pure
                        Tx.TrieFact
                            { Tx.factKey = key
                            , Tx.factValue = mValue
                            , Tx.factMpfProof = proofBytes
                            }
                Nothing ->
                    error
                        "readTrieFact: persistent trie \
                        \could not produce an MPF proof"

-- | Enumerate all raw facts for a token's trie inside the
-- indexer transaction.
readTrieFacts
    :: TokenId -> IndexerTx [(ByteString, ByteString)]
readTrieFacts tid =
    IndexerTx
        $ mapColumns InCage
        $ Trie.enumerate
        $ mkUnifiedTrie
        $ tokenHexPrefix tid

addressScopedLeafKey
    :: FromKV key value hash
    -> Key
    -> Indirect leaf
    -> Either String key
addressScopedLeafKey fkv addressKey Indirect{jump} =
    case stripPrefix addressKey jump of
        Just key ->
            Right $ review (isoK fkv) key
        Nothing ->
            Left
                "indexer CSMT column produced a leaf whose key \
                \did not start with queried address prefix"

decodeTxOut
    :: ByteString -> Either DecoderError (TxOut ConwayEra)
decodeTxOut =
    decodeFull (natVersion @11) . BSL.fromStrict

txOutCarriesToken
    :: PolicyID -> TokenId -> TxOut ConwayEra -> Bool
txOutCarriesToken policyId (TokenId assetName) txOut =
    case txOut ^. valueTxOutL of
        MaryValue _ (MultiAsset ma) ->
            case Map.lookup policyId ma of
                Just assets -> Map.member assetName assets
                Nothing -> False
