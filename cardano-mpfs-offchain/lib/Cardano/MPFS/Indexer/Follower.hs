{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Follower
-- Description : Block processor for cage protocol events
-- License     : Apache-2.0
--
-- Core block-processing pipeline for the cage indexer.
-- All functions are generic over 'Monad m' so they
-- compose into a single DB transaction:
--
--   1. Extract Conway-era transactions ('extractConwayTxs',
--      pure)
--   2. Resolve spent UTxOs and detect cage events
--      ('detectCageBlockEvents')
--   3. Compute inverse ops and apply state\/trie
--      mutations ('applyCageBlockEvents')
--   4. Rollback via 'applyCageInverses'
--
-- The only IO-specific parts are the UTxO resolver
-- callback (provided by the caller) and
-- 'extractConwayTxs' which is pure.
module Cardano.MPFS.Indexer.Follower
    ( -- * Detection (Monad m)
      detectCageBlockEvents
    , detectFromTx

      -- * Application (Monad m)
    , applyCageBlockEvents
    , applyCageEvent
    , computeInverse
    , applyRequestOp

      -- * Rollback (Monad m)
    , applyCageInverses

      -- * Transaction extraction (pure)
    , extractConwayTxs
    ) where

import Control.Monad (void)
import Data.Foldable (toList)
import Data.Maybe (catMaybes)
import Data.Set qualified as Set
import Lens.Micro ((^.))

import Cardano.Ledger.Api.Tx (Tx, bodyTxL)
import Cardano.Ledger.Api.Tx.Body (inputsTxBodyL)
import Cardano.Ledger.Block (bbody)
import Cardano.Ledger.Core (fromTxSeq)
import Cardano.Ledger.Hashes (ScriptHash)
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Shelley.Ledger
    ( ShelleyBlock (..)
    )

import Cardano.MPFS.Core.Types
    ( ConwayEra
    , Operation (..)
    , Request (..)
    , TokenId
    , TokenState (..)
    , TxIn
    , TxOut
    )
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp (..)
    , detectCageEvents
    )
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Cardano.Node.Client.Types qualified as NodeTypes

-- | Extract Conway-era transactions from a multi-era
-- Cardano block. Returns empty for non-Conway blocks.
extractConwayTxs
    :: NodeTypes.Block -> [Tx ConwayEra]
extractConwayTxs = \case
    O.BlockConway (ShelleyBlock raw _) ->
        toList (fromTxSeq (bbody raw))
    _ -> []

-- --------------------------------------------------------
-- Detection (Monad m)
-- --------------------------------------------------------

-- | Detect all cage events from a block. Resolves
-- spent UTxOs via the provided callback and
-- inspects transactions for cage-protocol activity.
detectCageBlockEvents
    :: (Monad m)
    => ScriptHash
    -- ^ Cage script hash
    -> (TxIn -> m (Maybe (TxOut ConwayEra)))
    -- ^ UTxO resolver for spent inputs
    -> [Tx ConwayEra]
    -- ^ Conway transactions from the block
    -> m [CageEvent]
detectCageBlockEvents scriptHash resolveUtxo txs =
    concat
        <$> traverse
            (detectFromTx scriptHash resolveUtxo)
            txs

-- | Detect cage events from a single transaction.
detectFromTx
    :: (Monad m)
    => ScriptHash
    -- ^ Cage script hash
    -> (TxIn -> m (Maybe (TxOut ConwayEra)))
    -- ^ UTxO resolver for spent inputs
    -> Tx ConwayEra
    -- ^ Transaction to inspect
    -> m [CageEvent]
detectFromTx scriptHash resolveUtxo tx = do
    let inputSet = tx ^. bodyTxL . inputsTxBodyL
    resolved <-
        resolveInputs
            resolveUtxo
            (Set.toList inputSet)
    pure $ detectCageEvents scriptHash resolved tx

-- | Resolve a list of 'TxIn' references to their
-- 'TxOut' values using the UTxO resolver.
resolveInputs
    :: (Monad m)
    => (TxIn -> m (Maybe (TxOut ConwayEra)))
    -> [TxIn]
    -> m [(TxIn, TxOut ConwayEra)]
resolveInputs resolve =
    fmap catMaybes . traverse go
  where
    go txIn = do
        mOut <- resolve txIn
        pure $ fmap (txIn,) mOut

-- --------------------------------------------------------
-- Application (Monad m)
-- --------------------------------------------------------

-- | Apply all cage events from a block, returning
-- collected inverse ops for rollback. Each event
-- is processed sequentially: compute inverse THEN
-- apply mutations.
applyCageBlockEvents
    :: (Monad m)
    => State m
    -- ^ Cage state interface
    -> TrieManager m
    -- ^ Trie manager for per-token tries
    -> [CageEvent]
    -- ^ Detected events
    -> m [CageInverseOp]
applyCageBlockEvents st tm events =
    concat <$> traverse (processEvent st tm) events

-- | Process a single cage event: compute its cage
-- inverse, apply the event (collecting trie
-- inverses), and return both.
processEvent
    :: (Monad m)
    => State m
    -> TrieManager m
    -> CageEvent
    -> m [CageInverseOp]
processEvent st tm evt = do
    cageInvs <- computeInverse st evt
    trieInvs <- applyCageEvent st tm evt
    pure $ cageInvs ++ trieInvs

-- | Compute inverse operations for a cage event,
-- reading the current state before the event is
-- applied.
computeInverse
    :: (Monad m)
    => State m
    -> CageEvent
    -> m [CageInverseOp]
computeInverse
    State
        { tokens = Tokens{getToken}
        , requests = Requests{getRequest}
        } = \case
        CageBoot tid _ts ->
            pure [InvRemoveToken tid]
        CageRequest txIn _req ->
            pure [InvRemoveRequest txIn]
        CageUpdate tid _newRoot consumed -> do
            mTs <- getToken tid
            let restoreRoot = case mTs of
                    Just ts ->
                        [InvRestoreRoot tid (root ts)]
                    Nothing -> []
            restoreReqs <-
                concat
                    <$> traverse
                        ( \txIn -> do
                            mReq <- getRequest txIn
                            pure $ case mReq of
                                Just req ->
                                    [ InvRestoreRequest
                                        txIn
                                        req
                                    ]
                                Nothing -> []
                        )
                        consumed
            pure $ restoreRoot ++ restoreReqs
        CageRetract txIn -> do
            mReq <- getRequest txIn
            pure $ case mReq of
                Just req ->
                    [InvRestoreRequest txIn req]
                Nothing -> []
        CageBurn tid -> do
            mTs <- getToken tid
            pure $ case mTs of
                Just ts ->
                    [InvRestoreToken tid ts]
                Nothing -> []

-- | Apply a cage event to the state and trie
-- manager. Returns trie-level inverse operations
-- for rollback.
applyCageEvent
    :: (Monad m)
    => State m
    -> TrieManager m
    -> CageEvent
    -> m [CageInverseOp]
applyCageEvent st tm = \case
    CageBoot tid ts -> do
        putToken (tokens st) tid ts
        createTrie tm tid
        pure []
    CageRequest txIn req -> do
        putRequest (requests st) txIn req
        pure []
    CageUpdate tid newRoot consumed -> do
        -- Apply trie mutations, collecting inverses
        trieInvs <-
            withTrie tm tid $ \trie ->
                concat
                    <$> mapM
                        ( \txIn -> do
                            mReq <-
                                getRequest
                                    (requests st)
                                    txIn
                            case mReq of
                                Just req ->
                                    applyRequestOp
                                        tid
                                        trie
                                        req
                                Nothing -> pure []
                        )
                        consumed
        -- Remove consumed requests
        mapM_
            (removeRequest (requests st))
            consumed
        -- Update token root
        mTs <- getToken (tokens st) tid
        case mTs of
            Just ts ->
                putToken
                    (tokens st)
                    tid
                    ts{root = newRoot}
            Nothing -> pure ()
        pure trieInvs
    CageRetract txIn -> do
        removeRequest (requests st) txIn
        pure []
    CageBurn tid -> do
        removeToken (tokens st) tid
        hideTrie tm tid
        pure []

-- | Apply a request's operation to a trie,
-- returning inverse ops that can undo the mutation.
applyRequestOp
    :: (Monad m)
    => TokenId
    -- ^ Token owning the trie
    -> Trie m
    -- ^ Handle to the token's trie
    -> Request
    -- ^ Request whose operation to apply
    -> m [CageInverseOp]
applyRequestOp
    tid
    Trie
        { insert = trieInsert
        , delete = trieDelete
        , lookup = trieLookup
        }
    Request{requestKey, requestValue} =
        case requestValue of
            Insert v -> do
                oldVal <- trieLookup requestKey
                void $ trieInsert requestKey v
                pure $ case oldVal of
                    Nothing ->
                        [InvTrieDelete tid requestKey]
                    Just old ->
                        [ InvTrieInsert
                            tid
                            requestKey
                            old
                        ]
            Delete _ -> do
                oldVal <- trieLookup requestKey
                void $ trieDelete requestKey
                pure $ case oldVal of
                    Nothing -> []
                    Just old ->
                        [ InvTrieInsert
                            tid
                            requestKey
                            old
                        ]
            Update _ newV -> do
                oldVal <- trieLookup requestKey
                void $ trieDelete requestKey
                void $ trieInsert requestKey newV
                pure $ case oldVal of
                    Nothing ->
                        [InvTrieDelete tid requestKey]
                    Just old ->
                        [ InvTrieInsert
                            tid
                            requestKey
                            old
                        ]

-- --------------------------------------------------------
-- Rollback (Monad m)
-- --------------------------------------------------------

-- | Apply inverse operations for rollback,
-- restoring the state to what it was before the
-- events. Ops should be applied in reverse
-- chronological order.
applyCageInverses
    :: (Monad m)
    => State m
    -- ^ Cage state interface
    -> TrieManager m
    -- ^ Trie manager for per-token tries
    -> [CageInverseOp]
    -- ^ Inverse ops to replay
    -> m ()
applyCageInverses st tm = mapM_ applyInv
  where
    applyInv = \case
        InvRestoreToken tid ts -> do
            putToken (tokens st) tid ts
            unhideTrie tm tid
        InvRemoveToken tid -> do
            removeToken (tokens st) tid
            deleteTrie tm tid
        InvRestoreRequest txIn req ->
            putRequest (requests st) txIn req
        InvRemoveRequest txIn ->
            removeRequest (requests st) txIn
        InvRestoreRoot tid r -> do
            mTs <- getToken (tokens st) tid
            case mTs of
                Just ts ->
                    putToken
                        (tokens st)
                        tid
                        ts{root = r}
                Nothing -> pure ()
        InvTrieInsert tid key val ->
            withTrie tm tid $ \trie ->
                void $ insert trie key val
        InvTrieDelete tid key ->
            withTrie tm tid $ \trie ->
                void $ delete trie key
