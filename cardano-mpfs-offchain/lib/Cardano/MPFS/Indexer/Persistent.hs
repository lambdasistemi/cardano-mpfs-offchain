{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Persistent
-- Description : RocksDB-backed persistent State
-- License     : Apache-2.0
--
-- Implements the 'State' interface on top of
-- RocksDB via @rocksdb-kv-transactions@.
--
-- Two construction modes:
--
--   * 'mkTransactionalState': operates inside a
--     @'Transaction' m cf 'AllColumns' ops@ — each
--     read\/write composes into the caller's transaction,
--     enabling one-block-one-commit atomicity.
--   * 'mkPersistentState': wraps the transactional
--     layer via 'hoistState', auto-committing each
--     operation individually (used outside block
--     processing).
--
-- Column families are defined in
-- "Cardano.MPFS.Indexer.Columns" and serialization
-- codecs in "Cardano.MPFS.Indexer.Codecs".
module Cardano.MPFS.Indexer.Persistent
    ( -- * Construction
      mkPersistentState

      -- * Transactional construction
    , mkTransactionalState
    ) where

import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , nextEntry
    )
import Database.KV.Transaction
    ( RunTransaction (..)
    , Transaction
    , delete
    , insert
    , iterating
    , query
    )

import Cardano.MPFS.Core.Types (Request (..))
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , CageCheckpoint (..)
    )
import Cardano.MPFS.State
    ( Checkpoints (..)
    , Requests (..)
    , State (..)
    , Tokens (..)
    , hoistState
    )

-- | Create a persistent 'State' backed by RocksDB.
-- Each operation runs in its own serialized
-- transaction via 'RunTransaction'.
mkPersistentState
    :: RunTransaction IO cf AllColumns op
    -- ^ Transaction runner from @rocksdb-kv-transactions@
    -> State IO
mkPersistentState RunTransaction{runTransaction = run} =
    hoistState run mkTransactionalState

-- | A 'State' whose operations run directly in the
-- 'Transaction' monad without committing. Compose
-- with other transactional operations into a single
-- atomic block commit.
mkTransactionalState
    :: (Monad m)
    => State
        (Transaction m cf AllColumns ops)
mkTransactionalState =
    State
        { tokens = mkTransactionalTokens
        , requests = mkTransactionalRequests
        , checkpoints = mkTransactionalCheckpoints
        }

-- --------------------------------------------------------
-- Transactional Tokens
-- --------------------------------------------------------

mkTransactionalTokens
    :: (Monad m)
    => Tokens
        (Transaction m cf AllColumns ops)
mkTransactionalTokens =
    Tokens
        { getToken = query CageTokens
        , putToken = insert CageTokens
        , removeToken = delete CageTokens
        , listTokens =
            iterating CageTokens allKeys
        }
  where
    allKeys = do
        me <- firstEntry
        case me of
            Nothing -> pure []
            Just (Entry k _) -> goK [k]
    goK acc = do
        me <- nextEntry
        case me of
            Nothing -> pure (reverse acc)
            Just (Entry k _) -> goK (k : acc)

-- --------------------------------------------------------
-- Transactional Requests
-- --------------------------------------------------------

mkTransactionalRequests
    :: (Monad m)
    => Requests
        (Transaction m cf AllColumns ops)
mkTransactionalRequests =
    Requests
        { getRequest = query CageRequests
        , putRequest = insert CageRequests
        , removeRequest = delete CageRequests
        , requestsByToken =
            iterating CageRequests . filterReqs
        }
  where
    filterReqs tid = do
        me <- firstEntry
        case me of
            Nothing -> pure []
            Just (Entry _ v) ->
                goR tid (add tid v [])
    goR tid acc = do
        me <- nextEntry
        case me of
            Nothing -> pure (reverse acc)
            Just (Entry _ v) ->
                goR tid (add tid v acc)
    add tid v acc
        | requestToken v == tid = v : acc
        | otherwise = acc

-- --------------------------------------------------------
-- Transactional Checkpoints
-- --------------------------------------------------------

mkTransactionalCheckpoints
    :: (Monad m)
    => Checkpoints
        (Transaction m cf AllColumns ops)
mkTransactionalCheckpoints =
    Checkpoints
        { getCheckpoint = do
            mc <- query CageCfg ()
            pure $ case mc of
                Nothing -> Nothing
                Just CageCheckpoint{..} ->
                    Just
                        ( checkpointSlot
                        , checkpointBlockId
                        , rollbackSlots
                        )
        , putCheckpoint = \s b slots ->
            insert CageCfg ()
                $ CageCheckpoint s b slots
        }
