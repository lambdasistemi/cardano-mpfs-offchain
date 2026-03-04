{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Mock.State
-- Description : In-memory mock State implementation
-- License     : Apache-2.0
--
-- In-memory implementations of the 'Tokens',
-- 'Requests', and 'Checkpoints' interfaces, each
-- backed by an 'IORef' holding a 'Map'. Useful for
-- unit tests and development where persistent
-- RocksDB state is not desired. See
-- "Cardano.MPFS.Indexer.Persistent" for the
-- production implementation.
module Cardano.MPFS.Mock.State
    ( -- * Construction
      mkMockTokens
    , mkMockRequests
    , mkMockCheckpoints
    , mkMockState
    ) where

import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map

import Cardano.Ledger.Slot (SlotNo)
import Cardano.Ledger.TxIn (TxIn)

import Cardano.MPFS.Core.Types
    ( BlockId
    , Request (..)
    , TokenId
    , TokenState
    )
import Cardano.MPFS.State
    ( Checkpoints (..)
    , Requests (..)
    , State (..)
    , Tokens (..)
    )

-- | Create a mock 'Tokens IO' backed by an 'IORef'.
mkMockTokens :: IO (Tokens IO)
mkMockTokens = do
    ref <-
        newIORef (Map.empty :: Map TokenId TokenState)
    pure
        Tokens
            { getToken = \tid ->
                Map.lookup tid <$> readIORef ref
            , putToken = \tid ts ->
                modifyIORef' ref (Map.insert tid ts)
            , removeToken =
                modifyIORef' ref . Map.delete
            , listTokens =
                Map.keys <$> readIORef ref
            }

-- | Create a mock 'Requests IO' backed by an 'IORef'.
mkMockRequests :: IO (Requests IO)
mkMockRequests = do
    ref <-
        newIORef (Map.empty :: Map TxIn Request)
    pure
        Requests
            { getRequest = \txin ->
                Map.lookup txin <$> readIORef ref
            , putRequest = \txin req ->
                modifyIORef' ref (Map.insert txin req)
            , removeRequest =
                modifyIORef' ref . Map.delete
            , requestsByToken = \tid ->
                filter
                    (\r -> requestToken r == tid)
                    . Map.elems
                    <$> readIORef ref
            }

-- | Create a mock 'Checkpoints IO' backed by an
-- 'IORef'.
mkMockCheckpoints :: IO (Checkpoints IO)
mkMockCheckpoints = do
    ref <-
        newIORef
            ( Nothing
                :: Maybe (SlotNo, BlockId)
            )
    pure
        Checkpoints
            { getCheckpoint = readIORef ref
            , putCheckpoint = \s b ->
                modifyIORef'
                    ref
                    (const (Just (s, b)))
            }

-- | Create a complete mock 'State IO' bundling
-- tokens, requests, and checkpoints.
mkMockState :: IO (State IO)
mkMockState = do
    tok <- mkMockTokens
    req <- mkMockRequests
    cp <- mkMockCheckpoints
    pure
        State
            { tokens = tok
            , requests = req
            , checkpoints = cp
            }
