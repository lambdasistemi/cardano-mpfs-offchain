{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.State
-- Description : Token and request state tracking interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for tracking the
-- off-chain projection of token state, pending requests,
-- and chain sync checkpoints. Implementations:
-- "Cardano.MPFS.Indexer.Persistent" (RocksDB-backed)
-- and "Cardano.MPFS.Mock.State" (in-memory for tests).
module Cardano.MPFS.State
    ( -- * Combined state
      State (..)

      -- * Token state
    , Tokens (..)

      -- * Request state
    , Requests (..)

      -- * Checkpoint state
    , Checkpoints (..)

      -- * Natural transformations
    , hoistState
    , hoistTokens
    , hoistRequests
    , hoistCheckpoints
    ) where

import Cardano.MPFS.Core.Types
    ( BlockId
    , LocatedRequest
    , LocatedTokenState
    , SlotNo
    , TokenId
    , TxIn
    )

-- | Combined state interface bundling token,
-- request, and checkpoint tracking.
data State m = State
    { tokens :: Tokens m
    -- ^ Token state operations
    , requests :: Requests m
    -- ^ Request state operations
    , checkpoints :: Checkpoints m
    -- ^ Checkpoint operations
    }

-- | Interface for managing token state.
data Tokens m = Tokens
    { getToken :: TokenId -> m (Maybe LocatedTokenState)
    -- ^ Look up a token's current located state
    , putToken :: TokenId -> LocatedTokenState -> m ()
    -- ^ Store or update a token's located state
    , removeToken :: TokenId -> m ()
    -- ^ Remove a token
    , listTokens :: m [TokenId]
    -- ^ List all known tokens
    }

-- | Interface for managing pending requests.
data Requests m = Requests
    { getRequest
        :: TxIn -> m (Maybe LocatedRequest)
    -- ^ Look up a located request by its UTxO reference
    , putRequest
        :: LocatedRequest -> m ()
    -- ^ Store a new located request
    , removeRequest :: TxIn -> m ()
    -- ^ Remove a fulfilled or retracted request
    , requestsByToken
        :: TokenId -> m [LocatedRequest]
    -- ^ List all located pending requests for a token
    }

-- | Interface for chain sync checkpoints.
data Checkpoints m = Checkpoints
    { getCheckpoint
        :: m (Maybe (SlotNo, BlockId))
    -- ^ Get the last processed checkpoint:
    -- (slot, blockId).
    , putCheckpoint
        :: SlotNo -> BlockId -> m ()
    -- ^ Store a new checkpoint
    }

-- | Lift a 'State' across a natural transformation.
hoistState :: (forall a. m a -> n a) -> State m -> State n
hoistState f State{..} =
    State
        { tokens = hoistTokens f tokens
        , requests = hoistRequests f requests
        , checkpoints =
            hoistCheckpoints f checkpoints
        }

-- | Lift 'Tokens' across a natural transformation.
hoistTokens
    :: (forall a. m a -> n a)
    -> Tokens m
    -> Tokens n
hoistTokens f Tokens{..} =
    Tokens
        { getToken = f . getToken
        , putToken = \tid lts -> f (putToken tid lts)
        , removeToken = f . removeToken
        , listTokens = f listTokens
        }

-- | Lift 'Requests' across a natural transformation.
hoistRequests
    :: (forall a. m a -> n a)
    -> Requests m
    -> Requests n
hoistRequests f Requests{..} =
    Requests
        { getRequest = f . getRequest
        , putRequest = f . putRequest
        , removeRequest = f . removeRequest
        , requestsByToken = f . requestsByToken
        }

-- | Lift 'Checkpoints' across a natural
-- transformation.
hoistCheckpoints
    :: (forall a. m a -> n a)
    -> Checkpoints m
    -> Checkpoints n
hoistCheckpoints f Checkpoints{..} =
    Checkpoints
        { getCheckpoint = f getCheckpoint
        , putCheckpoint = \s b ->
            f (putCheckpoint s b)
        }
