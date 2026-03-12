{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Types
-- Description : JSON request/response types for HTTP API
-- License     : Apache-2.0
--
-- Data types for the HTTP API layer. Each type has
-- 'ToJSON' and 'FromJSON' instances for transport.
-- These are decoupled from the internal domain types
-- to allow independent evolution of the wire format.
module Cardano.MPFS.HTTP.Types
    ( -- * Status
      StatusResponse (..)
    ) where

import Data.Aeson
    ( ToJSON (..)
    , object
    , (.=)
    )
import Data.Word (Word64)

import Cardano.MPFS.HTTP.Encoding (Hex (..))

-- | Response for @GET \/status@.
data StatusResponse = StatusResponse
    { tipSlot :: Word64
    -- ^ Current chain tip slot
    , tipBlockId :: Hex
    -- ^ Current chain tip block hash (hex)
    , checkpointSlot :: Maybe Word64
    -- ^ Last processed checkpoint slot
    , checkpointBlockId :: Maybe Hex
    -- ^ Last processed checkpoint block hash
    }
    deriving (Eq, Show)

instance ToJSON StatusResponse where
    toJSON StatusResponse{..} =
        object
            [ "tip_slot" .= tipSlot
            , "tip_block_id" .= tipBlockId
            , "checkpoint_slot" .= checkpointSlot
            , "checkpoint_block_id"
                .= checkpointBlockId
            ]
