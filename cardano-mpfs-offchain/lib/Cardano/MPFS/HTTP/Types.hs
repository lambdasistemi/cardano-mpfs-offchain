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

      -- * Tokens
    , TokenIdJSON (..)
    ) where

import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withText
    , (.=)
    )
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Text.Encoding qualified as T
import Data.Word (Word64)

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Core.Types (TokenId (..))
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

-- | Hex-encoded token identifier for JSON transport.
newtype TokenIdJSON = TokenIdJSON
    { unTokenIdJSON :: TokenId
    }
    deriving (Eq, Show)

instance ToJSON TokenIdJSON where
    toJSON (TokenIdJSON (TokenId (AssetName sbs))) =
        toJSON
            ( T.decodeUtf8
                (B16.encode (SBS.fromShort sbs))
            )

instance FromJSON TokenIdJSON where
    parseJSON = withText "TokenId" $ \t ->
        case B16.decode (T.encodeUtf8 t) of
            Right bs ->
                pure
                    $ TokenIdJSON
                    $ TokenId
                    $ AssetName
                    $ SBS.toShort bs
            Left err -> fail err
