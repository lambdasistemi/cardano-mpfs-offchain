{-# LANGUAGE DataKinds #-}
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
    , TokenStateJSON (..)
    , tokenStateToJSON

      -- * Requests
    , RequestJSON (..)
    , requestToJSON
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
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import Servant.API (FromHttpApiData (..))

import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.Crypto.Hash.Class qualified as Crypto

import Cardano.MPFS.Core.Types
    ( Coin (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
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
            ( TE.decodeUtf8
                (B16.encode (SBS.fromShort sbs))
            )

instance FromJSON TokenIdJSON where
    parseJSON = withText "TokenId" $ \t ->
        case B16.decode (TE.encodeUtf8 t) of
            Right bs ->
                pure
                    $ TokenIdJSON
                    $ TokenId
                    $ AssetName
                    $ SBS.toShort bs
            Left err -> fail err

instance FromHttpApiData TokenIdJSON where
    parseUrlPiece t =
        case B16.decode (TE.encodeUtf8 t) of
            Right bs ->
                Right
                    $ TokenIdJSON
                    $ TokenId
                    $ AssetName
                    $ SBS.toShort bs
            Left err -> Left (T.pack err)

-- | JSON representation of on-chain token state.
data TokenStateJSON = TokenStateJSON
    { owner :: Text
    -- ^ Owner payment key hash (hex)
    , root :: Hex
    -- ^ Current trie root hash
    , maxFee :: Integer
    -- ^ Maximum fee in lovelace
    , processTime :: Integer
    -- ^ Processing window (ms)
    , retractTime :: Integer
    -- ^ Retract window (ms)
    }
    deriving (Eq, Show)

instance ToJSON TokenStateJSON where
    toJSON TokenStateJSON{..} =
        object
            [ "owner" .= owner
            , "root" .= root
            , "max_fee" .= maxFee
            , "process_time" .= processTime
            , "retract_time" .= retractTime
            ]

-- | Convert internal 'TokenState' to JSON type.
tokenStateToJSON :: TokenState -> TokenStateJSON
tokenStateToJSON
    TokenState
        { owner = tsOwner
        , root = tsRoot
        , maxFee = tsMaxFee
        , processTime = tsProcessTime
        , retractTime = tsRetractTime
        } =
        TokenStateJSON
            { owner = hashToHex tsOwner
            , root = Hex (unRoot tsRoot)
            , maxFee = unCoin tsMaxFee
            , processTime = tsProcessTime
            , retractTime = tsRetractTime
            }

-- | Render a 'KeyHash' as hex text.
hashToHex :: KeyHash 'Payment -> Text
hashToHex (KeyHash h) =
    Crypto.hashToTextAsHex h

-- | JSON representation of a pending request.
data RequestJSON = RequestJSON
    { rjToken :: TokenIdJSON
    -- ^ Token this request targets
    , rjOwner :: Text
    -- ^ Requester's payment key hash (hex)
    , rjKey :: Hex
    -- ^ Trie key
    , rjOperation :: Text
    -- ^ "insert", "delete", or "update"
    , rjValue :: Maybe Hex
    -- ^ New value (for insert/update)
    , rjFee :: Integer
    -- ^ Fee in lovelace
    , rjSubmittedAt :: Integer
    -- ^ POSIXTime (ms)
    }
    deriving (Eq, Show)

instance ToJSON RequestJSON where
    toJSON RequestJSON{..} =
        object
            [ "token" .= rjToken
            , "owner" .= rjOwner
            , "key" .= rjKey
            , "operation" .= rjOperation
            , "value" .= rjValue
            , "fee" .= rjFee
            , "submitted_at" .= rjSubmittedAt
            ]

-- | Convert internal 'Request' to JSON type.
requestToJSON :: Request -> RequestJSON
requestToJSON
    Request
        { requestToken = tok
        , requestOwner = own
        , requestKey = k
        , requestValue = op
        , requestFee = fee
        , requestSubmittedAt = sat
        } =
        let (opName, mVal) = case op of
                Insert v -> ("insert", Just (Hex v))
                Delete _v -> ("delete", Nothing)
                Update _old new' ->
                    ("update", Just (Hex new'))
        in  RequestJSON
                { rjToken = TokenIdJSON tok
                , rjOwner = hashToHex own
                , rjKey = Hex k
                , rjOperation = opName
                , rjValue = mVal
                , rjFee = unCoin fee
                , rjSubmittedAt = sat
                }
