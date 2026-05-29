{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.API.Encoding
-- Description : Hex encoding newtypes for HTTP JSON
-- License     : Apache-2.0
--
-- Newtype wrappers for hex-encoding 'ByteString'
-- values in JSON. Used by the HTTP API types to
-- transport binary data as hex strings over JSON.
module Cardano.MPFS.API.Encoding
    ( -- * Hex-encoded ByteString
      Hex (..)
    ) where

import Control.Lens ((&), (?~))
import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , withText
    )
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.Proxy (Proxy (..))
import Data.Swagger
    ( ToParamSchema (..)
    , ToSchema (..)
    , description
    , toSchema
    )
import Data.Swagger qualified as Swagger
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Servant.API (FromHttpApiData (..), ToHttpApiData (..))

-- | A 'ByteString' that serialises to\/from JSON
-- as a hex-encoded string.
newtype Hex = Hex
    { unHex :: ByteString
    }
    deriving (Eq, Show)

instance ToJSON Hex where
    toJSON (Hex bs) =
        toJSON (TE.decodeUtf8 (B16.encode bs))

instance FromJSON Hex where
    parseJSON = withText "Hex" $ \t ->
        case B16.decode (TE.encodeUtf8 t) of
            Right bs -> pure (Hex bs)
            Left err -> fail err

instance FromHttpApiData Hex where
    parseUrlPiece t =
        case B16.decode (TE.encodeUtf8 t) of
            Right bs -> Right (Hex bs)
            Left err -> Left (T.pack err)

instance ToHttpApiData Hex where
    toUrlPiece (Hex bs) = TE.decodeUtf8 (B16.encode bs)

instance ToSchema Hex where
    declareNamedSchema _ =
        pure
            $ Swagger.NamedSchema (Just "Hex")
            $ toSchema (Proxy @String)
            & description
                ?~ "Hex-encoded byte string"

instance ToParamSchema Hex where
    toParamSchema _ =
        Swagger.toParamSchema (Proxy @String)
