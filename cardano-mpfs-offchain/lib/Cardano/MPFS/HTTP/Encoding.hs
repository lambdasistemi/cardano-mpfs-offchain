{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Encoding
-- Description : Hex encoding newtypes for HTTP JSON
-- License     : Apache-2.0
--
-- Newtype wrappers for hex-encoding 'ByteString'
-- values in JSON. Used by the HTTP API types to
-- transport binary data as hex strings over JSON.
module Cardano.MPFS.HTTP.Encoding
    ( -- * Hex-encoded ByteString
      Hex (..)
    ) where

import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , withText
    )
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.Text.Encoding qualified as T

-- | A 'ByteString' that serialises to\/from JSON
-- as a hex-encoded string.
newtype Hex = Hex
    { unHex :: ByteString
    }
    deriving (Eq, Show)

instance ToJSON Hex where
    toJSON (Hex bs) =
        toJSON (T.decodeUtf8 (B16.encode bs))

instance FromJSON Hex where
    parseJSON = withText "Hex" $ \t ->
        case B16.decode (T.encodeUtf8 t) of
            Right bs -> pure (Hex bs)
            Left err -> fail err
