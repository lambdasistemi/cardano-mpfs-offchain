-- |
-- Module      : Cardano.MPFS.CLI.Key
-- Description : Bech32 .skey loading into an Ed25519 signing key.
--
-- Reads a Bech32-encoded ed25519 signing key (CIP-5 @ed25519_sk1…@) from
-- a file and decodes it to a 'SignKeyDSIGN' 'Ed25519DSIGN'. This is the
-- only key format the CLI accepts; no hardware wallet, encrypted
-- keystore, or TextEnvelope JSON.
module Cardano.MPFS.CLI.Key
    ( KeyError (..)
    , loadSigningKey
    , decodeSigningKey
    ) where

import Cardano.Crypto.DSIGN
    ( Ed25519DSIGN
    , SignKeyDSIGN
    , rawDeserialiseSignKeyDSIGN
    )
import Codec.Binary.Bech32 (dataPartToBytes, decode)
import Data.Bifunctor (first)
import Data.ByteString qualified as BS
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.IO qualified as TIO

-- | Why a key file could not be turned into a signing key.
data KeyError
    = -- | The file held no non-whitespace content.
      KeyFileEmpty
    | -- | The content was not valid Bech32.
      KeyBech32Error String
    | -- | The Bech32 data part did not decode to bytes.
      KeyNotBech32Bytes
    | -- | The decoded bytes were not a valid Ed25519 signing key.
      KeyInvalidSigningKey Int
    deriving stock (Eq, Show)

-- | Read a Bech32 @.skey@ file and decode it to a signing key.
loadSigningKey
    :: FilePath
    -> IO (Either KeyError (SignKeyDSIGN Ed25519DSIGN))
loadSigningKey path = decodeSigningKey <$> TIO.readFile path

-- | Decode the textual content of a Bech32 @.skey@ file. Pure, so it is
-- directly testable.
decodeSigningKey
    :: Text
    -> Either KeyError (SignKeyDSIGN Ed25519DSIGN)
decodeSigningKey raw
    | T.null stripped = Left KeyFileEmpty
    | otherwise = do
        (_hrp, dp) <-
            first (KeyBech32Error . show) (decode stripped)
        bytes <-
            maybe (Left KeyNotBech32Bytes) Right (dataPartToBytes dp)
        maybe
            (Left (KeyInvalidSigningKey (BS.length bytes)))
            Right
            (rawDeserialiseSignKeyDSIGN bytes)
  where
    stripped = T.strip raw
