-- |
-- Module      : Cardano.MPFS.CLI.Hex
-- Description : Hex-encoded command-line argument reader.
--
-- A small wrapper that validates @--key@ / @--value@ style arguments as
-- hex at parse time, before any network call, so malformed input fails
-- fast with a clear message.
module Cardano.MPFS.CLI.Hex
    ( HexArg (..)
    , hexReader
    , hexArgText
    , decodeHexText
    , encodeHexText
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Options.Applicative (ReadM, eitherReader)

-- | A command-line argument that has been validated as hex and decoded
-- to its raw bytes.
newtype HexArg = HexArg {hexBytes :: ByteString}
    deriving stock (Eq, Show)

-- | An @optparse-applicative@ reader that decodes a hex string to bytes,
-- failing the parse with a clear message on non-hex input.
hexReader :: ReadM HexArg
hexReader = eitherReader $ \s ->
    case B16.decode (TE.encodeUtf8 (T.pack s)) of
        Right bs -> Right (HexArg bs)
        Left err -> Left ("invalid hex: " <> err)

-- | Re-encode a 'HexArg' to its lowercase hex text (for JSON output).
hexArgText :: HexArg -> Text
hexArgText = TE.decodeUtf8 . B16.encode . hexBytes

-- | Decode hex text to bytes, for arguments validated outside the
-- parser (e.g. @--token@).
decodeHexText :: Text -> Either String ByteString
decodeHexText = B16.decode . TE.encodeUtf8

-- | Encode raw bytes to lowercase hex text (for JSON output).
encodeHexText :: ByteString -> Text
encodeHexText = TE.decodeUtf8 . B16.encode
