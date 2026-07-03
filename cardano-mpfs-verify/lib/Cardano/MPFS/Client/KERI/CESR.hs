-- |
-- Module      : Cardano.MPFS.Client.KERI.CESR
-- Description : Minimal CESR primitive reader for KERI event parsing.
--
-- Stub covering only the Ed25519 public-key (code @B@, 44 chars),
-- Ed25519 signature (code @0B@, 88 chars), and self-addressing
-- identifier (code @E@, 44 chars) primitives.  Full KEL replay is
-- in scope for #369; this module provides the types and decoding
-- surface needed by the #365 spike.
--
-- CESR encoding recap:
--  * All primitives are base64url-unpadded strings.
--  * 1-char codes (B, E …): total 44 chars, 1 lead byte stripped → 32 bytes.
--  * 2-char codes (0B …): total 88 chars, 2 lead bytes stripped → 64 bytes.
--  * Lead bytes are always @0x00@; the code character(s) replace the
--    base64url encoding of the lead byte(s) so the code is self-framing.
module Cardano.MPFS.Client.KERI.CESR
    ( Primitive (..)
    , parsePrimitive
    ) where

import Data.ByteArray.Encoding
    ( Base (Base64URLUnpadded)
    , convertFromBase
    )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS

-- | A decoded CESR primitive (raw bytes, no further interpretation).
data Primitive
    = -- | Ed25519 non-transferable public key — 32 bytes (code @B@).
      Ed25519PublicKey !ByteString
    | -- | Ed25519 signature — 64 bytes (code @0B@).
      Ed25519Signature !ByteString
    | -- | Self-addressing identifier (blake2b-256 digest) — 32 bytes (code @E@).
      SelfAddressing !ByteString
    deriving stock (Show, Eq)

-- | Decode one CESR primitive from the start of a base64url stream.
-- Returns the primitive and the unconsumed remainder.
parsePrimitive :: ByteString -> Either String (Primitive, ByteString)
parsePrimitive bs = case BS.uncons bs of
    Nothing -> Left "CESR: empty input"
    Just (b, _rest) -> case b of
        0x42 -> decode1 bs Ed25519PublicKey -- 'B'
        0x45 -> decode1 bs SelfAddressing -- 'E'
        0x30 -> case BS.index bs 1 of -- '0'
            0x42 -> decode2 bs Ed25519Signature -- "0B"
            c -> Left ("CESR: unknown 2-char code 0" <> show c)
        c -> Left ("CESR: unknown 1-char code " <> show c)

-- | Decode a 1-char-code primitive (44 chars total, 1 lead byte).
decode1
    :: ByteString
    -> (ByteString -> Primitive)
    -> Either String (Primitive, ByteString)
decode1 bs ctor =
    let (chunk, rest) = BS.splitAt 44 bs
    in  if BS.length chunk < 44
            then Left "CESR: truncated 44-char primitive"
            else
                -- Restore the 'A' that the code replaced (lead byte = 0x00 → 'A' in base64url)
                let fixed = BS.cons 0x41 (BS.tail chunk) -- 'A'
                in  case decodeB64Url fixed of
                        Left err -> Left ("CESR: base64url error: " <> err)
                        Right raw ->
                            -- Strip 1 lead byte; expect 32 bytes
                            let payload = BS.drop 1 raw
                            in  if BS.length payload == 32
                                    then Right (ctor payload, rest)
                                    else Left "CESR: decoded size mismatch (expected 32)"

-- | Decode a 2-char-code primitive (88 chars total, 2 lead bytes).
decode2
    :: ByteString
    -> (ByteString -> Primitive)
    -> Either String (Primitive, ByteString)
decode2 bs ctor =
    let (chunk, rest) = BS.splitAt 88 bs
    in  if BS.length chunk < 88
            then Left "CESR: truncated 88-char primitive"
            else
                -- Restore 'AA' for the two zero lead bytes
                let fixed = BS.pack [0x41, 0x41] <> BS.drop 2 chunk
                in  case decodeB64Url fixed of
                        Left err -> Left ("CESR: base64url error: " <> err)
                        Right raw ->
                            let payload = BS.drop 2 raw
                            in  if BS.length payload == 64
                                    then Right (ctor payload, rest)
                                    else Left "CESR: decoded size mismatch (expected 64)"

decodeB64Url :: ByteString -> Either String ByteString
decodeB64Url input =
    case convertFromBase Base64URLUnpadded input :: Either String ByteString of
        Left err -> Left err
        Right bs -> Right bs
