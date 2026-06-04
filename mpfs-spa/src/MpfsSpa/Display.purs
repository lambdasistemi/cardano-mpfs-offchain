-- | Presentation-only helpers for turning wire-level fields into readable UI.
module MpfsSpa.Display
  ( currentTimeMillis
  , displayUtf8Hex
  , encodeUtf8Hex
  , formatAgeMillis
  ) where

import Prelude

import Data.Int (floor)
import Data.Maybe (fromMaybe)
import Data.Nullable (Nullable, toMaybe)
import Effect (Effect)

foreign import _decodeUtf8Hex :: String -> Nullable String

foreign import currentTimeMillis :: Effect Number

foreign import encodeUtf8Hex :: String -> String

displayUtf8Hex :: String -> String
displayUtf8Hex hex =
  fromMaybe ("0x" <> hex) (toMaybe (_decodeUtf8Hex hex))

formatAgeMillis :: Number -> String
formatAgeMillis millis =
  let
    seconds = max 0 (floor (millis / 1000.0))
    minutes = seconds `div` 60
    hours = minutes `div` 60
  in
    if seconds < 60 then
      show seconds <> "s old"
    else if minutes < 60 then
      show minutes <> "m old"
    else
      show hours <> "h " <> show (minutes `mod` 60) <> "m old"
