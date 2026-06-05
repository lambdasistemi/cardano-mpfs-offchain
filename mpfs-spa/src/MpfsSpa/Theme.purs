module MpfsSpa.Theme
  ( initialThemeMode
  , storeThemeMode
  ) where

import Prelude

import Effect (Effect)

foreign import initialThemeMode :: Effect String
foreign import storeThemeMode :: String -> Effect Unit
