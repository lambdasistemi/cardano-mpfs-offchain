-- | Thin FFI bindings to Material UI v5 (`@mui/material`) components.
-- |
-- | Each binding wraps a MUI React component so it can be applied to a
-- | props record and an array of child `JSX` nodes. The props rows are
-- | left open (`forall r`) so call sites pass exactly the MUI props they
-- | need; type safety on individual props is intentionally traded away for
-- | the flexibility of MUI's very large prop surface. No protocol logic
-- | lives here — this module is purely presentational glue.
module MpfsSpa.Material
  ( cssBaseline
  , container
  , box
  , stack
  , typography
  , button
  , appBar
  , toolbar
  , themeProvider
  , Theme
  , defaultTheme
  ) where

import React.Basic (JSX)

-- | Opaque MUI theme object produced by `createTheme`.
foreign import data Theme :: Type

-- | The default light theme used across the app.
foreign import defaultTheme :: Theme

-- | `<CssBaseline />` — normalises browser styling, MUI baseline.
foreign import cssBaseline :: JSX

foreign import container
  :: forall r. Record r -> Array JSX -> JSX

foreign import box
  :: forall r. Record r -> Array JSX -> JSX

foreign import stack
  :: forall r. Record r -> Array JSX -> JSX

foreign import typography
  :: forall r. Record r -> Array JSX -> JSX

foreign import button
  :: forall r. Record r -> Array JSX -> JSX

foreign import appBar
  :: forall r. Record r -> Array JSX -> JSX

foreign import toolbar
  :: forall r. Record r -> Array JSX -> JSX

foreign import themeProvider
  :: forall r. Record r -> Array JSX -> JSX
