-- | Application entry point.
-- |
-- | Mounts the React (react-basic-hooks) component tree into the static
-- | `#root` element from `dist/index.html`. For this bootstrap slice the
-- | component is a minimal "hello Material" smoke screen proving that
-- | Material UI v5 renders through the PureScript FFI; later slices replace
-- | the body with the four-tab MPFS shell.
module Main where

import Prelude

import Effect (Effect)
import Effect.Console (log)
import Effect.Exception (throw)
import Data.Maybe (Maybe(..))
import React.Basic.DOM as R
import React.Basic.DOM.Client (createRoot, renderRoot)
import React.Basic.Hooks (JSX, component)
import Web.DOM.NonElementParentNode (getElementById)
import Web.HTML (window)
import Web.HTML.HTMLDocument (toNonElementParentNode)
import Web.HTML.Window (document)

import MpfsSpa.Material
  ( appBar
  , button
  , container
  , cssBaseline
  , defaultTheme
  , stack
  , themeProvider
  , toolbar
  , typography
  )

main :: Effect Unit
main = do
  doc <- document =<< window
  mRoot <- getElementById "root" (toNonElementParentNode doc)
  case mRoot of
    Nothing -> throw "MPFS SPA: #root element not found"
    Just rootEl -> do
      app <- mkApp
      reactRoot <- createRoot rootEl
      renderRoot reactRoot (app unit)

-- | Root component. No hooks yet — a static Material smoke screen.
mkApp :: Effect (Unit -> JSX)
mkApp = component "App" \_ -> pure shell

shell :: JSX
shell =
  themeProvider { theme: defaultTheme }
    [ cssBaseline
    , appBar { position: "static" }
        [ toolbar {}
            [ typography { variant: "h6", component: "div" } [ R.text "MPFS" ] ]
        ]
    , container { maxWidth: "sm", sx: { mt: 4 } }
        [ stack { spacing: 2 }
            [ typography { variant: "h5" } [ R.text "Hello, Material UI" ]
            , button
                { variant: "contained"
                , onClick: log "MPFS SPA: button clicked"
                }
                [ R.text "It works" ]
            ]
        ]
    ]
