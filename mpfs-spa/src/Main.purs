-- | Application entry point.
-- |
-- | Mounts the React (react-basic-hooks) component tree into the static
-- | `#root` element from `dist/index.html`, wiring the app to the mock
-- | `CageHelpers` and the server config read from the page. Swapping to the
-- | real WASM cage-helper bridge (when %351 ships) changes only the `helpers`
-- | binding below.
module Main where

import Prelude

import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Exception (throw)
import React.Basic.DOM.Client (createRoot, renderRoot)
import Web.DOM.NonElementParentNode (getElementById)
import Web.HTML (window)
import Web.HTML.HTMLDocument (toNonElementParentNode)
import Web.HTML.Window (document)

import MpfsSpa.App (mkApp)
import MpfsSpa.CageHelpers.Mock (mockCageHelpers)
import MpfsSpa.Config (serverConfig)

main :: Effect Unit
main = do
  doc <- document =<< window
  mRoot <- getElementById "root" (toNonElementParentNode doc)
  case mRoot of
    Nothing -> throw "MPFS SPA: #root element not found"
    Just rootEl -> do
      cfg <- serverConfig
      app <- mkApp { helpers: mockCageHelpers, cfg }
      reactRoot <- createRoot rootEl
      renderRoot reactRoot (app unit)
