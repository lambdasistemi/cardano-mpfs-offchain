-- | Runtime configuration sourced from the browser, with safe defaults.
-- |
-- | `baseUrl` for the MPFS server is read from `window.MPFS_BASE_URL` (set in
-- | `index.html` or by the host page); it defaults to same-origin. The
-- | `placeholderCageConfig` is a stand-in for the mock era — its fields are
-- | ignored by the mock `CageHelpers`. The real config (script bytes, hash,
-- | phase windows, network) is wired when the wasm cage-helper bridge lands.
module MpfsSpa.Config
  ( serverConfig
  , placeholderCageConfig
  ) where

import Prelude

import Effect (Effect)
import MpfsSpa.Http (Config)
import MpfsSpa.Types (CageConfig)

-- | Read the server base URL from the page; defaults to same-origin ("").
serverConfig :: Effect Config
serverConfig = do
  base <- _baseUrl
  pure { baseUrl: base }

foreign import _baseUrl :: Effect String

-- | Placeholder cage config for the mock era (ignored by the mock).
placeholderCageConfig :: CageConfig
placeholderCageConfig =
  { cageScriptBytes: ""
  , requestScriptBytes: ""
  , cfgScriptHash: ""
  , defaultProcessTime: 0
  , defaultRetractTime: 0
  , defaultTip: 0
  , network: "testnet"
  }
