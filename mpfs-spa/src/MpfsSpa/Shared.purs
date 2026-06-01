-- | Types and small view helpers shared between the app shell and the tabs.
-- |
-- | Lives in its own module so `App` and the per-tab modules can both depend
-- | on it without an import cycle. All session state is in-memory only —
-- | nothing here is persisted (no localStorage / cookies / IndexedDB).
module MpfsSpa.Shared
  ( WalletState
  , Env
  , Remote(..)
  , remoteView
  , centred
  ) where

import Prelude

import Data.Maybe (Maybe)
import React.Basic (JSX)
import React.Basic.DOM as R

import MpfsSpa.CageHelpers (CageHelpers)
import MpfsSpa.Http (Config)
import MpfsSpa.Material as M
import MpfsSpa.Wallet.Cip30 (WalletApi)

-- | The connected wallet, held only in memory for the session.
type WalletState =
  { api :: WalletApi
  , name :: String
  , address :: String
  , networkId :: Int
  , balance :: Maybe String
  }

-- | Ambient dependencies handed to every tab: the protocol boundary and the
-- | server config.
type Env =
  { helpers :: CageHelpers
  , cfg :: Config
  }

-- | A four-state remote value for async server reads.
data Remote a
  = NotAsked
  | Loading
  | Failure String
  | Success a

-- | Render a `Remote` value: spinner while loading, error alert on failure,
-- | the supplied view on success, nothing before the first request.
remoteView :: forall a. Remote a -> (a -> JSX) -> JSX
remoteView r view = case r of
  NotAsked -> mempty
  Loading -> centred [ M.circularProgress {} ]
  Failure msg ->
    M.alert { severity: "error", sx: { mt: 2 } } [ R.text msg ]
  Success a -> view a

-- | Centre a row of children horizontally with vertical padding.
centred :: Array JSX -> JSX
centred children =
  M.box
    { sx: { display: "flex", justifyContent: "center", py: 3 } }
    children
