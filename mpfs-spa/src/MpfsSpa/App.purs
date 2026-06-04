-- | Application shell.
-- |
-- | Holds the in-memory session state (connected wallet, token list, selected
-- | token, active tab) and routes between the four tabs. The End tab is only
-- | present in the tab bar while a wallet is connected. Nothing is persisted —
-- | a reload re-prompts the wallet and re-fetches the token list.
module MpfsSpa.App (mkApp) where

import Prelude

import Data.Array (index, length, mapWithIndex)
import Data.Either (either)
import Data.Maybe (Maybe(..), isJust, maybe)
import Effect (Effect)
import Effect.Aff (Milliseconds(..), delay, launchAff_)
import Effect.Class (liftEffect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useEffectOnce, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.Http (getTokens)
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, Remote(..), WalletState)
import MpfsSpa.Tab.Connect (mkConnectTab)
import MpfsSpa.Tab.End (mkEndTab)
import MpfsSpa.Tab.Facts (mkFactsTab)
import MpfsSpa.Tab.Tokens (mkTokensTab)
import MpfsSpa.Types (TokenId)

-- | Build the root component against the given environment.
mkApp :: Env -> Effect (Unit -> JSX)
mkApp env = do
  connectTab <- mkConnectTab
  tokensTab <- mkTokensTab
  factsTab <- mkFactsTab
  endTab <- mkEndTab

  component "App" \_ -> React.do
    tabIndex /\ setTab <- useState 0
    wallet /\ setWallet <- useState (Nothing :: Maybe WalletState)
    tokens /\ setTokens <- useState (NotAsked :: Remote (Array TokenId))
    selected /\ setSelected <- useState (Nothing :: Maybe TokenId)

    let
      loadTokens :: Effect Unit
      loadTokens = do
        setTokens (const Loading)
        launchAff_ do
          res <- getTokens env.cfg
          liftEffect (setTokens (const (either Failure Success res)))

      refreshTokensAfterSubmit :: Effect Unit
      refreshTokensAfterSubmit = do
        loadTokens
        launchAff_ do
          delay (Milliseconds 5000.0)
          liftEffect loadTokens
          delay (Milliseconds 10000.0)
          liftEffect loadTokens
          delay (Milliseconds 15000.0)
          liftEffect loadTokens
          delay (Milliseconds 30000.0)
          liftEffect loadTokens

    useEffectOnce do
      loadTokens
      pure (pure unit)

    let
      panels :: Array { label :: String, node :: JSX }
      panels =
        [ { label: "Connect"
          , node: connectTab
              { wallet
              , onConnect: \w -> setWallet (const (Just w))
              , onDisconnect: setWallet (const Nothing)
              }
          }
        , { label: "Tokens"
          , node: tokensTab
              { env
              , wallet
              , tokens
              , selected
              , onRefresh: loadTokens
              , onSubmitted: refreshTokensAfterSubmit
              , onSelect: \t -> setSelected (const (Just t))
              }
          }
        , { label: "Facts"
          , node: factsTab { env, wallet, selected }
          }
        ]
          <>
            if isJust wallet then
              [ { label: "End"
                , node: endTab
                    { env
                    , wallet
                    , selected
                    , onSubmitted: do
                        setSelected (const Nothing)
                        refreshTokensAfterSubmit
                    }
                }
              ]
            else []

      clamped = min tabIndex (length panels - 1)
      active = maybe mempty _.node (index panels clamped)

    pure $ M.themeProvider { theme: M.defaultTheme }
      [ M.cssBaseline
      , M.appBar { position: "static" }
          [ M.toolbar {}
              [ M.typography
                  { variant: "h6", component: "div", sx: { flexGrow: 1 } }
                  [ R.text "MPFS" ]
              , networkBadge wallet
              ]
          ]
      , M.box { sx: { borderBottom: 1, borderColor: "divider" } }
          [ M.tabs
              { value: clamped
              , onChange: M.onTabChange (\i -> setTab (const i))
              }
              (mapWithIndex (\_ p -> M.tab { label: p.label }) panels)
          ]
      , M.container { maxWidth: "md", sx: { pb: 6 } } [ active ]
      ]

networkBadge :: Maybe WalletState -> JSX
networkBadge = case _ of
  Nothing -> mempty
  Just w ->
    M.chip
      { label: w.name
      , color: "default"
      , size: "small"
      , variant: "outlined"
      , sx: { color: "white", borderColor: "rgba(255,255,255,0.5)" }
      }
