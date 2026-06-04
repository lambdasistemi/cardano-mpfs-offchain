-- | Tokens tab: list tokens and register a new one.
-- |
-- | Lists token ids from `GET /tokens`, lets the user select one (shared with
-- | the Facts/End tabs), and registers a new token through the `CageHelpers`
-- | boundary (mock today). Token registration requires a connected wallet.
module MpfsSpa.Tab.Tokens (mkTokensTab, TokensProps) where

import Prelude

import Data.Array (null)
import Data.Maybe (Maybe(..))
import Effect (Effect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.Config (placeholderCageConfig)
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, Remote, WalletState, remoteView)
import MpfsSpa.Submit (OpStatus(..), runOpAfterSubmit, statusView)
import MpfsSpa.Types (TokenId(..), WalletAddr(..))

type TokensProps =
  { env :: Env
  , wallet :: Maybe WalletState
  , tokens :: Remote (Array TokenId)
  , selected :: Maybe TokenId
  , onRefresh :: Effect Unit
  , onSubmitted :: Effect Unit
  , onSelect :: TokenId -> Effect Unit
  }

mkTokensTab :: Effect (TokensProps -> JSX)
mkTokensTab = component "TokensTab" \props -> React.do
  status /\ setStatus <- useState Idle

  let
    register :: WalletState -> Effect Unit
    register w =
      runOpAfterSubmit (\_ -> props.onSubmitted)
        (Just w)
        ( props.env.helpers.registerToken
            (WalletAddr w.address)
            placeholderCageConfig
        )
        setStatus

  pure $ M.stack { spacing: 2, sx: { mt: 2 } }
    [ M.stack
        { direction: "row", spacing: 2, sx: { justifyContent: "space-between" } }
        [ M.typography { variant: "h6" } [ R.text "Tokens" ]
        , M.stack { direction: "row", spacing: 1 }
            [ M.button
                { variant: "outlined", size: "small", onClick: props.onRefresh }
                [ R.text "Refresh" ]
            , registerButton props.wallet register
            ]
        ]
    , statusView status
    , remoteView props.tokens (tokenList props.selected props.onSelect)
    ]

registerButton :: Maybe WalletState -> (WalletState -> Effect Unit) -> JSX
registerButton mWallet register = case mWallet of
  Nothing ->
    M.tooltip { title: "Connect a wallet first" }
      [ M.box {}
          [ M.button
              { variant: "contained", size: "small", disabled: true }
              [ R.text "Register new token" ]
          ]
      ]
  Just w ->
    M.button
      { variant: "contained", size: "small", onClick: register w }
      [ R.text "Register new token" ]

tokenList :: Maybe TokenId -> (TokenId -> Effect Unit) -> Array TokenId -> JSX
tokenList selected onSelect tokens =
  if null tokens then
    M.alert { severity: "info" } [ R.text "No tokens yet." ]
  else
    M.paper { variant: "outlined" }
      [ M.list {} (map (tokenRow selected onSelect) tokens) ]

tokenRow :: Maybe TokenId -> (TokenId -> Effect Unit) -> TokenId -> JSX
tokenRow selected onSelect tid@(TokenId hex) =
  M.listItemButton
    { selected: selected == Just tid, onClick: onSelect tid }
    [ M.listItemText
        { primary: hex
        , primaryTypographyProps: { sx: { fontFamily: "monospace", fontSize: "0.85rem" } }
        }
    ]
