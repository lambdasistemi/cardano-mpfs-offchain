-- | End tab: close a cage.
-- |
-- | Builds an end-cage transaction for the selected token through the
-- | `CageHelpers` boundary. The tab is only shown by the app shell when the
-- | connected wallet owns at least one active token; here we additionally
-- | require a selected token.
module MpfsSpa.Tab.End (mkEndTab, EndProps) where

import Prelude

import Data.Maybe (Maybe(..))
import Effect (Effect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.Config (placeholderCageConfig)
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, WalletState)
import MpfsSpa.Submit (OpStatus(..), runOpAfterSubmit, statusView)
import MpfsSpa.Types (TokenId(..), WalletAddr(..))

type EndProps =
  { env :: Env
  , wallet :: Maybe WalletState
  , selected :: Maybe TokenId
  , onSubmitted :: Effect Unit
  }

mkEndTab :: Effect (EndProps -> JSX)
mkEndTab = component "EndTab" \props -> React.do
  status /\ setStatus <- useState Idle

  let
    end :: WalletState -> TokenId -> Effect Unit
    end w tid =
      runOpAfterSubmit (\_ -> props.onSubmitted)
        (Just w)
        ( props.env.helpers.endCage
            (WalletAddr w.address)
            placeholderCageConfig
            tid
        )
        setStatus

  pure $ M.stack { spacing: 2, sx: { mt: 2 } }
    [ M.typography { variant: "h6" } [ R.text "End cage" ]
    , case props.wallet, props.selected of
        Just w, Just tid@(TokenId hex) ->
          M.card { variant: "outlined" }
            [ M.cardContent {}
                [ M.alert { severity: "warning" }
                    [ R.text "Closing a cage is irreversible." ]
                , M.typography
                    { variant: "body2"
                    , sx: { fontFamily: "monospace", mt: 1, wordBreak: "break-all" }
                    }
                    [ R.text hex ]
                ]
            , M.cardActions {}
                [ M.button
                    { variant: "contained", color: "error", onClick: end w tid }
                    [ R.text "End this cage" ]
                ]
            ]
        Nothing, _ ->
          M.alert { severity: "info" } [ R.text "Connect a wallet first." ]
        _, Nothing ->
          M.alert { severity: "info" }
            [ R.text "Select a token in the Tokens tab first." ]
    , statusView status
    ]
