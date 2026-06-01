-- | Connect tab: CIP-30 wallet selector.
-- |
-- | Lists wallets injected into `window.cardano`, enables the chosen one, and
-- | reads its address / network / balance into the shared in-memory wallet
-- | state. When connected, shows those details and a disconnect button.
module MpfsSpa.Tab.Connect (mkConnectTab) where

import Prelude

import Data.Array (head, null)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..), fromMaybe)
import Effect (Effect)
import Effect.Aff (attempt, launchAff_)
import Effect.Class (liftEffect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useEffectOnce, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.Material as M
import MpfsSpa.Shared (WalletState)
import MpfsSpa.Wallet.Cip30 as W

-- | Props: the current wallet (if any) and the connect/disconnect callbacks
-- | that update the shared app state.
type ConnectProps =
  { wallet :: Maybe WalletState
  , onConnect :: WalletState -> Effect Unit
  , onDisconnect :: Effect Unit
  }

mkConnectTab :: Effect (ConnectProps -> JSX)
mkConnectTab = component "ConnectTab" \props -> React.do
  wallets /\ setWallets <- useState ([] :: Array W.WalletInfo)
  connecting /\ setConnecting <- useState (Nothing :: Maybe String)
  err /\ setErr <- useState (Nothing :: Maybe String)

  useEffectOnce do
    ws <- W.availableWallets
    setWallets (const ws)
    pure (pure unit)

  let
    connect :: W.WalletInfo -> Effect Unit
    connect info = do
      setErr (const Nothing)
      setConnecting (const (Just info.key))
      launchAff_ do
        result <- attempt do
          api <- W.enable info.key
          networkId <- W.getNetworkId api
          addrs <- W.getUsedAddresses api
          balanceCbor <- W.getBalance api
          change <- W.getChangeAddress api
          pure { api, networkId, address: fromMaybe change (head addrs), balanceCbor }
        liftEffect $ case result of
          Left _ -> do
            setErr (const (Just "Wallet connection failed or was declined."))
            setConnecting (const Nothing)
          Right w
            | w.networkId /= 0 -> do
                setErr (const (Just "Switch the wallet to preprod before connecting."))
                setConnecting (const Nothing)
            | otherwise -> do
                props.onConnect
                  { api: w.api
                  , name: info.name
                  , address: w.address
                  , networkId: w.networkId
                  , balance: W.lovelaceOfBalance w.balanceCbor
                  }
                setConnecting (const Nothing)

  pure case props.wallet of
    Just w -> connectedCard w props.onDisconnect
    Nothing -> selector wallets connecting err connect

-- | The connected-wallet detail card.
connectedCard :: WalletState -> Effect Unit -> JSX
connectedCard w onDisconnect =
  M.card { sx: { mt: 2 } }
    [ M.cardHeader
        { title: w.name
        , subheader: networkLabel w.networkId
        }
    , M.cardContent {}
        [ M.stack { spacing: 1 }
            [ field "Address" w.address
            , field "Balance"
                (maybeAda w.balance)
            , M.chip
                { label: "connected"
                , color: "success"
                , size: "small"
                , sx: { alignSelf: "flex-start" }
                }
            ]
        ]
    , M.cardActions {}
        [ M.button
            { variant: "outlined", color: "secondary", onClick: onDisconnect }
            [ R.text "Disconnect" ]
        ]
    ]

-- | The wallet-selector view (no wallet connected yet).
selector
  :: Array W.WalletInfo
  -> Maybe String
  -> Maybe String
  -> (W.WalletInfo -> Effect Unit)
  -> JSX
selector wallets connecting err connect =
  M.stack { spacing: 2, sx: { mt: 2 } }
    [ M.typography { variant: "h6" } [ R.text "Connect a wallet" ]
    , case err of
        Just msg -> M.alert { severity: "error" } [ R.text msg ]
        Nothing -> mempty
    , if null wallets then
        M.alert { severity: "info" }
          [ R.text "No CIP-30 wallet found. Install Nami, Eternl, or Lace." ]
      else
        M.list { sx: { width: "100%" } }
          (map (walletRow connecting connect) wallets)
    ]

walletRow
  :: Maybe String -> (W.WalletInfo -> Effect Unit) -> W.WalletInfo -> JSX
walletRow connecting connect info =
  M.listItem
    { secondaryAction:
        M.button
          { variant: "contained"
          , disabled: connecting == Just info.key
          , onClick: connect info
          }
          [ R.text (if connecting == Just info.key then "…" else "Connect") ]
    }
    [ M.listItemText { primary: info.name } ]

field :: String -> String -> JSX
field label value =
  M.box {}
    [ M.typography
        { variant: "caption", color: "text.secondary" }
        [ R.text label ]
    , M.typography
        { variant: "body2", sx: { wordBreak: "break-all" } }
        [ R.text value ]
    ]

-- | Render a lovelace string as ADA, or a dash if unknown.
maybeAda :: Maybe String -> String
maybeAda Nothing = "—"
maybeAda (Just lovelace) = lovelace <> " lovelace"

networkLabel :: Int -> String
networkLabel 0 = "preprod"
networkLabel n = W.networkName n
