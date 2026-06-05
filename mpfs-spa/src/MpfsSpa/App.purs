-- | Application shell.
-- |
-- | The SPA first screen is a compact token/fact workbench: token selection
-- | and fact administration live together, while wallet connection stays in
-- | the app bar. The UI only reads JSON fields and calls the CageHelpers
-- | boundary for write transactions; no protocol logic lives here.
module MpfsSpa.App (mkApp) where

import Prelude

import Data.Array (any, filter, head, length, null)
import Data.Either (Either(..), either)
import Data.Int (toNumber)
import Data.Maybe (Maybe(..), fromMaybe, isJust, maybe)
import Data.String.CodeUnits as CU
import Data.String.Common as String
import Data.String.Pattern (Pattern(..))
import Effect (Effect)
import Effect.Aff (Milliseconds(..), attempt, delay, launchAff_)
import Effect.Class (liftEffect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useEffect, useEffectOnce, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.CageHelpers (CageResult)
import MpfsSpa.Config (placeholderCageConfig)
import MpfsSpa.Display (displayUtf8Hex, formatAgeMillis, currentTimeMillis)
import MpfsSpa.Http
  ( FactEntry
  , PendingRequest
  , TokenState
  , getFacts
  , getRequests
  , getTokenState
  , getTokens
  )
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, Remote(..), WalletState, centred, remoteView)
import MpfsSpa.Submit (OpStatus(..), runOpAfterSubmit, statusView)
import MpfsSpa.Types
  ( Key(..)
  , RequestId(..)
  , TokenId(..)
  , Value(..)
  , WalletAddr(..)
  )
import MpfsSpa.Wallet.Cip30 as W

data FactDialog
  = NoDialog
  | InsertDialog { key :: String, value :: String }
  | EditDialog { key :: String, currentValue :: String, newValue :: String }
  | DeleteDialog { key :: String, value :: String }

data FactRow
  = KnownFact FactEntry (Array PendingRequest)
  | PendingOnly PendingRequest

-- | Build the root component against the given environment.
mkApp :: Env -> Effect (Unit -> JSX)
mkApp env = component "App" \_ -> React.do
  wallet /\ setWallet <- useState (Nothing :: Maybe WalletState)
  wallets /\ setWallets <- useState ([] :: Array W.WalletInfo)
  connecting /\ setConnecting <- useState (Nothing :: Maybe String)
  connectError /\ setConnectError <- useState (Nothing :: Maybe String)
  tokens /\ setTokens <- useState (NotAsked :: Remote (Array TokenId))
  selected /\ setSelected <- useState (Nothing :: Maybe TokenId)
  facts /\ setFacts <- useState (NotAsked :: Remote (Array FactEntry))
  requests /\ setRequests <- useState (NotAsked :: Remote (Array PendingRequest))
  tokenState /\ setTokenState <- useState (NotAsked :: Remote TokenState)
  nowMillis /\ setNowMillis <- useState 0.0
  status /\ setStatus <- useState Idle
  dialog /\ setDialog <- useState NoDialog
  confirmEnd /\ setConfirmEnd <- useState false

  let
    readWallet :: W.WalletApi -> _
    readWallet api = do
      networkId <- W.getNetworkId api
      addrs <- W.getUsedAddresses api
      balanceCbor <- W.getBalance api
      change <- W.getChangeAddress api
      pure
        { networkId
        , address: fromMaybe change (head addrs)
        , balance: W.lovelaceOfBalance balanceCbor
        }

    refreshWallet :: WalletState -> Effect Unit
    refreshWallet w =
      launchAff_ do
        result <- attempt (readWallet w.api)
        liftEffect case result of
          Left _ ->
            setConnectError
              (const (Just "Wallet account refresh failed."))
          Right fresh ->
            setWallet
              ( const
                  ( Just
                      ( w
                          { address = fresh.address
                          , networkId = fresh.networkId
                          , balance = fresh.balance
                          }
                      )
                  )
              )

    connectWallet :: W.WalletInfo -> Effect Unit
    connectWallet info = do
      setConnectError (const Nothing)
      setConnecting (const (Just info.key))
      launchAff_ do
        result <- attempt do
          api <- W.enable info.key
          fresh <- readWallet api
          pure { api, fresh }
        liftEffect do
          setConnecting (const Nothing)
          case result of
            Left _ ->
              setConnectError
                (const (Just "Wallet connection failed or was declined."))
            Right { api, fresh }
              | fresh.networkId /= 0 ->
                  setConnectError
                    ( const
                        ( Just
                            "Switch the wallet to preprod before connecting."
                        )
                    )
              | otherwise ->
                  setWallet
                    ( const
                        ( Just
                            { api
                            , key: info.key
                            , name: info.name
                            , address: fresh.address
                            , networkId: fresh.networkId
                            , balance: fresh.balance
                            }
                        )
                    )

    connectFirst :: Effect Unit
    connectFirst =
      maybe (setConnectError (const (Just "No CIP-30 wallet found."))) connectWallet
        (head wallets)

    loadTokenDetails :: TokenId -> Effect Unit
    loadTokenDetails tid = do
      setFacts (const Loading)
      setRequests (const Loading)
      setTokenState (const Loading)
      launchAff_ do
        now <- liftEffect currentTimeMillis
        stateRes <- getTokenState env.cfg tid
        factsRes <- getFacts env.cfg tid
        requestsRes <- getRequests env.cfg tid
        liftEffect do
          setNowMillis (const now)
          setTokenState (const (either Failure Success stateRes))
          setFacts (const (either Failure Success factsRes))
          setRequests (const (either Failure Success requestsRes))

    loadTokens :: Effect Unit
    loadTokens = do
      setTokens (const Loading)
      launchAff_ do
        res <- getTokens env.cfg
        liftEffect case res of
          Left err -> setTokens (const (Failure err))
          Right ts -> do
            setTokens (const (Success ts))
            case selected of
              Just current | any (\t -> t == current) ts -> pure unit
              _ -> setSelected (const (head ts))

    reloadLater :: Maybe TokenId -> Effect Unit
    reloadLater mtid = do
      loadTokens
      maybe (pure unit) loadTokenDetails mtid
      launchAff_ do
        delay (Milliseconds 5000.0)
        liftEffect do
          loadTokens
          maybe (pure unit) loadTokenDetails mtid
        delay (Milliseconds 10000.0)
        liftEffect do
          loadTokens
          maybe (pure unit) loadTokenDetails mtid
        delay (Milliseconds 15000.0)
        liftEffect do
          loadTokens
          maybe (pure unit) loadTokenDetails mtid

    runWriteWith :: (String -> Effect Unit) -> (WalletState -> CageResult) -> Effect Unit
    runWriteWith onSubmitted build = case wallet of
      Nothing -> setStatus (const (Failed "Connect a wallet first."))
      Just w ->
        runOpAfterSubmit onSubmitted (Just w) (build w) setStatus

    addr :: WalletState -> WalletAddr
    addr w = WalletAddr w.address

    registerToken :: Effect Unit
    registerToken =
      runWriteWith
        (\_ -> reloadLater Nothing)
        (\w -> env.helpers.registerToken (addr w) placeholderCageConfig)

    withSelected :: (TokenId -> Effect Unit) -> Effect Unit
    withSelected f = maybe (setStatus (const (Failed "Select a token first."))) f selected

    processRequests :: Effect Unit
    processRequests =
      withSelected \tid ->
        runWriteWith
          (\_ -> reloadLater (Just tid))
          (\w -> env.helpers.updateToken (addr w) placeholderCageConfig tid)

    rejectExpired :: Effect Unit
    rejectExpired =
      withSelected \tid ->
        runWriteWith
          (\_ -> reloadLater (Just tid))
          (\w -> env.helpers.rejectExpired (addr w) placeholderCageConfig tid)

    retractRequest :: RequestId -> Effect Unit
    retractRequest rid =
      withSelected \tid ->
        runWriteWith
          (\_ -> reloadLater (Just tid))
          (\w -> env.helpers.retractRequest (addr w) placeholderCageConfig tid rid)

    submitDialog :: Effect Unit
    submitDialog =
      withSelected \tid ->
        case dialog of
          NoDialog -> pure unit
          InsertDialog form -> do
            setDialog (const NoDialog)
            runWriteWith
              (\_ -> reloadLater (Just tid))
              ( \w ->
                  env.helpers.insertFact (addr w) placeholderCageConfig tid
                    (Key form.key)
                    (Value form.value)
              )
          EditDialog form -> do
            setDialog (const NoDialog)
            runWriteWith
              (\_ -> reloadLater (Just tid))
              ( \w ->
                  env.helpers.updateFact (addr w) placeholderCageConfig tid
                    (Key form.key)
                    (Value form.currentValue)
                    (Value form.newValue)
              )
          DeleteDialog form -> do
            setDialog (const NoDialog)
            runWriteWith
              (\_ -> reloadLater (Just tid))
              ( \w ->
                  env.helpers.deleteFact (addr w) placeholderCageConfig tid
                    (Key form.key)
                    (Value form.value)
              )

    endSelected :: Effect Unit
    endSelected =
      withSelected \tid -> do
        setConfirmEnd (const false)
        runWriteWith
          ( \_ -> do
              setSelected (const Nothing)
              reloadLater Nothing
          )
          (\w -> env.helpers.endCage (addr w) placeholderCageConfig tid)

  useEffectOnce do
    ws <- W.availableWallets
    setWallets (const ws)
    loadTokens
    pure (pure unit)

  useEffect (map _.key wallet) do
    case wallet of
      Nothing -> pure (pure unit)
      Just w -> W.subscribeAccountChanges w.api (refreshWallet w)

  useEffect (map (\(TokenId tid) -> tid) selected) do
    case selected of
      Nothing -> do
        setFacts (const NotAsked)
        setRequests (const NotAsked)
        setTokenState (const NotAsked)
      Just tid -> loadTokenDetails tid
    pure (pure unit)

  let
    processTime = case tokenState of
      Success st -> toNumber st.processTime
      _ -> toNumber placeholderCageConfig.defaultProcessTime

    pendingCount = case requests of
      Success rs -> length rs
      _ -> 0

    processable = case requests of
      Success rs -> length (filter (isProcessable nowMillis processTime) rs)
      _ -> 0

    selectedToken = selected

  pure $ M.themeProvider { theme: M.defaultTheme }
    [ M.cssBaseline
    , appBarView wallet wallets connecting connectFirst refreshWallet
    , M.container
        { maxWidth: false
        , sx:
            { py: 2
            , px: { xs: 1.5, md: 3 }
            , bgcolor: "background.default"
            }
        }
        [ M.box
            { sx:
                { display: "grid"
                , gridTemplateColumns: { xs: "1fr", md: "320px minmax(0, 1fr)" }
                , gap: 2
                , alignItems: "start"
                }
            }
            [ sidebarView
                { wallet
                , wallets
                , connecting
                , connectError
                , tokens
                , selected
                , onConnect: connectWallet
                , onRefresh: loadTokens
                , onRegister: registerToken
                , onSelect:
                    \tid -> do
                      setSelected (const (Just tid))
                      setDialog (const NoDialog)
                }
            , workspaceView
                { wallet
                , selected: selectedToken
                , tokenState
                , facts
                , requests
                , nowMillis
                , processTime
                , pendingCount
                , processable
                , status
                , onRefresh:
                    maybe loadTokens
                      ( \tid -> do
                          loadTokens
                          loadTokenDetails tid
                      )
                      selectedToken
                , onRegister: registerToken
                , onAdd: setDialog (const (InsertDialog { key: "", value: "" }))
                , onProcess: processRequests
                , onRejectExpired: rejectExpired
                , onEnd: setConfirmEnd (const true)
                , onEdit:
                    \fact ->
                      setDialog
                        ( const
                            ( EditDialog
                                { key: factText fact.key
                                , currentValue: valueText fact.value
                                , newValue: valueText fact.value
                                }
                            )
                        )
                , onDelete:
                    \fact ->
                      setDialog
                        ( const
                            ( DeleteDialog
                                { key: factText fact.key
                                , value: valueText fact.value
                                }
                            )
                        )
                , onRetract: retractRequest
                }
            ]
        ]
    , factDialogView dialog setDialog submitDialog
    , endDialogView confirmEnd (setConfirmEnd (const false)) endSelected selectedToken
    ]

appBarView
  :: Maybe WalletState
  -> Array W.WalletInfo
  -> Maybe String
  -> Effect Unit
  -> (WalletState -> Effect Unit)
  -> JSX
appBarView wallet wallets connecting connectFirst refreshWallet =
  M.appBar
    { position: "sticky"
    , color: "inherit"
    , elevation: 0
    , sx: { borderBottom: 1, borderColor: "divider", bgcolor: "background.paper" }
    }
    [ M.toolbar { variant: "dense", sx: { minHeight: 56, gap: 1 } }
        [ M.typography
            { variant: "h6"
            , component: "div"
            , sx: { flexGrow: 1, fontWeight: 700 }
            }
            [ R.text "MPFS" ]
        , case wallet of
            Nothing ->
              M.button
                { variant: "contained"
                , size: "small"
                , disabled: null wallets || isJust connecting
                , onClick: connectFirst
                , sx: { gap: 0.75 }
                }
                [ M.manageAccountsIcon { fontSize: "small" }
                , R.text
                    ( if isJust connecting then
                        "Connecting"
                      else
                        "Connect"
                    )
                ]
            Just w ->
              accountBar w (refreshWallet w)
        ]
    ]

accountBar :: WalletState -> Effect Unit -> JSX
accountBar w onRefresh =
  M.stack
    { direction: "row"
    , spacing: 1
    , sx: { alignItems: "center", minWidth: 0 }
    }
    [ M.accountCircleIcon { color: "primary", fontSize: "small" }
    , M.box { sx: { minWidth: 0, display: { xs: "none", sm: "block" } } }
        [ M.typography
            { variant: "body2"
            , sx: { lineHeight: 1.2, fontWeight: 600 }
            }
            [ R.text w.name ]
        , M.typography
            { variant: "caption"
            , color: "text.secondary"
            , sx: { fontFamily: "monospace" }
            }
            [ R.text (shortText 10 6 w.address) ]
        ]
    , M.chip
        { label: networkLabel w.networkId
        , size: "small"
        , variant: "outlined"
        }
    , maybe mempty accountHint (eternlHint w)
    , iconTip "Refresh account"
        [ M.iconButton
            { size: "small"
            , "aria-label": "Refresh account"
            , onClick: onRefresh
            }
            [ M.refreshIcon { fontSize: "small" } ]
        ]
    ]

accountHint :: String -> JSX
accountHint hint =
  M.tooltip { title: hint }
    [ M.box { sx: { display: "inline-flex" } }
        [ M.manageAccountsIcon { color: "action", fontSize: "small" } ]
    ]

sidebarView
  :: { wallet :: Maybe WalletState
     , wallets :: Array W.WalletInfo
     , connecting :: Maybe String
     , connectError :: Maybe String
     , tokens :: Remote (Array TokenId)
     , selected :: Maybe TokenId
     , onConnect :: W.WalletInfo -> Effect Unit
     , onRefresh :: Effect Unit
     , onRegister :: Effect Unit
     , onSelect :: TokenId -> Effect Unit
     }
  -> JSX
sidebarView props =
  M.stack { spacing: 2 }
    ( walletPanel props.wallet props.wallets props.connecting props.connectError
        props.onConnect
        <>
          [ M.paper
              { variant: "outlined"
              , sx: { overflow: "hidden" }
              }
              [ M.box
                  { sx:
                      { px: 1.5
                      , py: 1
                      , display: "flex"
                      , alignItems: "center"
                      , gap: 0.5
                      , borderBottom: 1
                      , borderColor: "divider"
                      }
                  }
                  [ M.typography
                      { variant: "subtitle2"
                      , sx: { flexGrow: 1, fontWeight: 700 }
                      }
                      [ R.text "Tokens" ]
                  , iconTip "Refresh tokens"
                      [ M.iconButton
                          { size: "small"
                          , "aria-label": "Refresh tokens"
                          , onClick: props.onRefresh
                          }
                          [ M.refreshIcon { fontSize: "small" } ]
                      ]
                  , iconTip "Register token"
                      [ M.iconButton
                          { size: "small"
                          , color: "primary"
                          , disabled: not (isJust props.wallet)
                          , "aria-label": "Register token"
                          , onClick: props.onRegister
                          }
                          [ M.appRegistrationIcon { fontSize: "small" } ]
                      ]
                  ]
              , remoteView props.tokens (tokenList props.selected props.onSelect)
              ]
          ]
    )

walletPanel
  :: Maybe WalletState
  -> Array W.WalletInfo
  -> Maybe String
  -> Maybe String
  -> (W.WalletInfo -> Effect Unit)
  -> Array JSX
walletPanel wallet wallets connecting connectError onConnect = case wallet of
  Just w ->
    [ M.paper { variant: "outlined", sx: { p: 1.5 } }
        [ M.stack { spacing: 1 }
            [ M.typography { variant: "subtitle2", sx: { fontWeight: 700 } }
                [ R.text "Account" ]
            , M.typography
                { variant: "body2", sx: { fontFamily: "monospace", wordBreak: "break-all" } }
                [ R.text (shortText 18 10 w.address) ]
            , M.stack { direction: "row", spacing: 1 }
                [ M.chip { label: w.name, size: "small" }
                , M.chip { label: networkLabel w.networkId, size: "small", variant: "outlined" }
                ]
            , case w.balance of
                Nothing -> mempty
                Just lovelace ->
                  M.typography { variant: "caption", color: "text.secondary" }
                    [ R.text (lovelace <> " lovelace") ]
            ]
        ]
    ]
  Nothing ->
    [ M.paper { variant: "outlined", sx: { p: 1.5 } }
        [ M.stack { spacing: 1 }
            ( [ M.typography { variant: "subtitle2", sx: { fontWeight: 700 } }
                  [ R.text "Wallet" ]
              ]
                <> maybe [] (\msg -> [ M.alert { severity: "error" } [ R.text msg ] ])
                  connectError
                <>
                  if null wallets then
                    [ M.alert { severity: "info" } [ R.text "No CIP-30 wallet found." ] ]
                  else
                    [ M.stack { spacing: 1 } (map (walletButton connecting onConnect) wallets) ]
            )
        ]
    ]

walletButton :: Maybe String -> (W.WalletInfo -> Effect Unit) -> W.WalletInfo -> JSX
walletButton connecting onConnect info =
  M.button
    { variant: "outlined"
    , size: "small"
    , disabled: connecting == Just info.key
    , onClick: onConnect info
    , sx: { justifyContent: "space-between" }
    }
    [ R.text info.name
    , M.manageAccountsIcon { fontSize: "small" }
    ]

tokenList :: Maybe TokenId -> (TokenId -> Effect Unit) -> Array TokenId -> JSX
tokenList selected onSelect ts =
  if null ts then
    M.box { sx: { p: 1.5 } }
      [ M.alert { severity: "info" } [ R.text "No tokens yet." ] ]
  else
    M.list { dense: true, disablePadding: true }
      (map (tokenRow selected onSelect) ts)

tokenRow :: Maybe TokenId -> (TokenId -> Effect Unit) -> TokenId -> JSX
tokenRow selected onSelect tid@(TokenId token) =
  M.listItemButton
    { selected: selected == Just tid
    , onClick: onSelect tid
    , sx: { borderBottom: 1, borderColor: "divider" }
    }
    [ M.listItemText
        { primary: shortText 12 8 token
        , secondary: token
        , primaryTypographyProps: { sx: { fontFamily: "monospace", fontSize: "0.85rem" } }
        , secondaryTypographyProps:
            { sx:
                { fontFamily: "monospace"
                , fontSize: "0.68rem"
                , whiteSpace: "nowrap"
                , overflow: "hidden"
                , textOverflow: "ellipsis"
                }
            }
        }
    ]

workspaceView
  :: { wallet :: Maybe WalletState
     , selected :: Maybe TokenId
     , tokenState :: Remote TokenState
     , facts :: Remote (Array FactEntry)
     , requests :: Remote (Array PendingRequest)
     , nowMillis :: Number
     , processTime :: Number
     , pendingCount :: Int
     , processable :: Int
     , status :: OpStatus
     , onRefresh :: Effect Unit
     , onRegister :: Effect Unit
     , onAdd :: Effect Unit
     , onProcess :: Effect Unit
     , onRejectExpired :: Effect Unit
     , onEnd :: Effect Unit
     , onEdit :: FactEntry -> Effect Unit
     , onDelete :: FactEntry -> Effect Unit
     , onRetract :: RequestId -> Effect Unit
     }
  -> JSX
workspaceView props =
  M.paper { variant: "outlined", sx: { overflow: "hidden" } }
    [ M.box
        { sx:
            { px: 2
            , py: 1
            , borderBottom: 1
            , borderColor: "divider"
            , display: "flex"
            , gap: 1
            , alignItems: "center"
            , flexWrap: "wrap"
            }
        }
        [ selectedTitle props.selected props.tokenState
        , M.stack { direction: "row", spacing: 0.5, sx: { ml: "auto" } }
            [ iconTip "Refresh"
                [ M.iconButton
                    { size: "small"
                    , "aria-label": "Refresh"
                    , onClick: props.onRefresh
                    }
                    [ M.refreshIcon { fontSize: "small" } ]
                ]
            , iconTip "Process requests"
                [ M.iconButton
                    { size: "small"
                    , color: "success"
                    , disabled: not (isJust props.selected) || not (isJust props.wallet) || props.processable == 0
                    , "aria-label": "Process requests"
                    , onClick: props.onProcess
                    }
                    [ M.badge { badgeContent: props.processable, color: "success" }
                        [ M.playlistAddCheckIcon { fontSize: "small" } ]
                    ]
                ]
            , iconTip "Reject expired"
                [ M.iconButton
                    { size: "small"
                    , color: "warning"
                    , disabled: not (isJust props.selected) || not (isJust props.wallet) || props.pendingCount == 0
                    , "aria-label": "Reject expired"
                    , onClick: props.onRejectExpired
                    }
                    [ M.blockIcon { fontSize: "small" } ]
                ]
            , iconTip "Register token"
                [ M.iconButton
                    { size: "small"
                    , color: "primary"
                    , disabled: not (isJust props.wallet)
                    , "aria-label": "Register token"
                    , onClick: props.onRegister
                    }
                    [ M.appRegistrationIcon { fontSize: "small" } ]
                ]
            , iconTip "Add fact"
                [ M.iconButton
                    { size: "small"
                    , color: "primary"
                    , disabled: not (isJust props.selected) || not (isJust props.wallet)
                    , "aria-label": "Add fact"
                    , onClick: props.onAdd
                    }
                    [ M.addIcon { fontSize: "small" } ]
                ]
            , iconTip "End token"
                [ M.iconButton
                    { size: "small"
                    , color: "error"
                    , disabled: not (isJust props.selected) || not (isJust props.wallet)
                    , "aria-label": "End token"
                    , onClick: props.onEnd
                    }
                    [ M.stopCircleIcon { fontSize: "small" } ]
                ]
            ]
        ]
    , M.box { sx: { px: 2 } } [ statusView props.status ]
    , case props.selected of
        Nothing ->
          M.box { sx: { p: 2 } }
            [ M.alert { severity: "info" } [ R.text "Select a token." ] ]
        Just _ ->
          M.box { sx: { p: 2 } }
            [ factsRegion
                props.nowMillis
                props.processTime
                props.facts
                props.requests
                props.onEdit
                props.onDelete
                props.onRetract
            ]
    ]

selectedTitle :: Maybe TokenId -> Remote TokenState -> JSX
selectedTitle selected tokenState =
  M.box { sx: { minWidth: 0 } }
    [ M.typography { variant: "subtitle2", sx: { fontWeight: 700 } }
        [ R.text "Facts" ]
    , case selected of
        Nothing -> mempty
        Just (TokenId token) ->
          M.stack { direction: "row", spacing: 1, sx: { alignItems: "center", flexWrap: "wrap" } }
            [ M.typography
                { variant: "caption"
                , sx: { fontFamily: "monospace", wordBreak: "break-all" }
                }
                [ R.text token ]
            , case tokenState of
                Success st ->
                  M.chip
                    { label: "root " <> shortText 8 6 st.root
                    , size: "small"
                    , variant: "outlined"
                    }
                _ -> mempty
            ]
    ]

factsRegion
  :: Number
  -> Number
  -> Remote (Array FactEntry)
  -> Remote (Array PendingRequest)
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> JSX
factsRegion nowMillis processTime facts requests onEdit onDelete onRetract =
  case facts, requests of
    Loading, _ -> centred [ M.circularProgress {} ]
    _, Loading -> centred [ M.circularProgress {} ]
    Failure msg, _ -> M.alert { severity: "error" } [ R.text msg ]
    _, Failure msg -> M.alert { severity: "error" } [ R.text msg ]
    Success fs, Success rs ->
      factTable nowMillis processTime fs rs onEdit onDelete onRetract
    _, _ -> mempty

factTable
  :: Number
  -> Number
  -> Array FactEntry
  -> Array PendingRequest
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> JSX
factTable nowMillis processTime fs rs onEdit onDelete onRetract =
  if null fs && null rs then
    M.alert { severity: "info" } [ R.text "No facts for this token." ]
  else
    M.tableContainer { sx: { maxHeight: "calc(100vh - 210px)" } }
      [ M.table { size: "small", stickyHeader: true, "aria-label": "Token facts" }
          [ M.tableHead {}
              [ M.tableRow {}
                  [ M.tableCell { sx: { width: "24%" } } [ R.text "Key" ]
                  , M.tableCell { sx: { width: "30%" } } [ R.text "Value" ]
                  , M.tableCell { sx: { width: "30%" } } [ R.text "Pending" ]
                  , M.tableCell { align: "right", sx: { width: "16%" } } [ R.text "Actions" ]
                  ]
              ]
          , M.tableBody {}
              (map (factRow nowMillis processTime onEdit onDelete onRetract) rows)
          ]
      ]
  where
  rows =
    map (\fact -> KnownFact fact (filter (\req -> req.key == fact.key) rs)) fs
      <> map PendingOnly
        (filter (\req -> not (any (\fact -> fact.key == req.key) fs)) rs)

factRow
  :: Number
  -> Number
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> FactRow
  -> JSX
factRow nowMillis processTime onEdit onDelete onRetract row =
  case row of
    KnownFact fact pending ->
      M.tableRow { key: "fact-" <> unKey fact.key, hover: true }
        [ keyCell fact.key
        , valueCell fact.value pending
        , pendingCell nowMillis processTime pending onRetract
        , M.tableCell { align: "right" }
            [ M.stack { direction: "row", spacing: 0.5, sx: { justifyContent: "flex-end" } }
                [ iconTip ("Edit " <> factText fact.key)
                    [ M.iconButton
                        { size: "small"
                        , "aria-label": "Edit fact " <> factText fact.key
                        , disabled: not (null pending)
                        , onClick: onEdit fact
                        }
                        [ M.editIcon { fontSize: "small" } ]
                    ]
                , iconTip ("Delete " <> factText fact.key)
                    [ M.iconButton
                        { size: "small"
                        , color: "error"
                        , "aria-label": "Delete fact " <> factText fact.key
                        , disabled: not (null pending)
                        , onClick: onDelete fact
                        }
                        [ M.deleteIcon { fontSize: "small" } ]
                    ]
                ]
            ]
        ]
    PendingOnly req ->
      M.tableRow { key: "pending-" <> unRequestId req.requestId, hover: true }
        [ keyCell req.key
        , M.tableCell {}
            [ M.typography { variant: "body2", color: "text.secondary" }
                [ R.text (maybe "" valueText req.value) ]
            ]
        , pendingCell nowMillis processTime [ req ] onRetract
        , M.tableCell { align: "right" } []
        ]

keyCell :: Key -> JSX
keyCell key =
  M.tableCell {}
    [ M.typography
        { variant: "body2", sx: { fontWeight: 600 } }
        [ R.text (factText key) ]
    , M.typography
        { variant: "caption"
        , color: "text.secondary"
        , sx: { fontFamily: "monospace", wordBreak: "break-all" }
        }
        [ R.text (unKey key) ]
    ]

valueCell :: Value -> Array PendingRequest -> JSX
valueCell value pending =
  M.tableCell {}
    [ M.typography { variant: "body2" } [ R.text (valueText value) ]
    , case head pending of
        Just req ->
          M.typography { variant: "caption", color: "text.secondary" }
            [ R.text (pendingValueText req) ]
        Nothing -> mempty
    ]

pendingCell
  :: Number
  -> Number
  -> Array PendingRequest
  -> (RequestId -> Effect Unit)
  -> JSX
pendingCell nowMillis processTime pending onRetract =
  M.tableCell {}
    [ if null pending then
        M.chip
          { label: "current"
          , size: "small"
          , variant: "outlined"
          , icon: M.checkCircleIcon {}
          }
      else
        M.stack { spacing: 0.75 } (map (requestInline nowMillis processTime onRetract) pending)
    ]

requestInline
  :: Number
  -> Number
  -> (RequestId -> Effect Unit)
  -> PendingRequest
  -> JSX
requestInline nowMillis processTime onRetract req =
  M.stack
    { direction: "row"
    , spacing: 0.75
    , sx: { alignItems: "center", flexWrap: "wrap" }
    }
    [ M.chip
        { label: opLabel req.operation
        , size: "small"
        , color: "primary"
        , variant: "outlined"
        }
    , M.chip
        { label: formatAgeMillis (nowMillis - req.submittedAt)
        , size: "small"
        , variant: "outlined"
        }
    , M.chip
        { label:
            if isProcessable nowMillis processTime req then
              "processable"
            else
              "expired"
        , size: "small"
        , color:
            if isProcessable nowMillis processTime req then
              "success"
            else
              "warning"
        , icon:
            if isProcessable nowMillis processTime req then
              M.checkCircleIcon {}
            else
              M.warningAmberIcon {}
        }
    , iconTip "Retract request"
        [ M.iconButton
            { size: "small"
            , "aria-label": "Retract request " <> unRequestId req.requestId
            , onClick: onRetract req.requestId
            }
            [ M.undoIcon { fontSize: "small" } ]
        ]
    ]

factDialogView
  :: FactDialog
  -> ((FactDialog -> FactDialog) -> Effect Unit)
  -> Effect Unit
  -> JSX
factDialogView dialog setDialog onSubmit = case dialog of
  NoDialog -> mempty
  InsertDialog form ->
    editDialog
      "Add fact"
      "Request insert"
      [ textField "Key" form.key
          \value -> setDialog \_ -> InsertDialog (form { key = value })
      , textField "Value" form.value
          \value -> setDialog \_ -> InsertDialog (form { value = value })
      ]
      (setDialog (const NoDialog))
      onSubmit
  EditDialog form ->
    editDialog
      "Edit fact"
      "Request update"
      [ textField "Key" form.key
          \_ -> pure unit
      , textField "Current value" form.currentValue
          \_ -> pure unit
      , textField "New value" form.newValue
          \value -> setDialog \_ -> EditDialog (form { newValue = value })
      ]
      (setDialog (const NoDialog))
      onSubmit
  DeleteDialog form ->
    editDialog
      "Delete fact"
      "Request delete"
      [ textField "Key" form.key \_ -> pure unit
      , textField "Value" form.value \_ -> pure unit
      ]
      (setDialog (const NoDialog))
      onSubmit

editDialog :: String -> String -> Array JSX -> Effect Unit -> Effect Unit -> JSX
editDialog title submitLabel fields onClose onSubmit =
  M.dialog { open: true, onClose, fullWidth: true, maxWidth: "sm" }
    [ M.dialogTitle
        { sx: { display: "flex", alignItems: "center", gap: 1 } }
        [ R.text title
        , M.box { sx: { flexGrow: 1 } } []
        , M.iconButton
            { size: "small"
            , "aria-label": "Close dialog"
            , onClick: onClose
            }
            [ M.closeIcon { fontSize: "small" } ]
        ]
    , M.dialogContent {}
        [ M.stack { spacing: 1.5, sx: { pt: 1 } } fields ]
    , M.dialogActions {}
        [ M.button { onClick: onClose } [ R.text "Cancel" ]
        , M.button
            { variant: "contained"
            , onClick: onSubmit
            , sx: { gap: 0.75 }
            }
            [ M.saveIcon { fontSize: "small" }, R.text submitLabel ]
        ]
    ]

textField :: String -> String -> (String -> Effect Unit) -> JSX
textField label value onChange =
  M.textField
    { label
    , value
    , fullWidth: true
    , size: "small"
    , onChange: M.onValueChange onChange
    }

endDialogView :: Boolean -> Effect Unit -> Effect Unit -> Maybe TokenId -> JSX
endDialogView open onClose onEnd selected =
  if not open then
    mempty
  else
    M.dialog { open, onClose, fullWidth: true, maxWidth: "xs" }
      [ M.dialogTitle {} [ R.text "End token" ]
      , M.dialogContent {}
          [ M.stack { spacing: 1 }
              [ M.alert { severity: "warning" } [ R.text "Closing a token is irreversible." ]
              , maybe mempty tokenChip selected
              ]
          ]
      , M.dialogActions {}
          [ M.button { onClick: onClose } [ R.text "Cancel" ]
          , M.button
              { variant: "contained"
              , color: "error"
              , onClick: onEnd
              }
              [ R.text "End token" ]
          ]
      ]

tokenChip :: TokenId -> JSX
tokenChip (TokenId token) =
  M.chip
    { label: token
    , size: "small"
    , sx: { fontFamily: "monospace", maxWidth: "100%" }
    }

isProcessable :: Number -> Number -> PendingRequest -> Boolean
isProcessable nowMillis processTime req =
  nowMillis - req.submittedAt <= processTime

pendingValueText :: PendingRequest -> String
pendingValueText req =
  case req.value of
    Nothing -> ""
    Just value -> opLabel req.operation <> " -> " <> valueText value

opLabel :: String -> String
opLabel "insert" = "Insert"
opLabel "update" = "Update"
opLabel "delete" = "Delete"
opLabel other = other

factText :: Key -> String
factText (Key key) = displayUtf8Hex key

valueText :: Value -> String
valueText (Value value) = displayUtf8Hex value

unKey :: Key -> String
unKey (Key key) = key

unRequestId :: RequestId -> String
unRequestId (RequestId requestId) = requestId

shortText :: Int -> Int -> String -> String
shortText front back text =
  let
    len = CU.length text
  in
    if len <= front + back + 3 then
      text
    else
      CU.take front text <> "..." <> CU.drop (len - back) text

networkLabel :: Int -> String
networkLabel 0 = "preprod"
networkLabel n = W.networkName n

eternlHint :: WalletState -> Maybe String
eternlHint w =
  let
    haystack = String.toLower (w.key <> " " <> w.name)
  in
    if CU.contains (Pattern "eternl") haystack then
      Just "Switch the active account in Eternl, then refresh if it does not update automatically."
    else
      Nothing

iconTip :: String -> Array JSX -> JSX
iconTip label children =
  M.tooltip { title: label } [ M.box { component: "span" } children ]
