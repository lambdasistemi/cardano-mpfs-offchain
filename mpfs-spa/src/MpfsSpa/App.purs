-- | Application shell.
-- |
-- | The SPA first screen is a compact token/fact workbench: token selection
-- | and fact administration live together, while wallet connection stays in
-- | the app bar. The UI only reads JSON fields and calls the CageHelpers
-- | boundary for write transactions; no protocol logic lives here.
module MpfsSpa.App (mkApp) where

import Prelude

import Data.Array (any, filter, find, head, length, null, zipWith)
import Data.Either (Either(..), either)
import Data.Int (toNumber)
import Data.Maybe (Maybe(..), fromMaybe, isJust, maybe)
import Data.String.CodeUnits as CU
import Data.String.Common as String
import Data.String.Pattern (Pattern(..))
import Data.Traversable (traverse)
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
  , RequestPhase(..)
  , TokenState
  , getFacts
  , getRequests
  , getTokenState
  , getTokens
  , phaseLabel
  , requestPhase
  )
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, Remote(..), WalletState, centred, remoteView)
import MpfsSpa.Submit (OpStatus(..), runOpAfterSubmit, statusView)
import MpfsSpa.Theme as Theme
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

type TokenSummary =
  { token :: TokenId
  , owner :: Maybe String
  , root :: Maybe String
  }

-- | Build the root component against the given environment.
mkApp :: Env -> Effect (Unit -> JSX)
mkApp env = component "App" \_ -> React.do
  wallet /\ setWallet <- useState (Nothing :: Maybe WalletState)
  wallets /\ setWallets <- useState ([] :: Array W.WalletInfo)
  connecting /\ setConnecting <- useState (Nothing :: Maybe String)
  connectError /\ setConnectError <- useState (Nothing :: Maybe String)
  tokens /\ setTokens <- useState (NotAsked :: Remote (Array TokenSummary))
  selected /\ setSelected <- useState (Nothing :: Maybe TokenId)
  myTokensOnly /\ setMyTokensOnly <- useState true
  hintOpen /\ setHintOpen <- useState true
  themeMode /\ setThemeMode <- useState "light"
  facts /\ setFacts <- useState (NotAsked :: Remote (Array FactEntry))
  requests /\ setRequests <- useState (NotAsked :: Remote (Array PendingRequest))
  selectedRequestIds /\ setSelectedRequestIds <- useState ([] :: Array RequestId)
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
      setSelectedRequestIds (const [])
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

    walletOwnerHash :: Maybe String
    walletOwnerHash =
      wallet >>= \w -> W.ownerKeyHashOfAddress w.address

    visibleTokenSummaries :: Array TokenSummary -> Array TokenSummary
    visibleTokenSummaries =
      visibleSummaries walletOwnerHash myTokensOnly

    loadTokens :: Effect Unit
    loadTokens = do
      setTokens (const Loading)
      launchAff_ do
        res <- getTokens env.cfg
        liftEffect case res of
          Left err -> setTokens (const (Failure err))
          Right ids -> launchAff_ do
            states <- traverse (getTokenState env.cfg) ids
            let
              summaries =
                zipWith tokenSummary ids states
              visible =
                visibleTokenSummaries summaries
            liftEffect do
              setTokens (const (Success summaries))
              case selected of
                Just current | any (\s -> s.token == current) visible -> pure unit
                _ -> setSelected (const (map _.token (head visible)))

    toggleMyTokensOnly :: Effect Unit
    toggleMyTokensOnly =
      setMyTokensOnly \current -> not current

    toggleTheme :: Effect Unit
    toggleTheme = do
      let next = if themeMode == "dark" then "light" else "dark"
      Theme.storeThemeMode next
      setThemeMode (const next)

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
      if null selectedRequestIds then
        setStatus (const (Failed "Select at least one processable request."))
      else
        withSelected \tid ->
          runWriteWith
            (\_ -> reloadLater (Just tid))
            ( \w ->
                env.helpers.updateToken (addr w) placeholderCageConfig tid
                  selectedRequestIds
            )

    rejectExpired :: Effect Unit
    rejectExpired =
      if null selectedRequestIds then
        setStatus (const (Failed "Select at least one expired request."))
      else
        withSelected \tid ->
          runWriteWith
            (\_ -> reloadLater (Just tid))
            ( \w ->
                env.helpers.rejectExpired (addr w) placeholderCageConfig tid
                  selectedRequestIds
            )

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

    toggleRequestSelection :: RequestId -> Effect Unit
    toggleRequestSelection rid =
      setSelectedRequestIds \current ->
        if any (_ == rid) current then
          filter (_ /= rid) current
        else
          current <> [ rid ]

  useEffectOnce do
    mode <- Theme.initialThemeMode
    setThemeMode (const mode)
    ws <- W.availableWallets
    setWallets (const ws)
    loadTokens
    pure (pure unit)

  useEffect (map _.key wallet) do
    loadTokens
    case wallet of
      Nothing -> pure (pure unit)
      Just w -> W.subscribeAccountChanges w.api (refreshWallet w)

  useEffect (show myTokensOnly <> fromMaybe "" walletOwnerHash) do
    case tokens of
      Success ts -> do
        let visible = visibleTokenSummaries ts
        case selected of
          Just current | any (\s -> s.token == current) visible -> pure unit
          _ -> setSelected (const (map _.token (head visible)))
      _ -> pure unit
    pure (pure unit)

  useEffect (map (\(TokenId tid) -> tid) selected) do
    case selected of
      Nothing -> do
        setFacts (const NotAsked)
        setRequests (const NotAsked)
        setSelectedRequestIds (const [])
        setTokenState (const NotAsked)
      Just tid -> loadTokenDetails tid
    pure (pure unit)

  let
    processTime = case tokenState of
      Success st -> toNumber st.processTime
      _ -> toNumber placeholderCageConfig.defaultProcessTime

    retractTime = case tokenState of
      Success st -> toNumber st.retractTime
      _ -> toNumber placeholderCageConfig.defaultRetractTime

    selectedToken = selected

    tokenSummaries = case tokens of
      Success ts -> ts
      _ -> []

    visibleTokens =
      visibleTokenSummaries tokenSummaries

    selectedSummary =
      selected >>= \tid -> find (\s -> s.token == tid) tokenSummaries

    selectedTokenOwner =
      case tokenState of
        Success st -> Just st.owner
        _ -> selectedSummary >>= _.owner

    selectedOwned =
      case selectedTokenOwner, walletOwnerHash of
        Just tokenOwner, Just owner -> tokenOwner == owner
        _, _ -> false

    canWriteSelected =
      isJust wallet && selectedOwned

  pure $ M.themeProvider { theme: M.themeForMode themeMode }
    [ M.cssBaseline
    , appBarView wallet wallets connecting themeMode connectFirst refreshWallet toggleTheme
    , M.container
        { maxWidth: false
        , sx:
            { py: 2
            , px: { xs: 1.5, md: 3 }
            , bgcolor: "background.default"
            }
        }
        [ howItWorksHint hintOpen (setHintOpen (const false))
        , M.box
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
                , visibleTokens
                , walletOwnerHash
                , myTokensOnly
                , selected
                , onConnect: connectWallet
                , onConnectFirst: connectFirst
                , onRefresh: loadTokens
                , onRegister: registerToken
                , onToggleMyTokens: toggleMyTokensOnly
                , onSelect:
                    \tid -> do
                      setSelected (const (Just tid))
                      setSelectedRequestIds (const [])
                      setDialog (const NoDialog)
                }
            , workspaceView
                { wallet
                , selected: selectedToken
                , tokenOwner: selectedTokenOwner
                , walletOwnerHash
                , tokenState
                , facts
                , requests
                , selectedRequestIds
                , nowMillis
                , processTime
                , retractTime
                , status
                , canWrite: canWriteSelected
                , myTokensOnly
                , totalTokenCount: length tokenSummaries
                , visibleTokenCount: length visibleTokens
                , writeBlockedReason: writeBlockedReason wallet selectedToken selectedOwned
                , onConnect: connectFirst
                , onRefresh:
                    maybe loadTokens
                      ( \tid -> do
                          loadTokens
                          loadTokenDetails tid
                      )
                      selectedToken
                , onRegister: registerToken
                , onShowAll:
                    if myTokensOnly then toggleMyTokensOnly else pure unit
                , onAdd: setDialog (const (InsertDialog { key: "", value: "" }))
                , onProcess: processRequests
                , onRejectExpired: rejectExpired
                , onToggleRequest: toggleRequestSelection
                , onSelectRequests: \ids -> setSelectedRequestIds (const ids)
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
  -> String
  -> Effect Unit
  -> (WalletState -> Effect Unit)
  -> Effect Unit
  -> JSX
appBarView wallet wallets connecting themeMode connectFirst refreshWallet toggleTheme =
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
        , iconTip (if themeMode == "dark" then "Use light theme" else "Use dark theme")
            [ M.iconButton
                { size: "small"
                , "aria-label": "Toggle theme"
                , onClick: toggleTheme
                }
                [ if themeMode == "dark" then
                    M.lightModeIcon { fontSize: "small" }
                  else
                    M.darkModeIcon { fontSize: "small" }
                ]
            ]
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
     , tokens :: Remote (Array TokenSummary)
     , visibleTokens :: Array TokenSummary
     , walletOwnerHash :: Maybe String
     , myTokensOnly :: Boolean
     , selected :: Maybe TokenId
     , onConnect :: W.WalletInfo -> Effect Unit
     , onConnectFirst :: Effect Unit
     , onRefresh :: Effect Unit
     , onRegister :: Effect Unit
     , onToggleMyTokens :: Effect Unit
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
                  ( [ M.typography
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
                    ]
                      <>
                        ( if isJust props.wallet then
                            [ iconTip "Register new token"
                                [ M.iconButton
                                    { size: "small"
                                    , color: "primary"
                                    , "aria-label": "Register new token"
                                    , onClick: props.onRegister
                                    }
                                    [ M.appRegistrationIcon { fontSize: "small" } ]
                                ]
                            ]
                          else
                            []
                        )
                  )
              , mineOnlyControl props.wallet props.myTokensOnly props.onToggleMyTokens
              , remoteView props.tokens
                  ( tokenList
                      props.wallet
                      props.walletOwnerHash
                      props.myTokensOnly
                      props.visibleTokens
                      props.onConnectFirst
                      props.onRegister
                      props.onToggleMyTokens
                      props.selected
                      props.onSelect
                  )
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

mineOnlyControl :: Maybe WalletState -> Boolean -> Effect Unit -> JSX
mineOnlyControl wallet checked onToggle =
  M.box
    { sx:
        { px: 1.5
        , py: 0.75
        , borderBottom: 1
        , borderColor: "divider"
        , display: "flex"
        , alignItems: "center"
        , gap: 1
        }
    }
    [ M.typography { variant: "caption", sx: { flexGrow: 1, fontWeight: 600 } }
        [ R.text "Mine only" ]
    , M.tooltip
        { title:
            if isJust wallet then
              "Show only tokens owned by the connected account."
            else
              "Connect a wallet to filter owned tokens."
        }
        [ M.box { component: "span" }
            [ M.switch
                { size: "small"
                , checked
                , disabled: not (isJust wallet)
                , onClick: onToggle
                , inputProps: { "aria-label": "Mine only" }
                }
            ]
        ]
    ]

tokenList
  :: Maybe WalletState
  -> Maybe String
  -> Boolean
  -> Array TokenSummary
  -> Effect Unit
  -> Effect Unit
  -> Effect Unit
  -> Maybe TokenId
  -> (TokenId -> Effect Unit)
  -> Array TokenSummary
  -> JSX
tokenList wallet ownerHash myOnly visible onConnect onRegister onToggle selected onSelect allTokens =
  if null allTokens then
    tokenEmptyState wallet onConnect onRegister
  else if null visible then
    M.box { sx: { p: 1.5 } }
      [ M.alert { severity: "info" }
          [ M.stack { spacing: 1 }
              ( [ R.text
                    ( if isJust wallet then
                        "No tokens owned by this account."
                      else
                        "Connect a wallet to show owned tokens."
                    )
                ]
                  <> case wallet of
                    Nothing ->
                      [ M.button
                          { variant: "contained"
                          , size: "small"
                          , onClick: onConnect
                          , sx: { gap: 0.75, alignSelf: "flex-start" }
                          }
                          [ M.manageAccountsIcon { fontSize: "small" }, R.text "Connect wallet" ]
                      ]
                    Just _ ->
                      [ M.stack { direction: "row", spacing: 1 }
                          [ M.button
                              { variant: "contained"
                              , size: "small"
                              , onClick: onRegister
                              , sx: { gap: 0.75 }
                              }
                              [ M.appRegistrationIcon { fontSize: "small" }, R.text "Register new token" ]
                          , M.button
                              { variant: "outlined"
                              , size: "small"
                              , disabled: not myOnly
                              , onClick: onToggle
                              }
                              [ R.text "Show all" ]
                          ]
                      ]
              )
          ]
      ]
  else
    M.list { dense: true, disablePadding: true }
      (map (tokenRow ownerHash selected onSelect) visible)

tokenEmptyState :: Maybe WalletState -> Effect Unit -> Effect Unit -> JSX
tokenEmptyState wallet onConnect onRegister =
  M.box { sx: { p: 1.5 } }
    [ M.alert { severity: "info" }
        [ M.stack { spacing: 1 }
            ( [ R.text "No tokens yet." ]
                <> case wallet of
                  Nothing ->
                    [ M.button
                        { variant: "contained"
                        , size: "small"
                        , onClick: onConnect
                        , sx: { gap: 0.75, alignSelf: "flex-start" }
                        }
                        [ M.manageAccountsIcon { fontSize: "small" }, R.text "Connect wallet" ]
                    ]
                  Just _ ->
                    [ M.button
                        { variant: "contained"
                        , size: "small"
                        , onClick: onRegister
                        , sx: { gap: 0.75, alignSelf: "flex-start" }
                        }
                        [ M.appRegistrationIcon { fontSize: "small" }, R.text "Register new token" ]
                    ]
            )
        ]
    ]

tokenRow
  :: Maybe String
  -> Maybe TokenId
  -> (TokenId -> Effect Unit)
  -> TokenSummary
  -> JSX
tokenRow ownerHash selected onSelect summary@{ token: tid@(TokenId token) } =
  M.listItemButton
    { selected: selected == Just tid
    , onClick: onSelect tid
    , sx: { borderBottom: 1, borderColor: "divider" }
    }
    [ M.box { sx: { minWidth: 0, flexGrow: 1 } }
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
    , ownershipChip ownerHash summary
    ]

ownershipChip :: Maybe String -> TokenSummary -> JSX
ownershipChip ownerHash summary =
  case ownerHash, summary.owner of
    Just owner, Just tokenOwner | owner == tokenOwner ->
      M.chip { label: "Mine", size: "small", color: "success", variant: "outlined" }
    Just _, Just _ ->
      M.chip { label: "Read-only", size: "small", variant: "outlined" }
    _, Just tokenOwner ->
      M.chip { label: shortText 6 4 tokenOwner, size: "small", variant: "outlined" }
    _, Nothing -> mempty

workspaceView
  :: { wallet :: Maybe WalletState
     , selected :: Maybe TokenId
     , tokenOwner :: Maybe String
     , walletOwnerHash :: Maybe String
     , tokenState :: Remote TokenState
     , facts :: Remote (Array FactEntry)
     , requests :: Remote (Array PendingRequest)
     , selectedRequestIds :: Array RequestId
     , nowMillis :: Number
     , processTime :: Number
     , retractTime :: Number
     , status :: OpStatus
     , canWrite :: Boolean
     , myTokensOnly :: Boolean
     , totalTokenCount :: Int
     , visibleTokenCount :: Int
     , writeBlockedReason :: Maybe String
     , onConnect :: Effect Unit
     , onRefresh :: Effect Unit
     , onRegister :: Effect Unit
     , onShowAll :: Effect Unit
     , onAdd :: Effect Unit
     , onProcess :: Effect Unit
     , onRejectExpired :: Effect Unit
     , onToggleRequest :: RequestId -> Effect Unit
     , onSelectRequests :: Array RequestId -> Effect Unit
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
        [ selectedTitle props.selected props.tokenState props.tokenOwner props.walletOwnerHash
        , M.stack { direction: "row", spacing: 0.5, sx: { ml: "auto" } }
            ( [ iconTip "Refresh"
                  [ M.iconButton
                      { size: "small"
                      , "aria-label": "Refresh"
                      , onClick: props.onRefresh
                      }
                      [ M.refreshIcon { fontSize: "small" } ]
                  ]
              ]
                <>
                  ( if props.canWrite then
                      [ iconTip "End token"
                          [ M.iconButton
                              { size: "small"
                              , color: "error"
                              , "aria-label": "End token"
                              , onClick: props.onEnd
                              }
                              [ M.stopCircleIcon { fontSize: "small" } ]
                          ]
                      ]
                    else
                      []
                  )
            )
        ]
    , M.box { sx: { px: 2 } } [ statusView props.status ]
    , case props.selected of
        Nothing ->
          emptyWorkspace props.wallet props.totalTokenCount props.visibleTokenCount
            props.myTokensOnly
            props.onConnect
            props.onRegister
            props.onShowAll
        Just _ ->
          M.box { sx: { p: 2 } }
            [ ownerRoleNotice props.wallet props.tokenOwner props.walletOwnerHash props.onConnect
            , factsRegion
                props.canWrite
                props.writeBlockedReason
                props.onAdd
                props.walletOwnerHash
                props.nowMillis
                props.processTime
                props.retractTime
                props.facts
                props.requests
                props.selectedRequestIds
                props.onEdit
                props.onDelete
                props.onRetract
                props.onProcess
                props.onRejectExpired
                props.onToggleRequest
                props.onSelectRequests
            ]
    ]

selectedTitle :: Maybe TokenId -> Remote TokenState -> Maybe String -> Maybe String -> JSX
selectedTitle selected tokenState tokenOwner walletOwnerHash =
  M.box { sx: { minWidth: 0 } }
    [ M.typography { variant: "subtitle2", sx: { fontWeight: 700 } }
        [ R.text "Token" ]
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
            , maybe mempty (ownerChip walletOwnerHash) tokenOwner
            ]
    ]

ownerChip :: Maybe String -> String -> JSX
ownerChip walletOwnerHash tokenOwner =
  M.chip
    { label:
        "owner "
          <> shortText 8 6 tokenOwner
          <> case walletOwnerHash of
            Just mine | mine == tokenOwner -> " (you)"
            Just _ -> " (not you)"
            Nothing -> ""
    , size: "small"
    , color:
        case walletOwnerHash of
          Just mine | mine == tokenOwner -> "success"
          _ -> "default"
    , variant: "outlined"
    }

ownerRoleNotice :: Maybe WalletState -> Maybe String -> Maybe String -> Effect Unit -> JSX
ownerRoleNotice wallet tokenOwner walletOwnerHash onConnect =
  case wallet of
    Nothing ->
      M.alert { severity: "info", sx: { mb: 2 } }
        [ M.stack { direction: "row", spacing: 1, sx: { alignItems: "center", flexWrap: "wrap" } }
            [ R.text "Connect a wallet to request changes or manage pending requests."
            , M.button
                { variant: "contained"
                , size: "small"
                , onClick: onConnect
                , sx: { gap: 0.75 }
                }
                [ M.manageAccountsIcon { fontSize: "small" }, R.text "Connect wallet" ]
            ]
        ]
    Just _ ->
      M.box { sx: { mb: 2 } }
        [ M.stack { direction: "row", spacing: 1, sx: { alignItems: "center", flexWrap: "wrap" } }
            ( [ case tokenOwner of
                  Just owner -> ownerChip walletOwnerHash owner
                  Nothing -> M.chip { label: "owner unknown", size: "small", variant: "outlined" }
              ]
                <>
                  ( case tokenOwner, walletOwnerHash of
                      Just owner, Just mine | owner == mine ->
                        [ M.chip { label: "you can manage this token", size: "small", color: "success" } ]
                      Just _, Just _ ->
                        [ M.chip { label: "read-only for this token", size: "small", variant: "outlined" } ]
                      _, Nothing ->
                        [ M.chip { label: "wallet owner hash unavailable", size: "small", variant: "outlined" } ]
                      _, _ -> []
                  )
            )
        ]

factsRegion
  :: Boolean
  -> Maybe String
  -> Effect Unit
  -> Maybe String
  -> Number
  -> Number
  -> Number
  -> Remote (Array FactEntry)
  -> Remote (Array PendingRequest)
  -> Array RequestId
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> Effect Unit
  -> Effect Unit
  -> (RequestId -> Effect Unit)
  -> (Array RequestId -> Effect Unit)
  -> JSX
factsRegion
  canWrite
  blockedReason
  onAdd
  walletOwnerHash
  nowMillis
  processTime
  retractTime
  facts
  requests
  selectedRequestIds
  onEdit
  onDelete
  onRetract
  onProcess
  onRejectExpired
  onToggleRequest
  onSelectRequests =
  case facts, requests of
    Loading, _ -> centred [ M.circularProgress {} ]
    _, Loading -> centred [ M.circularProgress {} ]
    Failure msg, _ -> M.alert { severity: "error" } [ R.text msg ]
    _, Failure msg -> M.alert { severity: "error" } [ R.text msg ]
    Success fs, Success rs ->
      M.stack { spacing: 2 }
        [ factsSection canWrite blockedReason onAdd fs rs onEdit onDelete
        , requestsSection
            canWrite
            walletOwnerHash
            nowMillis
            processTime
            retractTime
            rs
            selectedRequestIds
            onProcess
            onRejectExpired
            onToggleRequest
            onSelectRequests
            onRetract
        ]
    _, _ -> mempty

factsSection
  :: Boolean
  -> Maybe String
  -> Effect Unit
  -> Array FactEntry
  -> Array PendingRequest
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> JSX
factsSection canWrite blockedReason onAdd fs rs onEdit onDelete =
  M.box {}
    [ sectionHeader "Facts"
        [ M.typography { variant: "body2", color: "text.secondary" }
            [ R.text "Committed key/value facts. Edits create pending requests." ]
        ]
        ( if canWrite then
            [ M.button
                { variant: "contained"
                , size: "small"
                , onClick: onAdd
                , sx: { gap: 0.75 }
                }
                [ M.addIcon { fontSize: "small" }, R.text "Add fact" ]
            ]
          else
            []
        )
    , if null fs then
        M.alert { severity: "info" }
          [ M.stack { spacing: 1, sx: { alignItems: "flex-start" } }
              ( [ R.text "No committed facts for this token." ]
                  <>
                    ( if canWrite then
                        []
                      else
                        maybe [] (\msg -> [ M.typography { variant: "caption" } [ R.text msg ] ])
                          blockedReason
                    )
              )
          ]
      else
        factTable canWrite blockedReason fs rs onEdit onDelete
    ]

factTable
  :: Boolean
  -> Maybe String
  -> Array FactEntry
  -> Array PendingRequest
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> JSX
factTable canWrite blockedReason fs rs onEdit onDelete =
  M.tableContainer { sx: { maxHeight: "calc(50vh - 120px)" } }
    [ M.table { size: "small", stickyHeader: true, "aria-label": "Facts" }
        [ M.tableHead {}
            [ M.tableRow {}
                [ M.tableCell { sx: { width: "34%" } } [ R.text "Key" ]
                , M.tableCell { sx: { width: "46%" } } [ R.text "Value" ]
                , M.tableCell { align: "right", sx: { width: "20%" } } [ R.text "Actions" ]
                ]
            ]
        , M.tableBody {}
            (map (factRow canWrite blockedReason rs onEdit onDelete) fs)
        ]
    ]

factRow
  :: Boolean
  -> Maybe String
  -> Array PendingRequest
  -> (FactEntry -> Effect Unit)
  -> (FactEntry -> Effect Unit)
  -> FactEntry
  -> JSX
factRow canWrite blockedReason requestsForToken onEdit onDelete fact =
  M.tableRow { key: "fact-" <> unKey fact.key, hover: true }
    [ keyCell fact.key
    , valueCell fact.value
    , M.tableCell { align: "right" }
        [ if canWrite then
            M.stack { direction: "row", spacing: 0.5, sx: { justifyContent: "flex-end" } }
              [ iconTip (rowActionTip blockedReason pending "Edit fact")
                  [ M.iconButton
                      { size: "small"
                      , "aria-label": "Edit fact " <> factText fact.key
                      , disabled: not (null pending)
                      , onClick: onEdit fact
                      }
                      [ M.editIcon { fontSize: "small" } ]
                  ]
              , iconTip (rowActionTip blockedReason pending "Delete fact")
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
          else
            mempty
        ]
    ]
  where
  pending = filter (\req -> req.key == fact.key) requestsForToken

requestsSection
  :: Boolean
  -> Maybe String
  -> Number
  -> Number
  -> Number
  -> Array PendingRequest
  -> Array RequestId
  -> Effect Unit
  -> Effect Unit
  -> (RequestId -> Effect Unit)
  -> (Array RequestId -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> JSX
requestsSection
  canWrite
  walletOwnerHash
  nowMillis
  processTime
  retractTime
  requests
  selectedRequestIds
  onProcess
  onRejectExpired
  onToggleRequest
  onSelectRequests
  onRetract =
  M.box {}
    [ sectionHeader "Pending requests"
        [ M.typography { variant: "body2", color: "text.secondary" }
            [ R.text "Pending requests are selected individually before they are processed or rejected." ]
        ]
        (requestToolbar canWrite selectedCount canProcess canReject onProcess onRejectExpired)
    , if null requests then
        M.alert { severity: "info" } [ R.text "No pending requests." ]
      else
        M.tableContainer { sx: { maxHeight: "calc(50vh - 120px)" } }
          [ M.table { size: "small", stickyHeader: true, "aria-label": "Pending requests" }
              [ M.tableHead {}
                  [ M.tableRow {}
                      [ M.tableCell { padding: "checkbox", sx: { width: 48 } }
                          [ if canWrite then
                              M.checkbox
                                { size: "small"
                                , checked: allChecked
                                , indeterminate: someChecked && not allChecked
                                , onChange:
                                    M.onCheckedChange \checked ->
                                      onSelectRequests
                                        ( if checked then
                                            map _.requestId requests
                                          else
                                            []
                                        )
                                , inputProps: { "aria-label": "Select all requests" }
                                }
                            else
                              mempty
                          ]
                      , M.tableCell { sx: { width: "11%" } } [ R.text "Op" ]
                      , M.tableCell { sx: { width: "20%" } } [ R.text "Key" ]
                      , M.tableCell { sx: { width: "20%" } } [ R.text "Value" ]
                      , M.tableCell { sx: { width: "17%" } } [ R.text "Owner" ]
                      , M.tableCell { sx: { width: "11%" } } [ R.text "Age" ]
                      , M.tableCell { sx: { width: "13%" } } [ R.text "Phase" ]
                      , M.tableCell { align: "right", sx: { width: "8%" } } [ R.text "Actions" ]
                      ]
                  ]
              , M.tableBody {}
                  ( map
                      ( requestRow
                          canWrite
                          walletOwnerHash
                          selectedRequestIds
                          nowMillis
                          processTime
                          retractTime
                          onToggleRequest
                          onRetract
                      )
                      requests
                  )
              ]
          ]
    ]
  where
  selectedRequests =
    filter (\req -> requestSelected selectedRequestIds req.requestId) requests

  selectedCount =
    length selectedRequests

  someChecked =
    selectedCount > 0

  allChecked =
    someChecked && selectedCount == length requests

  canProcess =
    someChecked
      && length (filter (\req -> requestPhase nowMillis processTime retractTime req == PhaseProcessable) selectedRequests) == selectedCount

  canReject =
    someChecked
      && length (filter (\req -> requestPhase nowMillis processTime retractTime req == PhaseExpired) selectedRequests) == selectedCount

requestToolbar
  :: Boolean
  -> Int
  -> Boolean
  -> Boolean
  -> Effect Unit
  -> Effect Unit
  -> Array JSX
requestToolbar canWrite selectedCount canProcess canReject onProcess onRejectExpired =
  if canWrite then
    [ M.stack { direction: "row", spacing: 1, sx: { alignItems: "center", flexWrap: "wrap" } }
        [ M.typography { variant: "caption", color: "text.secondary" }
            [ R.text (show selectedCount <> " selected") ]
        , iconTip (selectionTip "processable" selectedCount canProcess)
            [ M.button
                { variant: "contained"
                , size: "small"
                , color: "success"
                , disabled: not canProcess
                , onClick: onProcess
                , sx: { gap: 0.75 }
                }
                [ M.playlistAddCheckIcon { fontSize: "small" }, R.text "Process selected" ]
            ]
        , iconTip (selectionTip "expired" selectedCount canReject)
            [ M.button
                { variant: "outlined"
                , size: "small"
                , color: "warning"
                , disabled: not canReject
                , onClick: onRejectExpired
                , sx: { gap: 0.75 }
                }
                [ M.blockIcon { fontSize: "small" }, R.text "Reject selected" ]
            ]
        ]
    ]
  else
    []

requestRow
  :: Boolean
  -> Maybe String
  -> Array RequestId
  -> Number
  -> Number
  -> Number
  -> (RequestId -> Effect Unit)
  -> (RequestId -> Effect Unit)
  -> PendingRequest
  -> JSX
requestRow
  canWrite
  walletOwnerHash
  selectedRequestIds
  nowMillis
  processTime
  retractTime
  onToggleRequest
  onRetract
  req =
  M.tableRow { key: "request-" <> unRequestId req.requestId, hover: true }
    [ M.tableCell { padding: "checkbox" }
        [ if canWrite then
            M.checkbox
              { size: "small"
              , checked: requestSelected selectedRequestIds req.requestId
              , onChange: M.onCheckedChange \_ -> onToggleRequest req.requestId
              , inputProps:
                  { "aria-label": "Select request " <> unRequestId req.requestId }
              }
          else
            mempty
        ]
    , M.tableCell {} [ M.chip { label: opLabel req.operation, size: "small", color: "primary", variant: "outlined" } ]
    , keyCell req.key
    , M.tableCell {} [ M.typography { variant: "body2" } [ R.text (maybe "" valueText req.value) ] ]
    , requestOwnerCell walletOwnerHash req.owner
    , M.tableCell {}
        [ M.chip { label: formatAgeMillis (nowMillis - req.submittedAt), size: "small", variant: "outlined" } ]
    , M.tableCell {} [ phaseChip phase ]
    , M.tableCell { align: "right" }
        [ if requestOwned walletOwnerHash req && phase == PhaseRetractable then
            iconTip "Retract request"
              [ M.iconButton
                  { size: "small"
                  , "aria-label": "Retract request " <> unRequestId req.requestId
                  , onClick: onRetract req.requestId
                  }
                  [ M.undoIcon { fontSize: "small" } ]
              ]
          else
            mempty
        ]
    ]
  where
  phase =
    requestPhase nowMillis processTime retractTime req

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

valueCell :: Value -> JSX
valueCell value =
  M.tableCell {}
    [ M.typography { variant: "body2" } [ R.text (valueText value) ] ]

requestOwnerCell :: Maybe String -> String -> JSX
requestOwnerCell walletOwnerHash owner =
  M.tableCell {}
    [ M.stack { direction: "row", spacing: 0.5, sx: { alignItems: "center", flexWrap: "wrap" } }
        ( [ M.typography { variant: "caption", sx: { fontFamily: "monospace" } }
              [ R.text (shortText 8 6 owner) ]
          ]
            <>
              ( case walletOwnerHash of
                  Just mine | mine == owner ->
                    [ M.chip { label: "you", size: "small", color: "success", variant: "outlined" } ]
                  Just _ ->
                    [ M.chip { label: "not you", size: "small", variant: "outlined" } ]
                  Nothing -> []
              )
        )
    ]

phaseChip :: RequestPhase -> JSX
phaseChip phase =
  M.chip
    { label: phaseLabel phase
    , size: "small"
    , color: phaseColor phase
    , icon:
        case phase of
          PhaseProcessable -> M.checkCircleIcon {}
          PhaseRetractable -> M.undoIcon {}
          PhaseExpired -> M.warningAmberIcon {}
    }

sectionHeader :: String -> Array JSX -> Array JSX -> JSX
sectionHeader title detail actions =
  M.box
    { sx:
        { display: "flex"
        , gap: 1
        , alignItems: "center"
        , justifyContent: "space-between"
        , flexWrap: "wrap"
        , mb: 1
        }
    }
    [ M.box { sx: { minWidth: 0 } }
        ( [ M.typography { variant: "h6", sx: { fontSize: "1rem", fontWeight: 700 } }
              [ R.text title ]
          ]
            <> detail
        )
    , M.stack { direction: "row", spacing: 1, sx: { alignItems: "center", flexWrap: "wrap" } }
        actions
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

howItWorksHint :: Boolean -> Effect Unit -> JSX
howItWorksHint open onClose =
  if not open then
    mempty
  else
    M.alert
      { severity: "info"
      , sx: { mb: 2 }
      , action:
          M.iconButton
            { size: "small"
            , "aria-label": "Dismiss"
            , onClick: onClose
            }
            [ M.closeIcon { fontSize: "small" } ]
      }
      [ R.text "Connect an account, register a token, add facts, then process pending requests to commit changes." ]

emptyWorkspace
  :: Maybe WalletState
  -> Int
  -> Int
  -> Boolean
  -> Effect Unit
  -> Effect Unit
  -> Effect Unit
  -> JSX
emptyWorkspace wallet totalTokenCount visibleTokenCount myOnly onConnect onRegister onShowAll =
  M.box { sx: { p: 2 } }
    [ M.alert { severity: "info" }
        [ M.stack { spacing: 1, sx: { alignItems: "flex-start" } }
            ( [ R.text message ]
                <> actions
            )
        ]
    ]
  where
  message =
    case wallet of
      Nothing -> "Connect a wallet to work with your tokens."
      Just _ | totalTokenCount == 0 -> "Register your first token."
      Just _ | myOnly && visibleTokenCount == 0 -> "No owned tokens are visible."
      _ -> "Select a token."

  actions =
    case wallet of
      Nothing ->
        [ M.button
            { variant: "contained"
            , size: "small"
            , onClick: onConnect
            , sx: { gap: 0.75 }
            }
            [ M.manageAccountsIcon { fontSize: "small" }, R.text "Connect wallet" ]
        ]
      Just _ | totalTokenCount == 0 ->
        [ M.button
            { variant: "contained"
            , size: "small"
            , onClick: onRegister
            , sx: { gap: 0.75 }
            }
            [ M.appRegistrationIcon { fontSize: "small" }, R.text "Register new token" ]
        ]
      Just _ | myOnly && visibleTokenCount == 0 ->
        [ M.stack { direction: "row", spacing: 1 }
            [ M.button
                { variant: "contained"
                , size: "small"
                , onClick: onRegister
                , sx: { gap: 0.75 }
                }
                [ M.appRegistrationIcon { fontSize: "small" }, R.text "Register new token" ]
            , M.button { variant: "outlined", size: "small", onClick: onShowAll }
                [ R.text "Show all" ]
            ]
        ]
      _ -> []

tokenSummary :: TokenId -> Either String TokenState -> TokenSummary
tokenSummary token stateResult =
  case stateResult of
    Right st -> { token, owner: Just st.owner, root: Just st.root }
    Left _ -> { token, owner: Nothing, root: Nothing }

visibleSummaries :: Maybe String -> Boolean -> Array TokenSummary -> Array TokenSummary
visibleSummaries ownerHash myOnly summaries =
  if not myOnly then
    summaries
  else
    case ownerHash of
      Nothing -> []
      Just owner ->
        filter (\summary -> summary.owner == Just owner) summaries

writeBlockedReason :: Maybe WalletState -> Maybe TokenId -> Boolean -> Maybe String
writeBlockedReason wallet selected owned =
  case wallet, selected of
    Nothing, _ -> Just "Connect a wallet to write."
    Just _, Nothing -> Just "Select a token to write."
    Just _, Just _ | owned -> Nothing
    Just _, Just _ -> Just "This token is owned by another account; writes are disabled."

selectionTip :: String -> Int -> Boolean -> String
selectionTip phaseName selectedCount enabled =
  if selectedCount == 0 then
    "Select " <> phaseName <> " requests."
  else if enabled then
    "Submit selected requests."
  else
    "Selected rows must all be " <> phaseName <> "."

rowActionTip :: Maybe String -> Array PendingRequest -> String -> String
rowActionTip blocked pending label =
  fromMaybe
    ( if null pending then
        label
      else
        "Resolve this fact's pending request first."
    )
    blocked

phaseColor :: RequestPhase -> String
phaseColor = case _ of
  PhaseProcessable -> "success"
  PhaseRetractable -> "info"
  PhaseExpired -> "warning"

requestSelected :: Array RequestId -> RequestId -> Boolean
requestSelected selectedRequestIds requestId =
  any (_ == requestId) selectedRequestIds

requestOwned :: Maybe String -> PendingRequest -> Boolean
requestOwned walletOwnerHash req =
  case walletOwnerHash of
    Just owner -> owner == req.owner
    Nothing -> false

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
