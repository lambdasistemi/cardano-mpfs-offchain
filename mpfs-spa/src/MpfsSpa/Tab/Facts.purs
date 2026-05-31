-- | Facts tab: per-token fact management.
-- |
-- | For the selected token: request insert / update / delete of a fact, view
-- | pending requests with retract + reject-expired, and look up a fact's
-- | current value (proof-bearing endpoint; the SPA shows the value, the
-- | verifier checks the proof). All write ops go through `CageHelpers`.
module MpfsSpa.Tab.Facts (mkFactsTab, FactsProps) where

import Prelude

import Data.Array (null)
import Data.Either (either)
import Data.Maybe (Maybe(..), maybe)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import React.Basic (JSX)
import React.Basic.DOM as R
import React.Basic.Hooks (component, useEffect, useState, (/\))
import React.Basic.Hooks as React

import MpfsSpa.Config (placeholderCageConfig)
import MpfsSpa.Http (PendingRequest, getFactValue, getRequests)
import MpfsSpa.Material as M
import MpfsSpa.Shared (Env, Remote(..), WalletState, remoteView)
import MpfsSpa.Submit (OpStatus(..), runOp, statusView)
import MpfsSpa.Types
  ( Key(..)
  , RequestId
  , TokenId(..)
  , Value(..)
  , WalletAddr(..)
  )

type FactsProps =
  { env :: Env
  , wallet :: Maybe WalletState
  , selected :: Maybe TokenId
  }

type FormState =
  { insKey :: String
  , insVal :: String
  , updKey :: String
  , updOld :: String
  , updNew :: String
  , delKey :: String
  , delVal :: String
  , lookupKey :: String
  }

emptyForm :: FormState
emptyForm =
  { insKey: ""
  , insVal: ""
  , updKey: ""
  , updOld: ""
  , updNew: ""
  , delKey: ""
  , delVal: ""
  , lookupKey: ""
  }

mkFactsTab :: Effect (FactsProps -> JSX)
mkFactsTab = component "FactsTab" \props -> React.do
  form /\ setForm <- useState emptyForm
  status /\ setStatus <- useState Idle
  requests /\ setRequests <- useState (NotAsked :: Remote (Array PendingRequest))
  lookup /\ setLookup <- useState (NotAsked :: Remote Value)

  let
    loadRequests :: TokenId -> Effect Unit
    loadRequests tid = do
      setRequests (const Loading)
      launchAff_ do
        res <- getRequests props.env.cfg tid
        liftEffect (setRequests (const (either Failure Success res)))

  useEffect (map (\(TokenId h) -> h) props.selected) do
    case props.selected of
      Just tid -> loadRequests tid
      Nothing -> pure unit
    pure (pure unit)

  pure case props.selected of
    Nothing ->
      M.alert { severity: "info", sx: { mt: 2 } }
        [ R.text "Select a token in the Tokens tab first." ]
    Just tid ->
      let
        helpers = props.env.helpers

        -- Run an op built from the connected wallet; refuse without one.
        op :: (WalletState -> _) -> Effect Unit
        op build = case props.wallet of
          Just w -> runOp (Just w) (build w) setStatus
          Nothing -> setStatus (const (Failed "Connect a wallet first."))

        addr w = WalletAddr w.address

        onInsert = op \w ->
          helpers.insertFact (addr w) placeholderCageConfig tid
            (Key form.insKey)
            (Value form.insVal)

        onUpdate = op \w ->
          helpers.updateFact (addr w) placeholderCageConfig tid
            (Key form.updKey)
            (Value form.updOld)
            (Value form.updNew)

        onDelete = op \w ->
          helpers.deleteFact (addr w) placeholderCageConfig tid
            (Key form.delKey)
            (Value form.delVal)

        onRejectExpired = op \w ->
          helpers.rejectExpired (addr w) placeholderCageConfig tid

        onRetract rid = op \w ->
          helpers.retractRequest (addr w) placeholderCageConfig tid rid

        onLookup =
          launchAff_ do
            liftEffect (setLookup (const Loading))
            res <- getFactValue props.env.cfg tid (Key form.lookupKey)
            liftEffect (setLookup (const (either Failure Success res)))

        upd f v = setForm (\s -> f s v)
      in
        M.stack { spacing: 2, sx: { mt: 2 } }
          [ M.typography { variant: "h6" } [ R.text "Facts" ]
          , tokenChip tid
          , statusView status
          , M.card { variant: "outlined" }
              [ M.cardHeader
                  { title: "Request a fact change"
                  , titleTypographyProps: { variant: "subtitle1" }
                  }
              , M.cardContent {}
                  [ M.stack { spacing: 3 }
                      [ opSection "Insert" "Request insert" onInsert
                          [ tf "Key" form.insKey (upd \s v -> s { insKey = v })
                          , tf "Value" form.insVal (upd \s v -> s { insVal = v })
                          ]
                      , M.divider {}
                      , opSection "Update" "Request update" onUpdate
                          [ tf "Key" form.updKey (upd \s v -> s { updKey = v })
                          , tf "Old value" form.updOld (upd \s v -> s { updOld = v })
                          , tf "New value" form.updNew (upd \s v -> s { updNew = v })
                          ]
                      , M.divider {}
                      , opSection "Delete" "Request delete" onDelete
                          [ tf "Key" form.delKey (upd \s v -> s { delKey = v })
                          , tf "Value" form.delVal (upd \s v -> s { delVal = v })
                          ]
                      ]
                  ]
              ]
          , lookupCard form.lookupKey (upd \s v -> s { lookupKey = v }) onLookup lookup
          , pendingCard (loadRequests tid) onRejectExpired onRetract requests
          ]

tokenChip :: TokenId -> JSX
tokenChip (TokenId hex) =
  M.chip
    { label: hex, variant: "outlined", sx: { fontFamily: "monospace" } }

-- | A labelled text field over a single form value.
tf :: String -> String -> (String -> Effect Unit) -> JSX
tf label value onChange =
  M.textField
    { label
    , value
    , size: "small"
    , fullWidth: true
    , onChange: M.onValueChange onChange
    }

-- | One insert/update/delete section: its fields and a submit button.
opSection :: String -> String -> Effect Unit -> Array JSX -> JSX
opSection title buttonLabel onSubmit fields =
  M.stack { spacing: 1 }
    [ M.typography { variant: "subtitle2" } [ R.text title ]
    , M.stack { spacing: 1 } fields
    , M.box {}
        [ M.button
            { variant: "contained", size: "small", onClick: onSubmit }
            [ R.text buttonLabel ]
        ]
    ]

lookupCard :: String -> (String -> Effect Unit) -> Effect Unit -> Remote Value -> JSX
lookupCard key onChange onLookup result =
  M.card { variant: "outlined" }
    [ M.cardHeader
        { title: "Look up a fact"
        , titleTypographyProps: { variant: "subtitle1" }
        }
    , M.cardContent {}
        [ M.stack { spacing: 1 }
            [ tf "Key" key onChange
            , M.box {}
                [ M.button
                    { variant: "outlined", size: "small", onClick: onLookup }
                    [ R.text "Look up" ]
                ]
            , remoteView result \(Value v) ->
                M.alert { severity: "success" }
                  [ M.box { sx: { fontFamily: "monospace", wordBreak: "break-all" } }
                      [ R.text v ]
                  ]
            ]
        ]
    ]

pendingCard
  :: Effect Unit
  -> Effect Unit
  -> (RequestId -> Effect Unit)
  -> Remote (Array PendingRequest)
  -> JSX
pendingCard onRefresh onRejectExpired onRetract requests =
  M.card { variant: "outlined" }
    [ M.cardHeader
        { title: "Pending requests"
        , titleTypographyProps: { variant: "subtitle1" }
        , action:
            M.stack { direction: "row", spacing: 1 }
              [ M.button
                  { size: "small", onClick: onRefresh }
                  [ R.text "Refresh" ]
              , M.button
                  { size: "small", color: "warning", onClick: onRejectExpired }
                  [ R.text "Reject expired" ]
              ]
        }
    , M.cardContent {}
        [ remoteView requests \rs ->
            if null rs then
              M.alert { severity: "info" } [ R.text "No pending requests." ]
            else
              M.list {} (map (requestRow onRetract) rs)
        ]
    ]

requestRow :: (RequestId -> Effect Unit) -> PendingRequest -> JSX
requestRow onRetract req =
  M.listItem
    { secondaryAction:
        M.button
          { size: "small"
          , color: "secondary"
          , onClick: onRetract req.requestId
          }
          [ R.text "Retract" ]
    }
    [ M.listItemText
        { primary: req.operation <> " · " <> unKey req.key
        , secondary: maybe "" unValue req.value
        }
    ]
  where
  unKey (Key k) = k
  unValue (Value v) = v
