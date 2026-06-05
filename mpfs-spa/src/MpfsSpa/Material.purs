-- | Thin FFI bindings to Material UI v5 (`@mui/material`) components.
-- |
-- | Each binding wraps a MUI React component so it can be applied to a props
-- | record and (for container components) an array of child `JSX` nodes. The
-- | props rows are left open (`forall r`) so call sites pass exactly the MUI
-- | props they need; per-prop type safety is intentionally traded for the
-- | flexibility of MUI's very large prop surface. No protocol logic lives
-- | here — this module is purely presentational glue.
module MpfsSpa.Material
  ( Theme
  , defaultTheme
  , cssBaseline
  , container
  , box
  , stack
  , paper
  , typography
  , button
  , iconButton
  , appBar
  , toolbar
  , tabs
  , tab
  , card
  , cardContent
  , cardActions
  , cardHeader
  , list
  , listItem
  , listItemButton
  , listItemText
  , chip
  , divider
  , alert
  , alertTitle
  , circularProgress
  , link
  , tooltip
  , textField
  , table
  , tableBody
  , tableCell
  , tableContainer
  , tableHead
  , tableRow
  , dialog
  , dialogTitle
  , dialogContent
  , dialogActions
  , badge
  , addIcon
  , appRegistrationIcon
  , accountCircleIcon
  , blockIcon
  , checkCircleIcon
  , closeIcon
  , deleteIcon
  , editIcon
  , manageAccountsIcon
  , playlistAddCheckIcon
  , refreshIcon
  , saveIcon
  , stopCircleIcon
  , syncIcon
  , undoIcon
  , warningAmberIcon
  , themeProvider
  , EventHandler1
  , onTabChange
  , onValueChange
  ) where

import Prelude

import Effect (Effect)
import React.Basic (JSX)

-- | Opaque MUI theme object produced by `createTheme`.
foreign import data Theme :: Type

-- | The default light theme used across the app.
foreign import defaultTheme :: Theme

-- | `<CssBaseline />` — normalises browser styling, MUI baseline.
foreign import cssBaseline :: JSX

-- container components: props + children
foreign import container :: forall r. Record r -> Array JSX -> JSX
foreign import box :: forall r. Record r -> Array JSX -> JSX
foreign import stack :: forall r. Record r -> Array JSX -> JSX
foreign import paper :: forall r. Record r -> Array JSX -> JSX
foreign import typography :: forall r. Record r -> Array JSX -> JSX
foreign import button :: forall r. Record r -> Array JSX -> JSX
foreign import iconButton :: forall r. Record r -> Array JSX -> JSX
foreign import appBar :: forall r. Record r -> Array JSX -> JSX
foreign import toolbar :: forall r. Record r -> Array JSX -> JSX
foreign import tabs :: forall r. Record r -> Array JSX -> JSX
foreign import card :: forall r. Record r -> Array JSX -> JSX
foreign import cardContent :: forall r. Record r -> Array JSX -> JSX
foreign import cardActions :: forall r. Record r -> Array JSX -> JSX
foreign import list :: forall r. Record r -> Array JSX -> JSX
foreign import listItem :: forall r. Record r -> Array JSX -> JSX
foreign import listItemButton :: forall r. Record r -> Array JSX -> JSX
foreign import alert :: forall r. Record r -> Array JSX -> JSX
foreign import themeProvider :: forall r. Record r -> Array JSX -> JSX
foreign import link :: forall r. Record r -> Array JSX -> JSX
foreign import tooltip :: forall r. Record r -> Array JSX -> JSX
foreign import tableContainer :: forall r. Record r -> Array JSX -> JSX
foreign import table :: forall r. Record r -> Array JSX -> JSX
foreign import tableHead :: forall r. Record r -> Array JSX -> JSX
foreign import tableBody :: forall r. Record r -> Array JSX -> JSX
foreign import tableRow :: forall r. Record r -> Array JSX -> JSX
foreign import tableCell :: forall r. Record r -> Array JSX -> JSX
foreign import dialog :: forall r. Record r -> Array JSX -> JSX
foreign import dialogTitle :: forall r. Record r -> Array JSX -> JSX
foreign import dialogContent :: forall r. Record r -> Array JSX -> JSX
foreign import dialogActions :: forall r. Record r -> Array JSX -> JSX
foreign import badge :: forall r. Record r -> Array JSX -> JSX

-- leaf components: props only
foreign import tab :: forall r. Record r -> JSX
foreign import textField :: forall r. Record r -> JSX
foreign import chip :: forall r. Record r -> JSX
foreign import divider :: forall r. Record r -> JSX
foreign import circularProgress :: forall r. Record r -> JSX
foreign import cardHeader :: forall r. Record r -> JSX
foreign import listItemText :: forall r. Record r -> JSX
foreign import alertTitle :: forall r. Record r -> JSX
foreign import addIcon :: forall r. Record r -> JSX
foreign import appRegistrationIcon :: forall r. Record r -> JSX
foreign import accountCircleIcon :: forall r. Record r -> JSX
foreign import blockIcon :: forall r. Record r -> JSX
foreign import checkCircleIcon :: forall r. Record r -> JSX
foreign import closeIcon :: forall r. Record r -> JSX
foreign import deleteIcon :: forall r. Record r -> JSX
foreign import editIcon :: forall r. Record r -> JSX
foreign import manageAccountsIcon :: forall r. Record r -> JSX
foreign import playlistAddCheckIcon :: forall r. Record r -> JSX
foreign import refreshIcon :: forall r. Record r -> JSX
foreign import saveIcon :: forall r. Record r -> JSX
foreign import stopCircleIcon :: forall r. Record r -> JSX
foreign import syncIcon :: forall r. Record r -> JSX
foreign import undoIcon :: forall r. Record r -> JSX
foreign import warningAmberIcon :: forall r. Record r -> JSX

-- | An opaque DOM event handler suitable for a MUI `onChange` prop.
foreign import data EventHandler1 :: Type

-- | Adapt a `Tabs.onChange (event, value)` callback to a value handler.
foreign import _onTabChange :: (Int -> Effect Unit) -> EventHandler1

-- | Adapt an input `onChange` callback to receive `event.target.value`.
foreign import _onValueChange :: (String -> Effect Unit) -> EventHandler1

onTabChange :: (Int -> Effect Unit) -> EventHandler1
onTabChange = _onTabChange

onValueChange :: (String -> Effect Unit) -> EventHandler1
onValueChange = _onValueChange
