-- | Shared write-operation runner + status rendering.
-- |
-- | Every MPFS write op follows the same shape: build an unsigned tx through
-- | the `CageHelpers` boundary, ask the wallet for a CIP-30 witness, assemble
-- | the signed transaction in the wasm reactor, then POST it to `/submit`.
module MpfsSpa.Submit
  ( OpStatus(..)
  , runOp
  , runOpAfterSubmit
  , statusView
  ) where

import Prelude

import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Data.String.CodeUnits as CU
import Data.String.Common as String
import Data.String.Pattern (Pattern(..))
import Effect (Effect)
import Effect.Aff (attempt, launchAff_)
import Effect.Class (liftEffect)
import React.Basic (JSX)
import React.Basic.DOM as R

import MpfsSpa.CageHelpers (CageResult)
import MpfsSpa.CageHelpers.Wasm (assembleTx)
import MpfsSpa.Config (serverConfig)
import MpfsSpa.Http (submitTx)
import MpfsSpa.Material as M
import MpfsSpa.Shared (WalletState)
import MpfsSpa.Types (UnsignedTxCbor(..), cageErrorMessage)
import MpfsSpa.Wallet.Cip30 as W

-- | Progress of a single write operation.
data OpStatus
  = Idle
  | Working
  | Built UnsignedTxCbor
  | Signed UnsignedTxCbor String
  | Submitted UnsignedTxCbor String String String
  | Failed String

-- | Run a cage operation: build the unsigned tx, then attempt to sign it with
-- | the connected wallet. State transitions are reported through `setStatus`,
-- | the React-style updater from `useState`.
runOp
  :: Maybe WalletState
  -> CageResult
  -> ((OpStatus -> OpStatus) -> Effect Unit)
  -> Effect Unit
runOp = runOpAfterSubmit (\_ -> pure unit)

runOpAfterSubmit
  :: (String -> Effect Unit)
  -> Maybe WalletState
  -> CageResult
  -> ((OpStatus -> OpStatus) -> Effect Unit)
  -> Effect Unit
runOpAfterSubmit onSubmitted mWallet build setStatus = do
  let put s = setStatus (const s)
  put Working
  launchAff_ do
    built <- build
    case built of
      Left err -> liftEffect (put (Failed (cageErrorMessage err)))
      Right cbor@(UnsignedTxCbor hex) ->
        case mWallet of
          Nothing -> liftEffect (put (Built cbor))
          Just w -> do
            signed <- attempt (W.signTx w.api hex true)
            case signed of
              Left _ ->
                liftEffect (put (Failed "Wallet signing failed or was declined."))
              Right witnessHex -> do
                assembled <- assembleTx hex witnessHex
                case assembled of
                  Left err -> liftEffect (put (Failed (cageErrorMessage err)))
                  Right signedTxHex -> do
                    httpCfg <- liftEffect serverConfig
                    submitted <- submitTx httpCfg signedTxHex
                    liftEffect $ case submitted of
                      Left err -> put (Failed err)
                      Right txId -> do
                        put (Submitted cbor witnessHex signedTxHex txId)
                        onSubmitted txId

-- | Render an operation status as Material feedback.
statusView :: OpStatus -> JSX
statusView = case _ of
  Idle -> mempty
  Working -> M.alert { severity: "info", sx: { mt: 2 } } [ R.text "Working…" ]
  Built (UnsignedTxCbor hex) ->
    M.alert { severity: "success", sx: { mt: 2 } }
      [ R.text "Built unsigned transaction"
      , hexBlock hex
      ]
  Signed (UnsignedTxCbor hex) witnessHex ->
    M.alert { severity: "success", sx: { mt: 2 } }
      [ R.text "Signed by wallet"
      , hexBlock hex
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "witness set" ]
      , hexBlock witnessHex
      ]
  Submitted (UnsignedTxCbor unsignedHex) witnessHex signedHex txId ->
    M.alert { severity: "success", sx: { mt: 2 } }
      [ R.text "Submitted transaction"
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "tx id" ]
      , hexBlock txId
      , M.link
          { href: "https://preprod.cardanoscan.io/transaction/" <> txId
          , target: "_blank"
          , rel: "noreferrer"
          }
          [ R.text "View on preprod Cardanoscan" ]
      , detailsBlock "Transaction details"
          [ M.typography { variant: "caption", color: "text.secondary" }
              [ R.text "unsigned tx" ]
          , hexBlock unsignedHex
          , M.typography { variant: "caption", color: "text.secondary" }
              [ R.text "witness set" ]
          , hexBlock witnessHex
          , M.typography { variant: "caption", color: "text.secondary" }
              [ R.text "signed tx" ]
          , hexBlock signedHex
          ]
      ]
  Failed msg -> errorView msg

errorView :: String -> JSX
errorView raw =
  let
    message = friendlyError raw
  in
    M.alert { severity: "error", sx: { mt: 2 } }
      ( [ R.text message ]
          <>
            if message == raw then
              []
            else
              [ detailsBlock "Raw detail" [ textBlock raw ] ]
      )

friendlyError :: String -> String
friendlyError raw =
  let
    lower = String.toLower raw
    contains needle = CU.contains (Pattern needle) lower
  in
    if
      contains "validity_upper_slot"
        && contains "must be greater than the snapshot slot" then
      "This request's processing window has expired - use Reject expired instead (or register a token, which now has a 30-min window)."
    else if contains "invalid bytestring size" then
      "Invalid input."
    else if contains "bad_facts:" then
      "The server returned facts the reactor could not use."
    else if contains "verify_error:" then
      "The transaction could not be verified against the current chain snapshot."
    else if contains "wallet" || contains "declined" then
      "Wallet signing failed or was declined."
    else if contains "http " || contains "submit" then
      "Transaction submission failed. Check the server response details and try again."
    else
      raw

detailsBlock :: String -> Array JSX -> JSX
detailsBlock summary children =
  M.box
    { component: "details"
    , sx: { mt: 1 }
    }
    ( [ M.box
          { component: "summary"
          , sx: { cursor: "pointer", fontSize: "0.85rem" }
          }
          [ R.text summary ]
      ]
        <> children
    )

textBlock :: String -> JSX
textBlock text =
  M.box
    { sx:
        { fontFamily: "monospace"
        , fontSize: "0.75rem"
        , whiteSpace: "pre-wrap"
        , wordBreak: "break-word"
        , mt: 1
        }
    }
    [ R.text text ]

hexBlock :: String -> JSX
hexBlock hex =
  M.box
    { sx:
        { fontFamily: "monospace"
        , fontSize: "0.75rem"
        , wordBreak: "break-all"
        , mt: 1
        }
    }
    [ R.text hex ]
