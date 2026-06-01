-- | Shared write-operation runner + status rendering.
-- |
-- | Every MPFS write op follows the same shape: build an unsigned tx through
-- | the `CageHelpers` boundary, ask the wallet for a CIP-30 witness, assemble
-- | the signed transaction in the wasm reactor, then POST it to `/submit`.
module MpfsSpa.Submit
  ( OpStatus(..)
  , runOp
  , statusView
  ) where

import Prelude

import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
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
runOp mWallet build setStatus = do
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
                    liftEffect $ put $ case submitted of
                      Left err -> Failed err
                      Right txId -> Submitted cbor witnessHex signedTxHex txId

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
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "unsigned tx" ]
      , hexBlock unsignedHex
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "witness set" ]
      , hexBlock witnessHex
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "signed tx" ]
      , hexBlock signedHex
      ]
  Failed msg -> M.alert { severity: "error", sx: { mt: 2 } } [ R.text msg ]

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
