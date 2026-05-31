-- | Shared write-operation runner + status rendering.
-- |
-- | Every MPFS write op follows the same shape: build an unsigned tx through
-- | the `CageHelpers` boundary, then (if a wallet is connected) ask the wallet
-- | to sign it. Final witness assembly + submission is deferred to the wasm
-- | cage-helper bridge — see questions/Q-01. No protocol logic lives here; the
-- | unsigned tx is opaque hex produced by the boundary.
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
            -- Attempt the real CIP-30 signing path. With the mock's stub tx
            -- the wallet rejects the invalid body; we keep the built tx
            -- visible rather than fabricating a signature.
            signed <- attempt (W.signTx w.api hex false)
            liftEffect $ put $ case signed of
              Left _ -> Built cbor
              Right witnessHex -> Signed cbor witnessHex

-- | Render an operation status as Material feedback.
statusView :: OpStatus -> JSX
statusView = case _ of
  Idle -> mempty
  Working -> M.alert { severity: "info", sx: { mt: 2 } } [ R.text "Working…" ]
  Built (UnsignedTxCbor hex) ->
    M.alert { severity: "success", sx: { mt: 2 } }
      [ R.text "Built unsigned transaction"
      , hexBlock hex
      , note
      ]
  Signed (UnsignedTxCbor hex) witnessHex ->
    M.alert { severity: "success", sx: { mt: 2 } }
      [ R.text "Signed by wallet"
      , hexBlock hex
      , M.typography { variant: "caption", color: "text.secondary" }
          [ R.text "witness set" ]
      , hexBlock witnessHex
      , note
      ]
  Failed msg -> M.alert { severity: "error", sx: { mt: 2 } } [ R.text msg ]

note :: JSX
note =
  M.typography
    { variant: "caption", color: "text.secondary", sx: { display: "block", mt: 1 } }
    [ R.text
        "Final assembly + submission completes with the wasm cage-helper bridge."
    ]

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
