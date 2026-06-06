-- | The `CageHelpers` boundary.
-- |
-- | This is the single seam between the SPA and MPFS protocol logic. Every
-- | transaction the SPA can build flows through one of these functions, which
-- | return a hex-encoded unsigned transaction (or a `CageError`). The SPA
-- | never constructs transactions itself — per the project constitution, no
-- | protocol logic lives in `.purs`.
-- |
-- | Following the constitution's "records of functions, not typeclasses" rule,
-- | the boundary is a record (`CageHelpers`) rather than the sketch's
-- | `MonadCageHelpers` class. The record is provided once at app startup:
-- | `MpfsSpa.CageHelpers.Mock.mockCageHelpers` today, a WASM-backed
-- | implementation when %351 ships green wasm.
module MpfsSpa.CageHelpers
  ( CageHelpers
  , CageResult
  ) where

import Data.Either (Either)
import Effect.Aff (Aff)

import MpfsSpa.Types
  ( CageConfig
  , CageError
  , Key
  , RequestId
  , TokenId
  , UnsignedTxCbor
  , Value
  , WalletAddr
  )

-- | The result of a cage operation: an unsigned tx or an error, in `Aff`.
type CageResult = Aff (Either CageError UnsignedTxCbor)

-- | The record of cage transaction builders. The implementation is swapped
-- | (mock ↔ wasm) without touching any call site.
type CageHelpers =
  { -- | Register (boot) a new token for the connected wallet.
    registerToken :: WalletAddr -> CageConfig -> CageResult
  , -- | Request insertion of a new fact `(key, value)`.
    insertFact :: WalletAddr -> CageConfig -> TokenId -> Key -> Value -> CageResult
  , -- | Request update of an existing fact's value (old -> new).
    updateFact ::
      WalletAddr -> CageConfig -> TokenId -> Key -> Value -> Value -> CageResult
  , -- | Request deletion of an existing fact `(key, value)`.
    deleteFact :: WalletAddr -> CageConfig -> TokenId -> Key -> Value -> CageResult
  , -- | Retract a still-pending request the wallet owns.
    retractRequest :: WalletAddr -> CageConfig -> TokenId -> RequestId -> CageResult
  , -- | Reject (sweep) selected requests whose phase-3 window has expired.
    rejectExpired :: WalletAddr -> CageConfig -> TokenId -> Array RequestId -> CageResult
  , -- | End (close) a cage the wallet owns.
    endCage :: WalletAddr -> CageConfig -> TokenId -> CageResult
  , -- | Owner/oracle: fold selected pending requests into the trie root.
    updateToken :: WalletAddr -> CageConfig -> TokenId -> Array RequestId -> CageResult
  }
