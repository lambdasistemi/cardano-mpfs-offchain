-- | Shared domain types for the MPFS SPA.
-- |
-- | These are thin newtypes over wire-level strings (hex / bech32). They
-- | carry no protocol logic — construction and meaning of the underlying
-- | bytes is owned entirely by the `CageHelpers` boundary (mock today, WASM
-- | cage-helper bridge tomorrow). The SPA only shuttles these values between
-- | the HTTP layer, the UI, and `CageHelpers`.
module MpfsSpa.Types
  ( WalletAddr(..)
  , TokenId(..)
  , Key(..)
  , Value(..)
  , RequestId(..)
  , UnsignedTxCbor(..)
  , TrustedRoot(..)
  , CageConfig
  , CageError(..)
  , cageErrorMessage
  ) where

import Prelude

import Data.Newtype (class Newtype)

-- | A wallet payment address (bech32 or hex), as surfaced by CIP-30.
newtype WalletAddr = WalletAddr String

derive instance Newtype WalletAddr _
derive instance Eq WalletAddr
derive newtype instance Show WalletAddr

-- | An MPFS token identifier (cage policy id, hex).
newtype TokenId = TokenId String

derive instance Newtype TokenId _
derive instance Eq TokenId
derive newtype instance Show TokenId

-- | A fact key.
newtype Key = Key String

derive instance Newtype Key _
derive instance Eq Key
derive newtype instance Show Key

-- | A fact value.
newtype Value = Value String

derive instance Newtype Value _
derive instance Eq Value
derive newtype instance Show Value

-- | A pending request identifier (`txid#ix`).
newtype RequestId = RequestId String

derive instance Newtype RequestId _
derive instance Eq RequestId
derive newtype instance Show RequestId

-- | A hex-encoded unsigned transaction (CBOR), as produced by `CageHelpers`.
newtype UnsignedTxCbor = UnsignedTxCbor String

derive instance Newtype UnsignedTxCbor _
derive instance Eq UnsignedTxCbor
derive newtype instance Show UnsignedTxCbor

-- | A hex-encoded trusted MPF root.
newtype TrustedRoot = TrustedRoot String

derive instance Newtype TrustedRoot _
derive instance Eq TrustedRoot
derive newtype instance Show TrustedRoot

-- | Client-side cage transaction configuration.
-- |
-- | Mirrors the Haskell `Cardano.MPFS.Client.Cage.Config.CageConfig`: the
-- | script byte fields and hash are hex-encoded; the time/tip fields are the
-- | phase windows and default fee; `network` is `"mainnet"` or `"testnet"`.
-- | The real WASM bridge consumes this record verbatim; the mock ignores it.
type CageConfig =
  { cageScriptBytes :: String
  , requestScriptBytes :: String
  , cfgScriptHash :: String
  , defaultProcessTime :: Int
  , defaultRetractTime :: Int
  , defaultTip :: Int
  , network :: String
  }

-- | An error returned by a `CageHelpers` operation.
newtype CageError = CageError String

derive instance Newtype CageError _
derive instance Eq CageError
derive newtype instance Show CageError

-- | Extract the human-readable message from a `CageError`.
cageErrorMessage :: CageError -> String
cageErrorMessage (CageError msg) = msg
