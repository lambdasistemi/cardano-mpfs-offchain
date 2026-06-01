-- | CIP-30 wallet connector.
-- |
-- | Thin `Aff` wrappers over the browser CIP-30 dApp-connector API exposed at
-- | `window.cardano.<key>`. The SPA uses this to enumerate installed wallets
-- | (Nami / Eternl / Lace / …), enable one, read its address / balance /
-- | network, and sign + submit a transaction the `CageHelpers` boundary
-- | produced. No MPFS protocol logic lives here.
module MpfsSpa.Wallet.Cip30
  ( WalletApi
  , WalletInfo
  , availableWallets
  , enable
  , getNetworkId
  , getUsedAddresses
  , getChangeAddress
  , getBalance
  , lovelaceOfBalance
  , signTx
  , submitTx
  , networkName
  ) where

import Prelude

import Control.Promise (Promise, toAffE)
import Data.Maybe (Maybe)
import Data.Nullable (Nullable, toMaybe)
import Effect (Effect)
import Effect.Aff (Aff)

-- | An enabled CIP-30 wallet API handle (opaque).
foreign import data WalletApi :: Type

-- | A wallet advertised on `window.cardano`. `key` is the injection key used
-- | to `enable`; `name` and `icon` are for display.
type WalletInfo = { key :: String, name :: String, icon :: String }

foreign import _availableWallets :: Effect (Array WalletInfo)
foreign import _enable :: String -> Effect (Promise WalletApi)
foreign import _getNetworkId :: WalletApi -> Effect (Promise Int)
foreign import _getUsedAddresses :: WalletApi -> Effect (Promise (Array String))
foreign import _getChangeAddress :: WalletApi -> Effect (Promise String)
foreign import _getBalance :: WalletApi -> Effect (Promise String)
foreign import _signTx :: WalletApi -> String -> Boolean -> Effect (Promise String)
foreign import _submitTx :: WalletApi -> String -> Effect (Promise String)
foreign import _coinOfBalance :: String -> Nullable String

-- | Wallets currently injected into `window.cardano`.
availableWallets :: Effect (Array WalletInfo)
availableWallets = _availableWallets

-- | Enable a wallet by its injection key, returning its API handle.
enable :: String -> Aff WalletApi
enable key = toAffE (_enable key)

-- | The wallet's network id: `0` testnet, `1` mainnet.
getNetworkId :: WalletApi -> Aff Int
getNetworkId api = toAffE (_getNetworkId api)

-- | The wallet's used addresses (hex).
getUsedAddresses :: WalletApi -> Aff (Array String)
getUsedAddresses api = toAffE (_getUsedAddresses api)

-- | The wallet's change address (hex).
getChangeAddress :: WalletApi -> Aff String
getChangeAddress api = toAffE (_getChangeAddress api)

-- | The wallet's balance as a CBOR-encoded ledger Value (hex).
getBalance :: WalletApi -> Aff String
getBalance api = toAffE (_getBalance api)

-- | Sign a hex CBOR transaction. `partial` requests partial (witness-only)
-- | signing per CIP-30.
signTx :: WalletApi -> String -> Boolean -> Aff String
signTx api tx partial = toAffE (_signTx api tx partial)

-- | Submit a hex CBOR transaction, returning the transaction id.
submitTx :: WalletApi -> String -> Aff String
submitTx api tx = toAffE (_submitTx api tx)

-- | Best-effort lovelace (coin) extraction from a balance CBOR value, for
-- | display only. `Nothing` if the value cannot be read.
lovelaceOfBalance :: String -> Maybe String
lovelaceOfBalance = toMaybe <<< _coinOfBalance

-- | Human-readable network name for a CIP-30 network id.
networkName :: Int -> String
networkName 1 = "mainnet"
networkName 0 = "testnet"
networkName n = "network " <> show n
