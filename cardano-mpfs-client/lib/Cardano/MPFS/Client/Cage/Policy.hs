-- |
-- Module      : Cardano.MPFS.Client.Cage.Policy
-- Description : Wallet-side policy caps for local cage builders.
module Cardano.MPFS.Client.Cage.Policy
    ( WalletPolicy (..)
    , PolicyViolationDetail (..)
    ) where

import Cardano.Ledger.Coin (Coin)
import Cardano.Ledger.Plutus.ExUnits (Prices)
import Cardano.Slotting.Slot (SlotNo)

-- | Wallet-side limits enforced before a locally-built transaction is
-- returned for signing.
data WalletPolicy = WalletPolicy
    { wpMaxFee :: !Coin
    -- ^ Maximum acceptable transaction fee.
    , wpMaxExUnitPrices :: !Prices
    -- ^ Maximum acceptable protocol parameter execution prices.
    , wpMaxMinUtxoCoinPerByte :: !Coin
    -- ^ Maximum acceptable min-UTxO coins-per-byte parameter.
    , wpMaxValidityWindow :: !SlotNo
    -- ^ Maximum acceptable validity interval width.
    }
    deriving stock (Eq, Show)

-- | Specific wallet policy cap that a candidate build exceeded.
data PolicyViolationDetail
    = FeeTooHigh !Coin !Coin
    | ExUnitPricesTooHigh !Prices !Prices
    | MinUtxoCoinPerByteTooHigh !Coin !Coin
    | ValidityWindowTooWide !SlotNo !SlotNo
    deriving stock (Eq, Show)
