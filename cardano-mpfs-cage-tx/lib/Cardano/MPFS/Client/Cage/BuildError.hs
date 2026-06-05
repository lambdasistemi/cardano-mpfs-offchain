-- |
-- Module      : Cardano.MPFS.Client.Cage.BuildError
-- Description : Local cage transaction construction errors.
module Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    ) where

import Data.Text (Text)

import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail
    )

-- | Failures returned by pure client-side cage builders before
-- the transaction is available for signing.
data BuildError
    = EmptyFunding
    | MalformedTxOut !Text
    | MalformedPParams !Text
    | LegacyRejectRefundRequiresTopUp !Text
    | PolicyViolation !PolicyViolationDetail
    | DSLBuildFailed !Text
    deriving stock (Eq, Show)
