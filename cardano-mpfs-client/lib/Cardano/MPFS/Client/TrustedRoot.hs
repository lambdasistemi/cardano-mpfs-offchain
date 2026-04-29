-- |
-- Module      : Cardano.MPFS.Client.TrustedRoot
-- Description : Trusted UTxO-CSMT root the verifier checks against.
--
-- The verifier is pure: it never fetches the trusted root itself.
-- The wrapping application obtains the root from a trusted CSMT
-- service and threads it into every verifier call as a 'TrustedRoot'.
-- The newtype makes the trust boundary visible at type level so it is
-- always clear at the call site that this value comes from outside the
-- offchain server.
--
-- Populated by feature #243 — see @specs\/243-proof-redesign\/@.
module Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    ) where

import Cardano.MPFS.API.Encoding (Hex)

-- | A UTxO-CSMT root the wrapping application has obtained from a
-- source it trusts.
newtype TrustedRoot = TrustedRoot {unTrustedRoot :: Hex}
    deriving stock (Eq, Show)
