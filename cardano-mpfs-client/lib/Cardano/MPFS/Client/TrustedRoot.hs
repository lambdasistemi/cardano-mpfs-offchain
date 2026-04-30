-- |
-- Module      : Cardano.MPFS.Client.TrustedRoot
-- Description : Trusted UTxO-CSMT root the verifier checks against.
--
-- The verifier is pure (constitution VIII): it never fetches
-- the trusted root itself. The wrapping application obtains the
-- root from a trusted CSMT service and threads it into every
-- verifier call as a 'TrustedRoot'. The newtype makes the
-- trust boundary visible at type level so it is always clear
-- at the call site that this value comes from outside the
-- offchain server.
--
-- This module also defines the 'Blueprint' record, the
-- second piece of out-of-band trust the verifier consumes:
-- the trusted Aiken blueprint's verifier-relevant
-- projections (state policy id, global state script
-- address, per-cage request script address derivation).
-- See @specs\/243-proof-redesign\/research.md@ §4.
--
-- Both types are 'Hex' / 'ByteString' newtypes. The verifier
-- compares decoded @txout_cbor@ bytes against these values
-- byte-for-byte; the wrapping application is responsible for
-- supplying values that match the on-chain encoding the
-- trusted blueprint pins.
module Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    , Blueprint (..)
    , Address (..)
    , AssetName (..)
    ) where

import Cardano.MPFS.API.Encoding (Hex)

-- | A UTxO-CSMT root the wrapping application has obtained
-- from a source it trusts.
newtype TrustedRoot = TrustedRoot
    { unTrustedRoot :: Hex
    }
    deriving stock (Eq, Show)

-- | A Cardano address as the bytes the on-chain decoder
-- compares against. The verifier does not parse the address
-- internals; it compares the decoded @txout_cbor@'s address
-- field byte-for-byte against the one supplied by the
-- 'Blueprint'.
newtype Address = Address
    { unAddress :: Hex
    }
    deriving stock (Eq, Show)

-- | A Cardano asset name (i.e. a token name within a
-- minting policy). 'Hex' is the canonical wire encoding;
-- the verifier compares bytes.
newtype AssetName = AssetName
    { unAssetName :: Hex
    }
    deriving stock (Eq, Show)

-- | The trusted Aiken blueprint's verifier-relevant
-- projections. Distributed out of band per
-- @specs\/243-proof-redesign\/research.md@ §4.
--
-- The wrapping application populates this record from a
-- source it trusts (e.g. a release-pinned blueprint file)
-- and threads it into every verifier call. The verifier
-- never fetches the blueprint itself.
data Blueprint = Blueprint
    { bpStatePolicyId :: Hex
    -- ^ The policy id of the global state-token NFT.
    , bpStateScriptAddress :: Address
    -- ^ Script address of the global state validator.
    , bpRequestScriptAddress :: AssetName -> Address
    -- ^ Per-cage request validator address derivation,
    -- parametrised by the cage's token name.
    }
