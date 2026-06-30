-- |
-- Module      : Cardano.MPFS.TxBuilder.Config
-- Description : Configuration for real transaction builders
-- License     : Apache-2.0
--
-- Configuration record for the real cage transaction
-- builders. Holds the applied PlutusV3 script bytes,
-- computed script hash, default token parameters, and
-- time-conversion constants needed for validity
-- interval calculations.
module Cardano.MPFS.TxBuilder.Config
    ( -- * Configuration
      CageConfig (..)
    ) where

import Data.ByteString.Short (ShortByteString)

import Cardano.Ledger.BaseTypes (Network)
import Cardano.Ledger.Hashes (ScriptHash)

import Cardano.MPFS.Core.Types (Coin)

-- | Configuration for the cage script transaction
-- builders.
--
-- 'cageScriptBytes' holds the flat-encoded UPLC of
-- the global state validator (upstream's
-- @validator state {}@, which is unparameterised
-- under PR #50). 'requestScriptBytes' holds the
-- /unapplied/ flat-encoded UPLC of the per-cage
-- request validator; per-cage request addresses
-- are derived at runtime by applying it to
-- @(statePolicyId, cageTokenName)@.
-- 'cfgScriptHash' is the state validator script
-- hash and therefore the global state policy id.
--
-- The cage seed is /not/ a 'CageConfig' field —
-- the wallet picks the seed @OutputReference@ at
-- runtime, unlike the upstream cage tests that
-- thread a fixed seed through configuration.
data CageConfig = CageConfig
    { cageScriptBytes :: !ShortByteString
    -- ^ Global state validator UPLC bytes
    , requestScriptBytes :: !ShortByteString
    -- ^ Unapplied per-cage request validator UPLC
    , cfgScriptHash :: !ScriptHash
    -- ^ Hash of the global state validator
    , defaultProcessTime :: !Integer
    -- ^ Phase 1 window (ms) for oracle processing
    , defaultRetractTime :: !Integer
    -- ^ Phase 2 window (ms) for requester retract
    , defaultTip :: !Coin
    -- ^ Default max fee for newly booted tokens
    , network :: !Network
    -- ^ Target network (Mainnet or Testnet)
    , cfgStakeScript :: !(Maybe (ShortByteString, ScriptHash))
    -- ^ Optional staking script: bytes + hash.
    -- When set, Modify\/End include a
    -- withdraw-zero from the staking credential.
    }
