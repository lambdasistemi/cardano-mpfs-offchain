{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Config
-- Description : Client-owned cage transaction configuration.
--
-- Minimal configuration needed by client-side cage transaction
-- builders. This deliberately mirrors only the shared ledger/script
-- fields and avoids depending on the offchain server package.
module Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cageAddrFromCfg
    , cagePolicyIdFromCfg
    , computeScriptHash
    , mkCageScript
    ) where

import Data.ByteString.Short (ShortByteString)

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Alonzo.Scripts
    ( fromPlutusScript
    )
import Cardano.Ledger.BaseTypes (Network)
import Cardano.Ledger.Conway.Scripts
    ( PlutusScript (ConwayPlutusV3)
    )
import Cardano.Ledger.Core
    ( Script
    , hashScript
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (ScriptHash)
import Cardano.Ledger.Mary.Value (PolicyID (..))
import Cardano.Ledger.Plutus.Language
    ( Language (PlutusV3)
    , Plutus (..)
    , PlutusBinary (..)
    )

import Cardano.MPFS.Cage.Ledger
    ( Coin
    , ConwayEra
    )

-- | Configuration for client-side cage transaction builders.
data CageConfig = CageConfig
    { cageScriptBytes :: !ShortByteString
    -- ^ Global state validator UPLC bytes.
    , requestScriptBytes :: !ShortByteString
    -- ^ Unapplied per-cage request validator UPLC bytes.
    , cfgScriptHash :: !ScriptHash
    -- ^ Hash of the global state validator.
    , defaultProcessTime :: !Integer
    -- ^ Phase 1 window (ms) for oracle processing.
    , defaultRetractTime :: !Integer
    -- ^ Phase 2 window (ms) for requester retract.
    , defaultTip :: !Coin
    -- ^ Default max fee for newly booted tokens.
    , network :: !Network
    -- ^ Target network.
    }
    deriving stock (Eq, Show)

-- | Build the cage script from config bytes.
mkCageScript :: CageConfig -> Script ConwayEra
mkCageScript cfg =
    fromPlutusScript
        $ ConwayPlutusV3
        $ Plutus @PlutusV3
        $ PlutusBinary
        $ cageScriptBytes cfg

-- | Compute the script hash from raw PlutusV3 bytes.
computeScriptHash :: ShortByteString -> ScriptHash
computeScriptHash sbs =
    hashScript @ConwayEra
        $ fromPlutusScript
        $ ConwayPlutusV3
        $ Plutus @PlutusV3
        $ PlutusBinary sbs

-- | Compute the cage minting policy id from config.
cagePolicyIdFromCfg :: CageConfig -> PolicyID
cagePolicyIdFromCfg =
    PolicyID . cfgScriptHash

-- | Compute the cage script address from config.
cageAddrFromCfg :: CageConfig -> Network -> Addr
cageAddrFromCfg cfg net =
    Addr
        net
        (ScriptHashObj $ cfgScriptHash cfg)
        StakeRefNull
