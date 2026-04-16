{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Core.OnChain
-- Description : On-chain type encodings (re-exports from cage)
-- License     : Apache-2.0
--
-- Re-exports on-chain types from @cardano-mpfs-cage@
-- and provides offchain-specific script identity
-- helpers (hard-coded script hash, policy ID, address).
module Cardano.MPFS.Core.OnChain
    ( -- * On-chain datum\/redeemer types (from cage)
      CageDatum (..)
    , MintRedeemer (..)
    , Mint (..)
    , Migration (..)
    , UpdateRedeemer (..)
    , RequestAction (..)

      -- * On-chain domain types (from cage)
    , OnChainTokenId (..)
    , OnChainOperation (..)
    , OnChainRoot (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)

      -- * Proof steps (from cage)
    , ProofStep (..)
    , Neighbor (..)

      -- * Script identity (offchain-specific)
    , cageScriptHash
    , cageScriptHashLedger
    , cagePolicyId
    , cageAddr

      -- * Asset-name derivation (from cage)
    , deriveAssetName

      -- * Blueprint loading (from cage)
    , Blueprint (..)
    , loadCageScript
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Maybe (fromJust)

import Cardano.Crypto.Hash (hashFromBytes)
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.BaseTypes (Network)
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Mary.Value (PolicyID (..))

import Cardano.MPFS.Cage.AssetName (deriveAssetName)
import Cardano.MPFS.Cage.Blueprint
    ( Blueprint (..)
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , Migration (..)
    , Mint (..)
    , MintRedeemer (..)
    , Neighbor (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , ProofStep (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
    )

-- ---------------------------------------------------------
-- Script identity (offchain-specific)
-- ---------------------------------------------------------

-- | The cage validator script hash (raw 28 bytes).
cageScriptHash :: ByteString
cageScriptHash =
    BS.pack
        [ 0x21
        , 0x87
        , 0xec
        , 0x4c
        , 0x76
        , 0x6a
        , 0xde
        , 0x9f
        , 0xe1
        , 0xcc
        , 0x84
        , 0x34
        , 0x72
        , 0xde
        , 0x41
        , 0x43
        , 0xf3
        , 0x09
        , 0x9e
        , 0x40
        , 0xdd
        , 0xe4
        , 0xf9
        , 0x57
        , 0x77
        , 0x13
        , 0x19
        , 0x26
        ]

-- | The cage validator 'ScriptHash' (ledger type).
cageScriptHashLedger :: ScriptHash
cageScriptHashLedger =
    ScriptHash
        $ fromJust
        $ hashFromBytes cageScriptHash

-- | Cage minting policy ID.
cagePolicyId :: PolicyID
cagePolicyId = PolicyID cageScriptHashLedger

-- | Cage script address for a given network.
cageAddr
    :: Network
    -> Addr
cageAddr net =
    Addr
        net
        (ScriptHashObj cageScriptHashLedger)
        StakeRefNull

-- | Load and parse a CIP-57 blueprint.
loadCageScript
    :: FilePath
    -> IO (Either String Blueprint)
loadCageScript = loadBlueprint
