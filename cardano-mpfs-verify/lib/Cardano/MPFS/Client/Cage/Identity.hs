{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Identity
-- Description : Client-owned cage identity helpers.
--
-- Derives per-token request validator identity from the client
-- cage configuration. These helpers mirror the offchain builder
-- identity calculation without depending on the server package.
module Cardano.MPFS.Client.Cage.Identity
    ( onChainTokenId
    , cageSetPrefixFromCfg
    , requestAddrFromCfg
    , requestScriptBytesFromCfg
    , requestSetPrefixFromCfg
    , tokenIdFromJSON
    ) where

import Data.ByteString.Short qualified as SBS

import Cardano.Crypto.Hash
    ( hashToBytes
    )
import Cardano.Ledger.Address
    ( Addr (..)
    , serialiseAddr
    )
import Cardano.Ledger.BaseTypes
    ( Network
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes
    ( ScriptHash (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import CSMT.Core.Hash
    ( byteStringToKey
    )
import CSMT.Core.Types
    ( Key
    )
import CSMT.Verify.Blake2b
    ( blake2b256
    )

import Cardano.MPFS.API.Types.Common
    ( TokenIdJSON (..)
    )
import Cardano.MPFS.Cage.Blueprint
    ( applyRequestParams
    )
import Cardano.MPFS.Cage.Ledger
    ( AssetName (..)
    , TokenId (..)
    )
import Cardano.MPFS.Cage.Types
    ( OnChainTokenId (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cageAddrFromCfg
    , computeScriptHash
    )

-- | Convert a wire token id to the ledger-side token id.
tokenIdFromJSON :: TokenIdJSON -> TokenId
tokenIdFromJSON (TokenIdJSON bs) =
    TokenId (AssetName (SBS.toShort bs))

-- | Convert a ledger-side token id to its on-chain bytes.
onChainTokenId :: TokenId -> OnChainTokenId
onChainTokenId tid =
    OnChainTokenId
        $ BuiltinByteString
        $ SBS.fromShort
        $ let AssetName sbs = unTokenId tid
          in  sbs

-- | Apply the request validator parameters for a token.
requestScriptBytesFromCfg
    :: CageConfig -> TokenId -> SBS.ShortByteString
requestScriptBytesFromCfg cfg tid =
    let ScriptHash h = cfgScriptHash cfg
    in  applyRequestParams
            (hashToBytes h)
            (onChainTokenId tid)
            (requestScriptBytes cfg)

-- | Compute the per-cage request validator address.
requestAddrFromCfg :: CageConfig -> TokenId -> Network -> Addr
requestAddrFromCfg cfg tid net =
    Addr
        net
        ( ScriptHashObj
            $ computeScriptHash
                (requestScriptBytesFromCfg cfg tid)
        )
        StakeRefNull

-- | Derive the request-set CSMT prefix locally from config and
-- token. Matches offchain @hashAddressKey (serialiseAddr addr)@.
requestSetPrefixFromCfg :: CageConfig -> TokenIdJSON -> Key
requestSetPrefixFromCfg cfg token =
    byteStringToKey
        $ blake2b256
        $ serialiseAddr
        $ requestAddrFromCfg
            cfg
            (tokenIdFromJSON token)
            (network cfg)

-- | Derive the cage-state UTxO-set CSMT prefix locally from config.
-- Matches offchain @hashAddressKey (serialiseAddr addr)@ for
-- @GET /tokens@ completeness verification.
cageSetPrefixFromCfg :: CageConfig -> Key
cageSetPrefixFromCfg cfg =
    byteStringToKey
        $ blake2b256
        $ serialiseAddr
        $ cageAddrFromCfg cfg (network cfg)
