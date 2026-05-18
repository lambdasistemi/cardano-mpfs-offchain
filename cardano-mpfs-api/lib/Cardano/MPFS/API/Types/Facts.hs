{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.API.Types.Facts
-- Description : Facts endpoint JSON DTOs.
-- License     : Apache-2.0
--
-- Wire types owned by facts endpoints. The module re-exports the
-- common facts/proof primitives needed by facts clients.
module Cardano.MPFS.API.Types.Facts
    ( -- * Facts responses
      BootFacts (..)
    , EndFacts (..)

      -- * Common facts/proof primitives
    , ChainPointJSON (..)
    , VerificationSnapshot (..)
    , TokenIdJSON (..)
    , UtxoRef (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoSetWitness (..)
    , UnverifiedPParams (..)
    ) where

import Control.Lens ((&), (.~), (?~))
import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withObject
    , (.:)
    , (.=)
    )
import Data.Proxy (Proxy (..))
import Data.Swagger
    ( ToSchema (..)
    , declareSchemaRef
    , description
    , properties
    , required
    )
import Data.Swagger qualified as Swagger
import GHC.IsList (IsList (..))

import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )

-- | Facts-only boot response.
--
-- Unlike 'Cardano.MPFS.API.Types.UnsignedTxResponse', this envelope
-- carries no transaction CBOR. Its verifier proves only that the
-- advertised wallet UTxOs are included in the trusted snapshot root.
data BootFacts = BootFacts
    { bfSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled inclusion proofs target.
    , bfWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs with CSMT inclusion proofs.
    , bfProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON BootFacts where
    toJSON BootFacts{..} =
        object
            [ "snapshot" .= bfSnapshot
            , "wallet_utxos" .= bfWalletUtxos
            , "protocol_parameters" .= bfProtocolParameters
            ]

instance FromJSON BootFacts where
    parseJSON =
        withObject "BootFacts" $ \o ->
            BootFacts
                <$> o .: "snapshot"
                <*> o .: "wallet_utxos"
                <*> o .: "protocol_parameters"

instance ToSchema BootFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "BootFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("wallet_utxos", walletSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "wallet_utxos"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only boot response. Carries \
                   \wallet UTxO witnesses and \
                   \unverified protocol parameters, \
                   \with no unsigned transaction CBOR."

-- | Facts-only end response.
--
-- Carries the current token state UTxO, wallet funding UTxOs,
-- and a request-set completeness witness. The verifier proves
-- these facts against the trusted snapshot before a client-side
-- transaction builder may consume them.
data EndFacts = EndFacts
    { efSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , efToken :: TokenIdJSON
    -- ^ Cage token being ended.
    , efStateUtxo :: UtxoEntry
    -- ^ State UTxO with a CSMT inclusion proof.
    , efWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs with CSMT inclusion proofs.
    , efRequestSet :: UtxoSetWitness
    -- ^ Request-address completeness witness.
    , efProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON EndFacts where
    toJSON EndFacts{..} =
        object
            [ "snapshot" .= efSnapshot
            , "token" .= efToken
            , "state_utxo" .= efStateUtxo
            , "wallet_utxos" .= efWalletUtxos
            , "request_set" .= efRequestSet
            , "protocol_parameters" .= efProtocolParameters
            ]

instance FromJSON EndFacts where
    parseJSON =
        withObject "EndFacts" $ \o ->
            EndFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "state_utxo"
                <*> o .: "wallet_utxos"
                <*> o .: "request_set"
                <*> o .: "protocol_parameters"

instance ToSchema EndFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        stateSchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        requestSetSchema <-
            declareSchemaRef (Proxy @UtxoSetWitness)
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "EndFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("state_utxo", stateSchema)
                    , ("wallet_utxos", walletSchema)
                    , ("request_set", requestSetSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "state_utxo"
                   , "wallet_utxos"
                   , "request_set"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only end response. Carries state and \
                   \wallet UTxO witnesses plus a request-set \
                   \completeness proof, with no unsigned \
                   \transaction CBOR."
