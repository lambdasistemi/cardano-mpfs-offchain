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
