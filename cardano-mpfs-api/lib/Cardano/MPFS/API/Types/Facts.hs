{-# LANGUAGE CPP #-}
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
    , RequestInsertFacts (..)
    , RequestDeleteFacts (..)
    , RequestUpdateFacts (..)
    , RejectFacts (..)
    , RetractFacts (..)
    , EndFacts (..)
    , TrieFact (..)
    , UpdateFacts (..)

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

#if !defined(wasm32_HOST_ARCH)
import Control.Lens ((&), (.~), (?~))
#endif
import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withObject
    , (.:)
    , (.=)
    )
#if !defined(wasm32_HOST_ARCH)
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
#endif

import Cardano.MPFS.API.Encoding (Hex)
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

#if !defined(wasm32_HOST_ARCH)
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
                ?~ "Facts-only boot response. Carries wallet UTxO witnesses and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Facts-only request-insert response.
--
-- Carries the request payload, server-selected submission
-- timestamp, wallet funding UTxOs, and protocol parameters. The
-- verifier proves only the UTxO witnesses against the trusted
-- snapshot; protocol parameters and timestamp remain wallet policy
-- inputs for local transaction construction.
data RequestInsertFacts = RequestInsertFacts
    { rifSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , rifToken :: TokenIdJSON
    -- ^ Cage token being modified.
    , rifKey :: Hex
    -- ^ Trie key to insert.
    , rifValue :: Hex
    -- ^ Trie value to insert.
    , rifAddress :: Hex
    -- ^ Serialized requester address.
    , rifSubmittedAt :: Integer
    -- ^ POSIXTime in milliseconds for the request datum.
    , rifWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs with CSMT inclusion proofs.
    , rifProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON RequestInsertFacts where
    toJSON RequestInsertFacts{..} =
        object
            [ "snapshot" .= rifSnapshot
            , "token" .= rifToken
            , "key" .= rifKey
            , "value" .= rifValue
            , "address" .= rifAddress
            , "submitted_at" .= rifSubmittedAt
            , "wallet_utxos" .= rifWalletUtxos
            , "protocol_parameters" .= rifProtocolParameters
            ]

instance FromJSON RequestInsertFacts where
    parseJSON =
        withObject "RequestInsertFacts" $ \o ->
            RequestInsertFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "key"
                <*> o .: "value"
                <*> o .: "address"
                <*> o .: "submitted_at"
                <*> o .: "wallet_utxos"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema RequestInsertFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "RequestInsertFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("value", hexSchema)
                    , ("address", hexSchema)
                    , ("submitted_at", integerSchema)
                    , ("wallet_utxos", walletSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "key"
                   , "value"
                   , "address"
                   , "submitted_at"
                   , "wallet_utxos"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only request-insert response. Carries request payload, submission timestamp, wallet UTxO witnesses, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Facts-only request-delete response.
--
-- Carries the request payload, server-selected submission
-- timestamp, wallet funding UTxOs, and protocol parameters. The
-- verifier proves only the UTxO witnesses against the trusted
-- snapshot; protocol parameters and timestamp remain wallet policy
-- inputs for local transaction construction.
data RequestDeleteFacts = RequestDeleteFacts
    { rdfSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , rdfToken :: TokenIdJSON
    -- ^ Cage token being modified.
    , rdfKey :: Hex
    -- ^ Trie key to delete.
    , rdfValue :: Hex
    -- ^ Trie value expected at delete time.
    , rdfAddress :: Hex
    -- ^ Serialized requester address.
    , rdfSubmittedAt :: Integer
    -- ^ POSIXTime in milliseconds for the request datum.
    , rdfWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs with CSMT inclusion proofs.
    , rdfProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON RequestDeleteFacts where
    toJSON RequestDeleteFacts{..} =
        object
            [ "snapshot" .= rdfSnapshot
            , "token" .= rdfToken
            , "key" .= rdfKey
            , "value" .= rdfValue
            , "address" .= rdfAddress
            , "submitted_at" .= rdfSubmittedAt
            , "wallet_utxos" .= rdfWalletUtxos
            , "protocol_parameters" .= rdfProtocolParameters
            ]

instance FromJSON RequestDeleteFacts where
    parseJSON =
        withObject "RequestDeleteFacts" $ \o ->
            RequestDeleteFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "key"
                <*> o .: "value"
                <*> o .: "address"
                <*> o .: "submitted_at"
                <*> o .: "wallet_utxos"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema RequestDeleteFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "RequestDeleteFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("value", hexSchema)
                    , ("address", hexSchema)
                    , ("submitted_at", integerSchema)
                    , ("wallet_utxos", walletSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "key"
                   , "value"
                   , "address"
                   , "submitted_at"
                   , "wallet_utxos"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only request-delete response. Carries request payload, submission timestamp, wallet UTxO witnesses, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Facts-only request-update response.
--
-- Carries the request payload, server-selected submission
-- timestamp, wallet funding UTxOs, and protocol parameters. The
-- verifier proves only the UTxO witnesses against the trusted
-- snapshot; protocol parameters and timestamp remain wallet policy
-- inputs for local transaction construction.
data RequestUpdateFacts = RequestUpdateFacts
    { rufSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , rufToken :: TokenIdJSON
    -- ^ Cage token being modified.
    , rufKey :: Hex
    -- ^ Trie key to update.
    , rufOldValue :: Hex
    -- ^ Trie value expected before the update.
    , rufNewValue :: Hex
    -- ^ Trie value to write after the update.
    , rufAddress :: Hex
    -- ^ Serialized requester address.
    , rufSubmittedAt :: Integer
    -- ^ POSIXTime in milliseconds for the request datum.
    , rufWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs with CSMT inclusion proofs.
    , rufProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON RequestUpdateFacts where
    toJSON RequestUpdateFacts{..} =
        object
            [ "snapshot" .= rufSnapshot
            , "token" .= rufToken
            , "key" .= rufKey
            , "old_value" .= rufOldValue
            , "new_value" .= rufNewValue
            , "address" .= rufAddress
            , "submitted_at" .= rufSubmittedAt
            , "wallet_utxos" .= rufWalletUtxos
            , "protocol_parameters" .= rufProtocolParameters
            ]

instance FromJSON RequestUpdateFacts where
    parseJSON =
        withObject "RequestUpdateFacts" $ \o ->
            RequestUpdateFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "key"
                <*> o .: "old_value"
                <*> o .: "new_value"
                <*> o .: "address"
                <*> o .: "submitted_at"
                <*> o .: "wallet_utxos"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema RequestUpdateFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "RequestUpdateFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("old_value", hexSchema)
                    , ("new_value", hexSchema)
                    , ("address", hexSchema)
                    , ("submitted_at", integerSchema)
                    , ("wallet_utxos", walletSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "key"
                   , "old_value"
                   , "new_value"
                   , "address"
                   , "submitted_at"
                   , "wallet_utxos"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only request-update response. Carries request payload, submission timestamp, wallet UTxO witnesses, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Shared JSON representation of a trie read fact.
--
-- The value is nullable so the same shape can carry both
-- membership and absence facts against an MPF trie root.
data TrieFact = TrieFact
    { tfKey :: Hex
    -- ^ Trie key the builder looked up.
    , tfValue :: Maybe Hex
    -- ^ Value bound to 'tfKey', or 'Nothing' for an
    -- absence fact.
    , tfMpfProof :: Hex
    -- ^ MPF inclusion\/exclusion proof.
    }
    deriving (Eq, Show)

instance ToJSON TrieFact where
    toJSON TrieFact{..} =
        object
            [ "key" .= tfKey
            , "value" .= tfValue
            , "mpf_proof" .= tfMpfProof
            ]

instance FromJSON TrieFact where
    parseJSON =
        withObject "TrieFact" $ \o ->
            TrieFact
                <$> o .: "key"
                <*> o .: "value"
                <*> o .: "mpf_proof"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema TrieFact where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        maybeHexSchema <-
            declareSchemaRef (Proxy @(Maybe Hex))
        pure
            $ Swagger.NamedSchema (Just "TrieFact")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("key", hexSchema)
                    , ("value", maybeHexSchema)
                    , ("mpf_proof", hexSchema)
                    ]
            & required
                .~ [ "key"
                   , "value"
                   , "mpf_proof"
                   ]
            & description
                ?~ "Trie read fact: key, optional value, and MPF proof against a trie root."
#endif

-- | Facts-only update response foundation.
--
-- Carries the state UTxO, pending request UTxOs, wallet
-- funding UTxOs, trie root/read facts, and protocol
-- parameters needed by a wallet-side update builder. Route
-- wiring is added by a later slice.
data UpdateFacts = UpdateFacts
    { ufSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled UTxO proofs target.
    , ufToken :: TokenIdJSON
    -- ^ Cage token being updated.
    , ufStateUtxo :: UtxoEntry
    -- ^ State UTxO consumed by the update.
    , ufRequestUtxos :: [UtxoEntry]
    -- ^ Pending request UTxOs batched into the update.
    , ufWalletUtxos :: [UtxoEntry]
    -- ^ Wallet UTxOs funding fees and collateral.
    , ufTrieRoot :: Hex
    -- ^ Trie root from the consumed state datum.
    , ufTrieFacts :: [TrieFact]
    -- ^ MPF facts for trie keys touched by the batch.
    , ufValidityUpperSlot :: Integer
    -- ^ Server-derived update validity upper slot.
    , ufProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON UpdateFacts where
    toJSON UpdateFacts{..} =
        object
            [ "snapshot" .= ufSnapshot
            , "token" .= ufToken
            , "state_utxo" .= ufStateUtxo
            , "request_utxos" .= ufRequestUtxos
            , "wallet_utxos" .= ufWalletUtxos
            , "trie_root" .= ufTrieRoot
            , "trie_facts" .= ufTrieFacts
            , "validity_upper_slot" .= ufValidityUpperSlot
            , "protocol_parameters" .= ufProtocolParameters
            ]

instance FromJSON UpdateFacts where
    parseJSON =
        withObject "UpdateFacts" $ \o ->
            UpdateFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "state_utxo"
                <*> o .: "request_utxos"
                <*> o .: "wallet_utxos"
                <*> o .: "trie_root"
                <*> o .: "trie_facts"
                <*> o .: "validity_upper_slot"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema UpdateFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        utxoSchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        utxoListSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        trieFactsSchema <-
            declareSchemaRef (Proxy @[TrieFact])
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "UpdateFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("state_utxo", utxoSchema)
                    , ("request_utxos", utxoListSchema)
                    , ("wallet_utxos", utxoListSchema)
                    , ("trie_root", hexSchema)
                    , ("trie_facts", trieFactsSchema)
                    , ("validity_upper_slot", integerSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "state_utxo"
                   , "request_utxos"
                   , "wallet_utxos"
                   , "trie_root"
                   , "trie_facts"
                   , "validity_upper_slot"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only update response. Carries state, request and wallet UTxO witnesses, trie root/read facts, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Facts-only retract response.
--
-- Carries the named request UTxO being retracted, the cage state
-- UTxO (referenced as input), requester wallet UTxOs for fees and
-- collateral, server-derived Phase 2 validity slot bounds, and
-- unverified protocol parameters. The verifier proves the UTxO
-- witnesses against the trusted snapshot; the slot bounds and
-- protocol parameters are unverified inputs whose tampering causes
-- the locally-built transaction to fail Phase 2 ledger validation.
data RetractFacts = RetractFacts
    { rfSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , rfToken :: TokenIdJSON
    -- ^ Cage token the retracted request points at.
    , rfRequestUtxo :: UtxoEntry
    -- ^ Named request UTxO with CSMT inclusion proof.
    , rfStateUtxo :: UtxoEntry
    -- ^ Cage state UTxO with CSMT inclusion proof. The
    -- builder consumes its inline datum and uses it as a
    -- reference input.
    , rfWalletUtxos :: [UtxoEntry]
    -- ^ Requester wallet UTxOs with CSMT inclusion proofs.
    , rfValidityStartSlot :: Integer
    -- ^ Server-derived Phase 2 lower validity slot
    -- (entirely_after(submitted_at + process_time)).
    , rfValidityEndSlot :: Integer
    -- ^ Server-derived Phase 2 upper validity slot
    -- (strictly before submitted_at + process_time +
    -- retract_time).
    , rfProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON RetractFacts where
    toJSON RetractFacts{..} =
        object
            [ "snapshot" .= rfSnapshot
            , "token" .= rfToken
            , "request_utxo" .= rfRequestUtxo
            , "state_utxo" .= rfStateUtxo
            , "wallet_utxos" .= rfWalletUtxos
            , "validity_start_slot" .= rfValidityStartSlot
            , "validity_end_slot" .= rfValidityEndSlot
            , "protocol_parameters" .= rfProtocolParameters
            ]

instance FromJSON RetractFacts where
    parseJSON =
        withObject "RetractFacts" $ \o ->
            RetractFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "request_utxo"
                <*> o .: "state_utxo"
                <*> o .: "wallet_utxos"
                <*> o .: "validity_start_slot"
                <*> o .: "validity_end_slot"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema RetractFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        utxoEntrySchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        walletSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "RetractFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("request_utxo", utxoEntrySchema)
                    , ("state_utxo", utxoEntrySchema)
                    , ("wallet_utxos", walletSchema)
                    , ("validity_start_slot", integerSchema)
                    , ("validity_end_slot", integerSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "request_utxo"
                   , "state_utxo"
                   , "wallet_utxos"
                   , "validity_start_slot"
                   , "validity_end_slot"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only retract response. Carries the named request UTxO, cage state UTxO, wallet funding UTxO witnesses, server-derived Phase 2 validity slot bounds, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

-- | Facts-only reject response.
--
-- Carries the cage state UTxO, the batch of rejectable request
-- UTxOs, requester wallet UTxOs for fees and collateral, the
-- server-derived Phase 3 validity slot bounds, and unverified
-- protocol parameters. The verifier proves the UTxO witnesses
-- against the trusted snapshot; the slot bounds and protocol
-- parameters are unverified inputs whose tampering causes the
-- locally-built transaction to fail Phase 2 ledger validation.
--
-- Per Q-S2-001, this envelope carries no @trie_root@ or
-- @trie_facts@ because a reject leaves the state root unchanged
-- and folds no MPF proofs.
data RejectFacts = RejectFacts
    { rfSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , rfToken :: TokenIdJSON
    -- ^ Cage token whose pending requests are being rejected.
    , rfStateUtxo :: UtxoEntry
    -- ^ Cage state UTxO with CSMT inclusion proof. The
    -- builder consumes its inline datum and re-outputs the
    -- same state with the same root.
    , rfRequestUtxos :: [UtxoEntry]
    -- ^ Batch of rejectable request UTxOs with CSMT
    -- inclusion proofs.
    , rfWalletUtxos :: [UtxoEntry]
    -- ^ Requester wallet UTxOs with CSMT inclusion proofs.
    , rfValidityLowerSlot :: Integer
    -- ^ Server-derived Phase 3 validity lower slot:
    -- strictly after the latest deadline among the
    -- rejected requests.
    , rfValidityUpperSlot :: Integer
    -- ^ Server-derived validity upper slot (explicit TTL
    -- above 'rfValidityLowerSlot').
    , rfProtocolParameters :: UnverifiedPParams
    -- ^ Unverified protocol parameter bytes.
    }
    deriving (Eq, Show)

instance ToJSON RejectFacts where
    toJSON RejectFacts{..} =
        object
            [ "snapshot" .= rfSnapshot
            , "token" .= rfToken
            , "state_utxo" .= rfStateUtxo
            , "request_utxos" .= rfRequestUtxos
            , "wallet_utxos" .= rfWalletUtxos
            , "validity_lower_slot" .= rfValidityLowerSlot
            , "validity_upper_slot" .= rfValidityUpperSlot
            , "protocol_parameters" .= rfProtocolParameters
            ]

instance FromJSON RejectFacts where
    parseJSON =
        withObject "RejectFacts" $ \o ->
            RejectFacts
                <$> o .: "snapshot"
                <*> o .: "token"
                <*> o .: "state_utxo"
                <*> o .: "request_utxos"
                <*> o .: "wallet_utxos"
                <*> o .: "validity_lower_slot"
                <*> o .: "validity_upper_slot"
                <*> o .: "protocol_parameters"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema RejectFacts where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef (Proxy @VerificationSnapshot)
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        utxoSchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        utxoListSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        integerSchema <-
            declareSchemaRef (Proxy @Integer)
        ppSchema <-
            declareSchemaRef (Proxy @UnverifiedPParams)
        pure
            $ Swagger.NamedSchema (Just "RejectFacts")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("token", tokenSchema)
                    , ("state_utxo", utxoSchema)
                    , ("request_utxos", utxoListSchema)
                    , ("wallet_utxos", utxoListSchema)
                    , ("validity_lower_slot", integerSchema)
                    , ("validity_upper_slot", integerSchema)
                    , ("protocol_parameters", ppSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "token"
                   , "state_utxo"
                   , "request_utxos"
                   , "wallet_utxos"
                   , "validity_lower_slot"
                   , "validity_upper_slot"
                   , "protocol_parameters"
                   ]
            & description
                ?~ "Facts-only reject response. Carries the cage state UTxO, the batch of rejectable request UTxOs, wallet funding UTxO witnesses, server-derived Phase 3 validity slot bounds, and unverified protocol parameters, with no unsigned transaction CBOR."
#endif

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

#if !defined(wasm32_HOST_ARCH)
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
                ?~ "Facts-only end response. Carries state and wallet UTxO witnesses plus a request-set completeness proof, with no unsigned transaction CBOR."
#endif
