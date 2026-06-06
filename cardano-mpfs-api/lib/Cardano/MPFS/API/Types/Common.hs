{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.API.Types.Common
-- Description : Shared facts/proof JSON DTO primitives.
-- License     : Apache-2.0
--
-- Shared wire primitives used by facts endpoints and the
-- compatibility API DTO surface.
module Cardano.MPFS.API.Types.Common
    ( -- * Proof-bearing snapshot
      ChainPointJSON (..)
    , VerificationSnapshot (..)

      -- * Tokens
    , TokenIdJSON (..)

      -- * Facts/proof UTxO witnesses
    , UtxoRef (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoSetWitness (..)
    , UnverifiedPParams (..)
    , EvalContext (..)
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
import Data.ByteString (ByteString)
import Data.Text (Text)
#if !defined(wasm32_HOST_ARCH)
import Data.ByteString.Base16 qualified as B16
import Data.Proxy (Proxy (..))
import Data.Swagger
    ( ToParamSchema (..)
    , ToSchema (..)
    , declareSchemaRef
    , description
    , properties
    , required
    )
import Data.Swagger qualified as Swagger
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
#endif
import Data.Word (Word64)
#if !defined(wasm32_HOST_ARCH)
import GHC.IsList (IsList (..))
import Servant.API (FromHttpApiData (..), ToHttpApiData (..))
#endif

import Cardano.MPFS.API.Encoding (Hex (..))

-- | Indexed chain point baked into proof-bearing
-- responses. Identifies the exact block at which
-- the bundled proofs are valid.
data ChainPointJSON = ChainPointJSON
    { cpSlot :: Word64
    -- ^ Slot of the indexed chain point
    , cpBlockId :: Hex
    -- ^ Block hash at that slot (hex)
    }
    deriving (Eq, Show)

instance ToJSON ChainPointJSON where
    toJSON ChainPointJSON{..} =
        object
            [ "slot" .= cpSlot
            , "block_id" .= cpBlockId
            ]

instance FromJSON ChainPointJSON where
    parseJSON = withObject "ChainPointJSON" $ \o ->
        ChainPointJSON
            <$> o .: "slot"
            <*> o .: "block_id"

-- | Verification snapshot: the exact UTxO-CSMT root
-- plus indexed chain point against which a
-- proof-bearing response's bundled proofs are valid.
-- Every proof-bearing response embeds one of these so
-- clients can verify offline without a separate
-- @GET \/status@ call.
data VerificationSnapshot = VerificationSnapshot
    { vsUtxoRoot :: Hex
    -- ^ UTxO-CSMT root hash
    , vsChainPoint :: ChainPointJSON
    -- ^ Indexed chain point
    }
    deriving (Eq, Show)

instance ToJSON VerificationSnapshot where
    toJSON VerificationSnapshot{..} =
        object
            [ "utxo_root" .= vsUtxoRoot
            , "chainpoint" .= vsChainPoint
            ]

instance FromJSON VerificationSnapshot where
    parseJSON =
        withObject "VerificationSnapshot" $ \o ->
            VerificationSnapshot
                <$> o .: "utxo_root"
                <*> o .: "chainpoint"

-- | Hex-encoded token identifier for JSON transport.
newtype TokenIdJSON = TokenIdJSON
    { unTokenIdJSON :: ByteString
    }
    deriving (Eq, Show)

instance ToJSON TokenIdJSON where
    toJSON (TokenIdJSON bs) =
        toJSON (Hex bs)

instance FromJSON TokenIdJSON where
    parseJSON value = do
        Hex bs <- parseJSON value
        pure (TokenIdJSON bs)

#if !defined(wasm32_HOST_ARCH)
instance FromHttpApiData TokenIdJSON where
    parseUrlPiece t =
        case B16.decode (TE.encodeUtf8 t) of
            Right bs -> Right (TokenIdJSON bs)
            Left err -> Left (T.pack err)

instance ToHttpApiData TokenIdJSON where
    toUrlPiece (TokenIdJSON bs) = TE.decodeUtf8 (B16.encode bs)
#endif

-- | UTxO reference (@{ tx_id, tx_ix }@).
data UtxoRef = UtxoRef
    { urTxId :: Hex
    -- ^ Transaction id (32-byte blake2b, hex)
    , urTxIx :: Word64
    -- ^ Output index within the transaction
    }
    deriving (Eq, Show)

instance ToJSON UtxoRef where
    toJSON UtxoRef{..} =
        object
            [ "tx_id" .= urTxId
            , "tx_ix" .= urTxIx
            ]

instance FromJSON UtxoRef where
    parseJSON = withObject "UtxoRef" $ \o ->
        UtxoRef
            <$> o .: "tx_id"
            <*> o .: "tx_ix"

-- | A single attested UTxO: reference, resolved
-- @TxOut@, and a CSMT inclusion proof against the
-- enclosing response's @snapshot.utxo_root@.
data UtxoEntry = UtxoEntry
    { ueRef :: UtxoRef
    -- ^ UTxO reference (@ref@)
    , ueTxOutCbor :: Hex
    -- ^ CBOR-encoded @TxOut@ (@txout_cbor@)
    , ueInclusionProof :: Hex
    -- ^ CSMT inclusion proof (@inclusion_proof@)
    }
    deriving (Eq, Show)

instance ToJSON UtxoEntry where
    toJSON UtxoEntry{..} =
        object
            [ "ref" .= ueRef
            , "txout_cbor" .= ueTxOutCbor
            , "inclusion_proof" .= ueInclusionProof
            ]

instance FromJSON UtxoEntry where
    parseJSON = withObject "UtxoEntry" $ \o ->
        UtxoEntry
            <$> o .: "ref"
            <*> o .: "txout_cbor"
            <*> o .: "inclusion_proof"

-- | A UTxO entry without a per-entry inclusion proof.
-- Appears inside a 'UtxoSetWitness' where the
-- enclosing 'uswCompletenessProof' attests the whole
-- set at once.
data UtxoEntryRefOnly = UtxoEntryRefOnly
    { uerRef :: UtxoRef
    , uerTxOutCbor :: Hex
    }
    deriving (Eq, Show)

instance ToJSON UtxoEntryRefOnly where
    toJSON UtxoEntryRefOnly{..} =
        object
            [ "ref" .= uerRef
            , "txout_cbor" .= uerTxOutCbor
            ]

instance FromJSON UtxoEntryRefOnly where
    parseJSON =
        withObject "UtxoEntryRefOnly" $ \o ->
            UtxoEntryRefOnly
                <$> o .: "ref"
                <*> o .: "txout_cbor"

-- | An enumerated set of UTxOs at a known script-hash
-- prefix together with a single CSMT prefix-completeness
-- proof attesting the set is exactly the leaves under
-- that prefix in the snapshot's CSMT.
--
-- The script-hash prefix itself is not part of the
-- witness — the verifier derives it locally from the
-- trusted blueprint.
data UtxoSetWitness = UtxoSetWitness
    { uswEntries :: [UtxoEntryRefOnly]
    -- ^ Enumerated entries (@entries@)
    , uswCompletenessProof :: Hex
    -- ^ CSMT prefix-completeness proof
    -- (@completeness_proof@)
    }
    deriving (Eq, Show)

instance ToJSON UtxoSetWitness where
    toJSON UtxoSetWitness{..} =
        object
            [ "entries" .= uswEntries
            , "completeness_proof"
                .= uswCompletenessProof
            ]

instance FromJSON UtxoSetWitness where
    parseJSON =
        withObject "UtxoSetWitness" $ \o ->
            UtxoSetWitness
                <$> o .: "entries"
                <*> o .: "completeness_proof"

-- | Protocol parameters carried by a facts-only boot response.
--
-- The offchain service can report these bytes to downstream
-- tooling, but this slice does not verify their ledger-level
-- meaning. The @verified@ flag is therefore part of the wire
-- contract and must be @false@ for the dummy/test values used
-- by the boot facts verifier.
data UnverifiedPParams = UnverifiedPParams
    { uppVerified :: Bool
    -- ^ Whether the protocol parameters have been verified.
    , uppCbor :: Hex
    -- ^ CBOR-encoded protocol parameters (@cbor@).
    }
    deriving (Eq, Show)

instance ToJSON UnverifiedPParams where
    toJSON UnverifiedPParams{..} =
        object
            [ "verified" .= uppVerified
            , "cbor" .= uppCbor
            ]

instance FromJSON UnverifiedPParams where
    parseJSON =
        withObject "UnverifiedPParams" $ \o ->
            UnverifiedPParams
                <$> o .: "verified"
                <*> o .: "cbor"

-- | Trusted evaluation context for pure wallet-side
-- ledger ex-unit evaluation.
--
-- This endpoint is an interim trust boundary: the
-- offchain service reports the ledger parameters and
-- hard-fork history needed to evaluate a transaction
-- locally, but clients must treat those bytes as
-- trusted-not-proven until they are included in a
-- proof system.
data EvalContext = EvalContext
    { ecProtocolParameters :: UnverifiedPParams
    -- ^ Full Conway protocol parameters.
    , ecSystemStartCbor :: Hex
    -- ^ CBOR-encoded @SystemStart@.
    , ecEpochSize :: Word64
    -- ^ Fixed Shelley-era epoch size in slots.
    , ecSlotLengthMs :: Word64
    -- ^ Fixed Shelley-era slot length in milliseconds.
    , ecEraHistoryCbor :: Hex
    -- ^ Reserved for future proof-bearing hard-fork
    -- history. Browser builders use the explicit
    -- epoch fields above to avoid consensus dependencies.
    , ecTrusted :: Bool
    -- ^ Whether the service is asserting this context
    -- as trusted.
    , ecTrustAssumption :: Text
    -- ^ Human-readable trust-boundary note.
    }
    deriving (Eq, Show)

instance ToJSON EvalContext where
    toJSON EvalContext{..} =
        object
            [ "protocol_parameters"
                .= ecProtocolParameters
            , "system_start_cbor"
                .= ecSystemStartCbor
            , "epoch_size" .= ecEpochSize
            , "slot_length_ms" .= ecSlotLengthMs
            , "era_history_cbor"
                .= ecEraHistoryCbor
            , "trusted" .= ecTrusted
            , "trust_assumption"
                .= ecTrustAssumption
            ]

instance FromJSON EvalContext where
    parseJSON =
        withObject "EvalContext" $ \o ->
            EvalContext
                <$> o .: "protocol_parameters"
                <*> o .: "system_start_cbor"
                <*> o .: "epoch_size"
                <*> o .: "slot_length_ms"
                <*> o .: "era_history_cbor"
                <*> o .: "trusted"
                <*> o .: "trust_assumption"

#if !defined(wasm32_HOST_ARCH)
instance ToSchema ChainPointJSON where
    declareNamedSchema _ = do
        word64Schema <-
            declareSchemaRef (Proxy @Word64)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "ChainPointJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("slot", word64Schema)
                    , ("block_id", hexSchema)
                    ]
            & required .~ ["slot", "block_id"]
            & description
                ?~ "Indexed chain point baked into proof-bearing responses"

instance ToSchema VerificationSnapshot where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        chainPointSchema <-
            declareSchemaRef (Proxy @ChainPointJSON)
        pure
            $ Swagger.NamedSchema
                (Just "VerificationSnapshot")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("utxo_root", hexSchema)
                    , ("chainpoint", chainPointSchema)
                    ]
            & required
                .~ ["utxo_root", "chainpoint"]
            & description
                ?~ "UTxO-CSMT root and indexed chain point against which bundled proofs are valid"

instance ToSchema TokenIdJSON where
    declareNamedSchema _ =
        pure
            $ Swagger.NamedSchema
                (Just "TokenIdJSON")
            $ Swagger.toSchema (Proxy @String)
            & description
                ?~ "Hex-encoded token identifier"

instance ToParamSchema TokenIdJSON where
    toParamSchema _ =
        Swagger.toParamSchema (Proxy @String)

instance ToSchema UtxoRef where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        word64Schema <-
            declareSchemaRef (Proxy @Word64)
        pure
            $ Swagger.NamedSchema (Just "UtxoRef")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("tx_id", hexSchema)
                    , ("tx_ix", word64Schema)
                    ]
            & required .~ ["tx_id", "tx_ix"]
            & description ?~ "UTxO reference"

instance ToSchema UtxoEntry where
    declareNamedSchema _ = do
        refSchema <-
            declareSchemaRef (Proxy @UtxoRef)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema (Just "UtxoEntry")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("ref", refSchema)
                    , ("txout_cbor", hexSchema)
                    , ("inclusion_proof", hexSchema)
                    ]
            & required
                .~ [ "ref"
                   , "txout_cbor"
                   , "inclusion_proof"
                   ]
            & description
                ?~ "UTxO ref, CBOR body, and CSMT inclusion proof against the enclosing snapshot's utxo_root"

instance ToSchema UtxoEntryRefOnly where
    declareNamedSchema _ = do
        refSchema <-
            declareSchemaRef (Proxy @UtxoRef)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "UtxoEntryRefOnly")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("ref", refSchema)
                    , ("txout_cbor", hexSchema)
                    ]
            & required .~ ["ref", "txout_cbor"]
            & description
                ?~ "UTxO ref and CBOR body, without a per-entry inclusion proof. Used inside a UtxoSetWitness."

instance ToSchema UtxoSetWitness where
    declareNamedSchema _ = do
        entrySchema <-
            declareSchemaRef
                (Proxy @[UtxoEntryRefOnly])
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "UtxoSetWitness")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("entries", entrySchema)
                    , ("completeness_proof", hexSchema)
                    ]
            & required
                .~ [ "entries"
                   , "completeness_proof"
                   ]
            & description
                ?~ "Enumerated UTxOs at a known script-hash prefix, with a single CSMT prefix-completeness proof attesting the set is exactly the leaves under that prefix."

instance ToSchema UnverifiedPParams where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        boolSchema <-
            declareSchemaRef (Proxy @Bool)
        pure
            $ Swagger.NamedSchema
                (Just "UnverifiedPParams")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("verified", boolSchema)
                    , ("cbor", hexSchema)
                    ]
            & required .~ ["verified", "cbor"]
            & description
                ?~ "CBOR-encoded protocol parameters reported as unverified facts."

instance ToSchema EvalContext where
    declareNamedSchema _ = do
        ppSchema <-
            declareSchemaRef
                (Proxy @UnverifiedPParams)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        boolSchema <-
            declareSchemaRef (Proxy @Bool)
        word64Schema <-
            declareSchemaRef (Proxy @Word64)
        textSchema <-
            declareSchemaRef (Proxy @Text)
        pure
            $ Swagger.NamedSchema
                (Just "EvalContext")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("protocol_parameters", ppSchema)
                    , ("system_start_cbor", hexSchema)
                    , ("epoch_size", word64Schema)
                    , ("slot_length_ms", word64Schema)
                    , ("era_history_cbor", hexSchema)
                    , ("trusted", boolSchema)
                    , ("trust_assumption", textSchema)
                    ]
            & required
                .~ [ "protocol_parameters"
                   , "system_start_cbor"
                   , "epoch_size"
                   , "slot_length_ms"
                   , "era_history_cbor"
                   , "trusted"
                   , "trust_assumption"
                   ]
            & description
                ?~ "Trusted-not-proven ledger evaluation context for wallet-side ex-unit evaluation."
#endif
