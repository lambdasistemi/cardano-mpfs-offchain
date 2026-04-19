{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Types
-- Description : JSON request/response types for HTTP API
-- License     : Apache-2.0
--
-- Data types for the HTTP API layer. Each type has
-- 'ToJSON' and 'FromJSON' instances for transport.
-- These are decoupled from the internal domain types
-- to allow independent evolution of the wire format.
module Cardano.MPFS.HTTP.Types
    ( -- * Status
      StatusResponse (..)

      -- * Proof-bearing snapshot
    , ChainPointJSON (..)
    , VerificationSnapshot (..)

      -- * Tokens
    , TokenIdJSON (..)
    , TokenStateJSON (..)
    , tokenStateToJSON

      -- * Requests
    , RequestJSON (..)
    , requestToJSON

      -- * UTxO witnesses
    , TxInJSON (..)
    , txInToJSON
    , WitnessedUtxo (..)

      -- * Proof-bearing read responses
    , WitnessedTokenState (..)
    , WitnessedRequest (..)
    , FactWitness (..)
    , TokenResponse (..)
    , FactResponse (..)
    , ProofResponse (..)
    , RequestsResponse (..)

      -- * Transaction requests
    , BootRequest (..)
    , InsertRequest (..)
    , DeleteRequest (..)
    , UpdateValueRequest (..)
    , RejectRequest (..)
    , UpdateRequest (..)
    , RetractRequest (..)
    , EndRequest (..)
    , SubmitRequest (..)

      -- * Address parsing
    , parseAddr
    ) where

import Control.Lens ((&), (.~), (?~))
import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withObject
    , withText
    , (.:)
    , (.=)
    )
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
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
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import GHC.IsList (IsList (..))
import Servant.API (FromHttpApiData (..))

import Cardano.Ledger.Address (decodeAddrEither)
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.Mary.Value (AssetName (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))

import Cardano.Crypto.Hash.Class qualified as Crypto

import Cardano.MPFS.Core.Types
    ( Addr
    , Coin (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))

-- | Response for @GET \/status@.
data StatusResponse = StatusResponse
    { tipSlot :: Word64
    -- ^ Current chain tip slot
    , tipBlockId :: Hex
    -- ^ Current chain tip block hash (hex)
    , checkpointSlot :: Maybe Word64
    -- ^ Last processed checkpoint slot
    , checkpointBlockId :: Maybe Hex
    -- ^ Last processed checkpoint block hash
    , currentUtxoRoot :: Maybe Hex
    -- ^ Current UTxO-CSMT root hash for the indexed
    -- snapshot at the checkpoint above. 'Nothing' if
    -- the CSMT is not yet available. Matches the root
    -- returned by @GET \/utxo\/root@ and the
    -- @utxo_root@ baked into proof-bearing responses.
    }
    deriving (Eq, Show)

instance ToJSON StatusResponse where
    toJSON StatusResponse{..} =
        object
            [ "tip_slot" .= tipSlot
            , "tip_block_id" .= tipBlockId
            , "checkpoint_slot" .= checkpointSlot
            , "checkpoint_block_id"
                .= checkpointBlockId
            , "utxo_root" .= currentUtxoRoot
            ]

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
    { unTokenIdJSON :: TokenId
    }
    deriving (Eq, Show)

instance ToJSON TokenIdJSON where
    toJSON (TokenIdJSON (TokenId (AssetName sbs))) =
        toJSON
            ( TE.decodeUtf8
                (B16.encode (SBS.fromShort sbs))
            )

instance FromJSON TokenIdJSON where
    parseJSON = withText "TokenId" $ \t ->
        case B16.decode (TE.encodeUtf8 t) of
            Right bs ->
                pure
                    $ TokenIdJSON
                    $ TokenId
                    $ AssetName
                    $ SBS.toShort bs
            Left err -> fail err

instance FromHttpApiData TokenIdJSON where
    parseUrlPiece t =
        case B16.decode (TE.encodeUtf8 t) of
            Right bs ->
                Right
                    $ TokenIdJSON
                    $ TokenId
                    $ AssetName
                    $ SBS.toShort bs
            Left err -> Left (T.pack err)

-- | JSON representation of on-chain token state.
data TokenStateJSON = TokenStateJSON
    { owner :: Text
    -- ^ Owner payment key hash (hex)
    , root :: Hex
    -- ^ Current trie root hash
    , tip :: Integer
    -- ^ Maximum fee in lovelace
    , processTime :: Integer
    -- ^ Processing window (ms)
    , retractTime :: Integer
    -- ^ Retract window (ms)
    }
    deriving (Eq, Show)

instance ToJSON TokenStateJSON where
    toJSON TokenStateJSON{..} =
        object
            [ "owner" .= owner
            , "root" .= root
            , "tip" .= tip
            , "process_time" .= processTime
            , "retract_time" .= retractTime
            ]

instance FromJSON TokenStateJSON where
    parseJSON = withObject "TokenStateJSON" $ \o ->
        TokenStateJSON
            <$> o .: "owner"
            <*> o .: "root"
            <*> o .: "tip"
            <*> o .: "process_time"
            <*> o .: "retract_time"

-- | Convert internal 'TokenState' to JSON type.
tokenStateToJSON :: TokenState -> TokenStateJSON
tokenStateToJSON
    TokenState
        { owner = tsOwner
        , root = tsRoot
        , tip = tsMaxFee
        , processTime = tsProcessTime
        , retractTime = tsRetractTime
        } =
        TokenStateJSON
            { owner = hashToHex tsOwner
            , root = Hex (unRoot tsRoot)
            , tip = unCoin tsMaxFee
            , processTime = tsProcessTime
            , retractTime = tsRetractTime
            }

-- | Render a 'KeyHash' as hex text.
hashToHex :: KeyHash 'Payment -> Text
hashToHex (KeyHash h) =
    Crypto.hashToTextAsHex h

-- | JSON representation of a pending request.
data RequestJSON = RequestJSON
    { rjToken :: TokenIdJSON
    -- ^ Token this request targets
    , rjOwner :: Text
    -- ^ Requester's payment key hash (hex)
    , rjKey :: Hex
    -- ^ Trie key
    , rjOperation :: Text
    -- ^ "insert", "delete", or "update"
    , rjValue :: Maybe Hex
    -- ^ New value (for insert/update)
    , rjFee :: Integer
    -- ^ Fee in lovelace
    , rjSubmittedAt :: Integer
    -- ^ POSIXTime (ms)
    }
    deriving (Eq, Show)

instance ToJSON RequestJSON where
    toJSON RequestJSON{..} =
        object
            [ "token" .= rjToken
            , "owner" .= rjOwner
            , "key" .= rjKey
            , "operation" .= rjOperation
            , "value" .= rjValue
            , "fee" .= rjFee
            , "submitted_at" .= rjSubmittedAt
            ]

instance FromJSON RequestJSON where
    parseJSON = withObject "RequestJSON" $ \o ->
        RequestJSON
            <$> o .: "token"
            <*> o .: "owner"
            <*> o .: "key"
            <*> o .: "operation"
            <*> o .: "value"
            <*> o .: "fee"
            <*> o .: "submitted_at"

-- | Convert internal 'Request' to JSON type.
requestToJSON :: Request -> RequestJSON
requestToJSON
    Request
        { requestToken = tok
        , requestOwner = own
        , requestKey = k
        , requestValue = op
        , requestFee = fee
        , requestSubmittedAt = sat
        } =
        let (opName, mVal) = case op of
                Insert v -> ("insert", Just (Hex v))
                Delete _v -> ("delete", Nothing)
                Update _old new' ->
                    ("update", Just (Hex new'))
        in  RequestJSON
                { rjToken = TokenIdJSON tok
                , rjOwner = hashToHex own
                , rjKey = Hex k
                , rjOperation = opName
                , rjValue = mVal
                , rjFee = unCoin fee
                , rjSubmittedAt = sat
                }

-- ---------------------------------------------------------
-- UTxO witnesses
-- ---------------------------------------------------------

-- | JSON representation of a 'TxIn' as a
-- @{ tx_id, tx_ix }@ object.
data TxInJSON = TxInJSON
    { tjTxId :: Hex
    -- ^ Transaction id (32-byte blake2b, hex)
    , tjTxIx :: Word64
    -- ^ Output index within the transaction
    }
    deriving (Eq, Show)

instance ToJSON TxInJSON where
    toJSON TxInJSON{..} =
        object
            [ "tx_id" .= tjTxId
            , "tx_ix" .= tjTxIx
            ]

instance FromJSON TxInJSON where
    parseJSON = withObject "TxInJSON" $ \o ->
        TxInJSON
            <$> o .: "tx_id"
            <*> o .: "tx_ix"

-- | Convert a ledger 'TxIn' to its JSON form.
txInToJSON :: TxIn -> TxInJSON
txInToJSON (TxIn (TxId sh) (TxIx ix)) =
    TxInJSON
        { tjTxId =
            Hex (Crypto.hashToBytes (extractHash sh))
        , tjTxIx = fromIntegral ix
        }

-- | A witnessed UTxO: reference, resolved @TxOut@,
-- and the UTxO-CSMT inclusion proof that ties the
-- pair into the snapshot's @utxo_root@.
data WitnessedUtxo = WitnessedUtxo
    { wuTxIn :: TxInJSON
    -- ^ UTxO reference
    , wuTxOut :: Hex
    -- ^ CBOR-encoded @TxOut@ body (hex)
    , wuProof :: Hex
    -- ^ UTxO-CSMT inclusion proof (hex)
    }
    deriving (Eq, Show)

instance ToJSON WitnessedUtxo where
    toJSON WitnessedUtxo{..} =
        object
            [ "tx_in" .= wuTxIn
            , "tx_out" .= wuTxOut
            , "utxo_proof" .= wuProof
            ]

instance FromJSON WitnessedUtxo where
    parseJSON = withObject "WitnessedUtxo" $ \o ->
        WitnessedUtxo
            <$> o .: "tx_in"
            <*> o .: "tx_out"
            <*> o .: "utxo_proof"

-- ---------------------------------------------------------
-- Proof-bearing read responses
-- ---------------------------------------------------------

-- | Decoded token state together with the UTxO
-- witness that proves it resides at the indexed
-- snapshot's @utxo_root@.
data WitnessedTokenState = WitnessedTokenState
    { wtsUtxo :: WitnessedUtxo
    -- ^ UTxO witness for the state output
    , wtsState :: TokenStateJSON
    -- ^ Decoded on-chain state payload
    }
    deriving (Eq, Show)

instance ToJSON WitnessedTokenState where
    toJSON WitnessedTokenState{..} =
        object
            [ "utxo" .= wtsUtxo
            , "state" .= wtsState
            ]

instance FromJSON WitnessedTokenState where
    parseJSON =
        withObject "WitnessedTokenState" $ \o ->
            WitnessedTokenState
                <$> o .: "utxo"
                <*> o .: "state"

-- | A pending request together with the UTxO
-- witness proving it existed in the indexed
-- snapshot.
data WitnessedRequest = WitnessedRequest
    { wrUtxo :: WitnessedUtxo
    -- ^ UTxO witness for the request output
    , wrRequest :: RequestJSON
    -- ^ Decoded request payload
    }
    deriving (Eq, Show)

instance ToJSON WitnessedRequest where
    toJSON WitnessedRequest{..} =
        object
            [ "utxo" .= wrUtxo
            , "request" .= wrRequest
            ]

instance FromJSON WitnessedRequest where
    parseJSON =
        withObject "WitnessedRequest" $ \o ->
            WitnessedRequest
                <$> o .: "utxo"
                <*> o .: "request"

-- | A fact witness: the state witness that carries
-- the trie root plus an MPF inclusion proof binding a
-- key (and optional value) to that root.
data FactWitness = FactWitness
    { fwState :: WitnessedTokenState
    -- ^ State witness carrying the MPF root
    , fwMpfProof :: Hex
    -- ^ MPF inclusion proof (hex)
    }
    deriving (Eq, Show)

instance ToJSON FactWitness where
    toJSON FactWitness{..} =
        object
            [ "state" .= fwState
            , "mpf_proof" .= fwMpfProof
            ]

instance FromJSON FactWitness where
    parseJSON = withObject "FactWitness" $ \o ->
        FactWitness
            <$> o .: "state"
            <*> o .: "mpf_proof"

-- | Response envelope for @GET \/tokens\/:id@.
data TokenResponse = TokenResponse
    { trSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target
    , trState :: WitnessedTokenState
    -- ^ State + UTxO witness
    }
    deriving (Eq, Show)

instance ToJSON TokenResponse where
    toJSON TokenResponse{..} =
        object
            [ "snapshot" .= trSnapshot
            , "state" .= trState
            ]

instance FromJSON TokenResponse where
    parseJSON = withObject "TokenResponse" $ \o ->
        TokenResponse
            <$> o .: "snapshot"
            <*> o .: "state"

-- | Response envelope for
-- @GET \/tokens\/:id\/facts\/:key@.
data FactResponse = FactResponse
    { frSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target
    , frValue :: Hex
    -- ^ The fact value (hex)
    , frFact :: FactWitness
    -- ^ State witness + MPF proof
    }
    deriving (Eq, Show)

instance ToJSON FactResponse where
    toJSON FactResponse{..} =
        object
            [ "snapshot" .= frSnapshot
            , "value" .= frValue
            , "fact" .= frFact
            ]

instance FromJSON FactResponse where
    parseJSON = withObject "FactResponse" $ \o ->
        FactResponse
            <$> o .: "snapshot"
            <*> o .: "value"
            <*> o .: "fact"

-- | Response envelope for
-- @GET \/tokens\/:id\/proofs\/:key@.
data ProofResponse = ProofResponse
    { prSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target
    , prFact :: FactWitness
    -- ^ State witness + MPF proof
    }
    deriving (Eq, Show)

instance ToJSON ProofResponse where
    toJSON ProofResponse{..} =
        object
            [ "snapshot" .= prSnapshot
            , "fact" .= prFact
            ]

instance FromJSON ProofResponse where
    parseJSON = withObject "ProofResponse" $ \o ->
        ProofResponse
            <$> o .: "snapshot"
            <*> o .: "fact"

-- | Response envelope for @GET \/tokens\/:id\/requests@.
data RequestsResponse = RequestsResponse
    { rrSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target
    , rrRequests :: [WitnessedRequest]
    -- ^ Witnessed pending requests
    }
    deriving (Eq, Show)

instance ToJSON RequestsResponse where
    toJSON RequestsResponse{..} =
        object
            [ "snapshot" .= rrSnapshot
            , "requests" .= rrRequests
            ]

instance FromJSON RequestsResponse where
    parseJSON = withObject "RequestsResponse" $ \o ->
        RequestsResponse
            <$> o .: "snapshot"
            <*> o .: "requests"

-- | Parse a hex-encoded serialized address.
parseAddr :: Hex -> Either String Addr
parseAddr (Hex bs) = decodeAddrEither bs

-- | @POST \/tx\/boot@ request body.
newtype BootRequest = BootRequest
    { brAddr :: Hex
    -- ^ Address (hex-encoded serialized)
    }

instance FromJSON BootRequest where
    parseJSON = withObject "BootRequest" $ \o ->
        BootRequest <$> o .: "address"

-- | @POST \/tx\/request\/insert@ request body.
data InsertRequest = InsertRequest
    { irToken :: TokenIdJSON
    , irKey :: Hex
    , irValue :: Hex
    , irAddr :: Hex
    }

instance FromJSON InsertRequest where
    parseJSON = withObject "InsertRequest" $ \o ->
        InsertRequest
            <$> o .: "token"
            <*> o .: "key"
            <*> o .: "value"
            <*> o .: "address"

-- | @POST \/tx\/request\/delete@ request body.
data DeleteRequest = DeleteRequest
    { drToken :: TokenIdJSON
    , drKey :: Hex
    , drValue :: Hex
    , drAddr :: Hex
    }

instance FromJSON DeleteRequest where
    parseJSON = withObject "DeleteRequest" $ \o ->
        DeleteRequest
            <$> o .: "token"
            <*> o .: "key"
            <*> o .: "value"
            <*> o .: "address"

-- | @POST \/tx\/request\/update@ request body.
data UpdateValueRequest = UpdateValueRequest
    { uvrToken :: TokenIdJSON
    , uvrKey :: Hex
    , uvrOldValue :: Hex
    , uvrNewValue :: Hex
    , uvrAddr :: Hex
    }

instance FromJSON UpdateValueRequest where
    parseJSON =
        withObject "UpdateValueRequest" $ \o ->
            UpdateValueRequest
                <$> o .: "token"
                <*> o .: "key"
                <*> o .: "old_value"
                <*> o .: "new_value"
                <*> o .: "address"

-- | @POST \/tx\/reject@ request body.
data RejectRequest = RejectRequest
    { rejToken :: TokenIdJSON
    , rejAddr :: Hex
    }

instance FromJSON RejectRequest where
    parseJSON =
        withObject "RejectRequest" $ \o ->
            RejectRequest
                <$> o .: "token"
                <*> o .: "address"

-- | @POST \/tx\/update@ request body.
data UpdateRequest = UpdateRequest
    { urToken :: TokenIdJSON
    , urAddr :: Hex
    }

instance FromJSON UpdateRequest where
    parseJSON = withObject "UpdateRequest" $ \o ->
        UpdateRequest
            <$> o .: "token"
            <*> o .: "address"

-- | @POST \/tx\/retract@ request body.
data RetractRequest = RetractRequest
    { rrUtxo :: Text
    -- ^ UTxO reference: @txhash#ix@
    , rrAddr :: Hex
    -- ^ Address
    }

instance FromJSON RetractRequest where
    parseJSON = withObject "RetractRequest" $ \o ->
        RetractRequest
            <$> o .: "utxo"
            <*> o .: "address"

-- | @POST \/tx\/end@ request body.
data EndRequest = EndRequest
    { erToken :: TokenIdJSON
    , erAddr :: Hex
    }

instance FromJSON EndRequest where
    parseJSON = withObject "EndRequest" $ \o ->
        EndRequest
            <$> o .: "token"
            <*> o .: "address"

-- | @POST \/tx\/submit@ request body.
-- Accepts a hex-encoded signed transaction CBOR.
newtype SubmitRequest = SubmitRequest
    { srTxCbor :: Hex
    -- ^ Signed transaction CBOR (hex)
    }

instance FromJSON SubmitRequest where
    parseJSON = withObject "SubmitRequest" $ \o ->
        SubmitRequest <$> o .: "tx"

-- ---------------------------------------------------------
-- Swagger ToSchema instances
-- ---------------------------------------------------------

instance ToSchema StatusResponse where
    declareNamedSchema _ = do
        word64Schema <-
            declareSchemaRef (Proxy @Word64)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        maybeWord64 <-
            declareSchemaRef (Proxy @(Maybe Word64))
        maybeHex <-
            declareSchemaRef (Proxy @(Maybe Hex))
        pure
            $ Swagger.NamedSchema
                (Just "StatusResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("tip_slot", word64Schema)
                    , ("tip_block_id", hexSchema)
                    ,
                        ( "checkpoint_slot"
                        , maybeWord64
                        )
                    ,
                        ( "checkpoint_block_id"
                        , maybeHex
                        )
                    , ("utxo_root", maybeHex)
                    ]
            & required
                .~ [ "tip_slot"
                   , "tip_block_id"
                   , "checkpoint_slot"
                   , "checkpoint_block_id"
                   , "utxo_root"
                   ]
            & description
                ?~ "Indexer chain tip, checkpoint, \
                   \and current UTxO-CSMT root"

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
                ?~ "Indexed chain point baked into \
                   \proof-bearing responses"

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
                ?~ "UTxO-CSMT root and indexed chain \
                   \point against which bundled \
                   \proofs are valid"

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

instance ToSchema TokenStateJSON where
    declareNamedSchema _ = do
        textSchema <-
            declareSchemaRef (Proxy @Text)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        intSchema <-
            declareSchemaRef (Proxy @Integer)
        pure
            $ Swagger.NamedSchema
                (Just "TokenStateJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("owner", textSchema)
                    , ("root", hexSchema)
                    , ("tip", intSchema)
                    , ("process_time", intSchema)
                    , ("retract_time", intSchema)
                    ]
            & required
                .~ [ "owner"
                   , "root"
                   , "tip"
                   , "process_time"
                   , "retract_time"
                   ]
            & description
                ?~ "On-chain token state"

instance ToSchema RequestJSON where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        textSchema <-
            declareSchemaRef (Proxy @Text)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        maybeHex <-
            declareSchemaRef (Proxy @(Maybe Hex))
        intSchema <-
            declareSchemaRef (Proxy @Integer)
        pure
            $ Swagger.NamedSchema
                (Just "RequestJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("owner", textSchema)
                    , ("key", hexSchema)
                    , ("operation", textSchema)
                    , ("value", maybeHex)
                    , ("fee", intSchema)
                    , ("submitted_at", intSchema)
                    ]
            & required
                .~ [ "token"
                   , "owner"
                   , "key"
                   , "operation"
                   , "fee"
                   , "submitted_at"
                   ]
            & description
                ?~ "Pending request"

instance ToSchema BootRequest where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "BootRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [("address", hexSchema)]
            & required .~ ["address"]
            & description
                ?~ "Boot a new token"

instance ToSchema InsertRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "InsertRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("value", hexSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ [ "token"
                   , "key"
                   , "value"
                   , "address"
                   ]
            & description
                ?~ "Insert a key-value pair"

instance ToSchema DeleteRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "DeleteRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("value", hexSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["token", "key", "value", "address"]
            & description
                ?~ "Delete a key (value is the \
                   \current stored value)"

instance ToSchema UpdateValueRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "UpdateValueRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("key", hexSchema)
                    , ("old_value", hexSchema)
                    , ("new_value", hexSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ [ "token"
                   , "key"
                   , "old_value"
                   , "new_value"
                   , "address"
                   ]
            & description
                ?~ "Update a key's value \
                   \(old and new values)"

instance ToSchema RejectRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "RejectRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["token", "address"]
            & description
                ?~ "Reject Phase 3 expired \
                   \requests for a token"

instance ToSchema UpdateRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "UpdateRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["token", "address"]
            & description
                ?~ "Process pending requests"

instance ToSchema RetractRequest where
    declareNamedSchema _ = do
        stringSchema <-
            declareSchemaRef (Proxy @String)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "RetractRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("utxo", stringSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["utxo", "address"]
            & description
                ?~ "Retract a pending request. \
                   \UTxO format: txhash#ix"

instance ToSchema EndRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "EndRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["token", "address"]
            & description
                ?~ "End a token"

instance ToSchema SubmitRequest where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "SubmitRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList [("tx", hexSchema)]
            & required .~ ["tx"]
            & description
                ?~ "Submit a signed transaction"

instance ToSchema TxInJSON where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        word64Schema <-
            declareSchemaRef (Proxy @Word64)
        pure
            $ Swagger.NamedSchema (Just "TxInJSON")
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

instance ToSchema WitnessedUtxo where
    declareNamedSchema _ = do
        txInSchema <-
            declareSchemaRef (Proxy @TxInJSON)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "WitnessedUtxo")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("tx_in", txInSchema)
                    , ("tx_out", hexSchema)
                    , ("utxo_proof", hexSchema)
                    ]
            & required
                .~ [ "tx_in"
                   , "tx_out"
                   , "utxo_proof"
                   ]
            & description
                ?~ "UTxO reference, CBOR body, and \
                   \UTxO-CSMT inclusion proof"

instance ToSchema WitnessedTokenState where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        stateSchema <-
            declareSchemaRef (Proxy @TokenStateJSON)
        pure
            $ Swagger.NamedSchema
                (Just "WitnessedTokenState")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("utxo", utxoSchema)
                    , ("state", stateSchema)
                    ]
            & required .~ ["utxo", "state"]
            & description
                ?~ "Token state plus UTxO witness"

instance ToSchema WitnessedRequest where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        requestSchema <-
            declareSchemaRef (Proxy @RequestJSON)
        pure
            $ Swagger.NamedSchema
                (Just "WitnessedRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("utxo", utxoSchema)
                    , ("request", requestSchema)
                    ]
            & required .~ ["utxo", "request"]
            & description
                ?~ "Pending request plus UTxO witness"

instance ToSchema FactWitness where
    declareNamedSchema _ = do
        stateSchema <-
            declareSchemaRef
                (Proxy @WitnessedTokenState)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema (Just "FactWitness")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("state", stateSchema)
                    , ("mpf_proof", hexSchema)
                    ]
            & required .~ ["state", "mpf_proof"]
            & description
                ?~ "State witness plus MPF inclusion \
                   \proof"

instance ToSchema TokenResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        stateSchema <-
            declareSchemaRef
                (Proxy @WitnessedTokenState)
        pure
            $ Swagger.NamedSchema
                (Just "TokenResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("state", stateSchema)
                    ]
            & required .~ ["snapshot", "state"]
            & description
                ?~ "Proof-bearing token state response"

instance ToSchema FactResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        factSchema <-
            declareSchemaRef (Proxy @FactWitness)
        pure
            $ Swagger.NamedSchema
                (Just "FactResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("value", hexSchema)
                    , ("fact", factSchema)
                    ]
            & required
                .~ ["snapshot", "value", "fact"]
            & description
                ?~ "Proof-bearing fact value response"

instance ToSchema ProofResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        factSchema <-
            declareSchemaRef (Proxy @FactWitness)
        pure
            $ Swagger.NamedSchema
                (Just "ProofResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("fact", factSchema)
                    ]
            & required .~ ["snapshot", "fact"]
            & description
                ?~ "Proof-bearing MPF proof response"

instance ToSchema RequestsResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        reqListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedRequest])
        pure
            $ Swagger.NamedSchema
                (Just "RequestsResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("requests", reqListSchema)
                    ]
            & required
                .~ ["snapshot", "requests"]
            & description
                ?~ "Proof-bearing pending requests \
                   \response"
