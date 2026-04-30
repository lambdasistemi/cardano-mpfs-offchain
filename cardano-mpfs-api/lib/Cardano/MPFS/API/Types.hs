{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.API.Types
-- Description : JSON request/response types for HTTP API
-- License     : Apache-2.0
--
-- Data types for the HTTP API layer. Each type has
-- 'ToJSON' and 'FromJSON' instances for transport.
-- These are decoupled from the internal domain types
-- to allow independent evolution of the wire format.
module Cardano.MPFS.API.Types
    ( -- * Status
      StatusResponse (..)

      -- * Proof-bearing snapshot
    , ChainPointJSON (..)
    , VerificationSnapshot (..)

      -- * Tokens
    , TokenIdJSON (..)
    , TokenStateJSON (..)

      -- * Requests
    , RequestJSON (..)

      -- * UTxO witnesses
    , TxInJSON (..)
    , WitnessedUtxo (..)

      -- * Post-split proof primitives (#243)
    , UtxoRef (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoSetWitness (..)
    , UnsignedTxResponse (..)
    , FactPresentResponse (..)
    , FactAbsentResponse (..)

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
    , SweepRequest (..)
    , EndRequest (..)
    , SubmitRequest (..)

      -- * Proof-bearing tx responses
    , TrieFactJSON (..)
    , BootProofJSON (..)
    , RequestProofJSON (..)
    , RetractProofJSON (..)
    , RejectProofJSON (..)
    , EndProofJSON (..)
    , UpdateProofJSON (..)
    , BootTxResponse (..)
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , SweepTxResponse (..)
    , UpdateTxResponse (..)
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
import Data.ByteString (ByteString)
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
import Data.Swagger.Declare (Declare)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import GHC.IsList (IsList (..))
import Servant.API (FromHttpApiData (..))

import Cardano.MPFS.API.Encoding (Hex (..))

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

instance FromJSON StatusResponse where
    parseJSON = withObject "StatusResponse" $ \o ->
        StatusResponse
            <$> o .: "tip_slot"
            <*> o .: "tip_block_id"
            <*> o .: "checkpoint_slot"
            <*> o .: "checkpoint_block_id"
            <*> o .: "utxo_root"

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

instance FromHttpApiData TokenIdJSON where
    parseUrlPiece t =
        case B16.decode (TE.encodeUtf8 t) of
            Right bs -> Right (TokenIdJSON bs)
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
-- Post-split proof primitives (#243)
-- ---------------------------------------------------------
-- Successors of 'TxInJSON' / 'WitnessedUtxo' for the
-- post-split API surface. The new types adopt the wire
-- vocabulary the redesign locked in:
--
--   * @ref@             — UTxO reference
--   * @txout_cbor@      — CBOR-encoded TxOut (hex)
--   * @inclusion_proof@ — CSMT inclusion proof against
--                         the snapshot's @utxo_root@
--   * @entries@         — flat list of refs in a witness
--   * @completeness_proof@ — CSMT prefix-completeness
--                            proof outside the entries
--                            list, attesting the set is
--                            exactly the leaves under a
--                            script-hash prefix
--
-- Old types stay until every endpoint is migrated; they
-- will be removed in the polish slice of #243.

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

-- | Uniform response envelope for proof-bearing write
-- endpoints under #243.
--
-- Carries the unsigned transaction CBOR plus the
-- verification snapshot and a single flat list of
-- inputs. Each entry in @inputs@ — spent or reference —
-- carries its CSMT inclusion proof against the
-- enclosing snapshot's @utxo_root@; the role of each
-- input is encoded by the redeemers in
-- @unsigned_tx_cbor@ and is not discriminated by the
-- envelope.
--
-- The endpoints that bundle a per-cage requests
-- completeness witness (@POST \/tx\/oracle\/update@,
-- @POST \/tx\/oracle\/end@) carry that witness in a
-- separate response type added by their slices; this
-- envelope intentionally omits it so that boot,
-- requester, reject, sweeps and submit share the
-- exact same shape.
data UnsignedTxResponse = UnsignedTxResponse
    { utrUnsignedTxCbor :: Hex
    -- ^ CBOR-hex of the unsigned transaction
    -- (@unsigned_tx_cbor@).
    , utrSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled inclusion proofs target
    -- (@snapshot@).
    , utrInputs :: [UtxoEntry]
    -- ^ Flat list of every spent and reference input
    -- (@inputs@). Roles are derived from the
    -- redeemers in @unsigned_tx_cbor@.
    }
    deriving (Eq, Show)

instance ToJSON UnsignedTxResponse where
    toJSON UnsignedTxResponse{..} =
        object
            [ "unsigned_tx_cbor" .= utrUnsignedTxCbor
            , "snapshot" .= utrSnapshot
            , "inputs" .= utrInputs
            ]

instance FromJSON UnsignedTxResponse where
    parseJSON =
        withObject "UnsignedTxResponse" $ \o ->
            UnsignedTxResponse
                <$> o .: "unsigned_tx_cbor"
                <*> o .: "snapshot"
                <*> o .: "inputs"

-- | Response envelope for @GET \/tokens\/:id\/facts\/:key@
-- when the trie contains the key (HTTP 200).
--
-- Carries the value plus an MPF inclusion proof against
-- the trie root recovered from @state_utxo@'s on-chain
-- datum, all anchored to one snapshot.
data FactPresentResponse = FactPresentResponse
    { fprSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , fprStateUtxo :: UtxoEntry
    -- ^ State UTxO with its CSMT inclusion proof.
    , fprValue :: Hex
    -- ^ The trie value associated with the key.
    , fprMpfInclusionProof :: Hex
    -- ^ Aiken MPF inclusion proof against the trie
    -- root recovered from @state_utxo.txout_cbor@'s
    -- inline datum.
    }
    deriving (Eq, Show)

instance ToJSON FactPresentResponse where
    toJSON FactPresentResponse{..} =
        object
            [ "snapshot" .= fprSnapshot
            , "state_utxo" .= fprStateUtxo
            , "value" .= fprValue
            , "mpf_inclusion_proof"
                .= fprMpfInclusionProof
            ]

instance FromJSON FactPresentResponse where
    parseJSON =
        withObject "FactPresentResponse" $ \o ->
            FactPresentResponse
                <$> o .: "snapshot"
                <*> o .: "state_utxo"
                <*> o .: "value"
                <*> o .: "mpf_inclusion_proof"

-- | Response envelope for @GET \/tokens\/:id\/facts\/:key@
-- when the trie does /not/ contain the key (HTTP 404
-- with body).
--
-- HTTP 404 with /no/ body is returned for the distinct
-- case where the indexer has no record of the cage
-- itself; that response is unverified by design and not
-- modelled by a Haskell type.
data FactAbsentResponse = FactAbsentResponse
    { farSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , farStateUtxo :: UtxoEntry
    -- ^ State UTxO with its CSMT inclusion proof.
    , farMpfExclusionProof :: Hex
    -- ^ Aiken MPF exclusion proof against the trie
    -- root recovered from @state_utxo.txout_cbor@'s
    -- inline datum.
    }
    deriving (Eq, Show)

instance ToJSON FactAbsentResponse where
    toJSON FactAbsentResponse{..} =
        object
            [ "snapshot" .= farSnapshot
            , "state_utxo" .= farStateUtxo
            , "mpf_exclusion_proof"
                .= farMpfExclusionProof
            ]

instance FromJSON FactAbsentResponse where
    parseJSON =
        withObject "FactAbsentResponse" $ \o ->
            FactAbsentResponse
                <$> o .: "snapshot"
                <*> o .: "state_utxo"
                <*> o .: "mpf_exclusion_proof"

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

-- | @POST \/tx\/boot@ request body.
newtype BootRequest = BootRequest
    { brAddr :: Hex
    -- ^ Address (hex-encoded serialized)
    }

instance ToJSON BootRequest where
    toJSON BootRequest{..} =
        object ["address" .= brAddr]

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

instance ToJSON InsertRequest where
    toJSON InsertRequest{..} =
        object
            [ "token" .= irToken
            , "key" .= irKey
            , "value" .= irValue
            , "address" .= irAddr
            ]

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

instance ToJSON DeleteRequest where
    toJSON DeleteRequest{..} =
        object
            [ "token" .= drToken
            , "key" .= drKey
            , "value" .= drValue
            , "address" .= drAddr
            ]

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

instance ToJSON UpdateValueRequest where
    toJSON UpdateValueRequest{..} =
        object
            [ "token" .= uvrToken
            , "key" .= uvrKey
            , "old_value" .= uvrOldValue
            , "new_value" .= uvrNewValue
            , "address" .= uvrAddr
            ]

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

instance ToJSON RejectRequest where
    toJSON RejectRequest{..} =
        object
            [ "token" .= rejToken
            , "address" .= rejAddr
            ]

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

instance ToJSON UpdateRequest where
    toJSON UpdateRequest{..} =
        object
            [ "token" .= urToken
            , "address" .= urAddr
            ]

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

instance ToJSON RetractRequest where
    toJSON RetractRequest{..} =
        object
            [ "utxo" .= rrUtxo
            , "address" .= rrAddr
            ]

instance FromJSON RetractRequest where
    parseJSON = withObject "RetractRequest" $ \o ->
        RetractRequest
            <$> o .: "utxo"
            <*> o .: "address"

-- | @POST \/tx\/sweep@ request body. Owner-only
-- request to sweep a non-legitimate UTxO at the
-- per-cage request address. The state UTxO is
-- referenced (not consumed) so the on-chain
-- validator can read the owner key from the state
-- datum.
data SweepRequest = SweepRequest
    { swrToken :: TokenIdJSON
    -- ^ Cage token whose request address is being
    -- swept
    , swrUtxo :: Text
    -- ^ UTxO reference of the garbage UTxO
    -- (@txhash#ix@)
    , swrAddr :: Hex
    -- ^ Owner address (signs and balances)
    }

instance ToJSON SweepRequest where
    toJSON SweepRequest{..} =
        object
            [ "token" .= swrToken
            , "utxo" .= swrUtxo
            , "address" .= swrAddr
            ]

instance FromJSON SweepRequest where
    parseJSON = withObject "SweepRequest" $ \o ->
        SweepRequest
            <$> o .: "token"
            <*> o .: "utxo"
            <*> o .: "address"

-- | @POST \/tx\/end@ request body.
data EndRequest = EndRequest
    { erToken :: TokenIdJSON
    , erAddr :: Hex
    }

instance ToJSON EndRequest where
    toJSON EndRequest{..} =
        object
            [ "token" .= erToken
            , "address" .= erAddr
            ]

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

instance ToJSON SubmitRequest where
    toJSON SubmitRequest{..} =
        object ["tx" .= srTxCbor]

instance FromJSON SubmitRequest where
    parseJSON = withObject "SubmitRequest" $ \o ->
        SubmitRequest <$> o .: "tx"

-- ---------------------------------------------------------
-- Proof-bearing tx envelope responses
-- ---------------------------------------------------------

-- | JSON representation of a 'TrieFact'.
data TrieFactJSON = TrieFactJSON
    { tfKey :: Hex
    -- ^ Trie key the builder looked up
    , tfValue :: Maybe Hex
    -- ^ Value bound to 'tfKey', or 'Nothing' for an
    -- absence fact
    , tfMpfProof :: Hex
    -- ^ MPF inclusion\/exclusion proof (hex)
    }
    deriving (Eq, Show)

instance ToJSON TrieFactJSON where
    toJSON TrieFactJSON{..} =
        object
            [ "key" .= tfKey
            , "value" .= tfValue
            , "mpf_proof" .= tfMpfProof
            ]

instance FromJSON TrieFactJSON where
    parseJSON = withObject "TrieFactJSON" $ \o ->
        TrieFactJSON
            <$> o .: "key"
            <*> o .: "value"
            <*> o .: "mpf_proof"

-- | Proof payload for @POST \/tx\/boot@ responses.
newtype BootProofJSON = BootProofJSON
    { bpFunding :: [WitnessedUtxo]
    -- ^ Witnessed wallet inputs funding the boot tx
    }
    deriving (Eq, Show)

instance ToJSON BootProofJSON where
    toJSON BootProofJSON{..} =
        object ["funding" .= bpFunding]

instance FromJSON BootProofJSON where
    parseJSON = withObject "BootProofJSON" $ \o ->
        BootProofJSON <$> o .: "funding"

-- | Proof payload for
-- @POST \/tx\/request\/{insert,delete,update}@
-- responses. All three request endpoints share the
-- same shape: they only spend wallet inputs and create
-- a fresh pending-request output.
newtype RequestProofJSON = RequestProofJSON
    { rqpFunding :: [WitnessedUtxo]
    -- ^ Witnessed wallet inputs funding the request tx
    }
    deriving (Eq, Show)

instance ToJSON RequestProofJSON where
    toJSON RequestProofJSON{..} =
        object ["funding" .= rqpFunding]

instance FromJSON RequestProofJSON where
    parseJSON =
        withObject "RequestProofJSON" $ \o ->
            RequestProofJSON <$> o .: "funding"

-- | Proof payload for @POST \/tx\/retract@ responses.
data RetractProofJSON = RetractProofJSON
    { rtpRequestIn :: WitnessedUtxo
    -- ^ The pending-request UTxO being retracted
    , rtpStateRef :: WitnessedUtxo
    -- ^ The state UTxO referenced for its timing
    -- parameters
    , rtpFunding :: [WitnessedUtxo]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

instance ToJSON RetractProofJSON where
    toJSON RetractProofJSON{..} =
        object
            [ "request_in" .= rtpRequestIn
            , "state_ref" .= rtpStateRef
            , "funding" .= rtpFunding
            ]

instance FromJSON RetractProofJSON where
    parseJSON =
        withObject "RetractProofJSON" $ \o ->
            RetractProofJSON
                <$> o .: "request_in"
                <*> o .: "state_ref"
                <*> o .: "funding"

-- | Proof payload for @POST \/tx\/reject@ responses.
data RejectProofJSON = RejectProofJSON
    { rjpState :: WitnessedUtxo
    -- ^ The consumed state UTxO
    , rjpRequestIns :: [WitnessedUtxo]
    -- ^ The pending-request UTxOs being rejected
    , rjpFunding :: [WitnessedUtxo]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

instance ToJSON RejectProofJSON where
    toJSON RejectProofJSON{..} =
        object
            [ "state" .= rjpState
            , "request_ins" .= rjpRequestIns
            , "funding" .= rjpFunding
            ]

instance FromJSON RejectProofJSON where
    parseJSON = withObject "RejectProofJSON" $ \o ->
        RejectProofJSON
            <$> o .: "state"
            <*> o .: "request_ins"
            <*> o .: "funding"

-- | Proof payload for @POST \/tx\/end@ responses.
data EndProofJSON = EndProofJSON
    { epState :: WitnessedUtxo
    -- ^ The consumed state UTxO
    , epFunding :: [WitnessedUtxo]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

instance ToJSON EndProofJSON where
    toJSON EndProofJSON{..} =
        object
            [ "state" .= epState
            , "funding" .= epFunding
            ]

instance FromJSON EndProofJSON where
    parseJSON = withObject "EndProofJSON" $ \o ->
        EndProofJSON
            <$> o .: "state"
            <*> o .: "funding"

-- | Proof payload for @POST \/tx\/update@ responses.
-- Includes the trie-level MPF reads performed during
-- batch application, rooted at the trie root encoded in
-- the consumed state datum.
data UpdateProofJSON = UpdateProofJSON
    { upState :: WitnessedUtxo
    -- ^ The consumed state UTxO
    , upRequests :: [WitnessedUtxo]
    -- ^ Pending-request UTxOs batched into this update
    , upFunding :: [WitnessedUtxo]
    -- ^ Wallet inputs covering fees
    , upTrieRoot :: Hex
    -- ^ Trie root from the consumed state datum
    , upTrieRead :: [TrieFactJSON]
    -- ^ MPF reads against 'upTrieRoot'
    }
    deriving (Eq, Show)

instance ToJSON UpdateProofJSON where
    toJSON UpdateProofJSON{..} =
        object
            [ "state" .= upState
            , "requests" .= upRequests
            , "funding" .= upFunding
            , "trie_root" .= upTrieRoot
            , "trie_read" .= upTrieRead
            ]

instance FromJSON UpdateProofJSON where
    parseJSON = withObject "UpdateProofJSON" $ \o ->
        UpdateProofJSON
            <$> o .: "state"
            <*> o .: "requests"
            <*> o .: "funding"
            <*> o .: "trie_root"
            <*> o .: "trie_read"

-- | Response envelope for @POST \/tx\/boot@.
data BootTxResponse = BootTxResponse
    { btrTx :: Hex
    -- ^ Unsigned transaction CBOR (hex)
    , btrSnapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target
    , btrProof :: BootProofJSON
    -- ^ Per-endpoint proof payload
    }
    deriving (Eq, Show)

instance ToJSON BootTxResponse where
    toJSON BootTxResponse{..} =
        object
            [ "tx" .= btrTx
            , "snapshot" .= btrSnapshot
            , "proof" .= btrProof
            ]

instance FromJSON BootTxResponse where
    parseJSON = withObject "BootTxResponse" $ \o ->
        BootTxResponse
            <$> o .: "tx"
            <*> o .: "snapshot"
            <*> o .: "proof"

-- | Response envelope for
-- @POST \/tx\/request\/{insert,delete,update}@.
data RequestTxResponse = RequestTxResponse
    { rqtTx :: Hex
    , rqtSnapshot :: VerificationSnapshot
    , rqtProof :: RequestProofJSON
    }
    deriving (Eq, Show)

instance ToJSON RequestTxResponse where
    toJSON RequestTxResponse{..} =
        object
            [ "tx" .= rqtTx
            , "snapshot" .= rqtSnapshot
            , "proof" .= rqtProof
            ]

instance FromJSON RequestTxResponse where
    parseJSON =
        withObject "RequestTxResponse" $ \o ->
            RequestTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

-- | Response envelope for @POST \/tx\/retract@.
data RetractTxResponse = RetractTxResponse
    { rttTx :: Hex
    , rttSnapshot :: VerificationSnapshot
    , rttProof :: RetractProofJSON
    }
    deriving (Eq, Show)

instance ToJSON RetractTxResponse where
    toJSON RetractTxResponse{..} =
        object
            [ "tx" .= rttTx
            , "snapshot" .= rttSnapshot
            , "proof" .= rttProof
            ]

instance FromJSON RetractTxResponse where
    parseJSON =
        withObject "RetractTxResponse" $ \o ->
            RetractTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

-- | Response envelope for @POST \/tx\/sweep@.
-- Carries only the unsigned CBOR; sweep does not
-- bundle a proof envelope (the on-chain validator
-- enforces the owner-signature predicate against
-- the referenced state UTxO).
newtype SweepTxResponse = SweepTxResponse
    { stTx :: Hex
    }
    deriving (Eq, Show)

instance ToJSON SweepTxResponse where
    toJSON SweepTxResponse{..} =
        object ["tx" .= stTx]

instance FromJSON SweepTxResponse where
    parseJSON =
        withObject "SweepTxResponse" $ \o ->
            SweepTxResponse <$> o .: "tx"

-- | Response envelope for @POST \/tx\/reject@.
data RejectTxResponse = RejectTxResponse
    { rjtTx :: Hex
    , rjtSnapshot :: VerificationSnapshot
    , rjtProof :: RejectProofJSON
    }
    deriving (Eq, Show)

instance ToJSON RejectTxResponse where
    toJSON RejectTxResponse{..} =
        object
            [ "tx" .= rjtTx
            , "snapshot" .= rjtSnapshot
            , "proof" .= rjtProof
            ]

instance FromJSON RejectTxResponse where
    parseJSON =
        withObject "RejectTxResponse" $ \o ->
            RejectTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

-- | Response envelope for @POST \/tx\/end@.
data EndTxResponse = EndTxResponse
    { etTx :: Hex
    , etSnapshot :: VerificationSnapshot
    , etProof :: EndProofJSON
    }
    deriving (Eq, Show)

instance ToJSON EndTxResponse where
    toJSON EndTxResponse{..} =
        object
            [ "tx" .= etTx
            , "snapshot" .= etSnapshot
            , "proof" .= etProof
            ]

instance FromJSON EndTxResponse where
    parseJSON = withObject "EndTxResponse" $ \o ->
        EndTxResponse
            <$> o .: "tx"
            <*> o .: "snapshot"
            <*> o .: "proof"

-- | Response envelope for @POST \/tx\/update@.
data UpdateTxResponse = UpdateTxResponse
    { uptTx :: Hex
    , uptSnapshot :: VerificationSnapshot
    , uptProof :: UpdateProofJSON
    }
    deriving (Eq, Show)

instance ToJSON UpdateTxResponse where
    toJSON UpdateTxResponse{..} =
        object
            [ "tx" .= uptTx
            , "snapshot" .= uptSnapshot
            , "proof" .= uptProof
            ]

instance FromJSON UpdateTxResponse where
    parseJSON =
        withObject "UpdateTxResponse" $ \o ->
            UpdateTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

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

instance ToSchema SweepRequest where
    declareNamedSchema _ = do
        tokenSchema <-
            declareSchemaRef (Proxy @TokenIdJSON)
        stringSchema <-
            declareSchemaRef (Proxy @String)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "SweepRequest")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("token", tokenSchema)
                    , ("utxo", stringSchema)
                    , ("address", hexSchema)
                    ]
            & required
                .~ ["token", "utxo", "address"]
            & description
                ?~ "Owner-only sweep of a \
                   \non-legitimate UTxO at the \
                   \per-cage request address. \
                   \UTxO format: txhash#ix"

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

-- Post-split (#243) primitives.

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
                ?~ "UTxO ref, CBOR body, and CSMT \
                   \inclusion proof against the \
                   \enclosing snapshot's utxo_root"

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
                ?~ "UTxO ref and CBOR body, without a \
                   \per-entry inclusion proof. Used \
                   \inside a UtxoSetWitness."

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
                ?~ "Enumerated UTxOs at a known \
                   \script-hash prefix, with a single \
                   \CSMT prefix-completeness proof \
                   \attesting the set is exactly the \
                   \leaves under that prefix."

instance ToSchema UnsignedTxResponse where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        inputsSchema <-
            declareSchemaRef (Proxy @[UtxoEntry])
        pure
            $ Swagger.NamedSchema
                (Just "UnsignedTxResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("unsigned_tx_cbor", hexSchema)
                    , ("snapshot", snapshotSchema)
                    , ("inputs", inputsSchema)
                    ]
            & required
                .~ [ "unsigned_tx_cbor"
                   , "snapshot"
                   , "inputs"
                   ]
            & description
                ?~ "Uniform proof-bearing response for \
                   \write endpoints. Carries the \
                   \unsigned transaction CBOR plus a \
                   \snapshot and a flat list of spent \
                   \and reference inputs, each with its \
                   \CSMT inclusion proof."

instance ToSchema FactPresentResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        utxoEntrySchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "FactPresentResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("state_utxo", utxoEntrySchema)
                    , ("value", hexSchema)
                    , ("mpf_inclusion_proof", hexSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "state_utxo"
                   , "value"
                   , "mpf_inclusion_proof"
                   ]
            & description
                ?~ "GET /tokens/:id/facts/:key — present \
                   \(HTTP 200). Carries the value plus \
                   \an MPF inclusion proof against the \
                   \trie root recovered from \
                   \state_utxo's datum."

instance ToSchema FactAbsentResponse where
    declareNamedSchema _ = do
        snapshotSchema <-
            declareSchemaRef
                (Proxy @VerificationSnapshot)
        utxoEntrySchema <-
            declareSchemaRef (Proxy @UtxoEntry)
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "FactAbsentResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("snapshot", snapshotSchema)
                    , ("state_utxo", utxoEntrySchema)
                    , ("mpf_exclusion_proof", hexSchema)
                    ]
            & required
                .~ [ "snapshot"
                   , "state_utxo"
                   , "mpf_exclusion_proof"
                   ]
            & description
                ?~ "GET /tokens/:id/facts/:key — absent \
                   \(HTTP 404 with body). Carries an \
                   \MPF exclusion proof against the \
                   \trie root recovered from \
                   \state_utxo's datum."

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

instance ToSchema TrieFactJSON where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        maybeHex <-
            declareSchemaRef (Proxy @(Maybe Hex))
        pure
            $ Swagger.NamedSchema
                (Just "TrieFactJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("key", hexSchema)
                    , ("value", maybeHex)
                    , ("mpf_proof", hexSchema)
                    ]
            & required
                .~ ["key", "value", "mpf_proof"]
            & description
                ?~ "Trie read: key, optional value, and \
                   \MPF proof against a trie root"

instance ToSchema BootProofJSON where
    declareNamedSchema _ = do
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        pure
            $ Swagger.NamedSchema
                (Just "BootProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [("funding", utxoListSchema)]
            & required .~ ["funding"]
            & description
                ?~ "Proof payload for POST /tx/boot"

instance ToSchema RequestProofJSON where
    declareNamedSchema _ = do
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        pure
            $ Swagger.NamedSchema
                (Just "RequestProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [("funding", utxoListSchema)]
            & required .~ ["funding"]
            & description
                ?~ "Proof payload for POST \
                   \/tx/request/{insert,delete,update}"

instance ToSchema RetractProofJSON where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        pure
            $ Swagger.NamedSchema
                (Just "RetractProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("request_in", utxoSchema)
                    , ("state_ref", utxoSchema)
                    , ("funding", utxoListSchema)
                    ]
            & required
                .~ [ "request_in"
                   , "state_ref"
                   , "funding"
                   ]
            & description
                ?~ "Proof payload for POST /tx/retract"

instance ToSchema RejectProofJSON where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        pure
            $ Swagger.NamedSchema
                (Just "RejectProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("state", utxoSchema)
                    , ("request_ins", utxoListSchema)
                    , ("funding", utxoListSchema)
                    ]
            & required
                .~ [ "state"
                   , "request_ins"
                   , "funding"
                   ]
            & description
                ?~ "Proof payload for POST /tx/reject"

instance ToSchema EndProofJSON where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        pure
            $ Swagger.NamedSchema
                (Just "EndProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("state", utxoSchema)
                    , ("funding", utxoListSchema)
                    ]
            & required
                .~ ["state", "funding"]
            & description
                ?~ "Proof payload for POST /tx/end"

instance ToSchema UpdateProofJSON where
    declareNamedSchema _ = do
        utxoSchema <-
            declareSchemaRef (Proxy @WitnessedUtxo)
        utxoListSchema <-
            declareSchemaRef
                (Proxy @[WitnessedUtxo])
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        factListSchema <-
            declareSchemaRef
                (Proxy @[TrieFactJSON])
        pure
            $ Swagger.NamedSchema
                (Just "UpdateProofJSON")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList
                    [ ("state", utxoSchema)
                    , ("requests", utxoListSchema)
                    , ("funding", utxoListSchema)
                    , ("trie_root", hexSchema)
                    , ("trie_read", factListSchema)
                    ]
            & required
                .~ [ "state"
                   , "requests"
                   , "funding"
                   , "trie_root"
                   , "trie_read"
                   ]
            & description
                ?~ "Proof payload for POST /tx/update"

instance ToSchema BootTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "BootTxResponse"
            (Proxy @BootProofJSON)
            "Proof-bearing response for POST /tx/boot"

instance ToSchema RequestTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "RequestTxResponse"
            (Proxy @RequestProofJSON)
            "Proof-bearing response for POST \
            \/tx/request/{insert,delete,update}"

instance ToSchema RetractTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "RetractTxResponse"
            (Proxy @RetractProofJSON)
            "Proof-bearing response for POST \
            \/tx/retract"

instance ToSchema RejectTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "RejectTxResponse"
            (Proxy @RejectProofJSON)
            "Proof-bearing response for POST /tx/reject"

instance ToSchema EndTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "EndTxResponse"
            (Proxy @EndProofJSON)
            "Proof-bearing response for POST /tx/end"

instance ToSchema SweepTxResponse where
    declareNamedSchema _ = do
        hexSchema <-
            declareSchemaRef (Proxy @Hex)
        pure
            $ Swagger.NamedSchema
                (Just "SweepTxResponse")
            $ mempty
            & Swagger.type_
                ?~ Swagger.SwaggerObject
            & properties
                .~ fromList [("tx", hexSchema)]
            & required
                .~ ["tx"]
            & description
                ?~ "Unsigned sweep transaction. \
                   \Sweep does not bundle a proof \
                   \envelope (the on-chain validator \
                   \enforces the owner-signature \
                   \predicate against the referenced \
                   \state UTxO)."

instance ToSchema UpdateTxResponse where
    declareNamedSchema _ =
        txEnvelopeSchema
            "UpdateTxResponse"
            (Proxy @UpdateProofJSON)
            "Proof-bearing response for POST /tx/update"

-- | Shared schema body for @POST \/tx\/…@ response
-- envelopes: all carry a hex-encoded tx, a verification
-- snapshot, and a per-endpoint proof payload.
txEnvelopeSchema
    :: (ToSchema proofJson)
    => Text
    -> Proxy proofJson
    -> Text
    -> Declare
        (Swagger.Definitions Swagger.Schema)
        Swagger.NamedSchema
txEnvelopeSchema name proofProxy desc = do
    hexSchema <- declareSchemaRef (Proxy @Hex)
    snapSchema <-
        declareSchemaRef
            (Proxy @VerificationSnapshot)
    proofSchema <- declareSchemaRef proofProxy
    pure
        $ Swagger.NamedSchema (Just name)
        $ mempty
        & Swagger.type_ ?~ Swagger.SwaggerObject
        & properties
            .~ fromList
                [ ("tx", hexSchema)
                , ("snapshot", snapSchema)
                , ("proof", proofSchema)
                ]
        & required .~ ["tx", "snapshot", "proof"]
        & description ?~ desc
