-- |
-- Module      : Cardano.MPFS.Client.Read
-- Description : Per-endpoint proof-bearing read response contracts.
--
-- Haskell mirrors of the read responses
-- @cardano-mpfs-offchain@ exposes on
-- @GET \/tokens\/:id@,
-- @GET \/tokens\/:id\/facts\/:key@,
-- @GET \/tokens\/:id\/proofs\/:key@, and
-- @GET \/tokens\/:id\/requests@. Each response carries the
-- 'VerificationSnapshot' the bundled proofs target plus
-- per-endpoint witness payloads. The mirrors stay pure-Haskell
-- and import only @aeson@, @bytestring@, and @text@: no
-- @cardano-ledger-*@ dependency and no C FFI, so the verifier
-- continues to build on GHC-native, GHC-WASM, and GHC-JS.
module Cardano.MPFS.Client.Read
    ( -- * Decoded payload mirrors
      TokenState (..)
    , Request (..)

      -- * Witnesses
    , WitnessedTokenState (..)
    , WitnessedRequest (..)
    , FactWitness (..)

      -- * Response envelopes
    , TokenResponse (..)
    , FactResponse (..)
    , ProofResponse (..)
    , RequestsResponse (..)
    ) where

import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withObject
    , (.:)
    , (.=)
    )
import Data.Text (Text)

import Cardano.MPFS.Client.Bundle
    ( WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( Hex (..)
    , VerificationSnapshot (..)
    )

-- | Decoded on-chain token state. Mirrors the field set
-- @cardano-mpfs-offchain@ already emits over the wire. The
-- @root@ field is the trie root the response carries, so
-- 'verifyFactResponse' and 'verifyProofResponse' replay their
-- MPF proof against it without re-deriving the root from the
-- inline datum bytes.
data TokenState = TokenState
    { owner :: Text
    -- ^ Owner payment-key hash (hex).
    , root :: Hex
    -- ^ Current MPF trie root.
    , tip :: Integer
    -- ^ Maximum-fee tip, in lovelace.
    , processTime :: Integer
    -- ^ Process window, in milliseconds.
    , retractTime :: Integer
    -- ^ Retract window, in milliseconds.
    }
    deriving stock (Eq, Show)

instance FromJSON TokenState where
    parseJSON = withObject "TokenState" $ \o ->
        TokenState
            <$> o .: "owner"
            <*> o .: "root"
            <*> o .: "tip"
            <*> o .: "process_time"
            <*> o .: "retract_time"

instance ToJSON TokenState where
    toJSON TokenState{..} =
        object
            [ "owner" .= owner
            , "root" .= root
            , "tip" .= tip
            , "process_time" .= processTime
            , "retract_time" .= retractTime
            ]

-- | Decoded pending request. Mirrors the wire format of
-- @Cardano.MPFS.API.Types.RequestJSON@. The verifier treats
-- the payload as opaque; downstream consumers (MOOG) read it
-- to drive their own application logic.
data Request = Request
    { token :: Hex
    -- ^ Token id this request targets.
    , owner :: Text
    -- ^ Requester payment-key hash (hex).
    , key :: Hex
    -- ^ Trie key the request acts on.
    , operation :: Text
    -- ^ @"insert"@, @"delete"@, or @"update"@.
    , value :: Maybe Hex
    -- ^ New value (for insert / update).
    , fee :: Integer
    -- ^ Offered fee, in lovelace.
    , submittedAt :: Integer
    -- ^ POSIX timestamp (ms) the request was submitted.
    }
    deriving stock (Eq, Show)

instance FromJSON Request where
    parseJSON = withObject "Request" $ \o ->
        Request
            <$> o .: "token"
            <*> o .: "owner"
            <*> o .: "key"
            <*> o .: "operation"
            <*> o .: "value"
            <*> o .: "fee"
            <*> o .: "submitted_at"

instance ToJSON Request where
    toJSON Request{..} =
        object
            [ "token" .= token
            , "owner" .= owner
            , "key" .= key
            , "operation" .= operation
            , "value" .= value
            , "fee" .= fee
            , "submitted_at" .= submittedAt
            ]

-- | UTxO witness for the state output paired with the decoded
-- token state. The verifier replays the witness against the
-- snapshot's @utxo_root@; the decoded state carries the trie
-- root that MPF proofs replay against.
data WitnessedTokenState = WitnessedTokenState
    { utxo :: WitnessedUtxo
    -- ^ UTxO witness for the state output.
    , state :: TokenState
    -- ^ Decoded on-chain state.
    }
    deriving stock (Eq, Show)

instance FromJSON WitnessedTokenState where
    parseJSON =
        withObject "WitnessedTokenState" $ \o ->
            WitnessedTokenState
                <$> o .: "utxo"
                <*> o .: "state"

instance ToJSON WitnessedTokenState where
    toJSON WitnessedTokenState{..} =
        object
            [ "utxo" .= utxo
            , "state" .= state
            ]

-- | UTxO witness for a pending request output paired with the
-- decoded request payload.
data WitnessedRequest = WitnessedRequest
    { utxo :: WitnessedUtxo
    -- ^ UTxO witness for the request output.
    , request :: Request
    -- ^ Decoded request payload.
    }
    deriving stock (Eq, Show)

instance FromJSON WitnessedRequest where
    parseJSON =
        withObject "WitnessedRequest" $ \o ->
            WitnessedRequest
                <$> o .: "utxo"
                <*> o .: "request"

instance ToJSON WitnessedRequest where
    toJSON WitnessedRequest{..} =
        object
            [ "utxo" .= utxo
            , "request" .= request
            ]

-- | A fact witness: the state witness carrying the trie root
-- plus an MPF inclusion or absence proof binding a key (and
-- optional value) to that root.
data FactWitness = FactWitness
    { state :: WitnessedTokenState
    -- ^ State witness carrying the trie root.
    , mpfProof :: Hex
    -- ^ MPF inclusion or absence proof (hex).
    }
    deriving stock (Eq, Show)

instance FromJSON FactWitness where
    parseJSON = withObject "FactWitness" $ \o ->
        FactWitness
            <$> o .: "state"
            <*> o .: "mpf_proof"

instance ToJSON FactWitness where
    toJSON FactWitness{..} =
        object
            [ "state" .= state
            , "mpf_proof" .= mpfProof
            ]

-- | Response envelope for @GET \/tokens\/:id@.
data TokenResponse = TokenResponse
    { snapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proof targets.
    , state :: WitnessedTokenState
    -- ^ State witness.
    }
    deriving stock (Eq, Show)

instance FromJSON TokenResponse where
    parseJSON = withObject "TokenResponse" $ \o ->
        TokenResponse
            <$> o .: "snapshot"
            <*> o .: "state"

instance ToJSON TokenResponse where
    toJSON TokenResponse{..} =
        object
            [ "snapshot" .= snapshot
            , "state" .= state
            ]

-- | Response envelope for
-- @GET \/tokens\/:id\/facts\/:key@.
data FactResponse = FactResponse
    { snapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , value :: Hex
    -- ^ The fact value (hex).
    , fact :: FactWitness
    -- ^ State witness + MPF inclusion proof.
    }
    deriving stock (Eq, Show)

instance FromJSON FactResponse where
    parseJSON = withObject "FactResponse" $ \o ->
        FactResponse
            <$> o .: "snapshot"
            <*> o .: "value"
            <*> o .: "fact"

instance ToJSON FactResponse where
    toJSON FactResponse{..} =
        object
            [ "snapshot" .= snapshot
            , "value" .= value
            , "fact" .= fact
            ]

-- | Response envelope for
-- @GET \/tokens\/:id\/proofs\/:key@.
data ProofResponse = ProofResponse
    { snapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , fact :: FactWitness
    -- ^ State witness + MPF proof.
    }
    deriving stock (Eq, Show)

instance FromJSON ProofResponse where
    parseJSON = withObject "ProofResponse" $ \o ->
        ProofResponse
            <$> o .: "snapshot"
            <*> o .: "fact"

instance ToJSON ProofResponse where
    toJSON ProofResponse{..} =
        object
            [ "snapshot" .= snapshot
            , "fact" .= fact
            ]

-- | Response envelope for @GET \/tokens\/:id\/requests@.
data RequestsResponse = RequestsResponse
    { snapshot :: VerificationSnapshot
    -- ^ Snapshot the bundled proofs target.
    , requests :: [WitnessedRequest]
    -- ^ Witnessed pending requests.
    }
    deriving stock (Eq, Show)

instance FromJSON RequestsResponse where
    parseJSON = withObject "RequestsResponse" $ \o ->
        RequestsResponse
            <$> o .: "snapshot"
            <*> o .: "requests"

instance ToJSON RequestsResponse where
    toJSON RequestsResponse{..} =
        object
            [ "snapshot" .= snapshot
            , "requests" .= requests
            ]
