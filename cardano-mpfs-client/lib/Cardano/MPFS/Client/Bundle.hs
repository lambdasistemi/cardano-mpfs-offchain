-- |
-- Module      : Cardano.MPFS.Client.Bundle
-- Description : Per-endpoint proof-bearing response contracts.
--
-- Haskell mirrors of the per-endpoint response envelopes that
-- cardano-mpfs-offchain's @POST \/tx\/…@ routes return. Each
-- response carries the unsigned transaction CBOR (hex), the
-- 'VerificationSnapshot' the bundled proofs target, and an
-- endpoint-specific proof payload whose named fields enumerate
-- the roles its inputs play (state, funding, pending request, …).
--
-- The module is pure-Haskell and imports only @aeson@,
-- @bytestring@, and @text@. No @cardano-ledger-*@ dependency and
-- no C FFI: the client verifier must build on GHC-native,
-- GHC-WASM, and GHC-JS.
module Cardano.MPFS.Client.Bundle
    ( -- * Witnesses
      TxIn (..)
    , WitnessedUtxo (..)
    , TrieFact (..)

      -- * Per-endpoint proof payloads
    , BootProof (..)
    , RequestProof (..)
    , RetractProof (..)
    , RejectProof (..)
    , EndProof (..)
    , UpdateProof (..)

      -- * Response envelopes
    , BootTxResponse (..)
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , UpdateTxResponse (..)
    ) where

import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , object
    , withObject
    , (.:)
    , (.=)
    )
import Data.Word (Word64)

import Cardano.MPFS.Client.Snapshot
    ( Hex (..)
    , VerificationSnapshot (..)
    )

-- | UTxO reference: a transaction id (hex) and the output
-- index within that transaction.
data TxIn = TxIn
    { txId :: Hex
    , txIx :: Word64
    }
    deriving stock (Eq, Ord, Show)

instance FromJSON TxIn where
    parseJSON = withObject "TxIn" $ \o ->
        TxIn <$> o .: "tx_id" <*> o .: "tx_ix"

instance ToJSON TxIn where
    toJSON TxIn{..} =
        object ["tx_id" .= txId, "tx_ix" .= txIx]

-- | A witnessed UTxO: the reference, the CBOR-encoded
-- @TxOut@ body (hex), and the UTxO-CSMT inclusion proof
-- binding the pair into the snapshot's @utxo_root@.
--
-- A 'WitnessedUtxo' covers both consumed inputs and
-- reference inputs: verifiers treat the two cases
-- identically.
data WitnessedUtxo = WitnessedUtxo
    { txIn :: TxIn
    , txOut :: Hex
    , utxoProof :: Hex
    }
    deriving stock (Eq, Show)

instance FromJSON WitnessedUtxo where
    parseJSON = withObject "WitnessedUtxo" $ \o ->
        WitnessedUtxo
            <$> o .: "tx_in"
            <*> o .: "tx_out"
            <*> o .: "utxo_proof"

instance ToJSON WitnessedUtxo where
    toJSON WitnessedUtxo{..} =
        object
            [ "tx_in" .= txIn
            , "tx_out" .= txOut
            , "utxo_proof" .= utxoProof
            ]

-- | A single trie read: a key, its optional value, and
-- an MPF inclusion\/absence proof against a trie root.
data TrieFact = TrieFact
    { key :: Hex
    , value :: Maybe Hex
    , mpfProof :: Hex
    }
    deriving stock (Eq, Show)

instance FromJSON TrieFact where
    parseJSON = withObject "TrieFact" $ \o ->
        TrieFact
            <$> o .: "key"
            <*> o .: "value"
            <*> o .: "mpf_proof"

instance ToJSON TrieFact where
    toJSON TrieFact{..} =
        object
            [ "key" .= key
            , "value" .= value
            , "mpf_proof" .= mpfProof
            ]

-- | Proof payload for @POST \/tx\/boot@.
newtype BootProof = BootProof
    { funding :: [WitnessedUtxo]
    }
    deriving stock (Eq, Show)

instance FromJSON BootProof where
    parseJSON = withObject "BootProof" $ \o ->
        BootProof <$> o .: "funding"

instance ToJSON BootProof where
    toJSON BootProof{funding = f} =
        object ["funding" .= f]

-- | Proof payload for
-- @POST \/tx\/request\/{insert,delete,update}@.
newtype RequestProof = RequestProof
    { funding :: [WitnessedUtxo]
    }
    deriving stock (Eq, Show)

instance FromJSON RequestProof where
    parseJSON = withObject "RequestProof" $ \o ->
        RequestProof <$> o .: "funding"

instance ToJSON RequestProof where
    toJSON RequestProof{funding = f} =
        object ["funding" .= f]

-- | Proof payload for @POST \/tx\/retract@.
data RetractProof = RetractProof
    { requestIn :: WitnessedUtxo
    , stateRef :: WitnessedUtxo
    , funding :: [WitnessedUtxo]
    }
    deriving stock (Eq, Show)

instance FromJSON RetractProof where
    parseJSON = withObject "RetractProof" $ \o ->
        RetractProof
            <$> o .: "request_in"
            <*> o .: "state_ref"
            <*> o .: "funding"

instance ToJSON RetractProof where
    toJSON (RetractProof ri sr fs) =
        object
            [ "request_in" .= ri
            , "state_ref" .= sr
            , "funding" .= fs
            ]

-- | Proof payload for @POST \/tx\/reject@.
data RejectProof = RejectProof
    { state :: WitnessedUtxo
    , requestIns :: [WitnessedUtxo]
    , funding :: [WitnessedUtxo]
    }
    deriving stock (Eq, Show)

instance FromJSON RejectProof where
    parseJSON = withObject "RejectProof" $ \o ->
        RejectProof
            <$> o .: "state"
            <*> o .: "request_ins"
            <*> o .: "funding"

instance ToJSON RejectProof where
    toJSON (RejectProof st ris fs) =
        object
            [ "state" .= st
            , "request_ins" .= ris
            , "funding" .= fs
            ]

-- | Proof payload for @POST \/tx\/end@.
data EndProof = EndProof
    { state :: WitnessedUtxo
    , funding :: [WitnessedUtxo]
    }
    deriving stock (Eq, Show)

instance FromJSON EndProof where
    parseJSON = withObject "EndProof" $ \o ->
        EndProof <$> o .: "state" <*> o .: "funding"

instance ToJSON EndProof where
    toJSON (EndProof st fs) =
        object ["state" .= st, "funding" .= fs]

-- | Proof payload for @POST \/tx\/update@.
data UpdateProof = UpdateProof
    { state :: WitnessedUtxo
    , requests :: [WitnessedUtxo]
    , funding :: [WitnessedUtxo]
    , trieRoot :: Hex
    , trieRead :: [TrieFact]
    }
    deriving stock (Eq, Show)

instance FromJSON UpdateProof where
    parseJSON = withObject "UpdateProof" $ \o ->
        UpdateProof
            <$> o .: "state"
            <*> o .: "requests"
            <*> o .: "funding"
            <*> o .: "trie_root"
            <*> o .: "trie_read"

instance ToJSON UpdateProof where
    toJSON (UpdateProof st rs fs tr tread) =
        object
            [ "state" .= st
            , "requests" .= rs
            , "funding" .= fs
            , "trie_root" .= tr
            , "trie_read" .= tread
            ]

-- | Response envelope for @POST \/tx\/boot@.
data BootTxResponse = BootTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: BootProof
    }
    deriving stock (Eq, Show)

instance FromJSON BootTxResponse where
    parseJSON = withObject "BootTxResponse" $ \o ->
        BootTxResponse
            <$> o .: "tx"
            <*> o .: "snapshot"
            <*> o .: "proof"

instance ToJSON BootTxResponse where
    toJSON (BootTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]

-- | Response envelope for
-- @POST \/tx\/request\/{insert,delete,update}@.
data RequestTxResponse = RequestTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: RequestProof
    }
    deriving stock (Eq, Show)

instance FromJSON RequestTxResponse where
    parseJSON =
        withObject "RequestTxResponse" $ \o ->
            RequestTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

instance ToJSON RequestTxResponse where
    toJSON (RequestTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]

-- | Response envelope for @POST \/tx\/retract@.
data RetractTxResponse = RetractTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: RetractProof
    }
    deriving stock (Eq, Show)

instance FromJSON RetractTxResponse where
    parseJSON =
        withObject "RetractTxResponse" $ \o ->
            RetractTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

instance ToJSON RetractTxResponse where
    toJSON (RetractTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]

-- | Response envelope for @POST \/tx\/reject@.
data RejectTxResponse = RejectTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: RejectProof
    }
    deriving stock (Eq, Show)

instance FromJSON RejectTxResponse where
    parseJSON =
        withObject "RejectTxResponse" $ \o ->
            RejectTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

instance ToJSON RejectTxResponse where
    toJSON (RejectTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]

-- | Response envelope for @POST \/tx\/end@.
data EndTxResponse = EndTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: EndProof
    }
    deriving stock (Eq, Show)

instance FromJSON EndTxResponse where
    parseJSON = withObject "EndTxResponse" $ \o ->
        EndTxResponse
            <$> o .: "tx"
            <*> o .: "snapshot"
            <*> o .: "proof"

instance ToJSON EndTxResponse where
    toJSON (EndTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]

-- | Response envelope for @POST \/tx\/update@.
data UpdateTxResponse = UpdateTxResponse
    { tx :: Hex
    , snapshot :: VerificationSnapshot
    , proof :: UpdateProof
    }
    deriving stock (Eq, Show)

instance FromJSON UpdateTxResponse where
    parseJSON =
        withObject "UpdateTxResponse" $ \o ->
            UpdateTxResponse
                <$> o .: "tx"
                <*> o .: "snapshot"
                <*> o .: "proof"

instance ToJSON UpdateTxResponse where
    toJSON (UpdateTxResponse t s p) =
        object ["tx" .= t, "snapshot" .= s, "proof" .= p]
