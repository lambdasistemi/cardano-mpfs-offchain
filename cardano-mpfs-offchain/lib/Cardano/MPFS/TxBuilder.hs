-- |
-- Module      : Cardano.MPFS.TxBuilder
-- Description : Transaction construction interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for building MPFS
-- protocol transactions. Each method corresponds to a
-- cage-protocol action (boot, request insert\/delete,
-- update, retract, end). The real implementation lives
-- in "Cardano.MPFS.TxBuilder.Real"; the mock in
-- "Cardano.MPFS.Mock.TxBuilder".
--
-- Every method returns a 'ProofEnvelope' parameterized
-- by an endpoint-specific proof shape. The shape makes
-- the tx's dataflow explicit: each named
-- 'WitnessedInput' field plays a fixed role (state,
-- funding, pending request, …) and carries the
-- UTxO-CSMT inclusion proof that binds its 'TxIn' to
-- 'snapshotUtxoRoot'. Verification is type-directed:
-- walk the fields, check every proof against the
-- snapshot, and cross-reference with @txIns envTx@.
--
-- Returned transactions are unsigned — callers add key
-- witnesses before submission.
module Cardano.MPFS.TxBuilder
    ( -- * Transaction builder interface
      TxBuilder (..)

      -- * Snapshot and witness primitives
    , BundleSnapshot (..)
    , WitnessedInput (..)
    , TrieFact (..)
    , UtxoProofFn

      -- * Per-endpoint proof envelopes
    , ProofEnvelope (..)
    , BootProof (..)
    , RequestProof (..)
    , RetractProof (..)
    , RejectProof (..)
    , EndProof (..)
    , UpdateProof (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Api.Tx (Tx)

import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId
    , ConwayEra
    , SlotNo
    , TokenId
    , TxIn
    )

-- | Interface for constructing proof-bearing
-- transactions for all MPFS protocol operations.
--
-- Every method takes the 'BundleSnapshot' the caller
-- has already resolved, so the returned envelope bakes
-- in a verified UTxO root and indexed chain point. The
-- result type is endpoint-specific: each proof shape
-- names the roles its inputs play, and the set of
-- 'witnessedRef's covers every @txIn@ of 'envTx'.
data TxBuilder m = TxBuilder
    { bootToken
        :: BundleSnapshot
        -> Addr
        -> m (ProofEnvelope BootProof)
    -- ^ Create a new MPFS token
    , requestInsert
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m (ProofEnvelope RequestProof)
    -- ^ Request inserting a key-value pair
    , requestDelete
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m (ProofEnvelope RequestProof)
    -- ^ Request deleting a key (key, old value)
    , requestUpdate
        :: BundleSnapshot
        -> TokenId
        -> ByteString
        -> ByteString
        -> ByteString
        -> Addr
        -> m (ProofEnvelope RequestProof)
    -- ^ Request updating a key (key, old val, new val)
    , updateToken
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m (ProofEnvelope UpdateProof)
    -- ^ Process pending requests for a token
    , retractRequest
        :: BundleSnapshot
        -> TxIn
        -> Addr
        -> m (ProofEnvelope RetractProof)
    -- ^ Cancel a pending request
    , rejectRequests
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m (ProofEnvelope RejectProof)
    -- ^ Reject Phase 3 requests for a token
    , endToken
        :: BundleSnapshot
        -> TokenId
        -> Addr
        -> m (ProofEnvelope EndProof)
    -- ^ Retire an MPFS token
    }

-- | Indexed snapshot every proof in an envelope
-- verifies against. Matches the shape of the
-- HTTP-level @VerificationSnapshot@ but carries raw
-- bytes rather than hex-wrapped JSON values.
data BundleSnapshot = BundleSnapshot
    { snapshotUtxoRoot :: ByteString
    -- ^ UTxO-CSMT Merkle root (raw bytes)
    , snapshotSlot :: SlotNo
    -- ^ Indexed chain point slot
    , snapshotBlockId :: BlockId
    -- ^ Indexed chain point block id
    }
    deriving (Eq, Show)

-- | A consumed input with its UTxO-CSMT inclusion
-- proof against 'snapshotUtxoRoot'.
--
-- The leaf is @hash (witnessedRef, witnessedTxOut)@,
-- so re-deriving 'snapshotUtxoRoot' along
-- 'witnessedCsmtProof' both proves the UTxO was in
-- the indexed set and binds that specific 'TxIn' to
-- the snapshot.
data WitnessedInput = WitnessedInput
    { witnessedRef :: TxIn
    -- ^ The spent input's reference
    , witnessedTxOut :: ByteString
    -- ^ Serialized ledger CBOR of the consumed 'TxOut'
    , witnessedCsmtProof :: ByteString
    -- ^ CSMT inclusion proof against the root
    }
    deriving (Eq, Show)

-- | Effectful CSMT proof lookup used by the real
-- builders to bind every witnessed input to the
-- current UTxO-CSMT root. 'Nothing' means the indexer
-- does not (yet) know about the input — the verifier
-- will reject the resulting envelope.
type UtxoProofFn = TxIn -> IO (Maybe ByteString)

-- | A trie fact the builder depended on. Produced by
-- endpoints that read from the MPF trie behind a
-- token's state (currently only batched
-- @POST \/tx\/update@). Each fact is an MPF proof
-- against the trie root encoded in the consumed state
-- datum, not against 'snapshotUtxoRoot'.
data TrieFact = TrieFact
    { factKey :: ByteString
    -- ^ Trie key the builder looked up
    , factValue :: Maybe ByteString
    -- ^ Value bound to 'factKey', or 'Nothing' for
    -- membership-of-absence facts
    , factMpfProof :: ByteString
    -- ^ MPF inclusion\/exclusion proof against the
    -- relevant trie root
    }
    deriving (Eq, Show)

-- | Envelope wrapping a tx and its snapshot around an
-- endpoint-specific proof payload.
--
-- The type parameter @p@ selects the proof shape, and
-- with it the verifier's fixed-shape traversal.
data ProofEnvelope p = ProofEnvelope
    { envTx :: Tx ConwayEra
    -- ^ Unsigned transaction ready for key witnesses
    , envSnapshot :: BundleSnapshot
    -- ^ UTxO-CSMT root and chain point all proofs in
    -- 'envProof' verify against
    , envProof :: p
    -- ^ Endpoint-specific proof payload
    }

-- | Proof payload for @POST \/tx\/boot@. Boot mints a
-- fresh state UTxO, so there is no state input — only
-- wallet inputs that fund the minimum ada and fees.
newtype BootProof = BootProof
    { bootFunding :: [WitnessedInput]
    -- ^ Wallet inputs funding the boot tx
    }
    deriving (Eq, Show)

-- | Proof payload for
-- @POST \/tx\/request\/{insert,delete,update}@. The
-- request tx does not consume or reference the state
-- UTxO: it only spends wallet inputs and creates a
-- fresh pending-request output. Tip consistency with
-- the token's state is verified off-band against
-- @GET \/tokens\/:id@.
newtype RequestProof = RequestProof
    { requestFunding :: [WitnessedInput]
    -- ^ Wallet inputs funding the request tx
    }
    deriving (Eq, Show)

-- | Proof payload for @POST \/tx\/retract@. Consumes
-- the caller's pending-request UTxO and references
-- (read-only) the token's state UTxO for its
-- @process_time@\/@retract_time@ window.
data RetractProof = RetractProof
    { retractRequestIn :: WitnessedInput
    -- ^ The pending-request UTxO being retracted
    , retractStateRef :: WitnessedInput
    -- ^ The state UTxO referenced for its timing
    -- parameters
    , retractFunding :: [WitnessedInput]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

-- | Proof payload for @POST \/tx\/reject@. Consumes
-- the state UTxO plus every pending-request UTxO
-- currently rejected for that token.
data RejectProof = RejectProof
    { rejectState :: WitnessedInput
    -- ^ The consumed state UTxO
    , rejectRequestIns :: [WitnessedInput]
    -- ^ The pending-request UTxOs being rejected
    , rejectFunding :: [WitnessedInput]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

-- | Proof payload for @POST \/tx\/end@. Burns the
-- token and settles the state UTxO's value to the
-- caller.
data EndProof = EndProof
    { endState :: WitnessedInput
    -- ^ The consumed state UTxO
    , endFunding :: [WitnessedInput]
    -- ^ Wallet inputs covering fees
    }
    deriving (Eq, Show)

-- | Proof payload for @POST \/tx\/update@. Consumes
-- the state UTxO, batches in pending-request UTxOs,
-- and carries MPF proofs for every trie key touched
-- during batch application.
--
-- The 'updateTrieRoot' must match the trie root
-- encoded in 'updateState'\''s datum; each
-- 'updateTrieRead' then verifies against it.
data UpdateProof = UpdateProof
    { updateState :: WitnessedInput
    -- ^ The consumed state UTxO
    , updateRequests :: [WitnessedInput]
    -- ^ Pending-request UTxOs batched into this update
    , updateFunding :: [WitnessedInput]
    -- ^ Wallet inputs covering fees
    , updateTrieRoot :: ByteString
    -- ^ Trie root from 'updateState'\''s datum
    , updateTrieRead :: [TrieFact]
    -- ^ MPF proofs for each key touched during batch
    -- application, rooted at 'updateTrieRoot'
    }
    deriving (Eq, Show)
