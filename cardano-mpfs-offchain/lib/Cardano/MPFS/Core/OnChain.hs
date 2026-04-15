{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Core.OnChain
-- Description : On-chain type encodings for the MPFS cage validator
-- License     : Apache-2.0
--
-- Haskell types matching the Aiken on-chain datum\/redeemer
-- structures, their PlutusData encoding, script identity,
-- and asset-name derivation.
--
-- These types use Plutus primitives directly (not
-- cardano-ledger types) because they model the exact
-- on-chain data layout expected by the Aiken validator.
-- The 'ToData'\/'FromData' instances are hand-written
-- (not TH-derived) to guarantee constructor indices and
-- field ordering match the Aiken source byte-for-byte.
--
-- The module also provides the hard-coded cage
-- 'cageScriptHash' and helpers to derive asset names
-- and addresses from it. See "Cardano.MPFS.Core.Types"
-- for the higher-level domain types that the rest of the
-- offchain service operates on.
module Cardano.MPFS.Core.OnChain
    ( -- * On-chain datum\/redeemer types
      CageDatum (..)
    , MintRedeemer (..)
    , Mint (..)
    , Migration (..)
    , UpdateRedeemer (..)
    , RequestAction (..)

      -- * On-chain domain types
    , OnChainTokenId (..)
    , OnChainOperation (..)
    , OnChainRoot (..)
    , OnChainRequest (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)

      -- * Proof steps (Aiken MPF proof encoding)
    , ProofStep (..)
    , Neighbor (..)

      -- * Script identity
    , cageScriptHash
    , cageScriptHashLedger
    , cagePolicyId
    , cageAddr

      -- * Asset-name derivation
    , deriveAssetName

      -- * Blueprint loading
    , Blueprint (..)
    , loadCageScript
    ) where

import Cardano.Crypto.Hash (hashFromBytes)
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.BaseTypes (Network)
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Mary.Value (PolicyID (..))
import Cardano.MPFS.Core.Blueprint
    ( Blueprint (..)
    , loadBlueprint
    )
import Crypto.Hash (Digest, SHA256, hash)
import Data.Bits (shiftR)
import Data.ByteArray (convert)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Maybe (fromJust)
import Data.Word (Word16)
import PlutusCore.Data (Data (..))
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    , UnsafeFromData (..)
    )

-- ---------------------------------------------------------
-- On-chain domain types (Plutus primitives)
-- ---------------------------------------------------------

-- | On-chain token identifier (asset name as raw
-- bytes). Matches Aiken @lib/TokenId@.
newtype OnChainTokenId = OnChainTokenId
    { unOnChainTokenId :: BuiltinByteString
    }
    deriving stock (Show, Eq)

-- | On-chain output reference. Matches Aiken
-- @cardano/transaction/OutputReference@.
data OnChainTxOutRef = OnChainTxOutRef
    { txOutRefId :: !BuiltinByteString
    -- ^ Transaction hash (32 bytes)
    , txOutRefIdx :: !Integer
    -- ^ Output index within the transaction
    }
    deriving stock (Show, Eq)

-- | On-chain MPF root hash (raw bytes).
newtype OnChainRoot = OnChainRoot
    { unOnChainRoot :: ByteString
    }
    deriving stock (Show, Eq)

-- | On-chain operation on a key in the trie.
-- Matches Aiken @types/Operation@.
data OnChainOperation
    = -- | Insert a new key-value pair (Constr 0)
      OpInsert
        !ByteString
        -- ^ Value to insert
    | -- | Delete a key (Constr 1)
      OpDelete
        !ByteString
        -- ^ Old value being removed (needed for proof)
    | -- | Update an existing key (Constr 2)
      OpUpdate
        !ByteString
        -- ^ Old value being replaced
        !ByteString
        -- ^ New value
    deriving stock (Show, Eq)

-- | On-chain request to modify a token's trie.
-- Matches Aiken @types/Request@.
data OnChainRequest = OnChainRequest
    { requestToken :: !OnChainTokenId
    -- ^ Token whose trie is being modified
    , requestOwner :: !BuiltinByteString
    -- ^ Payment key hash of the requester (28 bytes)
    , requestKey :: !ByteString
    -- ^ Trie key to operate on
    , requestValue :: !OnChainOperation
    -- ^ The insert\/delete\/update operation
    , requestFee :: !Integer
    -- ^ Fee (in lovelace) the requester agrees to pay
    , requestSubmittedAt :: !Integer
    -- ^ POSIX time (ms) when the request was submitted
    }
    deriving stock (Show, Eq)

-- | On-chain token state. Matches Aiken
-- @types/State@ (5 fields).
data OnChainTokenState = OnChainTokenState
    { stateOwner :: !BuiltinByteString
    -- ^ Payment key hash of the token owner (28 bytes)
    , stateRoot :: !OnChainRoot
    -- ^ Current Merkle root of the token's trie
    , stateTip :: !Integer
    -- ^ Maximum fee (lovelace) charged per request
    , stateProcessTime :: !Integer
    -- ^ Oracle processing window duration (ms)
    , stateRetractTime :: !Integer
    -- ^ Requester retract window duration (ms)
    }
    deriving stock (Show, Eq)

-- ---------------------------------------------------------
-- On-chain-only types (datum / redeemer wrappers)
-- ---------------------------------------------------------

-- | Cage datum: either a pending request or a token
-- state. Matches Aiken @types/CageDatum@.
data CageDatum
    = -- | A pending operation request (Constr 0)
      RequestDatum !OnChainRequest
    | -- | Current token state (Constr 1)
      StateDatum !OnChainTokenState
    deriving stock (Show, Eq)

-- | Minting redeemer. Matches Aiken
-- @types/MintRedeemer@.
data MintRedeemer
    = -- | Mint a new cage token (Constr 0)
      Minting !Mint
    | -- | Migrate from old validator (Constr 1)
      Migrating !Migration
    | -- | Burn a cage token (Constr 2)
      Burning
    deriving stock (Show, Eq)

-- | Minting parameters. Matches Aiken
-- @types/Mint@.
newtype Mint = Mint
    { mintAsset :: OnChainTxOutRef
    -- ^ UTxO consumed to derive the unique asset name
    }
    deriving stock (Show, Eq)

-- | Migration parameters. Matches Aiken
-- @types/Migration@.
data Migration = Migration
    { migrationOldPolicy :: !BuiltinByteString
    -- ^ Policy ID of the old cage validator being migrated from
    , migrationTokenId :: !OnChainTokenId
    -- ^ Token being migrated to the new policy
    }
    deriving stock (Show, Eq)

-- | Spending redeemer. Matches Aiken
-- @types/UpdateRedeemer@.
data UpdateRedeemer
    = -- | End the token (Constr 0)
      End
    | -- | Link a request to a state UTxO (Constr 1)
      Contribute !OnChainTxOutRef
    | -- | Fold requests with actions (Constr 2)
      Modify ![RequestAction]
    | -- | Reclaim a pending request (Constr 3)
      Retract !OnChainTxOutRef
    deriving stock (Show, Eq)

-- | Action per request in a Modify transaction.
-- Matches Aiken @types/RequestAction@.
data RequestAction
    = -- | Apply proofs to update the MPF (Constr 0)
      UpdateAction ![ProofStep]
    | -- | Reject an expired request (Constr 1)
      Rejected
    deriving stock (Show, Eq)

-- | A single step in an MPF Merkle proof, matching
-- the Aiken @ProofStep@ type from
-- @aiken-lang\/merkle-patricia-forestry@.
data ProofStep
    = -- | Branch step (Constr 0)
      Branch
        { branchSkip :: !Integer
        -- ^ Number of shared nibbles to skip
        , branchNeighbors :: !ByteString
        -- ^ Concatenated neighbor hashes (4 x 32 bytes)
        }
    | -- | Fork step (Constr 1)
      Fork
        { forkSkip :: !Integer
        -- ^ Number of shared nibbles to skip
        , forkNeighbor :: !Neighbor
        -- ^ The sibling branch at the fork point
        }
    | -- | Leaf step (Constr 2)
      Leaf
        { leafSkip :: !Integer
        -- ^ Number of shared nibbles to skip
        , leafKey :: !ByteString
        -- ^ Remaining key suffix at the leaf
        , leafValue :: !ByteString
        -- ^ Value hash stored at the leaf
        }
    deriving stock (Show, Eq)

-- | Neighbor node in a fork proof step.
data Neighbor = Neighbor
    { neighborNibble :: !Integer
    -- ^ Hex digit (0–15) identifying the fork branch
    , neighborPrefix :: !ByteString
    -- ^ Common prefix nibbles of the neighbor subtree
    , neighborRoot :: !ByteString
    -- ^ Merkle root hash of the neighbor subtree
    }
    deriving stock (Show, Eq)

-- ---------------------------------------------------------
-- Helpers for manual Data construction
-- ---------------------------------------------------------

-- | Wrap a raw 'Data' value as 'BuiltinData'.
mkD :: Data -> BuiltinData
mkD = BuiltinData

-- | Unwrap 'BuiltinData' to the raw 'Data' AST.
unD :: BuiltinData -> Data
unD (BuiltinData d) = d

-- | Lift a 'ByteString' into a 'Data' byte-literal.
bsToD :: ByteString -> Data
bsToD = B

-- | Extract a 'ByteString' from a 'Data' byte-literal.
bsFromD :: Data -> Maybe ByteString
bsFromD (B bs) = Just bs
bsFromD _ = Nothing

-- | Lift a 'BuiltinByteString' into a 'Data'
-- byte-literal.
bbsToD :: BuiltinByteString -> Data
bbsToD (BuiltinByteString bs) = B bs

-- | Extract a 'BuiltinByteString' from a 'Data'
-- byte-literal.
bbsFromD :: Data -> Maybe BuiltinByteString
bbsFromD (B bs) = Just (BuiltinByteString bs)
bbsFromD _ = Nothing

-- ---------------------------------------------------------
-- ToData / FromData instances
-- ---------------------------------------------------------

instance ToData OnChainTokenId where
    toBuiltinData (OnChainTokenId bbs) =
        mkD $ Constr 0 [bbsToD bbs]

instance FromData OnChainTokenId where
    fromBuiltinData bd = case unD bd of
        Constr 0 [x] ->
            OnChainTokenId <$> bbsFromD x
        _ -> Nothing

instance UnsafeFromData OnChainTokenId where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [x] -> case bbsFromD x of
            Just bbs -> OnChainTokenId bbs
            _ ->
                error
                    "unsafeFromBuiltinData: OnChainTokenId"
        _ ->
            error
                "unsafeFromBuiltinData: OnChainTokenId"

instance ToData OnChainTxOutRef where
    toBuiltinData OnChainTxOutRef{..} =
        mkD
            $ Constr
                0
                [bbsToD txOutRefId, I txOutRefIdx]

instance FromData OnChainTxOutRef where
    fromBuiltinData bd = case unD bd of
        Constr 0 [tid, I idx] ->
            OnChainTxOutRef
                <$> bbsFromD tid
                <*> pure idx
        _ -> Nothing

instance UnsafeFromData OnChainTxOutRef where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [B tid, I idx] ->
            OnChainTxOutRef
                (BuiltinByteString tid)
                idx
        _ ->
            error
                "unsafeFromBuiltinData: OnChainTxOutRef"

instance ToData OnChainOperation where
    toBuiltinData (OpInsert v) =
        mkD $ Constr 0 [bsToD v]
    toBuiltinData (OpDelete v) =
        mkD $ Constr 1 [bsToD v]
    toBuiltinData (OpUpdate old new) =
        mkD $ Constr 2 [bsToD old, bsToD new]

instance FromData OnChainOperation where
    fromBuiltinData bd = case unD bd of
        Constr 0 [v] -> OpInsert <$> bsFromD v
        Constr 1 [v] -> OpDelete <$> bsFromD v
        Constr 2 [o, n] ->
            OpUpdate <$> bsFromD o <*> bsFromD n
        _ -> Nothing

instance UnsafeFromData OnChainOperation where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [B v] -> OpInsert v
        Constr 1 [B v] -> OpDelete v
        Constr 2 [B o, B n] -> OpUpdate o n
        _ ->
            error
                "unsafeFromBuiltinData: OnChainOperation"

instance ToData OnChainRoot where
    toBuiltinData (OnChainRoot bs) = mkD $ bsToD bs

instance FromData OnChainRoot where
    fromBuiltinData bd =
        OnChainRoot <$> bsFromD (unD bd)

instance UnsafeFromData OnChainRoot where
    unsafeFromBuiltinData bd = case unD bd of
        B bs -> OnChainRoot bs
        _ ->
            error
                "unsafeFromBuiltinData: OnChainRoot"

instance ToData OnChainRequest where
    toBuiltinData OnChainRequest{..} =
        mkD
            $ Constr
                0
                [ unD (toBuiltinData requestToken)
                , bbsToD requestOwner
                , bsToD requestKey
                , unD (toBuiltinData requestValue)
                , I requestFee
                , I requestSubmittedAt
                ]

instance FromData OnChainRequest where
    fromBuiltinData bd = case unD bd of
        Constr
            0
            [tok, own, k, val, I fee, I sub] -> do
                requestToken <-
                    fromBuiltinData (mkD tok)
                requestOwner <- bbsFromD own
                requestKey <- bsFromD k
                requestValue <-
                    fromBuiltinData (mkD val)
                let requestFee = fee
                    requestSubmittedAt = sub
                Just OnChainRequest{..}
        _ -> Nothing

instance UnsafeFromData OnChainRequest where
    unsafeFromBuiltinData bd = case unD bd of
        Constr
            0
            [tok, B own, B k, val, I fee, I sub] ->
                OnChainRequest
                    { requestToken =
                        unsafeFromBuiltinData (mkD tok)
                    , requestOwner =
                        BuiltinByteString own
                    , requestKey = k
                    , requestValue =
                        unsafeFromBuiltinData (mkD val)
                    , requestFee = fee
                    , requestSubmittedAt = sub
                    }
        _ ->
            error
                "unsafeFromBuiltinData:\
                \ OnChainRequest"

instance ToData OnChainTokenState where
    toBuiltinData OnChainTokenState{..} =
        mkD
            $ Constr
                0
                [ bbsToD stateOwner
                , unD (toBuiltinData stateRoot)
                , I stateTip
                , I stateProcessTime
                , I stateRetractTime
                ]

instance FromData OnChainTokenState where
    fromBuiltinData bd = case unD bd of
        Constr 0 [own, r, I mf, I pt, I rt] -> do
            stateOwner <- bbsFromD own
            stateRoot <- fromBuiltinData (mkD r)
            let stateTip = mf
                stateProcessTime = pt
                stateRetractTime = rt
            Just OnChainTokenState{..}
        _ -> Nothing

instance UnsafeFromData OnChainTokenState where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [B own, r, I mf, I pt, I rt] ->
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString own
                , stateRoot =
                    unsafeFromBuiltinData (mkD r)
                , stateTip = mf
                , stateProcessTime = pt
                , stateRetractTime = rt
                }
        _ ->
            error
                "unsafeFromBuiltinData:\
                \ OnChainTokenState"

instance ToData CageDatum where
    toBuiltinData (RequestDatum r) =
        mkD
            $ Constr 0 [unD (toBuiltinData r)]
    toBuiltinData (StateDatum s) =
        mkD
            $ Constr 1 [unD (toBuiltinData s)]

instance FromData CageDatum where
    fromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            RequestDatum
                <$> fromBuiltinData (mkD d)
        Constr 1 [d] ->
            StateDatum
                <$> fromBuiltinData (mkD d)
        _ -> Nothing

instance UnsafeFromData CageDatum where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            RequestDatum
                $ unsafeFromBuiltinData (mkD d)
        Constr 1 [d] ->
            StateDatum
                $ unsafeFromBuiltinData (mkD d)
        _ -> error "unsafeFromBuiltinData: CageDatum"

instance ToData Mint where
    toBuiltinData (Mint ref) =
        mkD $ Constr 0 [unD (toBuiltinData ref)]

instance FromData Mint where
    fromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            Mint <$> fromBuiltinData (mkD d)
        _ -> Nothing

instance UnsafeFromData Mint where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            Mint $ unsafeFromBuiltinData (mkD d)
        _ -> error "unsafeFromBuiltinData: Mint"

instance ToData Migration where
    toBuiltinData Migration{..} =
        mkD
            $ Constr
                0
                [ bbsToD migrationOldPolicy
                , unD
                    (toBuiltinData migrationTokenId)
                ]

instance FromData Migration where
    fromBuiltinData bd = case unD bd of
        Constr 0 [pol, tid] -> do
            migrationOldPolicy <- bbsFromD pol
            migrationTokenId <-
                fromBuiltinData (mkD tid)
            Just Migration{..}
        _ -> Nothing

instance UnsafeFromData Migration where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [B pol, tid] ->
            Migration
                { migrationOldPolicy =
                    BuiltinByteString pol
                , migrationTokenId =
                    unsafeFromBuiltinData (mkD tid)
                }
        _ ->
            error
                "unsafeFromBuiltinData: Migration"

instance ToData MintRedeemer where
    toBuiltinData (Minting m) =
        mkD $ Constr 0 [unD (toBuiltinData m)]
    toBuiltinData (Migrating m) =
        mkD $ Constr 1 [unD (toBuiltinData m)]
    toBuiltinData Burning =
        mkD $ Constr 2 []

instance FromData MintRedeemer where
    fromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            Minting <$> fromBuiltinData (mkD d)
        Constr 1 [d] ->
            Migrating <$> fromBuiltinData (mkD d)
        Constr 2 [] -> Just Burning
        _ -> Nothing

instance UnsafeFromData MintRedeemer where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [d] ->
            Minting $ unsafeFromBuiltinData (mkD d)
        Constr 1 [d] ->
            Migrating
                $ unsafeFromBuiltinData (mkD d)
        Constr 2 [] -> Burning
        _ ->
            error
                "unsafeFromBuiltinData: MintRedeemer"

instance ToData Neighbor where
    toBuiltinData Neighbor{..} =
        mkD
            $ Constr
                0
                [ I neighborNibble
                , bsToD neighborPrefix
                , bsToD neighborRoot
                ]

instance FromData Neighbor where
    fromBuiltinData bd = case unD bd of
        Constr 0 [I nib, pfx, rt] ->
            Neighbor nib
                <$> bsFromD pfx
                <*> bsFromD rt
        _ -> Nothing

instance UnsafeFromData Neighbor where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [I nib, B pfx, B rt] ->
            Neighbor nib pfx rt
        _ -> error "unsafeFromBuiltinData: Neighbor"

instance ToData ProofStep where
    toBuiltinData Branch{..} =
        mkD
            $ Constr
                0
                [ I branchSkip
                , bsToD branchNeighbors
                ]
    toBuiltinData Fork{..} =
        mkD
            $ Constr
                1
                [ I forkSkip
                , unD (toBuiltinData forkNeighbor)
                ]
    toBuiltinData Leaf{..} =
        mkD
            $ Constr
                2
                [ I leafSkip
                , bsToD leafKey
                , bsToD leafValue
                ]

instance FromData ProofStep where
    fromBuiltinData bd = case unD bd of
        Constr 0 [I sk, nb] ->
            Branch sk <$> bsFromD nb
        Constr 1 [I sk, nd] ->
            Fork sk
                <$> fromBuiltinData (mkD nd)
        Constr 2 [I sk, k, v] ->
            Leaf sk <$> bsFromD k <*> bsFromD v
        _ -> Nothing

instance UnsafeFromData ProofStep where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [I sk, B nb] -> Branch sk nb
        Constr 1 [I sk, nd] ->
            Fork sk
                $ unsafeFromBuiltinData (mkD nd)
        Constr 2 [I sk, B k, B v] -> Leaf sk k v
        _ ->
            error "unsafeFromBuiltinData: ProofStep"

instance ToData UpdateRedeemer where
    toBuiltinData End = mkD $ Constr 0 []
    toBuiltinData (Contribute ref) =
        mkD $ Constr 1 [unD (toBuiltinData ref)]
    toBuiltinData (Modify actions) =
        mkD
            $ Constr
                2
                [ List
                    (map (unD . toBuiltinData) actions)
                ]
    toBuiltinData (Retract ref) =
        mkD $ Constr 3 [unD (toBuiltinData ref)]

instance ToData RequestAction where
    toBuiltinData (UpdateAction proofs) =
        mkD
            $ Constr
                0
                [ List
                    (map (unD . toBuiltinData) proofs)
                ]
    toBuiltinData Rejected =
        mkD $ Constr 1 []

instance FromData RequestAction where
    fromBuiltinData bd = case unD bd of
        Constr 0 [List steps] ->
            UpdateAction
                <$> traverse
                    (fromBuiltinData . mkD)
                    steps
        Constr 1 [] -> Just Rejected
        _ -> Nothing

instance FromData UpdateRedeemer where
    fromBuiltinData bd = case unD bd of
        Constr 0 [] -> Just End
        Constr 1 [d] ->
            Contribute <$> fromBuiltinData (mkD d)
        Constr 2 [List as] ->
            Modify
                <$> traverse
                    (fromBuiltinData . mkD)
                    as
        Constr 3 [d] ->
            Retract <$> fromBuiltinData (mkD d)
        _ -> Nothing

instance UnsafeFromData RequestAction where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [List steps] ->
            UpdateAction
                $ map
                    (unsafeFromBuiltinData . mkD)
                    steps
        Constr 1 [] -> Rejected
        _ ->
            error
                "unsafeFromBuiltinData:\
                \ RequestAction"

instance UnsafeFromData UpdateRedeemer where
    unsafeFromBuiltinData bd = case unD bd of
        Constr 0 [] -> End
        Constr 1 [d] ->
            Contribute
                $ unsafeFromBuiltinData (mkD d)
        Constr 2 [List as] ->
            Modify
                $ map
                    (unsafeFromBuiltinData . mkD)
                    as
        Constr 3 [d] ->
            Retract
                $ unsafeFromBuiltinData (mkD d)
        _ ->
            error
                "unsafeFromBuiltinData:\
                \ UpdateRedeemer"

-- ---------------------------------------------------------
-- Script identity
-- ---------------------------------------------------------

-- | The cage validator script hash (raw 28 bytes).
cageScriptHash :: ByteString
cageScriptHash =
    BS.pack
        [ 0xe4
        , 0x62
        , 0xb3
        , 0x8f
        , 0xe5
        , 0xcd
        , 0xb6
        , 0xee
        , 0x3e
        , 0xe5
        , 0x1f
        , 0x9d
        , 0xc2
        , 0xda
        , 0xa7
        , 0x3c
        , 0x77
        , 0x1b
        , 0x26
        , 0x99
        , 0x57
        , 0xa9
        , 0x9a
        , 0x06
        , 0x99
        , 0x99
        , 0x23
        , 0x93
        ]

-- | The cage validator 'ScriptHash' (ledger type).
cageScriptHashLedger :: ScriptHash
cageScriptHashLedger =
    ScriptHash
        $ fromJust
        $ hashFromBytes cageScriptHash

-- | Cage minting policy ID.
cagePolicyId :: PolicyID
cagePolicyId = PolicyID cageScriptHashLedger

-- | Cage script address for a given network.
cageAddr
    :: Network
    -- ^ Cardano network (mainnet or testnet)
    -> Addr
cageAddr net =
    Addr
        net
        (ScriptHashObj cageScriptHashLedger)
        StakeRefNull

-- ---------------------------------------------------------
-- Asset-name derivation
-- ---------------------------------------------------------

-- | Derive the asset name from an output reference,
-- matching Aiken's @lib.assetName@:
--
-- @SHA2-256(txId ++ bigEndian16(outputIndex))@
deriveAssetName :: OnChainTxOutRef -> ByteString
deriveAssetName OnChainTxOutRef{txOutRefId, txOutRefIdx} =
    convert digest
  where
    digest :: Digest SHA256
    digest = hash (txIdBytes <> indexBytes)

    txIdBytes :: ByteString
    txIdBytes =
        let BuiltinByteString bs = txOutRefId
        in  bs

    indexBytes :: ByteString
    indexBytes =
        let w16 =
                fromIntegral txOutRefIdx :: Word16
        in  BS.pack
                [ fromIntegral (w16 `shiftR` 8)
                , fromIntegral w16
                ]

-- ---------------------------------------------------------
-- Blueprint loading
-- ---------------------------------------------------------

-- | Load and parse a CIP-57 blueprint from a file
-- path. Returns the parsed 'Blueprint' on success.
loadCageScript
    :: FilePath
    -- ^ Path to the @plutus.json@ blueprint file
    -> IO (Either String Blueprint)
loadCageScript = loadBlueprint
