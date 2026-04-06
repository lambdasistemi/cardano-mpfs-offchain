{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Backend
-- Description : Composed Backend.Init for cage + UTxO
-- License     : Apache-2.0
--
-- Composes the UTxO CSMT and cage state backends
-- into a single 'Backend.Init' for the chain-follower
-- Runner. Follows the same pattern as
-- @createBackend@ in cardano-utxo-csmt but adds
-- cage event detection and state mutations.
--
-- Reference: @cardano-utxo-csmt\/Backend.hs@ and
-- @chain-follower\/tutorial\/Composed.hs@
module Cardano.MPFS.Indexer.Backend
    ( composedInit
    ) where

import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Lazy (LazyByteString)
import Data.ByteString.Short qualified as SBS

import Ouroboros.Consensus.HardFork.Combinator
    ( OneEraHash (..)
    )
import Ouroboros.Network.Block qualified as Network
import Ouroboros.Network.Point
    ( Block (..)
    , WithOrigin (..)
    )

import Cardano.Ledger.Binary
    ( DecCBOR
    , DecoderError
    , EncCBOR
    , decodeFull
    , natVersion
    , serialize
    )

import CSMT.MTS (Ops, kvCommon, toFull)
import Cardano.UTxOCSMT.Application.BlockFetch
    ( Fetched (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTOps (..)
    , fullOpsToCSMTOps
    , kvCommonToCSMTOps
    )
import Cardano.UTxOCSMT.Application.Database.Interface
    ( Operation (..)
    )
import Cardano.UTxOCSMT.Application.UTxOs
    ( Change (..)
    , uTxOs
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Point
    )
import MTS.Interface qualified as MTS (Mode (..))

import ChainFollower.Backend
    ( Following (..)
    , Init (..)
    , Restoring (..)
    )

import Database.KV.Transaction
    ( Transaction
    , mapColumns
    , query
    )

import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , ConwayEra
    , ScriptHash
    , TxIn
    , TxOut
    )
import Cardano.MPFS.Indexer.Columns
    ( UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.ComposedInv
    ( ComposedInv (..)
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageBlockEvents
    , applyCageInverses
    , detectCageBlockEvents
    , extractConwayTxs
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkTransactionalState
    )
import Cardano.MPFS.Trie.Persistent
    ( mkUnifiedTrieManager
    )

-- | Shorthand for the unified transaction.
type T hash cf op =
    Transaction
        IO
        cf
        (UnifiedColumns Point hash LazyByteString LazyByteString)
        op

-- | Convert a UTxO 'Change' to an 'Operation'.
changeToOp
    :: Change
    -> Operation LazyByteString LazyByteString
changeToOp (Spend k) = Delete k
changeToOp (Create k v) = Insert k v

-- | Extract the block hash from a 'Point'.
pointToBlockId :: Point -> BlockId
pointToBlockId (Network.Point Origin) =
    BlockId mempty
pointToBlockId
    (Network.Point (At (Block _ h))) =
        BlockId
            $ SBS.fromShort
            $ getOneEraHash h

-- | CBOR-encode using protocol version 11.
cborEncode :: EncCBOR a => a -> LazyByteString
cborEncode = serialize (natVersion @11)

-- | CBOR-decode using protocol version 11.
cborDecode
    :: DecCBOR a
    => LazyByteString
    -> Either DecoderError a
cborDecode = decodeFull (natVersion @11)

-- | Resolve a 'TxIn' to its 'TxOut' by querying
-- the UTxO KV column inside a unified transaction.
resolveUtxoT
    :: TxIn
    -> T hash cf op (Maybe (TxOut ConwayEra))
resolveUtxoT txIn = do
    let key = cborEncode txIn
    mVal <- mapColumns InUtxo $ query KVCol key
    pure $ case mVal of
        Nothing -> Nothing
        Just val -> case cborDecode val of
            Left _ -> Nothing
            Right txOut -> Just txOut

-- | Create a composed 'Backend.Init' for both
-- UTxO CSMT and cage state.
--
-- Takes the KVOnly 'Ops' (same as @createBackend@
-- in cardano-utxo-csmt). 'start' returns a Restoring
-- with KVOnly ops; 'toFollowing' calls 'toFull' for
-- journal replay and switches to Full ops.
composedInit
    :: ScriptHash
    -> Ops
        'MTS.KVOnly
        IO
        cf
        ( Columns
            Point
            hash
            LazyByteString
            LazyByteString
        )
        op
        LazyByteString
        LazyByteString
        hash
    -> Init
        IO
        (T hash cf op)
        Fetched
        ComposedInv
        BlockId
composedInit scriptHash ops =
    Init
        { start = pure $ mkRestoring kvOps
        }
  where
    kvOps = kvCommonToCSMTOps (kvCommon ops)

    mkRestoring csmtOps =
        Restoring
            { restore = \fetched -> do
                let conwayTxs =
                        extractConwayTxs
                            (fetchedBlock fetched)
                    utxoOps =
                        changeToOp
                            <$> uTxOs
                                (fetchedBlock fetched)

                -- 1. Detect cage events
                events <-
                    detectCageBlockEvents
                        scriptHash
                        resolveUtxoT
                        conwayTxs

                -- 2. Apply cage mutations (no inverses)
                _ <-
                    mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            events

                -- 3. Apply UTxO ops (KVOnly)
                mapColumns InUtxo
                    $ forM_
                        utxoOps
                    $ \case
                        Insert k v ->
                            csmtInsert csmtOps k v
                        Delete k ->
                            csmtDelete csmtOps k

                pure $ mkRestoring csmtOps
            , toFollowing = do
                mFull <- liftIO (toFull ops)
                case mFull of
                    Just fullOps ->
                        pure
                            $ mkFollowing
                                (fullOpsToCSMTOps fullOps)
                    Nothing ->
                        fail
                            "mkRestoring: toFull failed"
            }

    mkFollowing csmtOps =
        Following
            { follow = \fetched -> do
                let conwayTxs =
                        extractConwayTxs
                            (fetchedBlock fetched)
                    utxoOps =
                        changeToOp
                            <$> uTxOs
                                (fetchedBlock fetched)
                    blockId =
                        pointToBlockId
                            (fetchedPoint fetched)

                -- 1. Detect cage events
                events <-
                    detectCageBlockEvents
                        scriptHash
                        resolveUtxoT
                        conwayTxs

                -- 2. Apply cage mutations WITH inverses
                cageInvs <-
                    mapColumns InCage
                        $ applyCageBlockEvents
                            mkTransactionalState
                            mkUnifiedTrieManager
                            events

                -- 3. Apply UTxO ops WITH inverses
                utxoInvs <-
                    mapColumns InUtxo
                        $ concat
                            <$> mapM
                                ( \case
                                    Insert k v -> do
                                        csmtInsert
                                            csmtOps
                                            k
                                            v
                                        pure [Delete k]
                                    Delete k -> do
                                        mOld <-
                                            query KVCol k
                                        csmtDelete
                                            csmtOps
                                            k
                                        pure $ case mOld of
                                            Nothing -> []
                                            Just old ->
                                                [Insert k old]
                                )
                                utxoOps

                let inv =
                        ComposedInv
                            { utxoInverses = utxoInvs
                            , cageInverses = cageInvs
                            }

                pure
                    ( inv
                    , Just blockId
                    , mkFollowing csmtOps
                    )
            , toRestoring =
                pure $ mkRestoring kvOps
            , applyInverse =
                \ComposedInv{utxoInverses, cageInverses} ->
                    do
                        -- Undo cage first (applied after
                        -- UTxO), then undo UTxO
                        mapColumns InCage
                            $ applyCageInverses
                                mkTransactionalState
                                mkUnifiedTrieManager
                                (reverse cageInverses)
                        mapColumns InUtxo
                            $ forM_
                                (reverse utxoInverses)
                            $ \case
                                Insert k v ->
                                    csmtInsert csmtOps k v
                                Delete k ->
                                    csmtDelete csmtOps k
            }
