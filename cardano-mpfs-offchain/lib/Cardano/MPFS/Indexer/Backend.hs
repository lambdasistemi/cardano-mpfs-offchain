{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Backend
-- Description : Composed Backend.Init for cage + UTxO
-- License     : Apache-2.0
--
-- Composes the UTxO CSMT and cage state backends
-- into a single 'Backend.Init' for the chain-follower
-- Runner. Each block is processed in one atomic
-- transaction over 'UnifiedColumns'.
--
-- Reference: @chain-follower\/tutorial\/Composed.hs@
module Cardano.MPFS.Indexer.Backend
    ( composedInit
    ) where

import Control.Monad (forM_)
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

import Cardano.UTxOCSMT.Application.BlockFetch
    ( Fetched (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTOps (..)
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

-- | Shorthand for UTxO-only transaction.
type UTxOT hash cf op =
    Transaction
        IO
        cf
        (Columns Point hash LazyByteString LazyByteString)
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

{- | Create a composed 'Backend.Init' for both
UTxO CSMT and cage state.

The 'block' type is @(Point, [Operation])@ — the
slot\/point for the UTxO CSMT, plus the UTxO
operations extracted from the fetched block. Cage
events are detected from the full 'Fetched' block
inside the continuations.

The caller processes the 'Fetched' block and
extracts @(slot, utxoOps, conwayTxs)@ before
calling 'processBlock' — or we can make the
block type 'Fetched' itself and extract inside.
We choose 'Fetched' for simplicity.
-}
composedInit
    :: (Ord hash, Show hash)
    => ScriptHash
    -> CSMTOps
        (UTxOT hash cf op)
        LazyByteString
        LazyByteString
        hash
    -> (Point -> hash)
    -> Init
        IO
        (T hash cf op)
        Fetched
        ComposedInv
        BlockId
composedInit scriptHash ops slotHash =
    Init
        { startRestoring =
            pure
                $ composedRestoring
                    scriptHash
                    ops
                    slotHash
        , resumeFollowing =
            pure
                $ composedFollowing
                    scriptHash
                    ops
                    slotHash
        }

-- | Restoring continuation: fast apply, no inverses.
composedRestoring
    :: (Ord hash, Show hash)
    => ScriptHash
    -> CSMTOps
        (UTxOT hash cf op)
        LazyByteString
        LazyByteString
        hash
    -> (Point -> hash)
    -> Restoring
        IO
        (T hash cf op)
        Fetched
        ComposedInv
        BlockId
composedRestoring scriptHash ops slotHash =
    Restoring
        { restore = \fetched -> do
            let conwayTxs =
                    extractConwayTxs
                        (fetchedBlock fetched)
                utxoOps =
                    changeToOp <$> uTxOs (fetchedBlock fetched)

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

            -- 3. Apply UTxO ops (KVOnly, no inverses)
            mapColumns InUtxo
                $ mapM_
                    ( \case
                        Insert k v ->
                            csmtInsert ops k v
                        Delete k ->
                            csmtDelete ops k
                    )
                    utxoOps

            pure
                $ composedRestoring
                    scriptHash
                    ops
                    slotHash
        , toFollowing =
            pure
                $ composedFollowing
                    scriptHash
                    ops
                    slotHash
        }

-- | Following continuation: full apply with inverses.
composedFollowing
    :: (Ord hash, Show hash)
    => ScriptHash
    -> CSMTOps
        (UTxOT hash cf op)
        LazyByteString
        LazyByteString
        hash
    -> (Point -> hash)
    -> Following
        IO
        (T hash cf op)
        Fetched
        ComposedInv
        BlockId
composedFollowing scriptHash ops slotHash =
    Following
        { follow = \fetched -> do
            let conwayTxs =
                    extractConwayTxs
                        (fetchedBlock fetched)
                utxoOps =
                    changeToOp <$> uTxOs (fetchedBlock fetched)
                blockId =
                    pointToBlockId (fetchedPoint fetched)

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
                                    csmtInsert ops k v
                                    pure [Delete k]
                                Delete k -> do
                                    mOld <-
                                        query KVCol k
                                    csmtDelete ops k
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
                , composedFollowing
                    scriptHash
                    ops
                    slotHash
                )
        , toRestoring =
            pure
                $ composedRestoring
                    scriptHash
                    ops
                    slotHash
        , applyInverse =
            \ComposedInv{utxoInverses, cageInverses} ->
                do
                    -- Undo cage first (applied after UTxO),
                    -- then undo UTxO
                    mapColumns InCage
                        $ applyCageInverses
                            mkTransactionalState
                            mkUnifiedTrieManager
                            (reverse cageInverses)
                    mapColumns InUtxo
                        $ forM_ (reverse utxoInverses)
                        $ \case
                            Insert k v ->
                                csmtInsert ops k v
                            Delete k ->
                                csmtDelete ops k
        }
