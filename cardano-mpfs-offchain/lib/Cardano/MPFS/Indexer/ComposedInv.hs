{-# LANGUAGE DeriveGeneric #-}

-- |
-- Module      : Cardano.MPFS.Indexer.ComposedInv
-- Description : Combined inverse type for UTxO + cage
-- License     : Apache-2.0
--
-- Combined inverse operations from both the UTxO CSMT
-- backend and the cage state backend. Stored in a
-- single 'RollbackPoint' by the chain-follower Runner.
module Cardano.MPFS.Indexer.ComposedInv
    ( ComposedInv (..)
    ) where

import Data.ByteString.Lazy (LazyByteString)

import Cardano.UTxOCSMT.Application.Database.Interface
    ( Operation
    )

import Cardano.MPFS.Indexer.Event (CageInverseOp)

-- | Combined inverse from both backends.
data ComposedInv = ComposedInv
    { utxoInverses
        :: [Operation LazyByteString LazyByteString]
    -- ^ UTxO CSMT inverse operations
    , cageInverses :: [CageInverseOp]
    -- ^ Cage state inverse operations
    }
