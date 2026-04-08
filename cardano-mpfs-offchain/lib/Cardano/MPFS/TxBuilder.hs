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
-- "Cardano.MPFS.Mock.TxBuilder". Returned transactions
-- are unsigned — callers add key witnesses before
-- submission.
module Cardano.MPFS.TxBuilder
    ( -- * Transaction builder interface
      TxBuilder (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Api.Tx (Tx)

import Cardano.MPFS.Core.Types
    ( Addr
    , ConwayEra
    , TokenId
    , TxIn
    )

-- | Interface for constructing transactions for
-- all MPFS protocol operations. Returns full
-- ledger 'Tx' values ready for signing.
data TxBuilder m = TxBuilder
    { bootToken
        :: Addr -> m (Tx ConwayEra)
    -- ^ Create a new MPFS token
    , requestInsert
        :: TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m (Tx ConwayEra)
    -- ^ Request inserting a key-value pair
    , requestDelete
        :: TokenId
        -> ByteString
        -> ByteString
        -> Addr
        -> m (Tx ConwayEra)
    -- ^ Request deleting a key (key, old value)
    , updateToken
        :: TokenId
        -> Addr
        -> m (Tx ConwayEra)
    -- ^ Process pending requests for a token
    , retractRequest
        :: TxIn
        -> Addr
        -> m (Tx ConwayEra)
    -- ^ Cancel a pending request
    , endToken
        :: TokenId
        -> Addr
        -> m (Tx ConwayEra)
    -- ^ Retire an MPFS token
    }
