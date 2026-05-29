{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.Serialize
-- Description : Submission-ready CBOR for cage transactions.
--
-- The cage builders in "Cardano.MPFS.Client.Cage.*" return a
-- @'Tx' 'ConwayEra'@. This module provides the single ledger-aware
-- step that turns that transaction into submission-ready CBOR, so
-- downstream packages can emit wire bytes without importing any
-- @Cardano.Ledger.*@ module themselves.
module Cardano.MPFS.Client.Cage.Serialize
    ( serializeCageTx
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Api.Tx (Tx)
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.MPFS.Cage.Ledger (ConwayEra)

-- | Serialize a Conway cage transaction to submission-ready CBOR,
-- using protocol version 11 (the version the on-chain validators
-- and the rest of the client encode against).
serializeCageTx :: Tx ConwayEra -> ByteString
serializeCageTx = serialize' (natVersion @11)
