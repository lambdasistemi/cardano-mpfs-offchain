{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot.Inputs
-- Description : Decoding pre-resolved boot inputs
-- License     : Apache-2.0
--
-- Converts the @[ResolvedWalletInput]@ produced by the
-- indexer into the ledger-typed view
-- 'Cardano.MPFS.TxBuilder.Real.Internal.evaluateAndBalance'
-- consumes, preserving the original CBOR bytes for
-- verbatim pass-through into 'WitnessedInput'.
module Cardano.MPFS.TxBuilder.Real.Boot.Inputs
    ( -- * Decoded input row
      InputRow (..)

      -- * Decoding
    , decodeAll

      -- * Projections
    , ledgerPair
    , rowToWitness
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL

import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    )

import Cardano.MPFS.Core.Types (ConwayEra, TxIn)
import Cardano.MPFS.TxBuilder
    ( ResolvedWalletInput
    , WitnessedInput (..)
    )

-- | Decoded view of an indexer-resolved input. The
-- ledger 'TxOut' is needed to feed
-- @evaluateAndBalance@; the original CBOR bytes are
-- preserved so they pass through to 'witnessedTxOut'
-- verbatim (matching what the indexer applied and what
-- on-chain validators compute).
data InputRow = InputRow
    { rowRef :: TxIn
    , rowOut :: TxOut ConwayEra
    , rowOutBytes :: ByteString
    , rowProof :: ByteString
    }

-- | Decode every triple in the list. Returns
-- @Left@ on the first decoding failure.
decodeAll
    :: [ResolvedWalletInput]
    -> Either DecoderError [InputRow]
decodeAll = traverse decodeOne

decodeOne
    :: ResolvedWalletInput
    -> Either DecoderError InputRow
decodeOne (tin, outBytes, proofBytes) =
    case decodeFull
        (natVersion @11)
        (BSL.fromStrict outBytes) of
        Left err -> Left err
        Right out ->
            Right
                InputRow
                    { rowRef = tin
                    , rowOut = out
                    , rowOutBytes = outBytes
                    , rowProof = proofBytes
                    }

-- | Project to the @(TxIn, TxOut)@ shape
-- @evaluateAndBalance@ accepts.
ledgerPair
    :: InputRow -> (TxIn, TxOut ConwayEra)
ledgerPair r = (rowRef r, rowOut r)

-- | Convert to the wire-level 'WitnessedInput' the
-- response carries.
rowToWitness :: InputRow -> WitnessedInput
rowToWitness r =
    WitnessedInput
        { witnessedRef = rowRef r
        , witnessedTxOut = rowOutBytes r
        , witnessedCsmtProof = rowProof r
        }
