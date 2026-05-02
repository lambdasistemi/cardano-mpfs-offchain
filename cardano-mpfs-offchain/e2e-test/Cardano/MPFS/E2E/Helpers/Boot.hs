{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.E2E.Helpers.Boot
-- Description : Test helper — wallet-side boot inputs
-- License     : Apache-2.0
--
-- Tests act as their own wallet on a private LSQ
-- connection against a tiny devnet UTxO set, so
-- 'queryUTxOs' is acceptable here even though it is
-- forbidden on the server-side hot path (see #252).
--
-- This helper materialises the
-- @[ResolvedWalletInput]@ argument that
-- 'Cardano.MPFS.TxBuilder.bootToken' now expects, by:
--
-- 1. Calling 'queryUTxOs' against the wallet
--    address.
-- 2. Re-encoding each 'TxOut' to the canonical
--    ledger CBOR.
-- 3. Pairing each input with empty proof bytes
--    (these tests use sentinel snapshots whose
--    CSMT root is empty, so the proofs are not
--    verified).
--
-- Production servers MUST NOT use this path. They
-- get their @[ResolvedWalletInput]@ from
-- 'Cardano.MPFS.Context.AtomicCageReader' which
-- reads the local indexer in a single transaction.
module Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    ) where

import Data.ByteString qualified as BS

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )

import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder (ResolvedWalletInput)

-- | Build a list of @[ResolvedWalletInput]@ from
-- the wallet's own LSQ view. Empty proof bytes are
-- emitted; tests use empty-root sentinel snapshots
-- where proofs are not verified.
walletBootInputs
    :: Provider IO -> Addr -> IO [ResolvedWalletInput]
walletBootInputs prov addr = do
    utxos <- queryUTxOs prov addr
    pure
        [ ( tin
          , serialize' (natVersion @11) tout
          , BS.empty
          )
        | (tin, tout) <- utxos
        ]
