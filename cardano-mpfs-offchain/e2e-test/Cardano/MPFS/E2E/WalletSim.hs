{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.E2E.WalletSim
-- Description : Test-only wallet simulator
-- License     : Apache-2.0
--
-- E2E test fixtures act as the wallet from the
-- server's perspective: they discover their own
-- UTxOs by querying cardano-node's
-- 'LocalStateQuery' and pass the chosen refs to
-- proof-bearing endpoints in the request body.
--
-- This module isolates that wallet-side query
-- behind a single helper. The server's tx-build
-- path NEVER calls 'queryUTxOs' (#252):
-- cardano-node implements 'GetUTxOByAddress' as a
-- linear scan over the entire ledger UTxO set,
-- so it is unfit for any hot-path use. Wallets
-- can use it because each wallet's call is rare;
-- the server cannot, because it would amplify
-- one HTTP request into a full ledger scan.
--
-- Any new e2e fixture that needs to simulate a
-- wallet picking inputs MUST go through this
-- helper rather than calling 'queryUTxOs'
-- directly, so the next time we audit the
-- codebase the test-only uses are easy to find
-- and the production-side ban remains
-- enforceable.
module Cardano.MPFS.E2E.WalletSim
    ( walletBoot
    ) where

import Cardano.Ledger.Address (Addr)

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BootProof
    , ProofEnvelope
    , TxBuilder (..)
    )

-- | Simulate a wallet building a boot tx: query
-- the node-side UTxO state at @addr@, hand those
-- refs to the server's 'bootToken'. Test-only.
walletBoot
    :: Context IO
    -> Addr
    -> IO (ProofEnvelope BootProof)
walletBoot ctx addr = do
    utxos <- queryUTxOs (provider ctx) addr
    bootToken
        (txBuilder ctx)
        addr
        (map fst utxos)
