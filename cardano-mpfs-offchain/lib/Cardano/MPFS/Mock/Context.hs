-- |
-- Module      : Cardano.MPFS.Mock.Context
-- Description : Complete mock Context for testing
-- License     : Apache-2.0
--
-- Convenience function that wires all mock
-- implementations into a complete 'Context IO':
-- 'mkMockProvider', 'mkMockSubmitter',
-- 'mkMockTxBuilder', 'mkMockState',
-- and 'mkPureTrieManager'. Useful for integration
-- tests and development workflows that need a fully
-- typed 'Context' without any real infrastructure.
-- See "Cardano.MPFS.Application" for the production
-- wiring.
module Cardano.MPFS.Mock.Context
    ( -- * Construction
      mkMockContext
    ) where

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Mock.Provider (mkMockProvider)
import Cardano.MPFS.Mock.State (mkMockState)
import Cardano.MPFS.Mock.Submitter (mkMockSubmitter)
import Cardano.MPFS.Mock.TxBuilder (mkMockTxBuilder)
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )

-- | Create a complete mock 'Context IO' with all
-- interfaces wired to in-memory implementations.
--
-- The 'TrieManager' and 'State' are backed by
-- 'IORef' maps. Provider, submitter, and tx builder
-- are stubs.
mkMockContext :: IO (Context IO)
mkMockContext = do
    tm <- mkPureTrieManager
    st <- mkMockState
    pure
        Context
            { provider = mkMockProvider
            , trieManager = tm
            , state = st
            , submitter = mkMockSubmitter
            , txBuilder = mkMockTxBuilder
            , cfgCage =
                error
                    "mkMockContext: cfgCage \
                    \not implemented"
            , utxoExists = \_ -> pure False
            , resolveUtxo = \_ -> pure Nothing
            , awaitUtxo = \_ _ -> pure Nothing
            , utxoRoot = pure Nothing
            , utxoProof = \_ -> pure Nothing
            , atomicCageReader =
                \_ ->
                    error
                        "mkMockContext: atomicCageReader \
                        \not implemented (mock context \
                        \does not exercise tx-build paths)"
            , readMetrics = pure Nothing
            }
