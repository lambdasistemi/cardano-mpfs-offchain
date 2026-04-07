-- |
-- Module      : Cardano.MPFS.Mock.Provider
-- Description : No-op mock Provider implementation
-- License     : Apache-2.0
--
-- Stub implementation of the 'Provider' interface
-- for unit tests and wiring checks. Returns empty
-- UTxO sets and empty evaluation results.
-- 'queryProtocolParams' throws — tests that need
-- real params should use a custom fixture instead.
-- See "Cardano.MPFS.Provider.NodeClient" for the
-- production implementation.
module Cardano.MPFS.Mock.Provider
    ( -- * Construction
      mkMockProvider
    ) where

import Data.Map.Strict qualified as Map

import Cardano.MPFS.Provider (Provider (..))

-- | Create a mock 'Provider IO'. Returns empty UTxO
-- sets, default protocol params, and empty evaluation
-- results.
mkMockProvider :: Provider IO
mkMockProvider =
    Provider
        { queryUTxOs = \_ -> pure []
        , queryProtocolParams =
            error
                "mkMockProvider: queryProtocolParams \
                \not implemented"
        , evaluateTx = \_ ->
            pure Map.empty
        , posixMsToSlot =
            error
                "mkMockProvider: posixMsToSlot \
                \not implemented"
        , posixMsCeilSlot =
            error
                "mkMockProvider: posixMsCeilSlot \
                \not implemented"
        }
