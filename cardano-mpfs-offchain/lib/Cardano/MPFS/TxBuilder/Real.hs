-- |
-- Module      : Cardano.MPFS.TxBuilder.Real
-- Description : Real transaction builders for the MPFS cage
-- License     : Apache-2.0
--
-- Assembles the real 'TxBuilder' by wiring
-- per-operation implementations from the @Real.*@
-- submodules to a 'Provider', 'State', and
-- 'TrieManager'. Returned transactions are unsigned
-- Conway-era ledger values ready for key-witness
-- addition and submission.
--
-- Also re-exports 'computeScriptHash' and datum
-- helpers used in tests.
module Cardano.MPFS.TxBuilder.Real
    ( -- * Construction
      mkRealTxBuilder

      -- * Script hash
    , computeScriptHash

      -- * Request locked ADA
    , requestLockedAda

      -- * Internals (for testing)
    , mkInlineDatum
    , mkRequestDatum
    , toPlcData
    , extractCageDatum
    , spendingIndex
    ) where

import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.Trie (TrieManager (..))
import Cardano.MPFS.TxBuilder (TxBuilder (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig
    )
import Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    )
import Cardano.MPFS.TxBuilder.Real.End
    ( endTokenImpl
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( computeScriptHash
    , extractCageDatum
    , mkInlineDatum
    , mkRequestDatum
    , spendingIndex
    , toPlcData
    )
import Cardano.MPFS.TxBuilder.Real.Request
    ( requestDeleteImpl
    , requestInsertImpl
    , requestLockedAda
    , requestUpdateImpl
    )
import Cardano.MPFS.TxBuilder.Real.Reject
    ( rejectRequestsImpl
    )
import Cardano.MPFS.TxBuilder.Real.Retract
    ( retractRequestImpl
    )
import Cardano.MPFS.TxBuilder.Real.Update
    ( updateTokenImpl
    )

-- | Create a real 'TxBuilder IO' wired to a
-- 'Provider', 'State', and 'TrieManager'.
mkRealTxBuilder
    :: CageConfig
    -- ^ Cage script config (bytes, hash, params)
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Token and request state
    -> TrieManager IO
    -- ^ Per-token trie manager
    -> TxBuilder IO
mkRealTxBuilder cfg prov st tm =
    TxBuilder
        { bootToken = bootTokenImpl cfg prov
        , requestInsert =
            requestInsertImpl cfg prov st
        , requestDelete =
            requestDeleteImpl cfg prov st
        , requestUpdate =
            requestUpdateImpl cfg prov st
        , updateToken =
            updateTokenImpl cfg prov st tm
        , retractRequest =
            retractRequestImpl cfg prov st
        , rejectRequests =
            rejectRequestsImpl cfg prov st
        , endToken = endTokenImpl cfg prov
        }
