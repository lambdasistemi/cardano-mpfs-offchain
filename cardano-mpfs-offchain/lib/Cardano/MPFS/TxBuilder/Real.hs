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

      -- * Owner-only sweep
    , sweepUtxoImpl

      -- * Script hash
    , computeScriptHash

      -- * Request locked ADA
    , requestLockedAda

      -- * Refund computation
    , computeRefund

      -- * Internals (for testing)
    , mkInlineDatum
    , mkRequestDatum
    , toPlcData
    , extractCageDatum
    , extractOwnerBytes
    , addrFromKeyHashBytes
    , spendingIndex
    ) where

import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.Trie (TrieManager (..))
import Cardano.MPFS.TxBuilder
    ( TxBuilder (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig
    )
import Cardano.MPFS.TxBuilder.Real.End
    ( endTokenImpl
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( addrFromKeyHashBytes
    , computeRefund
    , computeScriptHash
    , extractCageDatum
    , extractOwnerBytes
    , mkInlineDatum
    , mkRequestDatum
    , spendingIndex
    , toPlcData
    )
import Cardano.MPFS.TxBuilder.Real.Reject
    ( rejectRequestsImpl
    )
import Cardano.MPFS.TxBuilder.Real.Request
    ( requestDeleteImpl
    , requestInsertImpl
    , requestLockedAda
    , requestUpdateImpl
    )
import Cardano.MPFS.TxBuilder.Real.Retract
    ( retractRequestImpl
    )
import Cardano.MPFS.TxBuilder.Real.Sweep
    ( sweepUtxoImpl
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
    -> UtxoProofFn
    -- ^ CSMT inclusion proof lookup
    -> TxBuilder IO
mkRealTxBuilder cfg prov st tm proofFn =
    TxBuilder
        { bootToken =
            \_ _ _ ->
                error
                    "bootToken: server-side boot \
                    \transaction construction has \
                    \been removed; use POST \
                    \/facts/boot and build in the \
                    \wallet client"
        , requestInsert =
            requestInsertImpl cfg prov st proofFn
        , requestDelete =
            requestDeleteImpl cfg prov st proofFn
        , requestUpdate =
            requestUpdateImpl cfg prov st proofFn
        , updateToken =
            updateTokenImpl cfg prov st tm proofFn
        , retractRequest =
            retractRequestImpl cfg prov st proofFn
        , rejectRequests =
            rejectRequestsImpl cfg prov st proofFn
        , endToken = endTokenImpl cfg prov proofFn
        }
