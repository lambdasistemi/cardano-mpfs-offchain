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

import Cardano.Ledger.Address (Addr)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..))
import Cardano.MPFS.Trie (TrieManager (..))
import Cardano.MPFS.TxBuilder
    ( BootProof (..)
    , BundleSnapshot
    , ProofEnvelope (..)
    , ResolvedWalletInput
    , TxBuilder (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig
    )
import Cardano.MPFS.TxBuilder.Real.Boot
    ( BootCore (..)
    , bootTokenCore
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
import Cardano.Node.Client.TxBuild
    ( InterpretIO (..)
    , build
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
        { bootToken = runBootBuilder cfg prov
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

-- | IO orchestrator for @POST \/tx\/boot@. Builds a
-- pure 'BootCore' via 'bootTokenCore', fetches the
-- protocol parameters from the 'Provider', then runs
-- the DSL 'build' loop with the provider's script
-- evaluator. The 'Boot' module itself stays pure;
-- this is where IO meets the cage protocol.
runBootBuilder
    :: CageConfig
    -> Provider IO
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> Addr
    -> IO (ProofEnvelope BootProof)
runBootBuilder cfg prov snap inputs addr =
    case bootTokenCore cfg snap inputs addr of
        Left e ->
            error
                $ "bootToken: malformed inputs: "
                    <> show e
        Right spec -> do
            pp <- queryProtocolParams prov
            let evalAdapter tx =
                    fmap
                        ( fmap
                            ( either
                                (Left . show)
                                Right
                            )
                        )
                        (evaluateTx prov tx)
            result <-
                build
                    pp
                    noCtxInterpretIO
                    evalAdapter
                    (bcInputs spec)
                    (bcAddr spec)
                    (bcProgram spec)
            case result of
                Left e ->
                    error
                        $ "bootToken: DSL build \
                          \failed: "
                            <> show e
                Right tx ->
                    pure
                        ProofEnvelope
                            { envTx = tx
                            , envSnapshot =
                                bcSnapshot spec
                            , envProof =
                                BootProof
                                    { bootFunding =
                                        bcFunding
                                            spec
                                    }
                            }

-- | No-op interpreter for the empty domain-query
-- context: boot has no @ctx@-driven values.
noCtxInterpretIO :: InterpretIO q
noCtxInterpretIO =
    InterpretIO
        $ const
        $ error
            "bootToken: TxBuild program issued a \
            \ctx query but no interpreter is wired"
