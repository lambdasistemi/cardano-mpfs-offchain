-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot
-- Description : Boot token minting transaction
-- License     : Apache-2.0
--
-- Top-level orchestrator for @POST \/tx\/boot@. The
-- per-step builders (asset name, datum, output, script,
-- body, tx) live in
-- "Cardano.MPFS.TxBuilder.Real.Boot.Components"; the
-- pre-resolved input decoding lives in
-- "Cardano.MPFS.TxBuilder.Real.Boot.Inputs".
--
-- __Invariant__: this module MUST NOT call
-- 'Cardano.MPFS.Provider.queryUTxOs'. Wallet UTxO
-- discovery is the responsibility of the caller (the
-- HTTP layer reads the indexer atomically and supplies
-- 'ResolvedWalletInput' values). See spec
-- @specs\/249-atomic-boot-handler@.
module Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    ) where

import Cardano.Ledger.Address (Addr)

import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BootProof (..)
    , BundleSnapshot
    , ProofEnvelope (..)
    , ResolvedWalletInput
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Boot.Components
    ( bootAssembledTx
    , bootAssetName
    , bootMintValue
    , bootScriptAndRedeemers
    , bootStateDatum
    , bootStateOutput
    , bootTxBody
    )
import Cardano.MPFS.TxBuilder.Real.Boot.Inputs
    ( InputRow (..)
    , decodeAll
    , ledgerPair
    , rowToWitness
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , evaluateAndBalance
    )

-- | Build a boot-token minting transaction.
--
-- Picks the seed input from the @[ResolvedWalletInput]@
-- list (sourced atomically from the local indexer by
-- the HTTP layer), composes the per-step builders from
-- "Cardano.MPFS.TxBuilder.Real.Boot.Components", then
-- evaluates and balances against the provider's
-- protocol parameters.
--
-- The @prov@ argument is consulted only for protocol
-- parameters and tx evaluation. Its @queryUTxOs@ field
-- is __forbidden on this path__ and not invoked.
bootTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Provider — used for protocol params + evaluate
    -- only. @queryUTxOs@ MUST NOT be called.
    -> BundleSnapshot
    -- ^ Snapshot this bundle will be anchored to
    -> [ResolvedWalletInput]
    -- ^ Pre-resolved wallet inputs from the indexer
    -> Addr
    -- ^ Owner address (receives change, owns the token)
    -> IO (ProofEnvelope BootProof)
bootTokenImpl cfg prov snap inputs addr = do
    pp <- queryProtocolParams prov
    case decodeAll inputs of
        Left err ->
            error
                $ "bootToken: failed to decode \
                  \indexer-resolved TxOut bytes: "
                    <> show err
        Right [] ->
            error
                "bootToken: empty input list — \
                \HTTP layer should have rejected \
                \with NoUtxos"
        Right (seedRow : restRows) -> do
            let pickedRows = case restRows of
                    [] -> [seedRow]
                    (u : _) -> [seedRow, u]
                pickedLedgerPairs =
                    map ledgerPair pickedRows
                seedRef = rowRef seedRow
                collatRef =
                    fst (last pickedLedgerPairs)
                policyId = cagePolicyIdFromCfg cfg
                an = bootAssetName seedRef
                mintMA = bootMintValue policyId an
                stateOut =
                    bootStateOutput
                        cfg
                        mintMA
                        (bootStateDatum cfg addr)
                (script, scriptHash, rdmrs) =
                    bootScriptAndRedeemers cfg seedRef
                body =
                    bootTxBody
                        pp
                        seedRef
                        collatRef
                        stateOut
                        mintMA
                        rdmrs
                tx =
                    bootAssembledTx
                        body
                        scriptHash
                        script
                        rdmrs
            balanced <-
                evaluateAndBalance
                    prov
                    pp
                    pickedLedgerPairs
                    addr
                    tx
            pure
                ProofEnvelope
                    { envTx = balanced
                    , envSnapshot = snap
                    , envProof =
                        BootProof
                            { bootFunding =
                                map
                                    rowToWitness
                                    pickedRows
                            }
                    }
