{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot
-- Description : Boot token minting transaction
-- License     : Apache-2.0
--
-- Top-level orchestrator for @POST \/tx\/boot@.
--
-- The transaction is described as a small program in
-- the 'Cardano.Node.Client.TxBuild' DSL: spend the
-- seed input, attach the cage script, mint +1 token,
-- pay the cage state to the script address, and add
-- collateral. The DSL handles ExUnits patching,
-- script-integrity hashing, fee balancing, and
-- change-output construction; this module only
-- describes the domain content.
--
-- The two domain helpers that remain
-- (asset-name derivation, on-chain state datum) live
-- in "Cardano.MPFS.TxBuilder.Real.Boot.Transaction".
-- Pre-resolved input decoding lives in
-- "Cardano.MPFS.TxBuilder.Real.Boot.Inputs".
--
-- __Invariant__: this module MUST NOT call
-- 'Cardano.MPFS.Provider.queryUTxOs'. Wallet UTxO
-- discovery is the responsibility of the caller (the
-- HTTP layer reads the indexer atomically and
-- supplies 'ResolvedWalletInput' values). See spec
-- @specs\/249-atomic-boot-handler@.
module Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    ) where

import Data.Functor.Const (Const)
import Data.Map.Strict qualified as Map
import Data.Void (Void)

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )

import Cardano.MPFS.Core.OnChain
    ( MintRedeemer (..)
    )
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
import Cardano.MPFS.TxBuilder.Real.Boot.Inputs
    ( InputRow (..)
    , decodeAll
    , ledgerPair
    , rowToWitness
    )
import Cardano.MPFS.TxBuilder.Real.Boot.Transaction
    ( bootAssetName
    , bootStateDatum
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , cagePolicyIdFromCfg
    , mkCageScript
    , txInToRef
    )

import Cardano.Node.Client.TxBuild
    ( InterpretIO (..)
    , TxBuild
    , attachScript
    , build
    , collateral
    , mint
    , payTo'
    , spend
    )

-- | Build a boot-token minting transaction.
--
-- Picks the seed input from the @[ResolvedWalletInput]@
-- list (sourced atomically from the local indexer by
-- the HTTP layer), describes the cage-protocol mint as
-- a 'TxBuild' program, then hands it to the DSL's
-- 'build' loop which handles ExUnits patching,
-- integrity-hash computation, fee balancing, and
-- change-output construction.
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
                an = bootAssetName seedRef
                policyId = cagePolicyIdFromCfg cfg
                script = mkCageScript cfg
                scriptAddr =
                    cageAddrFromCfg cfg (network cfg)
                mintAssets = Map.singleton an 1
                mintMA =
                    MultiAsset
                        $ Map.singleton policyId mintAssets
                stateValue =
                    MaryValue (Coin 2_000_000) mintMA
                redeemer = Minting (txInToRef seedRef)
                program :: TxBuild (Const ()) Void ()
                program = do
                    _ <- spend seedRef
                    attachScript script
                    mint policyId mintAssets redeemer
                    _ <-
                        payTo'
                            scriptAddr
                            stateValue
                            (bootStateDatum cfg addr)
                    collateral collatRef
                evalAdapter tx =
                    fmap
                        (fmap (either (Left . show) Right))
                        (evaluateTx prov tx)
            result <-
                build
                    pp
                    noCtxInterpretIO
                    evalAdapter
                    pickedLedgerPairs
                    addr
                    program
            case result of
                Left e ->
                    error
                        $ "bootToken: DSL build \
                          \failed: "
                            <> show e
                Right balanced ->
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

-- | No-op interpreter for the empty domain-query
-- context: boot has no @ctx@-driven values, so any
-- @ctx@ call is a programming error.
noCtxInterpretIO :: InterpretIO q
noCtxInterpretIO =
    InterpretIO
        $ const
        $ error
            "bootToken: TxBuild program issued a \
            \ctx query but no interpreter is wired"
