{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot
-- Description : Boot token minting transaction
-- License     : Apache-2.0
--
-- Builds the minting transaction for a new MPFS cage
-- token. Picks a wallet UTxO as seed for asset-name
-- derivation, mints +1 token at the cage policy, and
-- creates a State UTxO with empty root and configured
-- default parameters.
--
-- __Invariant__: this module MUST NOT call
-- 'Cardano.MPFS.Provider.queryUTxOs'. Wallet UTxO
-- discovery is the responsibility of the caller (the
-- HTTP layer's 'Cardano.MPFS.Context.AtomicCageReader');
-- the builder consumes the pre-resolved tuples and
-- builds the transaction. See spec
-- @specs\/249-atomic-boot-handler@.
module Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Sequence.Strict qualified as StrictSeq
import Data.Set qualified as Set
import Lens.Micro ((&), (.~))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Alonzo.Scripts (AsIx (..))
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.Tx
    ( mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull
    , natVersion
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Core (hashScript)
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , deriveAssetName
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , ConwayEra
    , TxIn
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BootProof (..)
    , BundleSnapshot
    , ProofEnvelope (..)
    , ResolvedWalletInput
    , WitnessedInput (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build a boot-token minting transaction.
--
-- Picks the seed input from the @[ResolvedWalletInput]@
-- list (sourced atomically from the local indexer by an
-- 'Cardano.MPFS.Context.AtomicCageReader'), derives the
-- asset name from it, mints +1 token at the cage policy,
-- and creates a State UTxO with empty root and default
-- parameters.
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
            -- 1. Derive asset name from seed
            let onChainRef = txInToRef seedRef
                assetNameBs =
                    deriveAssetName onChainRef
                assetName =
                    AssetName
                        (SBS.toShort assetNameBs)
            -- 2. Build mint value (+1 token)
            let policyId =
                    cagePolicyIdFromCfg cfg
                mintMA =
                    MultiAsset
                        $ Map.singleton
                            policyId
                        $ Map.singleton
                            assetName
                            1
            -- 3. Build output datum
            let stateDatum =
                    StateDatum
                        OnChainTokenState
                            { stateOwner =
                                BuiltinByteString
                                    (addrKeyHashBytes addr)
                            , stateRoot =
                                OnChainRoot emptyRoot
                            , stateMaxFee =
                                let Coin c =
                                        defaultTip cfg
                                in  c
                            , stateProcessTime =
                                defaultProcessTime cfg
                            , stateRetractTime =
                                defaultRetractTime cfg
                            }
                datumData = toPlcData stateDatum
            -- 4. Build output with ada + token
            let scriptAddr =
                    cageAddrFromCfg
                        cfg
                        (network cfg)
                outValue =
                    MaryValue
                        (Coin 2_000_000)
                        mintMA
                txOut =
                    mkBasicTxOut
                        scriptAddr
                        outValue
                        & datumTxOutL
                            .~ mkInlineDatum
                                datumData
            -- 5. Build script + redeemer
            let script = mkCageScript cfg
                scriptHash = hashScript script
                redeemer =
                    Minting onChainRef
                mintPurpose =
                    ConwayMinting (AsIx 0)
                redeemers =
                    Redeemers
                        $ Map.singleton
                            mintPurpose
                            ( toLedgerData redeemer
                            , placeholderExUnits
                            )
            -- 6. Build tx body
            let integrity =
                    computeScriptIntegrity
                        pp
                        redeemers
                body =
                    mkBasicTxBody
                        & inputsTxBodyL
                            .~ Set.singleton
                                seedRef
                        & outputsTxBodyL
                            .~ StrictSeq.singleton
                                txOut
                        & mintTxBodyL .~ mintMA
                        & collateralInputsTxBodyL
                            .~ Set.singleton
                                ( fst
                                    $ last
                                        pickedLedgerPairs
                                )
                        & scriptIntegrityHashTxBodyL
                            .~ integrity
                tx =
                    mkBasicTx body
                        & witsTxL . scriptTxWitsL
                            .~ Map.singleton
                                scriptHash
                                script
                        & witsTxL . rdmrsTxWitsL
                            .~ redeemers
            -- 7. Evaluate and balance
            balanced <-
                evaluateAndBalance
                    prov
                    pp
                    pickedLedgerPairs
                    addr
                    tx
            let fundingWitnesses =
                    map rowToWitness pickedRows
            pure
                ProofEnvelope
                    { envTx = balanced
                    , envSnapshot = snap
                    , envProof =
                        BootProof
                            { bootFunding =
                                fundingWitnesses
                            }
                    }

-- | Decoded view of an indexer-resolved input. The
-- ledger 'TxOut' is needed to feed
-- 'evaluateAndBalance'; the original CBOR bytes are
-- preserved so they pass through to
-- 'witnessedTxOut' verbatim (matching what the
-- indexer applied and what on-chain validators
-- compute).
data InputRow = InputRow
    { rowRef :: TxIn
    , rowOut :: TxOut ConwayEra
    , rowOutBytes :: ByteString
    , rowProof :: ByteString
    }

decodeAll
    :: [ResolvedWalletInput]
    -> Either DecoderError [InputRow]
decodeAll = traverse decodeOne
  where
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

ledgerPair
    :: InputRow -> (TxIn, TxOut ConwayEra)
ledgerPair r = (rowRef r, rowOut r)

rowToWitness :: InputRow -> WitnessedInput
rowToWitness r =
    WitnessedInput
        { witnessedRef = rowRef r
        , witnessedTxOut = rowOutBytes r
        , witnessedCsmtProof = rowProof r
        }
