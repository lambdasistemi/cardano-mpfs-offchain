{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications #-}

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
module Cardano.MPFS.TxBuilder.Real.Boot
    ( bootTokenImpl
    ) where

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
    ( datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Binary
    ( decodeFull
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

import Cardano.MPFS.Context (AtomicCageReader)
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
    , TxIn
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BootProof (..)
    , ProofEnvelope (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal

-- | Build a boot-token minting transaction.
--
-- Picks a wallet UTxO as seed for the asset name,
-- mints +1 token at the cage policy, and creates
-- a State UTxO with empty root and tip.
bootTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface (used only for
    -- 'queryProtocolParams' and 'evaluateTx' /
    -- 'evaluateAndBalance', never for UTxO state).
    -> AtomicCageReader IO
    -- ^ Atomic indexer reader. Returns the
    -- 'BundleSnapshot' together with the resolved
    -- @TxOut@ bytes and CSMT inclusion proof for
    -- every supplied 'TxIn', all read in ONE
    -- database transaction. This is the only
    -- source of UTxO state on the tx-build hot
    -- path (#250 / #252).
    -> Addr
    -- ^ Owner address (receives change, owns the token)
    -> [TxIn]
    -- ^ Wallet-supplied funding inputs. The wallet
    -- selects which UTxOs to spend; the server does
    -- not pick them itself (per Principle IV —
    -- External Signing — and #252: cardano-node's
    -- @GetUTxOByAddress@ is forbidden because it
    -- is O(total UTxOs on chain)). The first ref
    -- becomes the seed for asset-name derivation
    -- and the spending input; the last ref becomes
    -- the collateral input. At least one ref is
    -- required.
    -> IO (ProofEnvelope BootProof)
bootTokenImpl cfg prov atomicReader addr fundingRefs = do
    pp <- queryProtocolParams prov
    case fundingRefs of
        [] ->
            error
                "bootToken: empty funding inputs \
                \(wallet must supply at least one)"
        (seedRef : _) -> do
            let collateralRef =
                    last fundingRefs
            -- ATOMIC INDEXER READ: snapshot +
            -- (TxOut bytes, inclusion proof) for
            -- every supplied input, all in one DB
            -- transaction. No cardano-node UTxO
            -- query is involved: the indexer is the
            -- single source of truth on the
            -- tx-build path (#250, #252).
            mAtomic <-
                atomicReader fundingRefs
            (snap, triples) <-
                case mAtomic of
                    Nothing ->
                        error
                            "bootToken: snapshot or \
                            \input data unavailable \
                            \in indexer (chain \
                            \follower behind, or \
                            \input not indexed)"
                    Just (s, ts) -> pure (s, ts)
            -- Decode the resolved 'TxOut' bytes
            -- once, here, for tx balancing.
            let decodeTxOut bs =
                    case decodeFull
                        (natVersion @11)
                        ( BSL.fromStrict
                            bs
                        ) of
                        Left e ->
                            error
                                $ "bootToken: \
                                  \TxOut CBOR \
                                  \decode failed: "
                                    <> show e
                        Right t -> t
                allInputUtxos =
                    [ (r, decodeTxOut v)
                    | (r, v, _) <- triples
                    ]
                proofMap =
                    Map.fromList
                        [ (r, p)
                        | (r, _, p) <- triples
                        ]
                proofFn ref =
                    pure
                        $ Map.lookup
                            ref
                            proofMap
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
                                collateralRef
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
                    allInputUtxos
                    addr
                    tx
            fundingWitnesses <-
                witnesses proofFn allInputUtxos
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
