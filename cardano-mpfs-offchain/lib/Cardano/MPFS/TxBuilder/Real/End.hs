{-# LANGUAGE EmptyCase #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.End
-- Description : End token (burn) transaction
-- License     : Apache-2.0
--
-- Builds the burn transaction that retires an MPFS
-- cage token. Consumes the State UTxO with an @End@
-- spending redeemer, mints -1 with @Burning@, and
-- returns remaining ADA to the owner.
module Cardano.MPFS.TxBuilder.Real.End
    ( endTokenImpl
    ) where

import Data.Map.Strict qualified as Map
import Data.Void (Void)
import Lens.Micro ((^.))

import Cardano.Ledger.Address (Addr)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Data.ByteString.Short qualified as SBS

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , MintRedeemer (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , TokenId (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , EndProof (..)
    , ProofEnvelope (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Tx.Build qualified as Tx

import Data.List (sortOn)
import Data.Ord (Down (..))

import Cardano.Ledger.Api.Tx.Out (coinTxOutL)

data NoCtx a

interpretNoCtx :: NoCtx a -> IO a
interpretNoCtx q = case q of {}

-- | Build an end-token (burn) transaction.
--
-- Consumes the state UTxO with an @End@ spending
-- redeemer, mints -1 of the cage token with a
-- @Burning@ minting redeemer, and returns remaining
-- ADA to the owner. The token is permanently
-- destroyed.
endTokenImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> UtxoProofFn
    -- ^ CSMT inclusion proof lookup
    -> BundleSnapshot
    -- ^ Snapshot this bundle will be anchored to
    -> TokenId
    -- ^ Token to retire
    -> Addr
    -- ^ Fee-paying address (receives remaining ADA)
    -> IO (ProofEnvelope EndProof)
endTokenImpl cfg prov proofFn snap tid addr = do
    -- 1. Query cage UTxOs
    let scriptAddr =
            cageAddrFromCfg cfg (network cfg)
    cageUtxos <- queryUTxOs prov scriptAddr
    -- 2. Find state UTxO
    let policyId = cagePolicyIdFromCfg cfg
    stateUtxo <-
        case findStateUtxo policyId tid cageUtxos of
            Nothing ->
                throwTxBuilderFailure
                    "endToken: state UTxO not found"
            Just x -> pure x
    let (stateIn, stateOut) = stateUtxo
    -- 3. Extract owner from state datum
    stateDatum <-
        case extractCageDatum stateOut of
            Just (StateDatum s) -> pure s
            _ ->
                throwTxBuilderFailure
                    "endToken: invalid state datum"
    let OnChainTokenState
            { stateOwner =
                BuiltinByteString ownerBs
            } = stateDatum
        ownerKh = addrWitnessKeyHash ownerBs
    -- 4. Get wallet UTxO for fees
    pp <- queryProtocolParams prov
    walletUtxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        walletUtxos of
        [] ->
            throwTxBuilderFailure
                "endToken: no UTxOs"
        (u : _) -> pure u
    let assetName = unTokenId tid
        script = mkCageScript cfg
        spendRedeemer = End
        onChainTid =
            OnChainTokenId
                $ BuiltinByteString
                $ SBS.fromShort
                $ let AssetName sbs = unTokenId tid
                  in  sbs
        mintRedeemer = Burning onChainTid
        prog = do
            _ <- Tx.spendScript stateIn spendRedeemer
            Tx.attachScript script
            Tx.mint
                policyId
                (Map.singleton assetName (-1))
                mintRedeemer
            Tx.requireSignature
                (witnessKeyHashToGuard ownerKh)
            Tx.collateral (fst feeUtxo)
        evalTx tx =
            Map.map
                (either (Left . show) Right)
                <$> evaluateTx prov tx
    result <-
        Tx.build
            (Tx.mkPParamsBound pp)
            (Tx.InterpretIO interpretNoCtx)
            evalTx
            [feeUtxo, stateUtxo]
            []
            addr
            (prog :: Tx.TxBuild NoCtx Void ())
    balanced <-
        case result of
            Right tx -> pure tx
            Left err ->
                throwTxBuilderFailure
                    $ "endToken: TxBuild failed: "
                        <> show err
    stateWitness <- witness proofFn stateUtxo
    fundingWitnesses <- witnesses proofFn [feeUtxo]
    pure
        ProofEnvelope
            { envTx = balanced
            , envSnapshot = snap
            , envProof =
                EndProof
                    { endState = stateWitness
                    , endFunding = fundingWitnesses
                    }
            }
