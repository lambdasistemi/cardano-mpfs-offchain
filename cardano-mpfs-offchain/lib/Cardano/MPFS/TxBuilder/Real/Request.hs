-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Request
-- Description : Request insert/delete/update transactions
-- License     : Apache-2.0
--
-- Builds request transactions for inserting, deleting,
-- or updating a key in a token's trie. No script
-- execution occurs — the transaction simply pays to
-- the cage address with an inline 'RequestDatum'.
-- The locked ADA includes the token's @tip@ plus a
-- fee buffer for the oracle's update transaction.
module Cardano.MPFS.TxBuilder.Real.Request
    ( requestInsertImpl
    , requestDeleteImpl
    , requestUpdateImpl
    , requestLockedAda
    ) where

import Data.ByteString (ByteString)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Ord (Down (..))
import Data.Void (Void)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Api.PParams
    ( CoinPerByte (..)
    , ppMaxTxExUnitsL
    , ppPricesL
    , ppTxFeeFixedL
    , ppTxFeePerByteL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.BaseTypes (Inject (..))
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Plutus.ExUnits
    ( txscriptfee
    )

import Cardano.MPFS.Core.OnChain
    ( OnChainOperation (..)
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , LocatedTokenState (..)
    , PParams
    , TokenId
    , TokenState (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..), Tokens (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , ProofEnvelope (..)
    , RequestProof (..)
    , UtxoProofFn
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Tx.Build qualified as Tx

data NoCtx a

-- | Build a request-insert transaction.
requestInsertImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> UtxoProofFn
    -> BundleSnapshot
    -> TokenId
    -> ByteString
    -- ^ Key to insert
    -> ByteString
    -- ^ Value to insert
    -> Addr
    -> IO (ProofEnvelope RequestProof)
requestInsertImpl cfg prov st proofFn snap tid key value =
    requestImpl
        cfg
        prov
        st
        proofFn
        snap
        tid
        key
        (OpInsert value)

-- | Build a request-delete transaction.
requestDeleteImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> UtxoProofFn
    -> BundleSnapshot
    -> TokenId
    -> ByteString
    -- ^ Key to delete
    -> ByteString
    -- ^ Old value (for on-chain proof)
    -> Addr
    -> IO (ProofEnvelope RequestProof)
requestDeleteImpl cfg prov st proofFn snap tid key val =
    requestImpl
        cfg
        prov
        st
        proofFn
        snap
        tid
        key
        (OpDelete val)

-- | Build a request-update transaction.
requestUpdateImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> UtxoProofFn
    -> BundleSnapshot
    -> TokenId
    -> ByteString
    -- ^ Key to update
    -> ByteString
    -- ^ Old value (must match current)
    -> ByteString
    -- ^ New value
    -> Addr
    -> IO (ProofEnvelope RequestProof)
requestUpdateImpl
    cfg
    prov
    st
    proofFn
    snap
    tid
    key
    oldVal
    newVal =
        requestImpl
            cfg
            prov
            st
            proofFn
            snap
            tid
            key
            (OpUpdate oldVal newVal)

-- | Generic request transaction builder.
--
-- 1. Look up the token's tip.
-- 2. Pick the largest wallet UTxO for fees.
-- 3. Build an inline 'RequestDatum' with the
--    operation and current timestamp.
-- 4. Compute locked ADA (tip + fee buffer +
--    minUTxO for the refund output).
-- 5. Balance and return.
requestImpl
    :: CageConfig
    -> Provider IO
    -> State IO
    -> UtxoProofFn
    -> BundleSnapshot
    -> TokenId
    -> ByteString
    -- ^ Trie key
    -> OnChainOperation
    -- ^ Insert, Delete, or Update
    -> Addr
    -- ^ Requester's address
    -> IO (ProofEnvelope RequestProof)
requestImpl cfg prov st proofFn snap tid key op addr = do
    mTs <- getToken (tokens st) tid
    LocatedTokenState
        { tokenState = TokenState{tip = Coin mf}
        } <-
        case mTs of
            Nothing ->
                error "requestImpl: unknown token"
            Just x -> pure x
    pp <- queryProtocolParams prov
    utxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        utxos of
        [] -> error "requestImpl: no UTxOs"
        (u : _) -> pure u
    now <- currentPosixMs
    let datum =
            mkRequestDatum tid addr key op mf now
        scriptAddr =
            requestAddrFromCfg
                cfg
                tid
                (network cfg)
        draftOut =
            mkBasicTxOut
                scriptAddr
                (inject (Coin 0))
                & datumTxOutL
                    .~ mkInlineDatum datum
        refundDraft =
            mkBasicTxOut addr (inject (Coin 0))
        minAda =
            requestLockedAda
                pp
                draftOut
                refundDraft
                mf
        txOut =
            mkBasicTxOut
                scriptAddr
                (inject minAda)
                & datumTxOutL
                    .~ mkInlineDatum datum
        prog = do
            _ <- Tx.spend (fst feeUtxo)
            _ <- Tx.output txOut
            pure ()
    result <-
        Tx.build
            (Tx.mkPParamsBound pp)
            (Tx.InterpretIO (const (pure undefined)))
            (const $ pure Map.empty)
            [feeUtxo]
            []
            addr
            (prog :: Tx.TxBuild NoCtx Void ())
    case result of
        Left err ->
            error
                $ "requestImpl: TxBuild failed: "
                    <> show err
        Right tx -> do
            fundingWitnesses <-
                witnesses proofFn [feeUtxo]
            pure
                ProofEnvelope
                    { envTx = tx
                    , envSnapshot = snap
                    , envProof =
                        RequestProof
                            { requestFunding =
                                fundingWitnesses
                            }
                    }

-- | Compute the ADA to lock in a request output.
--
-- Two constraints must be satisfied:
--
-- 1. The locked amount >= minUTxO for the request
--    output (which carries an inline datum).
-- 2. After the oracle deducts @tip@, the
--    remaining ADA (the refund) >= minUTxO for the
--    refund output (a plain payment).
--
-- A protocol-parameter-derived fee buffer covers a
-- conservative per-request share of the oracle's
-- update tx. Excess becomes a larger refund.
--
-- Returns @max(reqMinUTxO, tip + buffer + refundMinUTxO)@.
requestLockedAda
    :: PParams ConwayEra
    -- ^ Protocol parameters
    -> TxOut ConwayEra
    -- ^ Draft request output (with datum)
    -> TxOut ConwayEra
    -- ^ Draft refund output (plain address)
    -> Integer
    -- ^ tip (lovelace)
    -> Coin
requestLockedAda pp reqDraft refDraft tip =
    let Coin refMin =
            getMinCoinTxOut pp refDraft
        Coin feeBuffer = requestFeeBufferUpperBound pp
        locked = tip + feeBuffer + refMin
        adjusted =
            getMinCoinTxOut
                pp
                ( reqDraft
                    & valueTxOutL
                        .~ inject (Coin locked)
                )
    in  max adjusted (Coin locked)

-- Keep this in lockstep with
-- Cardano.MPFS.Client.Cage.Request.requestFeeBufferUpperBound.
requestFeeBufferUpperBound :: PParams ConwayEra -> Coin
requestFeeBufferUpperBound pp =
    let CoinPerByte minFeeACompact = pp ^. ppTxFeePerByteL
        Coin minFeeA = fromCompact minFeeACompact
        Coin minFeeB = pp ^. ppTxFeeFixedL
        Coin scriptFee =
            txscriptfee (pp ^. ppPricesL) (pp ^. ppMaxTxExUnitsL)
    in  Coin
            ( minFeeB
                + minFeeA * maxUpdateTxBytes
                + scriptFee
            )

-- Note [size bound]
-- The request UTxO pre-pays the oracle's later update transaction. We do
-- not know that future transaction's exact CBOR size when building the
-- request, so this uses a 16 KiB envelope for a single-request update with
-- state, request, wallet, scripts, redeemers, one state output, one refund,
-- and change. Excess lovelace is returned by the positioned refund output.
maxUpdateTxBytes :: Integer
maxUpdateTxBytes = 16384
