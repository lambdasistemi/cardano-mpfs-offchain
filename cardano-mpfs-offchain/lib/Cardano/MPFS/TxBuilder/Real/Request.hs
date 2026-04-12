-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Request
-- Description : Request insert/delete transactions
-- License     : Apache-2.0
--
-- Builds request transactions for inserting or
-- deleting a key in a token's trie. No script
-- execution occurs — the transaction simply pays to
-- the cage address with an inline 'RequestDatum'.
-- The locked ADA includes the token's @tip@.
module Cardano.MPFS.TxBuilder.Real.Request
    ( requestInsertImpl
    , requestDeleteImpl
    , requestUpdateImpl
    , requestLockedAda
    ) where

import Data.ByteString (ByteString)
import Data.List (sortOn)
import Data.Ord (Down (..))
import Data.Sequence.Strict qualified as StrictSeq
import Data.Time.Clock.POSIX (getPOSIXTime)
import Lens.Micro ((&), (.~), (^.))

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Api.Tx
    ( Tx
    , mkBasicTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( mkBasicTxBody
    , outputsTxBodyL
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

import Cardano.MPFS.Core.OnChain
    ( OnChainOperation (..)
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , PParams
    , TokenId
    , TokenState (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State (State (..), Tokens (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
import Cardano.Node.Client.Balance (balanceTx)

-- | Build a request-insert transaction.
--
-- No script execution — just pays to the cage
-- address with an inline 'RequestDatum'.
requestInsertImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Token state (to look up tip)
    -> TokenId
    -- ^ Token whose trie to modify
    -> ByteString
    -- ^ Key to insert
    -> ByteString
    -- ^ Value to insert
    -> Addr
    -- ^ Requester's address (pays fee, owns request)
    -> IO (Tx ConwayEra)
requestInsertImpl cfg prov st tid key value addr =
    do
        mTs <- getToken (tokens st) tid
        TokenState{tip = Coin mf} <- case mTs of
            Nothing ->
                error
                    "requestInsert: unknown token"
            Just x -> pure x
        pp <- queryProtocolParams prov
        utxos <- queryUTxOs prov addr
        feeUtxo <- case sortOn
            (Down . (^. coinTxOutL) . snd)
            utxos of
            [] ->
                error "requestInsert: no UTxOs"
            (u : _) -> pure u
        now <- currentPosixMs
        let datum =
                mkRequestDatum
                    tid
                    addr
                    key
                    (OpInsert value)
                    mf
                    now
            scriptAddr = cageAddrFromCfg cfg (network cfg)
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
            body =
                mkBasicTxBody
                    & outputsTxBodyL
                        .~ StrictSeq.singleton
                            txOut
            tx = mkBasicTx body
        case balanceTx pp [feeUtxo] addr tx of
            Left err ->
                error
                    $ "requestInsert: "
                        <> show err
            Right balanced -> pure balanced

-- | Build a request-delete transaction.
-- Same structure as requestInsert with 'OpDelete'.
-- The caller must supply the old value (needed for
-- the on-chain proof).
requestDeleteImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Token state (to look up tip)
    -> TokenId
    -- ^ Token whose trie to modify
    -> ByteString
    -- ^ Key to delete
    -> ByteString
    -- ^ Old value being deleted
    -> Addr
    -- ^ Requester's address
    -> IO (Tx ConwayEra)
requestDeleteImpl cfg prov st tid key val addr = do
    mTs <- getToken (tokens st) tid
    TokenState{tip = Coin mf} <- case mTs of
        Nothing ->
            error "requestDelete: unknown token"
        Just x -> pure x
    pp <- queryProtocolParams prov
    utxos <- queryUTxOs prov addr
    feeUtxo <- case sortOn
        (Down . (^. coinTxOutL) . snd)
        utxos of
        [] -> error "requestDelete: no UTxOs"
        (u : _) -> pure u
    now <- currentPosixMs
    let datum =
            mkRequestDatum
                tid
                addr
                key
                (OpDelete val)
                mf
                now
        scriptAddr = cageAddrFromCfg cfg (network cfg)
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
        body =
            mkBasicTxBody
                & outputsTxBodyL
                    .~ StrictSeq.singleton txOut
        tx = mkBasicTx body
    case balanceTx pp [feeUtxo] addr tx of
        Left err ->
            error
                $ "requestDelete: "
                    <> show err
        Right balanced -> pure balanced

-- | Build a request transaction for updating an
-- existing key's value. Same structure as
-- requestDelete but uses 'OpUpdate'.
requestUpdateImpl
    :: CageConfig
    -- ^ Cage script config
    -> Provider IO
    -- ^ Blockchain query interface
    -> State IO
    -- ^ Token state (to look up tip)
    -> TokenId
    -- ^ Token whose trie to modify
    -> ByteString
    -- ^ Key to update
    -> ByteString
    -- ^ Old value (must match current)
    -> ByteString
    -- ^ New value
    -> Addr
    -- ^ Requester's address
    -> IO (Tx ConwayEra)
requestUpdateImpl
    cfg
    prov
    st
    tid
    key
    oldVal
    newVal
    addr = do
        mTs <- getToken (tokens st) tid
        TokenState{tip = Coin mf} <- case mTs of
            Nothing ->
                error
                    "requestUpdate: unknown token"
            Just x -> pure x
        pp <- queryProtocolParams prov
        utxos <- queryUTxOs prov addr
        feeUtxo <- case sortOn
            (Down . (^. coinTxOutL) . snd)
            utxos of
            [] ->
                error "requestUpdate: no UTxOs"
            (u : _) -> pure u
        now <- currentPosixMs
        let datum =
                mkRequestDatum
                    tid
                    addr
                    key
                    (OpUpdate oldVal newVal)
                    mf
                    now
            scriptAddr =
                cageAddrFromCfg cfg (network cfg)
            draftOut =
                mkBasicTxOut
                    scriptAddr
                    (inject (Coin 0))
                    & datumTxOutL
                        .~ mkInlineDatum datum
            refundDraft =
                mkBasicTxOut
                    addr
                    (inject (Coin 0))
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
            body =
                mkBasicTxBody
                    & outputsTxBodyL
                        .~ StrictSeq.singleton txOut
            tx = mkBasicTx body
        case balanceTx pp [feeUtxo] addr tx of
            Left err ->
                error
                    $ "requestUpdate: "
                        <> show err
            Right balanced -> pure balanced

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
-- The actual tx fee share is NOT included here —
-- the user decides how much extra to lock. If the
-- refund after fee+tip is below minUTxO, the update
-- tx builder will skip or fail the request.
--
-- Returns @max(reqMinUTxO, tip + refundMinUTxO)@.
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
        -- Fee buffer: generous estimate of the
        -- per-request fee share in an update tx.
        -- Excess becomes a larger refund.
        feeBuffer = 600_000
        locked = tip + feeBuffer + refMin
        -- Ensure the request output itself meets
        -- minUTxO with the computed value.
        adjusted =
            getMinCoinTxOut
                pp
                ( reqDraft
                    & valueTxOutL
                        .~ inject (Coin locked)
                )
    in  max adjusted (Coin locked)

-- | Get current POSIX time in milliseconds.
currentPosixMs :: IO Integer
currentPosixMs = do
    t <- getPOSIXTime
    pure $ floor (t * 1000)
