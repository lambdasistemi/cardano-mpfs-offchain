{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Server
-- Description : Servant server wiring for MPFS HTTP API
-- License     : Apache-2.0
--
-- Wires the Servant handlers to the 'Context' record
-- of functions. Each handler extracts the relevant
-- interface from 'Context' and delegates to it.
module Cardano.MPFS.HTTP.Server
    ( -- * Application
      mkApp
    , mkBootFacts
    , mkRequestInsertFacts
    , mkRequestDeleteFacts
    , mkEndFacts
    ) where

import Control.Applicative ((<|>))
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Base16 qualified as B16
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import Servant
    ( Application
    , Handler
    , NoContent (..)
    , ServerError (..)
    , err400
    , err404
    , err502
    , err503
    , errBody
    , serve
    , throwError
    , (:<|>) (..)
    )
import Text.Read (readMaybe)

import Data.ByteString (ByteString)
import Data.ByteString.Lazy.Char8 qualified as BL

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Api.Tx (Tx)
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull'
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Hashes
    ( extractHash
    , unsafeMakeSafeHash
    )
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn
    , mkTxInPartial
    )

import Cardano.MPFS.API.Types.Facts
    ( EndFacts
    , RequestDeleteFacts
    , RequestInsertFacts
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
    , PParams
    , Root (..)
    , SlotNo (..)
    , TokenId
    )
import Cardano.MPFS.HTTP.API (API)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Swagger
    ( SwaggerAPI
    , swaggerServer
    )
import Cardano.MPFS.HTTP.Types
    ( BootFacts (..)
    , BootRequest (..)
    , ChainPointJSON (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , FactResponse (..)
    , FactWitness (..)
    , InsertRequest (..)
    , ProofResponse (..)
    , RejectRequest (..)
    , RejectTxResponse
    , RequestTxResponse
    , RequestsResponse (..)
    , RetractRequest (..)
    , RetractTxResponse
    , StatusResponse (..)
    , SubmitRequest (..)
    , SweepRequest (..)
    , SweepTxResponse (..)
    , TokenIdJSON
    , TokenResponse (..)
    , UnverifiedPParams (..)
    , UpdateRequest (..)
    , UpdateTxResponse
    , UpdateValueRequest (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    , bundleSnapshotToJSON
    , mkRejectTxResponse
    , mkRequestTxResponse
    , mkRetractTxResponse
    , mkSweepTxResponse
    , mkUpdateTxResponse
    , parseAddr
    , requestToJSON
    , resolvedWalletInputToUtxoEntry
    , tokenIdFromJSON
    , tokenIdToJSON
    , tokenStateToJSON
    , txInToJSON
    )
import Cardano.MPFS.HTTP.Types.Facts
    ( mkEndFacts
    , mkRequestDeleteFacts
    , mkRequestInsertFacts
    )
import Cardano.MPFS.Indexer.Reads
    ( readRequestSetAt
    , readSnapshot
    , readStateUtxoAt
    , readWalletInputsAt
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.UTxOCSMT.Application.Metrics
    ( Metrics (..)
    , renderPrometheus
    )

import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Submitter qualified as Sub
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    )
import Cardano.MPFS.TxBuilder qualified as Tx
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real (sweepUtxoImpl)
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , cagePolicyIdFromCfg
    , currentPosixMs
    , requestAddrFromCfg
    )

-- | Combined API with Swagger UI.
type FullAPI = SwaggerAPI :<|> API

-- | Build a WAI 'Application' from a 'Context IO'.
mkApp :: Context IO -> Application
mkApp ctx =
    serve (Proxy @FullAPI)
        $ swaggerServer
            :<|> metricsPrometheusHandler ctx
            :<|> metricsHandler ctx
            :<|> statusHandler ctx
            :<|> tokensHandler ctx
            :<|> tokenHandler ctx
            :<|> tokenRootHandler ctx
            :<|> tokenFactHandler ctx
            :<|> tokenProofHandler ctx
            :<|> tokenRequestsHandler ctx
            :<|> utxoResolveHandler ctx
            :<|> utxoProofHandler ctx
            :<|> utxoRootHandler ctx
            :<|> txAwaitHandler ctx
            :<|> factsBootHandler ctx
            :<|> factsRequestInsertHandler ctx
            :<|> factsRequestDeleteHandler ctx
            :<|> factsEndHandler ctx
            :<|> txUpdateValueHandler ctx
            :<|> txRejectHandler ctx
            :<|> txUpdateHandler ctx
            :<|> txRetractHandler ctx
            :<|> txSweepHandler ctx
            :<|> txSubmitHandler ctx

-- ---------------------------------------------------------
-- Metrics handlers
-- ---------------------------------------------------------

-- | @GET \/metrics\/prometheus@
metricsPrometheusHandler
    :: Context IO -> Handler Text
metricsPrometheusHandler ctx = do
    mm <- liftIO $ readMetrics ctx
    case mm of
        Just m -> pure $ renderPrometheus m
        Nothing -> throwError err404

-- | @GET \/metrics@
metricsHandler
    :: Context IO
    -> Handler Metrics
metricsHandler ctx = do
    mm <- liftIO $ readMetrics ctx
    case mm of
        Just m -> pure m
        Nothing -> throwError err404

-- ---------------------------------------------------------
-- Query handlers
-- ---------------------------------------------------------

statusHandler :: Context IO -> Handler StatusResponse
statusHandler ctx = do
    mm <- liftIO $ readMetrics ctx
    mcp <-
        liftIO
            $ St.getCheckpoint
                (St.checkpoints (state ctx))
    mRoot <- liftIO $ utxoRoot ctx
    pure
        StatusResponse
            { tipSlot =
                maybe
                    0
                    unSlotNo
                    (chainTipSlot =<< mm)
            , tipBlockId =
                maybe
                    (Hex mempty)
                    (Hex . unBlockId . snd)
                    mcp
            , checkpointSlot =
                fmap (unSlotNo . fst) mcp
            , checkpointBlockId =
                fmap (Hex . unBlockId . snd) mcp
            , currentUtxoRoot = fmap Hex mRoot
            }

tokensHandler
    :: Context IO -> Handler [TokenIdJSON]
tokensHandler ctx = do
    tids <-
        liftIO
            $ St.listTokens (St.tokens (state ctx))
    pure (map tokenIdToJSON tids)

tokenHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler TokenResponse
tokenHandler ctx tokenId = do
    let tid = tokenIdFromJSON tokenId
    mts <-
        liftIO
            $ St.getToken (St.tokens (state ctx)) tid
    case mts of
        Nothing -> throwError err404
        Just
            LocatedTokenState
                { tokenStateRef
                , tokenState = ts
                } -> do
                snapshot <- requireSnapshot ctx
                witness <-
                    requireUtxoWitness ctx tokenStateRef
                pure
                    TokenResponse
                        { trSnapshot = snapshot
                        , trState =
                            WitnessedTokenState
                                { wtsUtxo = witness
                                , wtsState =
                                    tokenStateToJSON ts
                                }
                        }

-- | Look up an indexed token state by id, or @404@.
requireToken
    :: Context IO
    -> TokenId
    -> Handler LocatedTokenState
requireToken ctx tid = do
    mts <-
        liftIO
            $ St.getToken (St.tokens (state ctx)) tid
    case mts of
        Nothing -> throwError err404
        Just lts -> pure lts

-- | Read the current 'VerificationSnapshot' from
-- context, or 503 if the indexer has not yet
-- produced a UTxO-CSMT root or a checkpoint.
requireSnapshot
    :: Context IO -> Handler VerificationSnapshot
requireSnapshot ctx = do
    mRoot <- liftIO $ utxoRoot ctx
    mCp <-
        liftIO
            $ St.getCheckpoint
                (St.checkpoints (state ctx))
    case (mRoot, mCp) of
        (Just r, Just (SlotNo s, BlockId b)) ->
            pure
                VerificationSnapshot
                    { vsUtxoRoot = Hex r
                    , vsChainPoint =
                        ChainPointJSON
                            { cpSlot = s
                            , cpBlockId = Hex b
                            }
                    }
        _ ->
            throwError
                err503
                    { errBody =
                        "Verification snapshot \
                        \not yet available"
                    }

-- | Read the current ledger-native 'BundleSnapshot'
-- from context, or 503 if the indexer has not yet
-- produced a UTxO-CSMT root or a checkpoint.
requireBundleSnapshot
    :: Context IO -> Handler BundleSnapshot
requireBundleSnapshot ctx = do
    mRoot <- liftIO $ utxoRoot ctx
    mCp <-
        liftIO
            $ St.getCheckpoint
                (St.checkpoints (state ctx))
    case (mRoot, mCp) of
        (Just r, Just (slot, blk)) ->
            pure
                BundleSnapshot
                    { snapshotUtxoRoot = r
                    , snapshotSlot = slot
                    , snapshotBlockId = blk
                    }
        _ ->
            throwError
                err503
                    { errBody =
                        "Verification snapshot \
                        \not yet available"
                    }

-- | Resolve a 'TxIn' to a 'WitnessedUtxo', or
-- @404@ if the UTxO or its CSMT proof is missing.
requireUtxoWitness
    :: Context IO -> TxIn -> Handler WitnessedUtxo
requireUtxoWitness ctx txIn = do
    mOut <- liftIO $ resolveUtxo ctx txIn
    mProof <- liftIO $ utxoProof ctx txIn
    case (mOut, mProof) of
        (Just out, Just proof) ->
            pure
                WitnessedUtxo
                    { wuTxIn = txInToJSON txIn
                    , wuTxOut = Hex out
                    , wuProof = Hex proof
                    }
        _ -> throwError err404

tokenRootHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler Hex
tokenRootHandler ctx tokenId =
    let tid = tokenIdFromJSON tokenId
    in  liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> do
                Root r <- Trie.getRoot trie
                pure (Hex r)

tokenFactHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler FactResponse
tokenFactHandler ctx tokenId (Hex k) = do
    let tid = tokenIdFromJSON tokenId
    LocatedTokenState
        { tokenStateRef
        , tokenState = ts
        } <-
        requireToken ctx tid
    snapshot <- requireSnapshot ctx
    witness <- requireUtxoWitness ctx tokenStateRef
    mv <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.lookup trie k
    v <- case mv of
        Just v -> pure v
        Nothing -> throwError err404
    proof <- requireMpfProof ctx tid k
    pure
        FactResponse
            { frSnapshot = snapshot
            , frValue = Hex v
            , frFact =
                FactWitness
                    { fwState =
                        WitnessedTokenState
                            { wtsUtxo = witness
                            , wtsState =
                                tokenStateToJSON ts
                            }
                    , fwMpfProof = proof
                    }
            }

tokenProofHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler ProofResponse
tokenProofHandler ctx tokenId (Hex k) = do
    let tid = tokenIdFromJSON tokenId
    LocatedTokenState
        { tokenStateRef
        , tokenState = ts
        } <-
        requireToken ctx tid
    snapshot <- requireSnapshot ctx
    witness <- requireUtxoWitness ctx tokenStateRef
    proof <- requireMpfProof ctx tid k
    pure
        ProofResponse
            { prSnapshot = snapshot
            , prFact =
                FactWitness
                    { fwState =
                        WitnessedTokenState
                            { wtsUtxo = witness
                            , wtsState =
                                tokenStateToJSON ts
                            }
                    , fwMpfProof = proof
                    }
            }

-- | Compute an MPF inclusion proof for a key under
-- a token's trie, or @404@ if absent.
requireMpfProof
    :: Context IO
    -> TokenId
    -> ByteString
    -> Handler Hex
requireMpfProof ctx tid k = do
    mp <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.getProof trie k
    case mp of
        Nothing -> throwError err404
        Just p -> pure (Hex (Trie.unProof p))

tokenRequestsHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler RequestsResponse
tokenRequestsHandler ctx tokenId = do
    let tid = tokenIdFromJSON tokenId
    _ <- requireToken ctx tid
    snapshot <- requireSnapshot ctx
    reqs <-
        liftIO
            $ St.requestsByToken
                (St.requests (state ctx))
                tid
    witnessed <- traverse (witnessRequest ctx) reqs
    pure
        RequestsResponse
            { rrSnapshot = snapshot
            , rrRequests = witnessed
            }

-- | Build a 'WitnessedRequest' from a 'LocatedRequest'
-- by resolving its UTxO witness against the snapshot's
-- @utxo_root@. @404@ if the witness cannot be assembled.
witnessRequest
    :: Context IO
    -> LocatedRequest
    -> Handler WitnessedRequest
witnessRequest
    ctx
    LocatedRequest{requestRef, request = req} = do
        witness <- requireUtxoWitness ctx requestRef
        pure
            WitnessedRequest
                { wrUtxo = witness
                , wrRequest = requestToJSON req
                }

-- ---------------------------------------------------------
-- UTxO CSMT handlers
-- ---------------------------------------------------------

-- | @GET \/utxo\/:txId\/:txIx@ — resolve a TxIn.
utxoResolveHandler
    :: Context IO
    -> Hex
    -> Word64
    -> Handler Hex
utxoResolveHandler ctx txIdHex txIx = do
    txIn <- requireTxIn txIdHex txIx
    mbs <- liftIO $ resolveUtxo ctx txIn
    case mbs of
        Nothing -> throwError err404
        Just bs -> pure (Hex bs)

-- | @GET \/utxo\/:txId\/:txIx\/proof@ — CSMT proof.
utxoProofHandler
    :: Context IO
    -> Hex
    -> Word64
    -> Handler Hex
utxoProofHandler ctx txIdHex txIx = do
    txIn <- requireTxIn txIdHex txIx
    mbs <- liftIO $ utxoProof ctx txIn
    case mbs of
        Nothing -> throwError err404
        Just bs -> pure (Hex bs)

-- | @GET \/utxo\/root@ — current CSMT root.
utxoRootHandler
    :: Context IO -> Handler Hex
utxoRootHandler ctx = do
    mbs <- liftIO $ utxoRoot ctx
    case mbs of
        Nothing -> throwError err404
        Just bs -> pure (Hex bs)

-- | Build a 'TxIn' from hex txId + index.
requireTxIn
    :: Hex -> Word64 -> Handler TxIn
requireTxIn txIdHex txIx = do
    tid <- parseTxIdRaw (unHex txIdHex)
    pure $ mkTxInPartial tid (fromIntegral txIx)

-- ---------------------------------------------------------
-- Confirmation handler
-- ---------------------------------------------------------

-- | Default timeout for tx confirmation (seconds).
defaultTimeout :: Int
defaultTimeout = 30

-- | @GET \/tx\/:txId?timeout=N@ — block until
-- TxIn(txId, 0) appears in the indexed UTxO set.
-- Uses STM-based push notification instead of polling.
txAwaitHandler
    :: Context IO
    -> Hex
    -> Maybe Word64
    -> Handler NoContent
txAwaitHandler ctx (Hex txIdBytes) mTimeout = do
    txId <- parseTxIdRaw txIdBytes
    let txIn = mkTxInPartial txId 0
        timeoutSec =
            fmap fromIntegral mTimeout
                <|> Just defaultTimeout
    mval <- liftIO $ awaitUtxo ctx txIn timeoutSec
    case mval of
        Just _ -> pure NoContent
        Nothing ->
            throwError
                ServerError
                    { errHTTPCode = 408
                    , errReasonPhrase = "Request Timeout"
                    , errBody =
                        "Transaction not confirmed"
                    , errHeaders = []
                    }

-- | Extract raw 32-byte hash from a 'TxId'.
txIdToBytes :: TxId -> ByteString
txIdToBytes (TxId sh) =
    Crypto.hashToBytes (extractHash sh)

-- | Parse a 'TxId' from 32 raw hash bytes.
parseTxIdRaw :: ByteString -> Handler TxId
parseTxIdRaw bs =
    case Crypto.hashFromBytes bs of
        Just h ->
            pure $ TxId $ unsafeMakeSafeHash h
        Nothing ->
            throwError
                err400
                    { errBody =
                        "Invalid transaction ID: \
                        \expected 32 bytes"
                    }

-- ---------------------------------------------------------
-- Transaction handlers
-- ---------------------------------------------------------

-- | Parse an address from hex or throw 400.
requireAddr :: Hex -> Handler Addr
requireAddr h =
    case parseAddr h of
        Right a -> pure a
        Left msg ->
            throwError
                err400
                    { errBody =
                        BL.pack msg
                    }

-- | @POST \/facts\/boot@. Reads snapshot and wallet
-- inputs at the owner address inside ONE indexer
-- transaction, then returns facts for wallet-side
-- boot transaction construction.
factsBootHandler
    :: Context IO
    -> BootRequest
    -> Handler BootFacts
factsBootHandler ctx (BootRequest addrHex) = do
    addr <- requireAddr addrHex
    (mSnap, inputs) <-
        liftIO
            $ runIndexerTx ctx
            $ do
                snap <- readSnapshot
                ins <- readWalletInputsAt addr
                pure (snap, ins)
    case mSnap of
        Nothing ->
            throwError
                err503
                    { errBody =
                        "Indexer not ready: \
                        \snapshot unavailable"
                    }
        Just snap
            | null inputs ->
                throwError
                    err400
                        { errBody =
                            "No wallet UTxOs \
                            \at address"
                        }
            | otherwise -> do
                pp <-
                    liftIO
                        $ queryProtocolParams
                            (provider ctx)
                pure (mkBootFacts snap inputs pp)

-- | Build the facts-only boot response from an indexed
-- snapshot, resolved wallet inputs, and protocol
-- parameters queried from the node.
mkBootFacts
    :: BundleSnapshot
    -> [ResolvedWalletInput]
    -> PParams ConwayEra
    -> BootFacts
mkBootFacts snap inputs pparams =
    BootFacts
        { bfSnapshot = bundleSnapshotToJSON snap
        , bfWalletUtxos =
            map resolvedWalletInputToUtxoEntry inputs
        , bfProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor =
                    Hex
                        ( serialize'
                            (natVersion @11)
                            pparams
                        )
                }
        }

-- | @POST \/facts\/request\/insert@. Reads snapshot and wallet
-- inputs at the requester address inside ONE indexer transaction,
-- then returns facts for wallet-side request transaction
-- construction.
factsRequestInsertHandler
    :: Context IO
    -> InsertRequest
    -> Handler RequestInsertFacts
factsRequestInsertHandler
    ctx
    InsertRequest
        { irToken = tokenId
        , irKey = Hex k
        , irValue = Hex v
        , irAddr = addrHex@(Hex addrBytes)
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        (mSnap, inputs) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    ins <- readWalletInputsAt addr
                    pure (snap, ins)
        case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just snap
                | null inputs ->
                    throwError
                        err400
                            { errBody =
                                "No wallet UTxOs \
                                \at address"
                            }
                | otherwise -> do
                    pp <-
                        liftIO
                            $ queryProtocolParams
                                (provider ctx)
                    submittedAt <- liftIO currentPosixMs
                    pure
                        $ mkRequestInsertFacts
                            snap
                            tid
                            k
                            v
                            addrBytes
                            submittedAt
                            inputs
                            pp

-- | @POST \/facts\/request\/delete@. Reads snapshot and wallet
-- inputs at the requester address inside ONE indexer transaction,
-- then returns facts for wallet-side request-delete transaction
-- construction.
factsRequestDeleteHandler
    :: Context IO
    -> DeleteRequest
    -> Handler RequestDeleteFacts
factsRequestDeleteHandler
    ctx
    DeleteRequest
        { drToken = tokenId
        , drKey = Hex k
        , drValue = Hex v
        , drAddr = addrHex@(Hex addrBytes)
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        (mSnap, inputs) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    ins <- readWalletInputsAt addr
                    pure (snap, ins)
        case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just snap
                | null inputs ->
                    throwError
                        err400
                            { errBody =
                                "No wallet UTxOs \
                                \at address"
                            }
                | otherwise -> do
                    pp <-
                        liftIO
                            $ queryProtocolParams
                                (provider ctx)
                    submittedAt <- liftIO currentPosixMs
                    pure
                        $ mkRequestDeleteFacts
                            snap
                            tid
                            k
                            v
                            addrBytes
                            submittedAt
                            inputs
                            pp

-- | @POST \/facts\/end@. Reads snapshot, owner funding
-- inputs, state UTxO, and the request-set completeness
-- witness inside ONE indexer transaction, then returns
-- facts for wallet-side end transaction construction.
factsEndHandler
    :: Context IO
    -> EndRequest
    -> Handler EndFacts
factsEndHandler
    ctx
    EndRequest
        { erToken = tokenId
        , erAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
            cfg = cfgCage ctx
            cageAddr = cageAddrFromCfg cfg (network cfg)
            requestAddr = requestAddrFromCfg cfg tid (network cfg)
            policyId = cagePolicyIdFromCfg cfg
        addr <- requireAddr addrHex
        (mSnap, funding, mStateUtxo, requestSet) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    ins <- readWalletInputsAt addr
                    stateUtxo <-
                        readStateUtxoAt
                            cageAddr
                            policyId
                            tid
                    reqSet <- readRequestSetAt requestAddr
                    pure (snap, ins, stateUtxo, reqSet)
        case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just snap -> do
                stateUtxo <- case mStateUtxo of
                    Nothing ->
                        throwError
                            err404
                                { errBody =
                                    "State UTxO not \
                                    \found for token"
                                }
                    Just row -> pure row
                if null funding
                    then
                        throwError
                            err400
                                { errBody =
                                    "No wallet UTxOs \
                                    \at address"
                                }
                    else case requestSet of
                        ([], _) -> do
                            pp <-
                                liftIO
                                    $ queryProtocolParams
                                        (provider ctx)
                            pure
                                $ mkEndFacts
                                    snap
                                    tid
                                    stateUtxo
                                    funding
                                    requestSet
                                    pp
                        (entries, _) ->
                            throwError
                                ServerError
                                    { errHTTPCode = 409
                                    , errReasonPhrase =
                                        "Conflict"
                                    , errBody =
                                        "Cannot end token: \
                                        \pending request \
                                        \UTxOs exist at \
                                        \the request \
                                        \address"
                                    , errHeaders =
                                        [
                                            ( "X-MPFS-Request-Set-Size"
                                            , TE.encodeUtf8
                                                $ T.pack
                                                $ show
                                                $ length entries
                                            )
                                        ]
                                    }

txUpdateValueHandler
    :: Context IO
    -> UpdateValueRequest
    -> Handler RequestTxResponse
txUpdateValueHandler
    ctx
    UpdateValueRequest
        { uvrToken = tokenId
        , uvrKey = Hex k
        , uvrOldValue = Hex oldV
        , uvrNewValue = Hex newV
        , uvrAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        snap <- requireBundleSnapshot ctx
        bundle <-
            liftIO
                $ Tx.requestUpdate
                    (txBuilder ctx)
                    snap
                    tid
                    k
                    oldV
                    newV
                    addr
        pure (mkRequestTxResponse bundle)

txRejectHandler
    :: Context IO
    -> RejectRequest
    -> Handler RejectTxResponse
txRejectHandler
    ctx
    RejectRequest
        { rejToken = tokenId
        , rejAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        snap <- requireBundleSnapshot ctx
        bundle <-
            liftIO
                $ Tx.rejectRequests
                    (txBuilder ctx)
                    snap
                    tid
                    addr
        pure (mkRejectTxResponse bundle)

txUpdateHandler
    :: Context IO
    -> UpdateRequest
    -> Handler UpdateTxResponse
txUpdateHandler
    ctx
    UpdateRequest
        { urToken = tokenId
        , urAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        snap <- requireBundleSnapshot ctx
        bundle <-
            liftIO
                $ Tx.updateToken
                    (txBuilder ctx)
                    snap
                    tid
                    addr
        pure (mkUpdateTxResponse bundle)

txRetractHandler
    :: Context IO
    -> RetractRequest
    -> Handler RetractTxResponse
txRetractHandler
    ctx
    RetractRequest
        { rrUtxo = utxoRef
        , rrAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        txIn <- parseUtxoRef utxoRef
        snap <- requireBundleSnapshot ctx
        bundle <-
            liftIO
                $ Tx.retractRequest
                    (txBuilder ctx)
                    snap
                    txIn
                    addr
        pure (mkRetractTxResponse bundle)

txSweepHandler
    :: Context IO
    -> SweepRequest
    -> Handler SweepTxResponse
txSweepHandler
    ctx
    SweepRequest
        { swrToken = tokenId
        , swrUtxo = utxoRef
        , swrAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
        addr <- requireAddr addrHex
        txIn <- parseUtxoRef utxoRef
        tx <-
            liftIO
                $ sweepUtxoImpl
                    (cfgCage ctx)
                    (provider ctx)
                    tid
                    txIn
                    addr
        pure (mkSweepTxResponse tx)

txSubmitHandler
    :: Context IO -> SubmitRequest -> Handler Hex
txSubmitHandler ctx (SubmitRequest (Hex txCbor)) = do
    tx <- case decodeTx txCbor of
        Right t -> pure t
        Left msg ->
            throwError
                err400
                    { errBody =
                        BL.pack (show msg)
                    }
    result <-
        liftIO
            $ Sub.submitTx (submitter ctx) tx
    case result of
        Sub.Submitted txId ->
            pure (Hex (txIdToBytes txId))
        Sub.Rejected reason ->
            throwError
                err502
                    { errBody =
                        BL.fromStrict reason
                    }

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | Parse a UTxO reference in @txhash#ix@ format.
parseUtxoRef :: Text -> Handler TxIn
parseUtxoRef t =
    case T.splitOn "#" t of
        [hashHex, ixText] -> do
            let hashBs =
                    B16.decode
                        (TE.encodeUtf8 hashHex)
            case hashBs of
                Right bs -> do
                    txId <- parseTxIdRaw bs
                    case readMaybe (T.unpack ixText) of
                        Just ix ->
                            pure
                                $ mkTxInPartial
                                    txId
                                    ix
                        Nothing ->
                            throwError
                                err400
                                    { errBody =
                                        "Invalid \
                                        \output index"
                                    }
                Left _ ->
                    throwError
                        err400
                            { errBody =
                                "Invalid tx hash \
                                \hex"
                            }
        _ ->
            throwError
                err400
                    { errBody =
                        "Invalid UTxO ref: \
                        \expected txhash#ix"
                    }

-- | Decode CBOR bytes to a 'Tx ConwayEra'.
decodeTx
    :: ByteString
    -> Either DecoderError (Tx ConwayEra)
decodeTx = decodeFull' (natVersion @11)
