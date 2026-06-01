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
    , mkRequestUpdateFacts
    , mkUpdateFacts
    , mkRetractFacts
    , mkEndFacts

      -- * Reject helpers (consumed by the S5 reject handler)
    , rejectableRequestUtxos
    , rejectValiditySlots
    ) where

import Control.Applicative ((<|>))
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson qualified as Aeson
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
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
import Cardano.Ledger.Binary
    ( Annotator
    , Decoder
    , DecoderError
    , decCBOR
    , decodeFullAnnotator
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
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.API.Types.Facts
    ( EndFacts
    , RejectFacts
    , RequestDeleteFacts
    , RequestInsertFacts
    , RequestUpdateFacts
    , RetractFacts
    , UpdateFacts
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
    , PParams
    , Request (..)
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
    , FactEntry (..)
    , FactResponse (..)
    , FactWitness (..)
    , FactsResponse (..)
    , InsertRequest (..)
    , ProofResponse (..)
    , RejectRequest (..)
    , RequestsResponse (..)
    , RetractRequest (..)
    , StatusResponse (..)
    , SubmitError (..)
    , SubmitRequest (..)
    , SubmitResponse (..)
    , SweepRequest (..)
    , SweepTxResponse (..)
    , TokenIdJSON
    , TokenResponse (..)
    , UnverifiedPParams (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    , bundleSnapshotToJSON
    , mkSweepTxResponse
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
    , mkRejectFacts
    , mkRequestDeleteFacts
    , mkRequestInsertFacts
    , mkRequestUpdateFacts
    , mkRetractFacts
    , mkUpdateFacts
    )
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx
    , readNamedRequestUtxo
    , readRequestSetAt
    , readRequestUtxosAt
    , readSnapshot
    , readStateUtxoAt
    , readTrieFact
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
    , extractCageDatum
    , requestAddrFromCfg
    )
import Cardano.MPFS.TxBuilder.Real.Update (computeUpperSlot)

import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Binary qualified as L
import Cardano.MPFS.Core.OnChain (CageDatum (..))
import Cardano.MPFS.Core.OnChain qualified as OnChain
import Cardano.MPFS.Provider qualified as Prov

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
            :<|> tokenFactsHandler ctx
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
            :<|> factsRequestUpdateHandler ctx
            :<|> factsUpdateHandler ctx
            :<|> factsRetractHandler ctx
            :<|> factsRejectHandler ctx
            :<|> factsEndHandler ctx
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

tokenFactsHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler FactsResponse
tokenFactsHandler ctx tokenId = do
    let tid = tokenIdFromJSON tokenId
    LocatedTokenState
        { tokenStateRef
        , tokenState = ts
        } <-
        requireToken ctx tid
    snapshot <- requireSnapshot ctx
    witness <- requireUtxoWitness ctx tokenStateRef
    facts <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.enumerate trie
    pure
        FactsResponse
            { frsSnapshot = snapshot
            , frsState =
                WitnessedTokenState
                    { wtsUtxo = witness
                    , wtsState = tokenStateToJSON ts
                    }
            , frsFacts =
                [ FactEntry
                    { feKey = Hex k
                    , feValue = Hex v
                    }
                | (k, v) <- facts
                ]
            }

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

-- | @POST \/facts\/request\/update@. Reads snapshot and wallet
-- inputs at the requester address inside ONE indexer transaction,
-- then returns facts for wallet-side request-update transaction
-- construction.
factsRequestUpdateHandler
    :: Context IO
    -> UpdateValueRequest
    -> Handler RequestUpdateFacts
factsRequestUpdateHandler
    ctx
    UpdateValueRequest
        { uvrToken = tokenId
        , uvrKey = Hex k
        , uvrOldValue = Hex oldV
        , uvrNewValue = Hex newV
        , uvrAddr = addrHex@(Hex addrBytes)
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
                        $ mkRequestUpdateFacts
                            snap
                            tid
                            k
                            oldV
                            newV
                            addrBytes
                            submittedAt
                            inputs
                            pp

-- | @POST \/facts\/update@. Reads snapshot, state UTxO,
-- pending request UTxOs, owner funding inputs, and each
-- request's MPF trie fact inside ONE indexer transaction,
-- then returns facts for wallet-side update construction.
factsUpdateHandler
    :: Context IO
    -> UpdateRequest
    -> Handler UpdateFacts
factsUpdateHandler
    ctx
    UpdateRequest
        { urToken = tokenId
        , urAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
            cfg = cfgCage ctx
            cageAddr = cageAddrFromCfg cfg (network cfg)
            requestAddr = requestAddrFromCfg cfg tid (network cfg)
            policyId = cagePolicyIdFromCfg cfg
        addr <- requireAddr addrHex
        (mSnap, mStateUtxo, requestUtxos, funding, eTrieFacts) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    stateUtxo <-
                        readStateUtxoAt
                            cageAddr
                            policyId
                            tid
                    reqs <- readRequestUtxosAt requestAddr
                    wallet <- readWalletInputsAt addr
                    trieFacts <-
                        readRequestTrieFacts tid reqs
                    pure
                        ( snap
                        , stateUtxo
                        , reqs
                        , wallet
                        , trieFacts
                        )
        snap <- case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just s -> pure s
        stateUtxo@(_, stateOutBytes, _) <- case mStateUtxo of
            Nothing ->
                throwError
                    err404
                        { errBody =
                            "State UTxO not \
                            \found for token"
                        }
            Just row -> pure row
        when (null funding)
            $ throwError
                err400
                    { errBody =
                        "No wallet UTxOs at \
                        \address"
                    }
        when (null requestUtxos)
            $ throwError
                err400
                    { errBody =
                        "No pending request UTxOs \
                        \at request address"
                    }
        trieFacts <- case eTrieFacts of
            Left msg -> throwInternal msg
            Right facts -> pure facts
        trieRoot <-
            case stateTrieRootBytes stateOutBytes of
                Left msg -> throwInternal msg
                Right root -> pure root
        validityUpperSlot <-
            updateValidityUpperSlot ctx stateOutBytes requestUtxos
        pp <-
            liftIO
                $ queryProtocolParams
                    (provider ctx)
        pure
            $ mkUpdateFacts
                snap
                tid
                stateUtxo
                requestUtxos
                funding
                trieRoot
                trieFacts
                validityUpperSlot
                pp

-- | @POST \/facts\/retract@. Reads snapshot, the named
-- request UTxO at the per-cage request address, the cage
-- state UTxO, and the requester wallet UTxOs inside ONE
-- indexer transaction, then derives Phase 2 validity slot
-- bounds from the on-chain datums and returns facts for
-- wallet-side retract transaction construction.
factsRetractHandler
    :: Context IO
    -> RetractRequest
    -> Handler RetractFacts
factsRetractHandler
    ctx
    RetractRequest
        { rrUtxo = utxoRef
        , rrAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        reqTxIn <- parseUtxoRef utxoRef
        mLoc <-
            liftIO
                $ St.getRequest
                    (St.requests (state ctx))
                    reqTxIn
        tid <- case mLoc of
            Nothing ->
                throwError
                    err404
                        { errBody =
                            "Unknown request: \
                            \not in pending set"
                        }
            Just LocatedRequest{request = r} ->
                pure (requestToken r)
        let cfg = cfgCage ctx
            reqAddr =
                requestAddrFromCfg
                    cfg
                    tid
                    (network cfg)
            stateAddr =
                cageAddrFromCfg cfg (network cfg)
            policyId = cagePolicyIdFromCfg cfg
        ( mSnap
            , mRequestUtxo
            , mStateUtxo
            , walletInputs
            ) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    reqU <-
                        readNamedRequestUtxo
                            reqAddr
                            reqTxIn
                    stU <-
                        readStateUtxoAt
                            stateAddr
                            policyId
                            tid
                    wall <- readWalletInputsAt addr
                    pure (snap, reqU, stU, wall)
        snap <- case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just s -> pure s
        requestUtxo@(_, reqOutBytes, _) <-
            case mRequestUtxo of
                Nothing ->
                    throwError
                        err404
                            { errBody =
                                "Request UTxO not \
                                \found at request \
                                \address"
                            }
                Just r -> pure r
        stateUtxo@(_, stateOutBytes, _) <-
            case mStateUtxo of
                Nothing ->
                    throwError
                        err404
                            { errBody =
                                "State UTxO not \
                                \found for token"
                            }
                Just s -> pure s
        when (null walletInputs)
            $ throwError
                err400
                    { errBody =
                        "No wallet UTxOs at \
                        \address"
                    }
        submittedAt <-
            requireRequestSubmittedAt reqOutBytes
        (procTime, retrTime) <-
            requireStateRetractTimesBytes stateOutBytes
        let phase2Start = submittedAt + procTime
            phase2End =
                submittedAt + procTime + retrTime
        lowerSlot <-
            liftIO
                $ Prov.posixMsCeilSlot
                    (provider ctx)
                    phase2Start
        upperSlot <-
            liftIO
                $ Prov.posixMsToSlot
                    (provider ctx)
                    phase2End
        pp <-
            liftIO
                $ queryProtocolParams
                    (provider ctx)
        let startSlot =
                toInteger (unSlotNo lowerSlot)
            endSlotRaw =
                toInteger (unSlotNo upperSlot)
            endSlot = max 0 (endSlotRaw - 1)
        pure
            $ mkRetractFacts
                snap
                tid
                requestUtxo
                stateUtxo
                walletInputs
                startSlot
                endSlot
                pp

-- | Decode a 'TxOut' from indexed CBOR bytes; throws a
-- 500 with a path-tagged error body on failure.
decodeIndexedTxOut
    :: Text -> ByteString -> Handler (TxOut ConwayEra)
decodeIndexedTxOut path bytes =
    case decodeIndexedTxOutEither path bytes of
        Right out -> pure out
        Left msg -> throwInternal msg

decodeIndexedTxOutEither
    :: Text -> ByteString -> Either Text (TxOut ConwayEra)
decodeIndexedTxOutEither path bytes =
    case L.decodeFull
        (natVersion @11)
        (BSL.fromStrict bytes) of
        Right out -> Right out
        Left err ->
            Left
                $ path
                    <> ": indexer TxOut decode failed: "
                    <> T.pack (show err)

throwInternal :: Text -> Handler a
throwInternal msg =
    throwError
        ServerError
            { errHTTPCode = 500
            , errReasonPhrase =
                "Internal Server Error"
            , errBody =
                BL.fromStrict
                    $ TE.encodeUtf8 msg
            , errHeaders = []
            }

readRequestTrieFacts
    :: TokenId
    -> [ResolvedWalletInput]
    -> IndexerTx (Either Text [Tx.TrieFact])
readRequestTrieFacts tid = go []
  where
    go acc [] = pure (Right (reverse acc))
    go acc ((_, requestOutBytes, _) : rest) =
        case requestDatumKeyBytes requestOutBytes of
            Left msg -> pure (Left msg)
            Right key -> do
                fact <- readTrieFact tid key
                go (fact : acc) rest

requestDatumKeyBytes :: ByteString -> Either Text ByteString
requestDatumKeyBytes bytes = do
    out <-
        decodeIndexedTxOutEither
            "facts/update.request_utxos[]"
            bytes
    case extractCageDatum out of
        Just (RequestDatum request) ->
            Right (OnChain.requestKey request)
        _ ->
            Left
                "facts/update.request_utxos[] missing request datum"

stateTrieRootBytes :: ByteString -> Either Text ByteString
stateTrieRootBytes bytes = do
    out <-
        decodeIndexedTxOutEither
            "facts/update.state_utxo"
            bytes
    case extractCageDatum out of
        Just (StateDatum state) ->
            Right
                $ OnChain.unOnChainRoot
                $ OnChain.stateRoot state
        _ ->
            Left
                "facts/update.state_utxo missing state datum"

updateValidityUpperSlot
    :: Context IO
    -> ByteString
    -> [ResolvedWalletInput]
    -> Handler Integer
updateValidityUpperSlot ctx stateOutBytes requestUtxos = do
    stateOut <-
        decodeIndexedTxOut "facts/update.state_utxo" stateOutBytes
    oldState <- case extractCageDatum stateOut of
        Just (StateDatum state) -> pure state
        _ ->
            throwInternal "facts/update.state_utxo missing state datum"
    requestPairs <-
        traverse decodeRequest requestUtxos
    slot <-
        liftIO
            $ computeUpperSlot
                (provider ctx)
                oldState
                requestPairs
    pure (toInteger (unSlotNo slot))
  where
    decodeRequest
        (txIn, bytes, _proof) = do
            out <-
                decodeIndexedTxOut
                    "facts/update.request_utxos[]"
                    bytes
            pure (txIn, out)

-- | Filter the indexer-returned request UTxO set for the
-- rejectable subset. Mirrors the legacy @isRejectable@
-- predicate from
-- @Cardano.MPFS.TxBuilder.Real.Reject.queryRejectContext@:
-- a request is rejectable when @now > submitted_at +
-- process_time + retract_time@ or when the request is
-- registered in the future (@submitted_at > now@).
--
-- The helper decodes the state UTxO once to extract the
-- process- and retract-time windows, then decodes each
-- request UTxO once. The S5 reject handler is responsible
-- for treating an empty result as a 400.
rejectableRequestUtxos
    :: ByteString
    -> [ResolvedWalletInput]
    -> Handler [ResolvedWalletInput]
rejectableRequestUtxos stateOutBytes requestUtxos = do
    stateOut <-
        decodeIndexedTxOut
            "facts/reject.state_utxo"
            stateOutBytes
    (pt, rt) <- case extractCageDatum stateOut of
        Just (StateDatum s) ->
            pure
                ( OnChain.stateProcessTime s
                , OnChain.stateRetractTime s
                )
        _ ->
            throwInternal
                "facts/reject.state_utxo missing state datum"
    now <- liftIO currentPosixMs
    let isRejectable (_, outBytes, _) = do
            rOut <-
                decodeIndexedTxOut
                    "facts/reject.request_utxos[]"
                    outBytes
            case extractCageDatum rOut of
                Just (RequestDatum r) ->
                    let sa = OnChain.requestSubmittedAt r
                        deadline = sa + pt + rt
                    in  pure (now > deadline || sa > now)
                _ -> pure False
    flags <- mapM isRejectable requestUtxos
    pure [u | (u, True) <- zip requestUtxos flags]

-- | Compute the validity slot bounds for a reject
-- transaction:
--
--   * Lower slot — strictly after the latest Phase 3
--     deadline among the rejected requests AND strictly
--     after the snapshot slot. For an expired request the
--     deadline POSIX timestamp is in the past, so we
--     POSIX → slot-convert it (cheap, always inside the
--     era horizon) and then bump to @snapshotSlot + 1@ if
--     the resulting slot would land at or before the
--     snapshot. The latter is what the S3 verifier
--     enforces with the "must be greater than the
--     snapshot slot" guard.
--   * Upper slot — fixed slot offset above the lower
--     bound (@lowerSlot + 'rejectTtlSlots'@). We do NOT
--     POSIX → slot-convert @latestDeadline + 600 s@,
--     because devnet's hard-fork forecast horizon (~50 s
--     in the live e2e) trips a 'PastHorizon' on any
--     conversion that far in the future. A slot-offset
--     keeps the TTL deterministic and era-agnostic.
rejectValiditySlots
    :: Context IO
    -> BundleSnapshot
    -> ByteString
    -> [ResolvedWalletInput]
    -> Handler (Integer, Integer)
rejectValiditySlots ctx snap stateOutBytes rejectableUtxos = do
    stateOut <-
        decodeIndexedTxOut
            "facts/reject.state_utxo"
            stateOutBytes
    (pt, rt) <- case extractCageDatum stateOut of
        Just (StateDatum s) ->
            pure
                ( OnChain.stateProcessTime s
                , OnChain.stateRetractTime s
                )
        _ ->
            throwInternal
                "facts/reject.state_utxo missing state datum"
    deadlines <- traverse (extractDeadline pt rt) rejectableUtxos
    let latest = maximum (0 : deadlines)
        SlotNo snapSlot = snapshotSlot snap
    deadlineSlot <-
        liftIO
            (Prov.posixMsCeilSlot (provider ctx) latest)
    let SlotNo deadlineRaw = deadlineSlot
        lowerRaw = max (deadlineRaw + 1) (snapSlot + 1)
        upperRaw = lowerRaw + rejectTtlSlots
    pure (toInteger lowerRaw, toInteger upperRaw)
  where
    extractDeadline pt rt (_, outBytes, _) = do
        out <-
            decodeIndexedTxOut
                "facts/reject.request_utxos[]"
                outBytes
        case extractCageDatum out of
            Just (RequestDatum r) ->
                pure
                    (OnChain.requestSubmittedAt r + pt + rt)
            _ ->
                throwInternal
                    "facts/reject.request_utxos[] missing \
                    \request datum"

-- | TTL (in slots) added to the lower validity slot to
-- derive the reject upper validity slot. Kept small so
-- @upper@ stays inside the current era's forecast
-- horizon (devnet's safe zone is ~30 slots; the Plutus
-- script context translates @validTo@ to time at phase-2
-- and trips 'TimeTranslationPastHorizon' if @upper@ is
-- past the safe zone). 20 slots is comfortably inside
-- every supported era's safe zone while still giving a
-- non-zero TTL above @validFrom@. Well within the S3
-- verifier's 660-slot @rejectValidityHorizonSlots@
-- ceiling.
rejectTtlSlots :: Word64
rejectTtlSlots = 20

-- | Extract @submitted_at@ from the inline datum of an
-- indexed request UTxO. 500 if the output is missing a
-- valid request datum.
requireRequestSubmittedAt
    :: ByteString -> Handler Integer
requireRequestSubmittedAt bytes = do
    out <-
        decodeIndexedTxOut "facts/retract.request_utxo" bytes
    case extractCageDatum out of
        Just (RequestDatum r) ->
            pure (OnChain.requestSubmittedAt r)
        _ ->
            throwError
                ServerError
                    { errHTTPCode = 500
                    , errReasonPhrase =
                        "Internal Server Error"
                    , errBody =
                        "facts/retract.request_utxo \
                        \missing request datum"
                    , errHeaders = []
                    }

-- | Extract @process_time@ and @retract_time@ from the
-- inline datum of an indexed state UTxO. 500 if the
-- output is missing a valid state datum.
requireStateRetractTimesBytes
    :: ByteString -> Handler (Integer, Integer)
requireStateRetractTimesBytes bytes = do
    out <-
        decodeIndexedTxOut "facts/retract.state_utxo" bytes
    case extractCageDatum out of
        Just (StateDatum s) ->
            pure
                ( OnChain.stateProcessTime s
                , OnChain.stateRetractTime s
                )
        _ ->
            throwError
                ServerError
                    { errHTTPCode = 500
                    , errReasonPhrase =
                        "Internal Server Error"
                    , errBody =
                        "facts/retract.state_utxo \
                        \missing state datum"
                    , errHeaders = []
                    }

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

-- | @POST \/facts\/reject@. Reads snapshot, cage state UTxO,
-- pending request UTxOs at the per-cage request address, and
-- requester wallet UTxOs inside ONE indexer transaction, filters
-- the rejectable subset against the server clock, derives Phase 3
-- validity slot bounds from the on-chain datums, and returns
-- facts for wallet-side reject transaction construction.
factsRejectHandler
    :: Context IO
    -> RejectRequest
    -> Handler RejectFacts
factsRejectHandler
    ctx
    RejectRequest
        { rejToken = tokenId
        , rejAddr = addrHex
        } = do
        let tid = tokenIdFromJSON tokenId
            cfg = cfgCage ctx
            cageAddr = cageAddrFromCfg cfg (network cfg)
            requestAddr =
                requestAddrFromCfg cfg tid (network cfg)
            policyId = cagePolicyIdFromCfg cfg
        addr <- requireAddr addrHex
        (mSnap, mStateUtxo, requestUtxos, walletInputs) <-
            liftIO
                $ runIndexerTx ctx
                $ do
                    snap <- readSnapshot
                    stateUtxo <-
                        readStateUtxoAt
                            cageAddr
                            policyId
                            tid
                    reqs <- readRequestUtxosAt requestAddr
                    wallet <- readWalletInputsAt addr
                    pure (snap, stateUtxo, reqs, wallet)
        snap <- case mSnap of
            Nothing ->
                throwError
                    err503
                        { errBody =
                            "Indexer not ready: \
                            \snapshot unavailable"
                        }
            Just s -> pure s
        stateUtxo@(_, stateOutBytes, _) <-
            case mStateUtxo of
                Nothing ->
                    throwError
                        err404
                            { errBody =
                                "State UTxO not \
                                \found for token"
                            }
                Just row -> pure row
        when (null walletInputs)
            $ throwError
                err400
                    { errBody =
                        "No wallet UTxOs at \
                        \address"
                    }
        rejectable <-
            rejectableRequestUtxos stateOutBytes requestUtxos
        when (null rejectable)
            $ throwError
                err400
                    { errBody =
                        "No rejectable request UTxOs \
                        \at request address"
                    }
        (lowerSlot, upperSlot) <-
            rejectValiditySlots ctx snap stateOutBytes rejectable
        pp <-
            liftIO
                $ queryProtocolParams
                    (provider ctx)
        pure
            $ mkRejectFacts
                snap
                tid
                stateUtxo
                rejectable
                walletInputs
                lowerSlot
                upperSlot
                pp

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

-- | Build a structured JSON error body for the
-- @POST \/submit@ endpoint.
submitError
    :: ServerError -> Text -> Text -> ServerError
submitError base errTxt detailTxt =
    base
        { errBody =
            Aeson.encode
                (SubmitError errTxt detailTxt)
        , errHeaders =
            [("Content-Type", "application/json")]
        }

txSubmitHandler
    :: Context IO -> SubmitRequest -> Handler SubmitResponse
txSubmitHandler ctx (SubmitRequest (Hex txCbor)) = do
    tx <- case decodeTx txCbor of
        Right t -> pure t
        Left msg ->
            throwError
                $ submitError
                    err400
                    "decode failed"
                    (T.pack (show msg))
    result <-
        liftIO
            $ Sub.submitTx (submitter ctx) tx
    case result of
        Sub.Submitted txId ->
            pure
                ( SubmitResponse
                    (Hex (txIdToBytes txId))
                )
        Sub.Rejected reason ->
            throwError
                $ submitError
                    err502
                    "submission rejected"
                    (TE.decodeUtf8 reason)

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

-- | Decode CBOR bytes to a 'ConwayTx'.
decodeTx
    :: ByteString
    -> Either DecoderError ConwayTx
decodeTx =
    decodeFullAnnotator
        (natVersion @11)
        "Conway transaction"
        (decCBOR :: forall s. Decoder s (Annotator ConwayTx))
        . BL.fromStrict
