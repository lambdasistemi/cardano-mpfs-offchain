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

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
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
    ( BootRequest (..)
    , ChainPointJSON (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , FactResponse (..)
    , FactWitness (..)
    , InsertRequest (..)
    , ProofResponse (..)
    , RejectRequest (..)
    , RequestsResponse (..)
    , RetractRequest (..)
    , StatusResponse (..)
    , SubmitRequest (..)
    , TokenIdJSON (..)
    , TokenResponse (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    , parseAddr
    , requestToJSON
    , tokenStateToJSON
    , txInToJSON
    )
import Cardano.UTxOCSMT.Application.Metrics
    ( Metrics (..)
    , renderPrometheus
    )

import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Submitter qualified as Sub
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.TxBuilder qualified as Tx

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
            :<|> txBootHandler ctx
            :<|> txInsertHandler ctx
            :<|> txDeleteHandler ctx
            :<|> txUpdateValueHandler ctx
            :<|> txRejectHandler ctx
            :<|> txUpdateHandler ctx
            :<|> txRetractHandler ctx
            :<|> txEndHandler ctx
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
    pure (map TokenIdJSON tids)

tokenHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler TokenResponse
tokenHandler ctx (TokenIdJSON tid) = do
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
tokenRootHandler ctx (TokenIdJSON tid) =
    liftIO
        $ Trie.withTrie (trieManager ctx) tid
        $ \trie -> do
            Root r <- Trie.getRoot trie
            pure (Hex r)

tokenFactHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler FactResponse
tokenFactHandler ctx (TokenIdJSON tid) (Hex k) = do
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
tokenProofHandler ctx (TokenIdJSON tid) (Hex k) = do
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
tokenRequestsHandler ctx (TokenIdJSON tid) = do
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

-- | Serialize a 'Tx ConwayEra' to hex CBOR.
serializeTx :: Tx ConwayEra -> Hex
serializeTx = Hex . serialize' (natVersion @11)

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

txBootHandler
    :: Context IO -> BootRequest -> Handler Hex
txBootHandler ctx (BootRequest addrHex) = do
    addr <- requireAddr addrHex
    tx <-
        liftIO $ Tx.bootToken (txBuilder ctx) addr
    pure (serializeTx tx)

txInsertHandler
    :: Context IO -> InsertRequest -> Handler Hex
txInsertHandler
    ctx
    InsertRequest
        { irToken = TokenIdJSON tid
        , irKey = Hex k
        , irValue = Hex v
        , irAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.requestInsert
                    (txBuilder ctx)
                    tid
                    k
                    v
                    addr
        pure (serializeTx tx)

txDeleteHandler
    :: Context IO -> DeleteRequest -> Handler Hex
txDeleteHandler
    ctx
    DeleteRequest
        { drToken = TokenIdJSON tid
        , drKey = Hex k
        , drValue = Hex v
        , drAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.requestDelete
                    (txBuilder ctx)
                    tid
                    k
                    v
                    addr
        pure (serializeTx tx)

txUpdateValueHandler
    :: Context IO
    -> UpdateValueRequest
    -> Handler Hex
txUpdateValueHandler
    ctx
    UpdateValueRequest
        { uvrToken = TokenIdJSON tid
        , uvrKey = Hex k
        , uvrOldValue = Hex oldV
        , uvrNewValue = Hex newV
        , uvrAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.requestUpdate
                    (txBuilder ctx)
                    tid
                    k
                    oldV
                    newV
                    addr
        pure (serializeTx tx)

txRejectHandler
    :: Context IO
    -> RejectRequest
    -> Handler Hex
txRejectHandler
    ctx
    RejectRequest
        { rejToken = TokenIdJSON tid
        , rejAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.rejectRequests
                    (txBuilder ctx)
                    tid
                    addr
        pure (serializeTx tx)

txUpdateHandler
    :: Context IO -> UpdateRequest -> Handler Hex
txUpdateHandler
    ctx
    UpdateRequest
        { urToken = TokenIdJSON tid
        , urAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.updateToken
                    (txBuilder ctx)
                    tid
                    addr
        pure (serializeTx tx)

txRetractHandler
    :: Context IO -> RetractRequest -> Handler Hex
txRetractHandler
    ctx
    RetractRequest
        { rrUtxo = utxoRef
        , rrAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        txIn <- parseUtxoRef utxoRef
        tx <-
            liftIO
                $ Tx.retractRequest
                    (txBuilder ctx)
                    txIn
                    addr
        pure (serializeTx tx)

txEndHandler
    :: Context IO -> EndRequest -> Handler Hex
txEndHandler
    ctx
    EndRequest
        { erToken = TokenIdJSON tid
        , erAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.endToken
                    (txBuilder ctx)
                    tid
                    addr
        pure (serializeTx tx)

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
