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

import Control.Concurrent (threadDelay)
import Control.Monad.IO.Class (liftIO)
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import Network.HTTP.Types
    ( hContentType
    , status200
    , status503
    )
import Network.Wai
    ( Request
    , Response
    , pathInfo
    , responseLBS
    )
import Servant
    ( Application
    , Handler
    , NoContent (..)
    , ServerError (..)
    , err400
    , err404
    , err502
    , errBody
    , serve
    , throwError
    , (:<|>) (..)
    )

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
    , Root (..)
    , SlotNo (..)
    )
import Cardano.MPFS.HTTP.API (API)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Swagger
    ( SwaggerAPI
    , swaggerServer
    )
import Cardano.MPFS.HTTP.Types
    ( BootRequest (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , InsertRequest (..)
    , RequestJSON
    , RetractRequest (..)
    , StatusResponse (..)
    , SubmitRequest (..)
    , TokenIdJSON (..)
    , TokenStateJSON
    , UpdateRequest (..)
    , parseAddr
    , requestToJSON
    , tokenStateToJSON
    )
import Cardano.MPFS.Indexer qualified as Indexer
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Submitter qualified as Sub
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.TxBuilder qualified as Tx

-- | Combined API with Swagger UI.
type FullAPI = SwaggerAPI :<|> API

-- | Build a WAI 'Application' from a 'Context IO'.
-- Intercepts @\/metrics@ before Servant to serve
-- Prometheus exposition text format.
mkApp :: Context IO -> Application
mkApp ctx req respond =
    case pathInfo req of
        ["metrics"] -> metricsHandler ctx req respond
        _ -> servantApp req respond
  where
    servantApp =
        serve (Proxy @FullAPI)
            $ swaggerServer
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
                :<|> txUpdateHandler ctx
                :<|> txRetractHandler ctx
                :<|> txEndHandler ctx
                :<|> txSubmitHandler ctx

-- | @GET \/metrics@ — Prometheus exposition format.
metricsHandler
    :: Context IO -> Request -> (Response -> IO a) -> IO a
metricsHandler ctx _req respond = do
    mText <- readMetrics ctx
    case mText of
        Just txt ->
            respond
                $ responseLBS
                    status200
                    [(hContentType, "text/plain; version=0.0.4; charset=utf-8")]
                    (BL.fromStrict $ TE.encodeUtf8 txt)
        Nothing ->
            respond
                $ responseLBS
                    status503
                    [(hContentType, "text/plain")]
                    "Metrics not yet available"

-- ---------------------------------------------------------
-- Query handlers
-- ---------------------------------------------------------

statusHandler :: Context IO -> Handler StatusResponse
statusHandler ctx = do
    tip <- liftIO $ Indexer.getTip (indexer ctx)
    mcp <-
        liftIO
            $ St.getCheckpoint
                (St.checkpoints (state ctx))
    pure
        StatusResponse
            { tipSlot =
                unSlotNo (Indexer.tipSlot tip)
            , tipBlockId =
                Hex
                    ( unBlockId
                        (Indexer.tipBlockId tip)
                    )
            , checkpointSlot =
                fmap (unSlotNo . fst) mcp
            , checkpointBlockId =
                fmap (Hex . unBlockId . snd) mcp
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
    -> Handler TokenStateJSON
tokenHandler ctx (TokenIdJSON tid) = do
    mts <-
        liftIO
            $ St.getToken (St.tokens (state ctx)) tid
    case mts of
        Nothing -> throwError err404
        Just ts -> pure (tokenStateToJSON ts)

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
    -> Handler Hex
tokenFactHandler ctx (TokenIdJSON tid) (Hex k) = do
    mv <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.lookup trie k
    case mv of
        Nothing -> throwError err404
        Just v -> pure (Hex v)

tokenProofHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler Hex
tokenProofHandler ctx (TokenIdJSON tid) (Hex k) = do
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
    -> Handler [RequestJSON]
tokenRequestsHandler ctx (TokenIdJSON tid) = do
    reqs <-
        liftIO
            $ St.requestsByToken
                (St.requests (state ctx))
                tid
    pure (map requestToJSON reqs)

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
defaultTimeout :: Word64
defaultTimeout = 30

-- | Poll interval (microseconds).
pollInterval :: Int
pollInterval = 500_000

-- | @GET \/tx\/:txId?timeout=N@ — block until
-- TxIn(txId, 0) appears in the indexed UTxO set.
txAwaitHandler
    :: Context IO
    -> Hex
    -> Maybe Word64
    -> Handler NoContent
txAwaitHandler ctx (Hex txIdBytes) mTimeout = do
    txId <- parseTxIdRaw txIdBytes
    let txIn = mkTxInPartial txId 0
        timeoutSec =
            fromMaybe defaultTimeout mTimeout
        maxIters =
            fromIntegral timeoutSec
                * 1_000_000
                `div` pollInterval
    found <- liftIO $ poll maxIters txIn
    if found
        then pure NoContent
        else
            throwError
                ServerError
                    { errHTTPCode = 408
                    , errReasonPhrase = "Request Timeout"
                    , errBody =
                        "Transaction not confirmed"
                    , errHeaders = []
                    }
  where
    poll 0 _ = pure False
    poll n txIn = do
        exists <- utxoExists ctx txIn
        if exists
            then pure True
            else do
                threadDelay pollInterval
                poll (n - 1) txIn

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
        , drAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        tx <-
            liftIO
                $ Tx.requestDelete
                    (txBuilder ctx)
                    tid
                    k
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
        { rrTxId = Hex tidBytes
        , rrTxIx = ix
        , rrAddr = addrHex
        } = do
        addr <- requireAddr addrHex
        txId <- parseTxId tidBytes
        let txIn = mkTxInPartial txId (fromIntegral ix)
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

-- | Decode CBOR bytes to a 'Tx ConwayEra'.
decodeTx
    :: ByteString
    -> Either DecoderError (Tx ConwayEra)
decodeTx = decodeFull' (natVersion @11)

-- | Parse a 32-byte TxId from raw bytes.
parseTxId :: ByteString -> Handler TxId
parseTxId bs =
    case decodeFull' (natVersion @11) bs of
        Right tid -> pure tid
        Left _err ->
            throwError
                err400
                    { errBody =
                        "Invalid transaction ID"
                    }
