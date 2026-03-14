{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

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

import Control.Monad.IO.Class (liftIO)
import Data.Proxy (Proxy (..))
import Servant
    ( Application
    , Handler
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

import Cardano.Ledger.Api.Tx (Tx)
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull'
    , natVersion
    , serialize'
    )
import Cardano.Ledger.TxIn (mkTxInPartial)

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , ConwayEra
    , Root (..)
    , SlotNo (..)
    , TxId
    )
import Cardano.MPFS.HTTP.API (API)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
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

-- | Build a WAI 'Application' from a 'Context IO'.
mkApp :: Context IO -> Application
mkApp ctx =
    serve (Proxy @API)
        $ statusHandler ctx
            :<|> tokensHandler ctx
            :<|> tokenHandler ctx
            :<|> tokenRootHandler ctx
            :<|> tokenFactHandler ctx
            :<|> tokenProofHandler ctx
            :<|> tokenRequestsHandler ctx
            :<|> txBootHandler ctx
            :<|> txInsertHandler ctx
            :<|> txDeleteHandler ctx
            :<|> txUpdateHandler ctx
            :<|> txRetractHandler ctx
            :<|> txEndHandler ctx
            :<|> txSubmitHandler ctx

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
            pure (Hex (serialize' (natVersion @11) txId))
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
