{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.CLI.Submit
-- Description : HTTP transport: submit, await, and read-only reads.
--
-- Servant-derived clients for the endpoints the CLI talks to directly:
-- @POST \/submit@ and the await @GET \/tx\/:txId@ used by write
-- subcommands, plus the read-only @GET \/tokens@ and
-- @GET \/tokens\/:id\/facts\/:key@. Protocol semantics live in
-- cardano-mpfs-workflows; this module is plain transport.
module Cardano.MPFS.CLI.Submit
    ( mkServerEnv
    , listTokens
    , getFact
    , submitSignedTx
    , awaitTx
    ) where

import Cardano.MPFS.API
    ( TokenFactAPI
    , TokensAPI
    , TxAwaitAPI
    , TxSubmitAPI
    )
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( FactResponse
    , SubmitRequest (..)
    , SubmitResponse (..)
    , TokenIdJSON (..)
    )
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Word (Word64)
import Network.HTTP.Client.TLS (newTlsManager)
import Servant.API (NoContent)
import Servant.Client
    ( ClientEnv
    , ClientError
    , ClientM
    , client
    , mkClientEnv
    , parseBaseUrl
    , runClientM
    )

-- | Build a client environment from a server base-URL string. Returns
-- 'Left' with a message if the URL does not parse.
mkServerEnv :: String -> IO (Either String ClientEnv)
mkServerEnv url =
    case parseBaseUrl url of
        Nothing -> pure (Left ("invalid server URL: " <> url))
        Just base -> do
            manager <- newTlsManager
            pure (Right (mkClientEnv manager base))

-- | @GET \/tokens@ — list known token ids.
listTokens :: ClientEnv -> IO (Either ClientError [TokenIdJSON])
listTokens = runClientM tokensClient

-- | @GET \/tokens\/:id\/facts\/:key@ — look up a fact with proof.
getFact
    :: ClientEnv
    -> TokenIdJSON
    -> Hex
    -> IO (Either ClientError FactResponse)
getFact env tid key = runClientM (factClient tid key) env

-- | @POST \/submit@ — submit signed tx CBOR, returning the txId bytes.
submitSignedTx
    :: ClientEnv
    -> ByteString
    -> IO (Either ClientError ByteString)
submitSignedTx env signedCbor =
    fmap
        (fmap (unHex . srTxId))
        (runClientM (submitClient (SubmitRequest (Hex signedCbor))) env)

-- | @GET \/tx\/:txId?timeout@ — block until the tx is indexed.
awaitTx
    :: ClientEnv
    -> ByteString
    -> Word64
    -> IO (Either ClientError NoContent)
awaitTx env txId timeout =
    runClientM (awaitClient (Hex txId) (Just timeout)) env

-- Servant-derived clients ----------------------------------------------

tokensClient :: ClientM [TokenIdJSON]
tokensClient = client (Proxy @TokensAPI)

factClient :: TokenIdJSON -> Hex -> ClientM FactResponse
factClient = client (Proxy @TokenFactAPI)

submitClient :: SubmitRequest -> ClientM SubmitResponse
submitClient = client (Proxy @TxSubmitAPI)

awaitClient :: Hex -> Maybe Word64 -> ClientM NoContent
awaitClient = client (Proxy @TxAwaitAPI)
