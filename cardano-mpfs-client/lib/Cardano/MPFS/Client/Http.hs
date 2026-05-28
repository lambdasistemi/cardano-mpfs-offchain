-- |
-- Module      : Cardano.MPFS.Client.Http
-- Description : Servant-derived HTTP wrappers for MPFS write endpoints.
--
-- Native-Haskell transport helpers for consumers such as the MOOG CLI.
-- The endpoint paths and request/response wire shapes are derived from
-- the shared Servant API package; this module keeps the existing client
-- response types so callers can verify before signing.
module Cardano.MPFS.Client.Http
    ( -- * Configuration
      BaseUrl (..)
    , Scheme (..)
    , VerifierMode (..)
    , MpfsHttp (..)

      -- * Errors
    , ClientError (..)

      -- * Request parameters
    , BootFactsParams (..)
    , RequestInsertParams (..)
    , RequestDeleteParams (..)
    , RequestUpdateParams (..)
    , RejectFactsParams (..)
    , UpdateFactsParams (..)

      -- * Write endpoints
    , bootFacts
    , requestInsertFacts
    , requestDeleteFacts
    , requestUpdateFacts
    , updateFacts
    , rejectFacts
    , sweepTx
    , SweepParams (..)
    ) where

import Data.Aeson
    ( FromJSON
    , ToJSON (..)
    , eitherDecode
    , encode
    , object
    , (.=)
    )
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Network.HTTP.Client (Manager)
import Network.HTTP.Types.Status (statusCode)
import Servant.Client
    ( BaseUrl (..)
    , ClientM
    , Scheme (..)
    , client
    , mkClientEnv
    , runClientM
    )
import Servant.Client qualified as Servant
import Servant.Client.Core.Response
    ( responseBody
    , responseStatusCode
    )

import Cardano.MPFS.API
    ( FactsBootAPI
    , FactsRejectAPI
    , FactsRequestDeleteAPI
    , FactsRequestInsertAPI
    , FactsRequestUpdateAPI
    , FactsUpdateAPI
    , TxWriteAPI
    )
import Cardano.MPFS.API.Types qualified as Wire
import Cardano.MPFS.API.Types.Facts qualified as FactsWire
import Cardano.MPFS.Client.Snapshot (Hex)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot)
import Cardano.MPFS.Client.Verify
    ( VerifyError
    , verifyBootFacts
    , verifyRejectFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    , verifyUpdateFacts
    )

-- | Whether HTTP wrappers run the offline verifier before returning a
-- decoded response.
data VerifierMode
    = RunVerifier
    | SkipVerifier
    deriving stock (Eq, Show)

-- | Shared HTTP client configuration.
data MpfsHttp = MpfsHttp
    { manager :: Manager
    , baseUrl :: BaseUrl
    , verifier :: VerifierMode
    }

-- | Errors surfaced by the typed HTTP wrappers.
data ClientError
    = TransportError Servant.ClientError
    | StatusError Int BS.ByteString
    | DecodeError String
    | RequestEncodingError String
    | VerifyFailed VerifyError
    deriving stock (Show)

-- | @POST /facts/boot@ request body.
newtype BootFactsParams = BootFactsParams
    { bootFactsAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON BootFactsParams where
    toJSON BootFactsParams{..} =
        object ["address" .= bootFactsAddress]

-- | @POST /facts/request/insert@ request body.
data RequestInsertParams = RequestInsertParams
    { requestInsertToken :: Hex
    , requestInsertKey :: Hex
    , requestInsertValue :: Hex
    , requestInsertAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RequestInsertParams where
    toJSON RequestInsertParams{..} =
        object
            [ "token" .= requestInsertToken
            , "key" .= requestInsertKey
            , "value" .= requestInsertValue
            , "address" .= requestInsertAddress
            ]

-- | @POST /facts/request/delete@ request body.
data RequestDeleteParams = RequestDeleteParams
    { requestDeleteToken :: Hex
    , requestDeleteKey :: Hex
    , requestDeleteValue :: Hex
    , requestDeleteAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RequestDeleteParams where
    toJSON RequestDeleteParams{..} =
        object
            [ "token" .= requestDeleteToken
            , "key" .= requestDeleteKey
            , "value" .= requestDeleteValue
            , "address" .= requestDeleteAddress
            ]

-- | @POST /facts/request/update@ request body.
data RequestUpdateParams = RequestUpdateParams
    { requestUpdateToken :: Hex
    , requestUpdateKey :: Hex
    , requestUpdateOldValue :: Hex
    , requestUpdateNewValue :: Hex
    , requestUpdateAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RequestUpdateParams where
    toJSON RequestUpdateParams{..} =
        object
            [ "token" .= requestUpdateToken
            , "key" .= requestUpdateKey
            , "old_value" .= requestUpdateOldValue
            , "new_value" .= requestUpdateNewValue
            , "address" .= requestUpdateAddress
            ]

-- | @POST /facts/reject@ request body.
data RejectFactsParams = RejectFactsParams
    { rejectFactsToken :: Hex
    , rejectFactsAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RejectFactsParams where
    toJSON RejectFactsParams{..} =
        object
            [ "token" .= rejectFactsToken
            , "address" .= rejectFactsAddress
            ]

-- | @POST /facts/update@ request body.
data UpdateFactsParams = UpdateFactsParams
    { updateFactsToken :: Hex
    , updateFactsAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON UpdateFactsParams where
    toJSON UpdateFactsParams{..} =
        object
            [ "token" .= updateFactsToken
            , "address" .= updateFactsAddress
            ]

-- | Fetch boot facts. The post-split #243 shape: caller supplies
-- an externally-trusted CSMT root; the verifier checks
-- @snapshot.utxo_root@ matches that root and replays each bundled
-- wallet UTxO inclusion proof against it.
bootFacts
    :: MpfsHttp
    -> TrustedRoot
    -> BootFactsParams
    -> IO (Either ClientError FactsWire.BootFacts)
bootFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsBootClient
        (verifyBootFacts trustedRoot)

-- | Fetch request-insert facts and verify the bundled wallet UTxO
-- witnesses against the externally-trusted CSMT root.
requestInsertFacts
    :: MpfsHttp
    -> TrustedRoot
    -> RequestInsertParams
    -> IO (Either ClientError FactsWire.RequestInsertFacts)
requestInsertFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsRequestInsertClient
        (verifyRequestInsertFacts trustedRoot)

-- | Fetch request-delete facts and verify the bundled wallet UTxO
-- witnesses against the externally-trusted CSMT root.
requestDeleteFacts
    :: MpfsHttp
    -> TrustedRoot
    -> RequestDeleteParams
    -> IO (Either ClientError FactsWire.RequestDeleteFacts)
requestDeleteFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsRequestDeleteClient
        (verifyRequestDeleteFacts trustedRoot)

-- | Fetch request-update facts and verify the bundled wallet UTxO
-- witnesses against the externally-trusted CSMT root.
requestUpdateFacts
    :: MpfsHttp
    -> TrustedRoot
    -> RequestUpdateParams
    -> IO (Either ClientError FactsWire.RequestUpdateFacts)
requestUpdateFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsRequestUpdateClient
        (verifyRequestUpdateFacts trustedRoot)

-- | Fetch update facts and verify the bundled state,
-- request, wallet, and trie witnesses against the
-- externally-trusted CSMT root.
updateFacts
    :: MpfsHttp
    -> TrustedRoot
    -> UpdateFactsParams
    -> IO (Either ClientError FactsWire.UpdateFacts)
updateFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsUpdateClient
        (verifyUpdateFacts trustedRoot)

-- | Fetch reject facts and verify the bundled state,
-- request, and wallet UTxO witnesses against the
-- externally-trusted CSMT root.
rejectFacts
    :: MpfsHttp
    -> TrustedRoot
    -> RejectFactsParams
    -> IO (Either ClientError FactsWire.RejectFacts)
rejectFacts http trustedRoot params =
    runWriteEndpoint
        http
        params
        factsRejectClient
        (verifyRejectFacts trustedRoot)

runWriteEndpoint
    :: ( ToJSON params
       , FromJSON wireRequest
       , ToJSON wireResponse
       , FromJSON response
       )
    => MpfsHttp
    -> params
    -> (wireRequest -> ClientM wireResponse)
    -> (response -> Either VerifyError verified)
    -> IO (Either ClientError response)
runWriteEndpoint MpfsHttp{..} params endpoint verifyResponse =
    case decodeRequest params of
        Left err -> pure (Left err)
        Right wireRequest -> do
            result <-
                runClientM
                    (endpoint wireRequest)
                    (mkClientEnv manager baseUrl)
            pure $ case result of
                Left err -> Left (fromServantError err)
                Right wireResponse ->
                    decodeResponse verifier verifyResponse wireResponse

decodeRequest
    :: (ToJSON params, FromJSON wireRequest)
    => params
    -> Either ClientError wireRequest
decodeRequest params =
    case eitherDecode (encode params) of
        Left err -> Left (RequestEncodingError err)
        Right wireRequest -> Right wireRequest

decodeResponse
    :: (ToJSON wireResponse, FromJSON response)
    => VerifierMode
    -> (response -> Either VerifyError verified)
    -> wireResponse
    -> Either ClientError response
decodeResponse mode verifyResponse wireResponse = do
    decoded <- case eitherDecode (encode wireResponse) of
        Left err -> Left (DecodeError err)
        Right response -> Right response
    case mode of
        SkipVerifier -> Right decoded
        RunVerifier ->
            case verifyResponse decoded of
                Left err -> Left (VerifyFailed err)
                Right _ -> Right decoded

fromServantError :: Servant.ClientError -> ClientError
fromServantError err =
    case err of
        Servant.FailureResponse _ response ->
            StatusError
                (statusCode $ responseStatusCode response)
                (BSL.toStrict $ responseBody response)
        Servant.DecodeFailure message _ ->
            DecodeError (T.unpack message)
        _ ->
            TransportError err

factsBootClient
    :: Wire.BootRequest -> ClientM FactsWire.BootFacts
factsRequestInsertClient
    :: Wire.InsertRequest -> ClientM FactsWire.RequestInsertFacts
factsRequestDeleteClient
    :: Wire.DeleteRequest -> ClientM FactsWire.RequestDeleteFacts
factsRequestUpdateClient
    :: Wire.UpdateValueRequest -> ClientM FactsWire.RequestUpdateFacts
factsUpdateClient
    :: Wire.UpdateRequest -> ClientM FactsWire.UpdateFacts
factsRejectClient
    :: Wire.RejectRequest -> ClientM FactsWire.RejectFacts
txSweepClient
    :: Wire.SweepRequest -> ClientM Wire.SweepTxResponse
factsBootClient =
    client (Proxy :: Proxy FactsBootAPI)

factsRequestInsertClient =
    client (Proxy :: Proxy FactsRequestInsertAPI)

factsRequestDeleteClient =
    client (Proxy :: Proxy FactsRequestDeleteAPI)

factsRequestUpdateClient =
    client (Proxy :: Proxy FactsRequestUpdateAPI)

factsUpdateClient =
    client (Proxy :: Proxy FactsUpdateAPI)

factsRejectClient =
    client (Proxy :: Proxy FactsRejectAPI)

txSweepClient =
    client (Proxy :: Proxy TxWriteAPI)

-- | @POST /tx/sweep@ request body.
data SweepParams = SweepParams
    { sweepToken :: Hex
    , sweepUtxo :: Text
    , sweepAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON SweepParams where
    toJSON SweepParams{..} =
        object
            [ "token" .= sweepToken
            , "utxo" .= sweepUtxo
            , "address" .= sweepAddress
            ]

-- | Build a sweep transaction.
--
-- Sweep responses do not carry a proof envelope (the
-- on-chain validator enforces the owner-signature
-- predicate against the referenced state UTxO), so
-- the verifier is a no-op. The caller is still
-- responsible for signing the returned CBOR before
-- submission.
sweepTx
    :: MpfsHttp
    -> SweepParams
    -> IO (Either ClientError Wire.SweepTxResponse)
sweepTx http params =
    runWriteEndpoint
        http
        params
        txSweepClient
        (\_ -> Right ())
