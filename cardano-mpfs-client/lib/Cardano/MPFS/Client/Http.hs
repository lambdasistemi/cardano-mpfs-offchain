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
    , BootTxParams (..)
    , RequestInsertParams (..)
    , RequestDeleteParams (..)
    , RequestUpdateParams (..)
    , RetractParams (..)
    , RejectParams (..)
    , UpdateParams (..)
    , EndParams (..)

      -- * Write endpoints
    , bootTx
    , requestInsertTx
    , requestDeleteTx
    , requestUpdateTx
    , retractTx
    , rejectTx
    , updateTx
    , sweepTx
    , SweepParams (..)
    , endTx
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
import Servant.API ((:<|>) (..))
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

import Cardano.MPFS.API (TxWriteAPI)
import Cardano.MPFS.API.Types qualified as Wire
import Cardano.MPFS.Client.Bundle
    ( BootTxResponse
    , EndTxResponse
    , RejectTxResponse
    , RequestTxResponse
    , RetractTxResponse
    , UpdateTxResponse
    )
import Cardano.MPFS.Client.Snapshot (Hex)
import Cardano.MPFS.Client.Verify
    ( VerifyError
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyUpdateTxResponse
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

-- | @POST /tx/boot@ request body.
newtype BootTxParams = BootTxParams
    { bootAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON BootTxParams where
    toJSON BootTxParams{..} =
        object ["address" .= bootAddress]

-- | @POST /tx/request/insert@ request body.
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

-- | @POST /tx/request/delete@ request body.
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

-- | @POST /tx/request/update@ request body.
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

-- | @POST /tx/retract@ request body.
data RetractParams = RetractParams
    { retractUtxo :: Text
    , retractAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RetractParams where
    toJSON RetractParams{..} =
        object
            [ "utxo" .= retractUtxo
            , "address" .= retractAddress
            ]

-- | @POST /tx/reject@ request body.
data RejectParams = RejectParams
    { rejectToken :: Hex
    , rejectAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON RejectParams where
    toJSON RejectParams{..} =
        object
            [ "token" .= rejectToken
            , "address" .= rejectAddress
            ]

-- | @POST /tx/update@ request body.
data UpdateParams = UpdateParams
    { updateToken :: Hex
    , updateAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON UpdateParams where
    toJSON UpdateParams{..} =
        object
            [ "token" .= updateToken
            , "address" .= updateAddress
            ]

-- | @POST /tx/end@ request body.
data EndParams = EndParams
    { endToken :: Hex
    , endAddress :: Hex
    }
    deriving stock (Eq, Show)

instance ToJSON EndParams where
    toJSON EndParams{..} =
        object
            [ "token" .= endToken
            , "address" .= endAddress
            ]

-- | Build a boot transaction.
bootTx
    :: MpfsHttp
    -> BootTxParams
    -> IO (Either ClientError BootTxResponse)
bootTx http params =
    runWriteEndpoint http params txBootClient verifyBootTxResponse

-- | Build an insert-request transaction.
requestInsertTx
    :: MpfsHttp
    -> RequestInsertParams
    -> IO (Either ClientError RequestTxResponse)
requestInsertTx http params =
    runWriteEndpoint http params txInsertClient verifyRequestTxResponse

-- | Build a delete-request transaction.
requestDeleteTx
    :: MpfsHttp
    -> RequestDeleteParams
    -> IO (Either ClientError RequestTxResponse)
requestDeleteTx http params =
    runWriteEndpoint http params txDeleteClient verifyRequestTxResponse

-- | Build an update-value request transaction.
requestUpdateTx
    :: MpfsHttp
    -> RequestUpdateParams
    -> IO (Either ClientError RequestTxResponse)
requestUpdateTx http params =
    runWriteEndpoint
        http
        params
        txRequestUpdateClient
        verifyRequestTxResponse

-- | Build a retract transaction.
retractTx
    :: MpfsHttp
    -> RetractParams
    -> IO (Either ClientError RetractTxResponse)
retractTx http params =
    runWriteEndpoint http params txRetractClient verifyRetractTxResponse

-- | Build a reject transaction.
rejectTx
    :: MpfsHttp
    -> RejectParams
    -> IO (Either ClientError RejectTxResponse)
rejectTx http params =
    runWriteEndpoint http params txRejectClient verifyRejectTxResponse

-- | Build an update transaction.
updateTx
    :: MpfsHttp
    -> UpdateParams
    -> IO (Either ClientError UpdateTxResponse)
updateTx http params =
    runWriteEndpoint http params txUpdateClient verifyUpdateTxResponse

-- | Build an end transaction.
endTx
    :: MpfsHttp
    -> EndParams
    -> IO (Either ClientError EndTxResponse)
endTx http params =
    runWriteEndpoint http params txEndClient verifyEndTxResponse

runWriteEndpoint
    :: ( ToJSON params
       , FromJSON wireRequest
       , ToJSON wireResponse
       , FromJSON response
       )
    => MpfsHttp
    -> params
    -> (wireRequest -> ClientM wireResponse)
    -> (response -> Either VerifyError ())
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
    -> (response -> Either VerifyError ())
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
                Right () -> Right decoded

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

txBootClient
    :: Wire.BootRequest -> ClientM Wire.BootTxResponse
txInsertClient
    :: Wire.InsertRequest -> ClientM Wire.RequestTxResponse
txDeleteClient
    :: Wire.DeleteRequest -> ClientM Wire.RequestTxResponse
txRequestUpdateClient
    :: Wire.UpdateValueRequest -> ClientM Wire.RequestTxResponse
txRejectClient
    :: Wire.RejectRequest -> ClientM Wire.RejectTxResponse
txUpdateClient
    :: Wire.UpdateRequest -> ClientM Wire.UpdateTxResponse
txRetractClient
    :: Wire.RetractRequest -> ClientM Wire.RetractTxResponse
txSweepClient
    :: Wire.SweepRequest -> ClientM Wire.SweepTxResponse
txEndClient
    :: Wire.EndRequest -> ClientM Wire.EndTxResponse
txBootClient
    :<|> txInsertClient
    :<|> txDeleteClient
    :<|> txRequestUpdateClient
    :<|> txRejectClient
    :<|> txUpdateClient
    :<|> txRetractClient
    :<|> txSweepClient
    :<|> txEndClient =
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
