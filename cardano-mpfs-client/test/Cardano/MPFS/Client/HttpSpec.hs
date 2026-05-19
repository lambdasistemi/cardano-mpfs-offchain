{-# LANGUAGE LambdaCase #-}

module Cardano.MPFS.Client.HttpSpec (spec) where

import Control.Monad
    ( forM_
    , void
    )
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.IORef
    ( IORef
    , newIORef
    , readIORef
    , writeIORef
    )
import Data.Text (Text)
import Data.Text qualified as T
import Network.HTTP.Client
    ( defaultManagerSettings
    , newManager
    )
import Network.HTTP.Types
    ( Status
    , status200
    , status500
    )
import Network.Wai qualified as Wai
import Network.Wai.Handler.Warp qualified as Warp
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types qualified as Wire
import Cardano.MPFS.Client
    ( BaseUrl (..)
    , BootFactsParams (..)
    , ClientError (..)
    , Hex (..)
    , MpfsHttp (..)
    , RejectParams (..)
    , RequestDeleteParams (..)
    , RequestInsertParams (..)
    , RequestUpdateParams (..)
    , RetractParams (..)
    , Scheme (..)
    , UpdateParams (..)
    , VerifierMode (..)
    , VerifyError (..)
    , bootFacts
    , rejectTx
    , requestDeleteTx
    , requestInsertFacts
    , requestUpdateTx
    , retractTx
    , updateTx
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootTrustedRoot
    , honestRejectResponse
    , honestRequestResponse
    , honestRetractResponse
    , honestUnsignedBootResponse
    , honestUpdateResponse
    )

spec :: Spec
spec = do
    describe "Cardano.MPFS.Client.Http write endpoints" $ do
        it "posts each write request to the expected endpoint" $ do
            forM_ writeEndpointCases $ \EndpointCase{..} ->
                withJsonServer status200 endpointResponse $ \seen base -> do
                    client <- mkClient base SkipVerifier
                    result <- endpointCall client
                    result `shouldSatisfy` isRight
                    assertSeen seen endpointPath endpointRequest

        it "runs the verifier when configured with RunVerifier"
            $ withJsonServer
                status200
                (Aeson.encode honestBootFacts)
            $ \_ base -> do
                client <- mkClient base RunVerifier
                result <-
                    bootFacts
                        client
                        honestBootTrustedRoot
                        bootFactsParams
                result `shouldBeRight` honestBootFacts

        it "returns VerifyFailed when the verifier rejects" $ do
            -- Forge: server emits a snapshot whose utxo_root
            -- doesn't match the externally-supplied trusted
            -- root. Verifier emits TrustedRootMismatch.
            let forged :: Wire.BootFacts
                forged =
                    honestBootFacts
                        { Wire.bfSnapshot =
                            ( Wire.bfSnapshot
                                honestBootFacts
                            )
                                { Wire.vsUtxoRoot =
                                    Wire.Hex
                                        (BS.replicate 32 0xAB)
                                }
                        }
            withJsonServer status200 (Aeson.encode forged) $ \_ base -> do
                client <- mkClient base RunVerifier
                result <-
                    bootFacts
                        client
                        honestBootTrustedRoot
                        bootFactsParams
                result `shouldSatisfy` isTrustedRootMismatch

        it "can skip verification for inspection tooling" $ do
            let forged :: Wire.BootFacts
                forged =
                    honestBootFacts
                        { Wire.bfSnapshot =
                            (Wire.bfSnapshot honestBootFacts)
                                { Wire.vsUtxoRoot =
                                    Wire.Hex
                                        (BS.replicate 32 0xAB)
                                }
                        }
            withJsonServer status200 (Aeson.encode forged) $ \_ base -> do
                client <- mkClient base SkipVerifier
                result <-
                    bootFacts
                        client
                        honestBootTrustedRoot
                        bootFactsParams
                result `shouldBeRight` forged

        it "surfaces non-success HTTP statuses"
            $ withJsonServer status500 "server failed"
            $ \_ base -> do
                client <- mkClient base SkipVerifier
                result <-
                    bootFacts
                        client
                        honestBootTrustedRoot
                        bootFactsParams
                result
                    `shouldSatisfy` \case
                        Left (StatusError 500 "server failed") -> True
                        _ -> False

        it "surfaces JSON decode failures"
            $ withJsonServer status200 "not-json"
            $ \_ base -> do
                client <- mkClient base SkipVerifier
                result <-
                    bootFacts
                        client
                        honestBootTrustedRoot
                        bootFactsParams
                result
                    `shouldSatisfy` \case
                        Left (DecodeError _) -> True
                        _ -> False

        it "surfaces transport failures" $ do
            client <-
                mkClient
                    (BaseUrl Http "127.0.0.1" 1 "")
                    SkipVerifier
            result <-
                bootFacts
                    client
                    honestBootTrustedRoot
                    bootFactsParams
            result
                `shouldSatisfy` \case
                    Left (TransportError _) -> True
                    _ -> False

data EndpointCase = EndpointCase
    { endpointPath :: [Text]
    , endpointRequest :: Aeson.Value
    , endpointResponse :: BSL.ByteString
    , endpointCall :: MpfsHttp -> IO (Either ClientError ())
    }

writeEndpointCases :: [EndpointCase]
writeEndpointCases =
    [ EndpointCase
        ["facts", "boot"]
        (Aeson.toJSON bootFactsParams)
        (Aeson.encode honestBootFacts)
        ( \http ->
            voidRight
                (bootFacts http honestBootTrustedRoot bootFactsParams)
        )
    , EndpointCase
        ["facts", "request", "insert"]
        (Aeson.toJSON insertParams)
        (Aeson.encode honestRequestInsertFacts)
        ( \http ->
            voidRight
                ( requestInsertFacts
                    http
                    honestBootTrustedRoot
                    insertParams
                )
        )
    , EndpointCase
        ["tx", "request", "delete"]
        (Aeson.toJSON deleteParams)
        (Aeson.encode honestRequestResponse)
        (voidRight . (`requestDeleteTx` deleteParams))
    , EndpointCase
        ["tx", "request", "update"]
        (Aeson.toJSON requestUpdateParams)
        (Aeson.encode honestRequestResponse)
        (voidRight . (`requestUpdateTx` requestUpdateParams))
    , EndpointCase
        ["tx", "retract"]
        (Aeson.toJSON retractParams)
        (Aeson.encode honestRetractResponse)
        (voidRight . (`retractTx` retractParams))
    , EndpointCase
        ["tx", "reject"]
        (Aeson.toJSON rejectParams)
        (Aeson.encode honestRejectResponse)
        (voidRight . (`rejectTx` rejectParams))
    , EndpointCase
        ["tx", "update"]
        (Aeson.toJSON updateParams)
        (Aeson.encode honestUpdateResponse)
        (voidRight . (`updateTx` updateParams))
    ]

bootFactsParams :: BootFactsParams
bootFactsParams = BootFactsParams sampleAddress

honestBootFacts :: Wire.BootFacts
honestBootFacts =
    Wire.BootFacts
        { Wire.bfSnapshot =
            Wire.utrSnapshot honestUnsignedBootResponse
        , Wire.bfWalletUtxos =
            Wire.utrInputs honestUnsignedBootResponse
        , Wire.bfProtocolParameters =
            Wire.UnverifiedPParams
                { Wire.uppVerified = False
                , Wire.uppCbor = Wire.Hex "\x82\x01\x02"
                }
        }

honestRequestInsertFacts :: Wire.RequestInsertFacts
honestRequestInsertFacts =
    Wire.RequestInsertFacts
        { Wire.rifSnapshot =
            Wire.utrSnapshot honestUnsignedBootResponse
        , Wire.rifToken = Wire.TokenIdJSON "00"
        , Wire.rifKey = Wire.Hex "11"
        , Wire.rifValue = Wire.Hex "22"
        , Wire.rifAddress = Wire.Hex "aabbcc"
        , Wire.rifSubmittedAt = 1_700_000_000_000
        , Wire.rifWalletUtxos =
            Wire.utrInputs honestUnsignedBootResponse
        , Wire.rifProtocolParameters =
            Wire.UnverifiedPParams
                { Wire.uppVerified = False
                , Wire.uppCbor = Wire.Hex "\x82\x01\x02"
                }
        }

insertParams :: RequestInsertParams
insertParams =
    RequestInsertParams sampleToken sampleKey sampleValue sampleAddress

deleteParams :: RequestDeleteParams
deleteParams =
    RequestDeleteParams sampleToken sampleKey sampleValue sampleAddress

requestUpdateParams :: RequestUpdateParams
requestUpdateParams =
    RequestUpdateParams
        sampleToken
        sampleKey
        sampleOldValue
        sampleNewValue
        sampleAddress

retractParams :: RetractParams
retractParams = RetractParams "abcd#0" sampleAddress

rejectParams :: RejectParams
rejectParams = RejectParams sampleToken sampleAddress

updateParams :: UpdateParams
updateParams = UpdateParams sampleToken sampleAddress

sampleAddress
    , sampleToken
    , sampleKey
    , sampleValue
    , sampleOldValue
    , sampleNewValue
        :: Hex
sampleAddress = Hex "aabbcc"
sampleToken = Hex "00"
sampleKey = Hex "11"
sampleValue = Hex "22"
sampleOldValue = Hex "33"
sampleNewValue = Hex "44"

data SeenRequest = SeenRequest
    { seenPath :: [Text]
    , seenMethod :: ByteString
    , seenBody :: BSL.ByteString
    }
    deriving stock (Eq, Show)

withJsonServer
    :: Status
    -> BSL.ByteString
    -> (IORef (Maybe SeenRequest) -> BaseUrl -> IO a)
    -> IO a
withJsonServer responseStatus responseBody action = do
    seen <- newIORef Nothing
    Warp.testWithApplication (pure $ app seen) $ \port ->
        action
            seen
            (BaseUrl Http "127.0.0.1" port "")
  where
    app seen request respond = do
        body <- Wai.strictRequestBody request
        writeIORef seen
            $ Just
            $ SeenRequest
                { seenPath = Wai.pathInfo request
                , seenMethod = Wai.requestMethod request
                , seenBody = body
                }
        respond
            $ Wai.responseLBS
                responseStatus
                [("Content-Type", "application/json")]
                responseBody

mkClient :: BaseUrl -> VerifierMode -> IO MpfsHttp
mkClient baseUrl verifier = do
    manager <- newManager defaultManagerSettings
    pure MpfsHttp{manager, baseUrl, verifier}

assertSeen
    :: IORef (Maybe SeenRequest)
    -> [Text]
    -> Aeson.Value
    -> IO ()
assertSeen seen expectedPath expectedBody = do
    observed <- readIORef seen
    case observed of
        Nothing -> expectationFailure "server did not receive a request"
        Just SeenRequest{..} -> do
            seenPath `shouldBe` expectedPath
            seenMethod `shouldBe` "POST"
            (Aeson.eitherDecode seenBody :: Either String Aeson.Value)
                `shouldBe` Right expectedBody

voidRight :: IO (Either ClientError a) -> IO (Either ClientError ())
voidRight = fmap void

isRight :: Either ClientError () -> Bool
isRight = \case
    Right () -> True
    Left _ -> False

isTrustedRootMismatch :: Either ClientError a -> Bool
isTrustedRootMismatch = \case
    Left
        ( VerifyFailed
                (TrustedRootMismatch path)
            ) ->
            path == T.pack "boot.snapshot.utxo_root"
    _ -> False

shouldBeRight :: (Eq a, Show a) => Either ClientError a -> a -> IO ()
shouldBeRight result expected =
    case result of
        Right actual -> actual `shouldBe` expected
        Left err -> expectationFailure $ "expected Right, got " <> show err
