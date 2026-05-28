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
    , shouldNotContain
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types qualified as Wire
import Cardano.MPFS.API.Types.Facts qualified as FactsWire
import Cardano.MPFS.Client
    ( BaseUrl (..)
    , BootFactsParams (..)
    , ClientError (..)
    , Hex (..)
    , MpfsHttp (..)
    , RejectFactsParams (..)
    , RequestDeleteParams (..)
    , RequestInsertParams (..)
    , RequestUpdateParams (..)
    , Scheme (..)
    , UpdateFactsParams (..)
    , VerifierMode (..)
    , VerifyError (..)
    , bootFacts
    , rejectFacts
    , requestDeleteFacts
    , requestInsertFacts
    , requestUpdateFacts
    , updateFacts
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootTrustedRoot
    , honestUnsignedBootResponse
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

        it "does not post any write case to the legacy update tx route"
            $ map endpointPath writeEndpointCases
            `shouldNotContain` [["tx", "update"]]

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
        ["facts", "request", "delete"]
        (Aeson.toJSON deleteParams)
        (Aeson.encode honestRequestDeleteFacts)
        ( \http ->
            voidRight
                ( requestDeleteFacts
                    http
                    honestBootTrustedRoot
                    deleteParams
                )
        )
    , EndpointCase
        ["facts", "request", "update"]
        (Aeson.toJSON requestUpdateParams)
        (Aeson.encode honestRequestUpdateFacts)
        ( \http ->
            voidRight
                ( requestUpdateFacts
                    http
                    honestBootTrustedRoot
                    requestUpdateParams
                )
        )
    , EndpointCase
        ["facts", "reject"]
        (Aeson.toJSON rejectFactsParams)
        (Aeson.encode honestRejectFacts)
        ( \http ->
            voidRight
                ( rejectFacts
                    http
                    honestBootTrustedRoot
                    rejectFactsParams
                )
        )
    , EndpointCase
        ["facts", "update"]
        (Aeson.toJSON updateFactsParams)
        (Aeson.encode honestUpdateFacts)
        ( \http ->
            voidRight
                ( updateFacts
                    http
                    honestBootTrustedRoot
                    updateFactsParams
                )
        )
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

honestRequestDeleteFacts :: Wire.RequestDeleteFacts
honestRequestDeleteFacts =
    Wire.RequestDeleteFacts
        { Wire.rdfSnapshot =
            Wire.utrSnapshot honestUnsignedBootResponse
        , Wire.rdfToken = Wire.TokenIdJSON "00"
        , Wire.rdfKey = Wire.Hex "11"
        , Wire.rdfValue = Wire.Hex "22"
        , Wire.rdfAddress = Wire.Hex "aabbcc"
        , Wire.rdfSubmittedAt = 1_700_000_000_000
        , Wire.rdfWalletUtxos =
            Wire.utrInputs honestUnsignedBootResponse
        , Wire.rdfProtocolParameters =
            Wire.UnverifiedPParams
                { Wire.uppVerified = False
                , Wire.uppCbor = Wire.Hex "\x82\x01\x02"
                }
        }

honestRequestUpdateFacts :: Wire.RequestUpdateFacts
honestRequestUpdateFacts =
    Wire.RequestUpdateFacts
        { Wire.rufSnapshot =
            Wire.utrSnapshot honestUnsignedBootResponse
        , Wire.rufToken = Wire.TokenIdJSON "00"
        , Wire.rufKey = Wire.Hex "11"
        , Wire.rufOldValue = Wire.Hex "33"
        , Wire.rufNewValue = Wire.Hex "44"
        , Wire.rufAddress = Wire.Hex "aabbcc"
        , Wire.rufSubmittedAt = 1_700_000_000_000
        , Wire.rufWalletUtxos =
            Wire.utrInputs honestUnsignedBootResponse
        , Wire.rufProtocolParameters =
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

rejectFactsParams :: RejectFactsParams
rejectFactsParams = RejectFactsParams sampleToken sampleAddress

updateFactsParams :: UpdateFactsParams
updateFactsParams = UpdateFactsParams sampleToken sampleAddress

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

honestUpdateFacts :: FactsWire.UpdateFacts
honestUpdateFacts =
    FactsWire.UpdateFacts
        { FactsWire.ufSnapshot =
            Wire.VerificationSnapshot
                { Wire.vsUtxoRoot =
                    Wire.Hex (BS.replicate 32 0x11)
                , Wire.vsChainPoint =
                    Wire.ChainPointJSON
                        { Wire.cpSlot = 42
                        , Wire.cpBlockId =
                            Wire.Hex (BS.replicate 32 0x22)
                        }
                }
        , FactsWire.ufToken = Wire.TokenIdJSON "\x00"
        , FactsWire.ufStateUtxo = sampleUtxoEntry 0
        , FactsWire.ufRequestUtxos = [sampleUtxoEntry 1]
        , FactsWire.ufWalletUtxos = [sampleUtxoEntry 2]
        , FactsWire.ufTrieRoot = Wire.Hex (BS.replicate 32 0x33)
        , FactsWire.ufTrieFacts =
            [ FactsWire.TrieFact
                { FactsWire.tfKey = Wire.Hex "key"
                , FactsWire.tfValue = Just (Wire.Hex "value")
                , FactsWire.tfMpfProof = Wire.Hex "proof"
                }
            ]
        , FactsWire.ufValidityUpperSlot = 100
        , FactsWire.ufProtocolParameters =
            Wire.UnverifiedPParams
                { Wire.uppVerified = False
                , Wire.uppCbor = Wire.Hex "\x82\x01\x02"
                }
        }

honestRejectFacts :: FactsWire.RejectFacts
honestRejectFacts =
    FactsWire.RejectFacts
        { FactsWire.rfSnapshot =
            Wire.VerificationSnapshot
                { Wire.vsUtxoRoot =
                    Wire.Hex (BS.replicate 32 0x11)
                , Wire.vsChainPoint =
                    Wire.ChainPointJSON
                        { Wire.cpSlot = 42
                        , Wire.cpBlockId =
                            Wire.Hex (BS.replicate 32 0x22)
                        }
                }
        , FactsWire.rfToken = Wire.TokenIdJSON "\x00"
        , FactsWire.rfStateUtxo = sampleUtxoEntry 0
        , FactsWire.rfRequestUtxos = [sampleUtxoEntry 1]
        , FactsWire.rfWalletUtxos = [sampleUtxoEntry 2]
        , FactsWire.rfValidityLowerSlot = 100
        , FactsWire.rfValidityUpperSlot = 200
        , FactsWire.rfProtocolParameters =
            Wire.UnverifiedPParams
                { Wire.uppVerified = False
                , Wire.uppCbor = Wire.Hex "\x82\x01\x02"
                }
        }

sampleUtxoEntry :: Int -> Wire.UtxoEntry
sampleUtxoEntry ix =
    Wire.UtxoEntry
        { Wire.ueRef =
            Wire.UtxoRef
                { Wire.urTxId = Wire.Hex (BS.replicate 32 0x44)
                , Wire.urTxIx = fromIntegral ix
                }
        , Wire.ueTxOutCbor = Wire.Hex "\x82\x01\x02"
        , Wire.ueInclusionProof = Wire.Hex "proof"
        }

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
