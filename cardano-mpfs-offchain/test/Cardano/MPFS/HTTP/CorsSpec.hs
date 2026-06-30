{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.CorsSpec
-- Description : Tests for CORS handling on the HTTP API
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.CorsSpec
    ( spec
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy.Char8 qualified as BSL8
import Network.HTTP.Types
    ( HeaderName
    , methodGet
    , methodOptions
    , status200
    )
import Network.Wai
    ( requestHeaders
    , requestMethod
    )
import Network.Wai.Test
    ( SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    )
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldContain
    , shouldSatisfy
    )

import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)

previewOrigin :: ByteString
previewOrigin = "https://preview.dev.plutimus.com"

corsHeader :: HeaderName -> SResponse -> Maybe ByteString
corsHeader name resp = lookup name (simpleHeaders resp)

shouldAllowPreviewOrigin :: Maybe ByteString -> IO ()
shouldAllowPreviewOrigin actual =
    actual `shouldBe` Just previewOrigin

shouldAllowRequiredMethods :: Maybe ByteString -> IO ()
shouldAllowRequiredMethods actual =
    actual
        `shouldSatisfy` maybe
            False
            ( \methods ->
                all
                    (`BS.isInfixOf` methods)
                    ["GET", "POST", "OPTIONS"]
            )

shouldAllowContentType :: Maybe ByteString -> IO ()
shouldAllowContentType actual =
    actual
        `shouldSatisfy` maybe
            False
            ("content-type" `BS.isInfixOf`)

requestCors :: ByteString -> IO SResponse
requestCors path = do
    ctx <- mkTestContext
    runSession
        ( request
            ( (setPath defaultRequest path)
                { requestHeaders =
                    [("Origin", previewOrigin)]
                , requestMethod = methodGet
                }
            )
        )
        (mkApp ctx)

preflightTokens :: IO SResponse
preflightTokens = do
    ctx <- mkTestContext
    runSession
        ( request
            ( (setPath defaultRequest "/tokens")
                { requestHeaders =
                    [ ("Origin", previewOrigin)
                    ,
                        ( "Access-Control-Request-Method"
                        , "GET"
                        )
                    ,
                        ( "Access-Control-Request-Headers"
                        , "content-type"
                        )
                    ]
                , requestMethod = methodOptions
                }
            )
        )
        (mkApp ctx)

spec :: Spec
spec = describe "CORS" $ do
    it "allows browser preflight for GET /tokens" $ do
        resp <- preflightTokens
        shouldAllowPreviewOrigin
            ( corsHeader
                "access-control-allow-origin"
                resp
            )
        shouldAllowRequiredMethods
            ( corsHeader
                "access-control-allow-methods"
                resp
            )
        shouldAllowContentType
            ( corsHeader
                "access-control-allow-headers"
                resp
            )

    it "adds CORS headers to actual Origin requests" $ do
        resp <- requestCors "/status"
        simpleStatus resp `shouldBe` status200
        shouldAllowPreviewOrigin
            ( corsHeader
                "access-control-allow-origin"
                resp
            )
        BSL8.unpack (simpleBody resp) `shouldContain` "tip_slot"
