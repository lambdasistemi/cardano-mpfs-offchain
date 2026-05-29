{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.SubmitSpec
-- Description : Tests for the POST /submit endpoint contract
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.SubmitSpec
    ( spec
    ) where

import Data.Aeson (decode, encode, toJSON)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Network.HTTP.Types
    ( methodPost
    , status400
    )
import Network.Wai
    ( requestHeaders
    , requestMethod
    )
import Network.Wai.Test
    ( SResponse (..)
    , defaultRequest
    , runSession
    , setPath
    )
import Network.Wai.Test qualified as WaiTest
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    )

import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Types (SubmitRequest (..))

-- | POST a raw JSON body to /submit through the WAI app.
postSubmit :: Value -> IO SResponse
postSubmit body = do
    ctx <- mkTestContext
    runSession
        ( WaiTest.srequest
            ( WaiTest.SRequest
                ( ( setPath
                        defaultRequest
                        "/submit"
                  )
                    { requestMethod = methodPost
                    , requestHeaders =
                        [("Content-Type", "application/json")]
                    }
                )
                (encode body)
            )
        )
        (mkApp ctx)

spec :: Spec
spec = describe "POST /submit" $ do
    it "SubmitRequest JSON uses the signedTxCbor key" $ do
        case toJSON (SubmitRequest (Hex "\NUL")) of
            Object obj -> do
                KM.member "signedTxCbor" obj
                    `shouldBe` True
                KM.member "tx" obj `shouldBe` False
            _ ->
                expectationFailure
                    "Expected SubmitRequest to encode \
                    \to a JSON object"

    it "returns 400 JSON error on undecodable CBOR" $ do
        resp <-
            postSubmit
                (Object (KM.fromList [("signedTxCbor", "00")]))
        simpleStatus resp `shouldBe` status400
        case decode (simpleBody resp) of
            Just (Object obj) -> do
                KM.member "error" obj `shouldBe` True
                KM.member "detail" obj `shouldBe` True
            _ ->
                expectationFailure
                    "Expected a JSON error body with \
                    \error and detail keys"
