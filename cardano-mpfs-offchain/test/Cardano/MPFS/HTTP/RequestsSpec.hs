{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RequestsSpec
-- Description : Tests for GET /tokens/:id/requests
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RequestsSpec (spec) where

import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString.Short qualified as SBS
import Data.Vector qualified as V
import Network.HTTP.Types (status200)
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
    , expectationFailure
    , it
    , shouldBe
    )
import Test.QuickCheck (generate)

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( LocatedRequest (LocatedRequest)
    , TokenId (..)
    )
import Cardano.MPFS.Generators
    ( genRequest
    , genTxIn
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.State qualified as St

-- | "cafe" as hex = "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | "aabb" as hex = "61616262".
aabbTid :: TokenId
aabbTid = TokenId (AssetName (SBS.toShort "aabb"))

spec :: Spec
spec = describe "GET /tokens/:id/requests" $ do
    it "returns empty list when no requests exist"
        $ do
            ctx <- mkTestContext
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status200
            assertJsonArray resp 0

    it "returns a single request after insertion"
        $ do
            ctx <- mkTestContext
            txIn <- generate genTxIn
            req <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn req)
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status200
            assertJsonArray resp 1
            assertRequestFields resp

    it "returns multiple requests for same token"
        $ do
            ctx <- mkTestContext
            txIn1 <- generate genTxIn
            txIn2 <- generate genTxIn
            req1 <- generate (genRequest cafeTid)
            req2 <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn1 req1)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn2 req2)
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status200
            assertJsonArray resp 2

    it "filters by token — other tokens excluded"
        $ do
            ctx <- mkTestContext
            txIn1 <- generate genTxIn
            txIn2 <- generate genTxIn
            req1 <- generate (genRequest cafeTid)
            req2 <- generate (genRequest aabbTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn1 req1)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn2 req2)
            resp1 <- getRequests ctx "63616665"
            simpleStatus resp1 `shouldBe` status200
            assertJsonArray resp1 1
            resp2 <- getRequests ctx "61616262"
            simpleStatus resp2 `shouldBe` status200
            assertJsonArray resp2 1

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | @GET \/tokens\/:id\/requests@
getRequests
    :: Context IO -> ByteString -> IO SResponse
getRequests ctx tokenHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ( "/tokens/"
                    <> tokenHex
                    <> "/requests"
                )
            )
        )
        (mkApp ctx)

-- | Assert response body is a JSON array of given
-- length.
assertJsonArray :: SResponse -> Int -> IO ()
assertJsonArray resp n =
    case decode (simpleBody resp) of
        Just (Array arr) ->
            length arr `shouldBe` n
        _ ->
            expectationFailure
                "Expected JSON array"

-- | Assert first element has expected request
-- fields.
assertRequestFields :: SResponse -> IO ()
assertRequestFields resp =
    case decode (simpleBody resp) of
        Just (Array arr)
            | not (V.null arr) ->
                case V.head arr of
                    Object obj -> do
                        KM.member "token" obj
                            `shouldBe` True
                        KM.member "owner" obj
                            `shouldBe` True
                        KM.member "key" obj
                            `shouldBe` True
                        KM.member "operation" obj
                            `shouldBe` True
                        KM.member "fee" obj
                            `shouldBe` True
                        KM.member "submitted_at" obj
                            `shouldBe` True
                    _ ->
                        expectationFailure
                            "Expected JSON object"
        _ ->
            expectationFailure
                "Expected non-empty JSON array"
