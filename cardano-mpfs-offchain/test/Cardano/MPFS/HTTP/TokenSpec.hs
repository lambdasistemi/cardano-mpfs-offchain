{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokenSpec
-- Description : Tests for GET /tokens/:id endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokenSpec (spec) where

import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString.Short qualified as SBS
import Network.HTTP.Types (status200, status404)
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

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types (TokenId (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.State qualified as St

spec :: Spec
spec = describe "GET /tokens/:id" $ do
    it "returns 404 for unknown token" $ do
        ctx <- mkTestContext
        resp <-
            runSession
                ( request
                    ( setPath
                        defaultRequest
                        "/tokens/deadbeef"
                    )
                )
                (mkApp ctx)
        simpleStatus resp `shouldBe` status404

    it "returns token state for known token" $ do
        ctx <- mkTestContext
        ts <- mkDummyTokenState
        let tid =
                TokenId
                    (AssetName (SBS.toShort "cafe"))
        St.putToken
            (St.tokens (state ctx))
            tid
            ts
        resp <-
            runSession
                ( request
                    ( setPath
                        defaultRequest
                        "/tokens/63616665"
                    )
                )
                (mkApp ctx)
        simpleStatus resp `shouldBe` status200
        case decode (simpleBody resp) of
            Just (Object obj) -> do
                KM.member "owner" obj
                    `shouldBe` True
                KM.member "root" obj
                    `shouldBe` True
                KM.member "max_fee" obj
                    `shouldBe` True
                KM.member "process_time" obj
                    `shouldBe` True
                KM.member "retract_time" obj
                    `shouldBe` True
            _ ->
                expectationFailure
                    "Expected JSON object"
