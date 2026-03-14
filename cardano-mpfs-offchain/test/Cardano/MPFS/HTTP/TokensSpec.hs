{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokensSpec
-- Description : Tests for GET /tokens endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokensSpec
    ( spec
    , mkDummyTokenState
    ) where

import Data.Aeson (decode)
import Data.Aeson.Types (Value (..))
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
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

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.State qualified as St

import Cardano.MPFS.Generators (genKeyHash)
import Test.QuickCheck (generate)

import Cardano.MPFS.Core.Types (Root (..))

getTokens :: Context IO -> IO SResponse
getTokens ctx =
    runSession
        ( request
            (setPath defaultRequest "/tokens")
        )
        (mkApp ctx)

-- | A dummy token state for testing.
mkDummyTokenState :: IO TokenState
mkDummyTokenState = do
    kh <- generate genKeyHash
    pure
        TokenState
            { owner = kh
            , root = Root (BS.replicate 32 0)
            , maxFee = Coin 1_000_000
            , processTime = 60_000
            , retractTime = 30_000
            }

spec :: Spec
spec = describe "GET /tokens" $ do
    it "returns empty list on fresh state" $ do
        ctx <- mkTestContext
        resp <- getTokens ctx
        simpleStatus resp `shouldBe` status200
        case decode (simpleBody resp) of
            Just (Array arr) ->
                length arr `shouldBe` 0
            _ ->
                expectationFailure
                    "Expected JSON array"

    it "returns token after insertion" $ do
        ctx <- mkTestContext
        ts <- mkDummyTokenState
        let tid =
                TokenId
                    (AssetName (SBS.toShort "deadbeef"))
        St.putToken
            (St.tokens (state ctx))
            tid
            ts
        resp <- getTokens ctx
        simpleStatus resp `shouldBe` status200
        case decode (simpleBody resp) of
            Just (Array arr) ->
                length arr `shouldBe` 1
            _ ->
                expectationFailure
                    "Expected JSON array with 1 element"
