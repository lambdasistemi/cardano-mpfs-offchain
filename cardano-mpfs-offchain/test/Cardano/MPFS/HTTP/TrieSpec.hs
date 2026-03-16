{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TrieSpec
-- Description : Tests for trie query endpoints
-- License     : Apache-2.0
--
-- Tests for @GET \/tokens\/:id\/root@,
-- @\/tokens\/:id\/facts\/:key@, and
-- @\/tokens\/:id\/proofs\/:key@.
module Cardano.MPFS.HTTP.TrieSpec (spec) where

import Data.Aeson (decode)
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Text qualified as T
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
    , shouldSatisfy
    )

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types (TokenId (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.Trie qualified as Trie

-- | "cafe" token — hex "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | Hex-encode raw bytes for URL paths.
hex :: ByteString -> ByteString
hex = B16.encode

spec :: Spec
spec = do
    describe "GET /tokens/:id/root" $ do
        it "returns root hash for token with trie"
            $ do
                ctx <- mkTestContext
                Trie.createTrie (trieManager ctx) cafeTid
                _ <-
                    Trie.withTrie
                        (trieManager ctx)
                        cafeTid
                        $ \trie ->
                            Trie.insert trie "k1" "v1"
                resp <- getRoot ctx "63616665"
                simpleStatus resp `shouldBe` status200
                case decode (simpleBody resp) of
                    Just (String s) ->
                        s `shouldSatisfy` (not . T.null)
                    _ ->
                        expectationFailure
                            "Expected JSON string"

    describe "GET /tokens/:id/facts/:key" $ do
        it "returns value for existing key" $ do
            ctx <- mkTestContext
            Trie.createTrie (trieManager ctx) cafeTid
            _ <-
                Trie.withTrie
                    (trieManager ctx)
                    cafeTid
                    $ \trie ->
                        Trie.insert trie "mykey" "myval"
            resp <-
                getFact
                    ctx
                    "63616665"
                    (hex "mykey")
            simpleStatus resp `shouldBe` status200

        it "returns 404 for missing key" $ do
            ctx <- mkTestContext
            Trie.createTrie (trieManager ctx) cafeTid
            resp <-
                getFact
                    ctx
                    "63616665"
                    (hex "nokey")
            simpleStatus resp `shouldBe` status404

    describe "GET /tokens/:id/proofs/:key" $ do
        it "returns proof for existing key" $ do
            ctx <- mkTestContext
            Trie.createTrie (trieManager ctx) cafeTid
            _ <-
                Trie.withTrie
                    (trieManager ctx)
                    cafeTid
                    $ \trie ->
                        Trie.insert trie "pk" "pv"
            resp <-
                getProof
                    ctx
                    "63616665"
                    (hex "pk")
            simpleStatus resp `shouldBe` status200
            case decode (simpleBody resp) of
                Just (String s) ->
                    s `shouldSatisfy` (not . T.null)
                _ ->
                    expectationFailure
                        "Expected JSON string"

        it "returns 404 for missing key" $ do
            ctx <- mkTestContext
            Trie.createTrie (trieManager ctx) cafeTid
            resp <-
                getProof
                    ctx
                    "63616665"
                    (hex "absent")
            simpleStatus resp `shouldBe` status404

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | @GET \/tokens\/:id\/root@
getRoot
    :: Context IO -> ByteString -> IO SResponse
getRoot ctx tokenHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ("/tokens/" <> tokenHex <> "/root")
            )
        )
        (mkApp ctx)

-- | @GET \/tokens\/:id\/facts\/:key@
getFact
    :: Context IO
    -> ByteString
    -> ByteString
    -> IO SResponse
getFact ctx tokenHex keyHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ( "/tokens/"
                    <> tokenHex
                    <> "/facts/"
                    <> keyHex
                )
            )
        )
        (mkApp ctx)

-- | @GET \/tokens\/:id\/proofs\/:key@
getProof
    :: Context IO
    -> ByteString
    -> ByteString
    -> IO SResponse
getProof ctx tokenHex keyHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ( "/tokens/"
                    <> tokenHex
                    <> "/proofs/"
                    <> keyHex
                )
            )
        )
        (mkApp ctx)
