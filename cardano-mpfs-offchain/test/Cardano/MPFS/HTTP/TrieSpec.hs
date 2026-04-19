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
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Text qualified as T
import Network.HTTP.Types
    ( status200
    , status404
    , status503
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
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.Ledger.Mary.Value (AssetName (..))
import Test.QuickCheck (generate)

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , LocatedTokenState (..)
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Trie qualified as Trie

-- | "cafe" token — hex "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | Hex-encode raw bytes for URL paths.
hex :: ByteString -> ByteString
hex = B16.encode

-- | Stub the verification snapshot and the UTxO
-- witness machinery (root, resolveUtxo, utxoProof)
-- and seed a checkpoint so the proof-bearing
-- endpoints can assemble their envelopes.
withSnapshot
    :: BS.ByteString
    -- ^ CSMT root bytes
    -> BS.ByteString
    -- ^ Resolved TxOut CBOR bytes
    -> BS.ByteString
    -- ^ CSMT inclusion proof bytes
    -> Context IO
    -> IO (Context IO)
withSnapshot rootBs outBs proofBs ctx = do
    St.putCheckpoint
        (St.checkpoints (state ctx))
        (SlotNo 42)
        (BlockId "block-id-bytes")
    pure
        ctx
            { utxoRoot = pure (Just rootBs)
            , resolveUtxo = \_ -> pure (Just outBs)
            , utxoProof = \_ -> pure (Just proofBs)
            }

-- | Seed an indexed token state for 'cafeTid' so
-- the proof-bearing handlers can find a state UTxO
-- to witness.
seedTokenState :: Context IO -> IO ()
seedTokenState ctx = do
    ts <- mkDummyTokenState
    txIn <- generate genTxIn
    St.putToken
        (St.tokens (state ctx))
        cafeTid
        (LocatedTokenState txIn ts)

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
        it
            "returns proof-bearing envelope for an \
            \existing key"
            $ do
                ctx0 <- mkTestContext
                ctx <-
                    withSnapshot
                        "root"
                        "tx-out"
                        "proof"
                        ctx0
                seedTokenState ctx
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
                case decode (simpleBody resp) of
                    Just (Object obj) -> do
                        KM.member "snapshot" obj
                            `shouldBe` True
                        KM.member "value" obj
                            `shouldBe` True
                        KM.member "fact" obj
                            `shouldBe` True
                        case KM.lookup "fact" obj of
                            Just (Object fObj) -> do
                                KM.member "state" fObj
                                    `shouldBe` True
                                KM.member "mpf_proof" fObj
                                    `shouldBe` True
                            _ ->
                                expectationFailure
                                    "fact is not an \
                                    \object"
                    _ ->
                        expectationFailure
                            "Expected JSON object"

        it "returns 404 for missing key" $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot "root" "tx-out" "proof" ctx0
            seedTokenState ctx
            Trie.createTrie (trieManager ctx) cafeTid
            resp <-
                getFact
                    ctx
                    "63616665"
                    (hex "nokey")
            simpleStatus resp `shouldBe` status404

        it "returns 404 for unknown token" $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot "root" "tx-out" "proof" ctx0
            resp <-
                getFact ctx "63616665" (hex "k")
            simpleStatus resp `shouldBe` status404

        it
            "returns 503 when snapshot not yet \
            \available"
            $ do
                ctx <- mkTestContext
                seedTokenState ctx
                Trie.createTrie (trieManager ctx) cafeTid
                _ <-
                    Trie.withTrie
                        (trieManager ctx)
                        cafeTid
                        $ \trie ->
                            Trie.insert trie "k" "v"
                resp <-
                    getFact ctx "63616665" (hex "k")
                simpleStatus resp `shouldBe` status503

    describe "GET /tokens/:id/proofs/:key" $ do
        it
            "returns proof-bearing envelope for an \
            \existing key"
            $ do
                ctx0 <- mkTestContext
                ctx <-
                    withSnapshot
                        "root"
                        "tx-out"
                        "proof"
                        ctx0
                seedTokenState ctx
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
                    Just (Object obj) -> do
                        KM.member "snapshot" obj
                            `shouldBe` True
                        KM.member "fact" obj
                            `shouldBe` True
                        case KM.lookup "fact" obj of
                            Just (Object fObj) -> do
                                KM.member "state" fObj
                                    `shouldBe` True
                                KM.member "mpf_proof" fObj
                                    `shouldBe` True
                            _ ->
                                expectationFailure
                                    "fact is not an \
                                    \object"
                    _ ->
                        expectationFailure
                            "Expected JSON object"

        it "returns 404 for missing key" $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot "root" "tx-out" "proof" ctx0
            seedTokenState ctx
            Trie.createTrie (trieManager ctx) cafeTid
            resp <-
                getProof
                    ctx
                    "63616665"
                    (hex "absent")
            simpleStatus resp `shouldBe` status404

        it "returns 404 for unknown token" $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot "root" "tx-out" "proof" ctx0
            resp <-
                getProof ctx "63616665" (hex "k")
            simpleStatus resp `shouldBe` status404

        it
            "returns 503 when snapshot not yet \
            \available"
            $ do
                ctx <- mkTestContext
                seedTokenState ctx
                Trie.createTrie (trieManager ctx) cafeTid
                _ <-
                    Trie.withTrie
                        (trieManager ctx)
                        cafeTid
                        $ \trie ->
                            Trie.insert trie "k" "v"
                resp <-
                    getProof ctx "63616665" (hex "k")
                simpleStatus resp `shouldBe` status503

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
