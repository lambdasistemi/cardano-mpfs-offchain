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
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Vector qualified as V
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
    )
import Test.QuickCheck (generate)

import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , LocatedRequest (LocatedRequest)
    , LocatedTokenState (..)
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators
    ( genRequest
    , genTxIn
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.State qualified as St

-- | "cafe" as hex = "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | "aabb" as hex = "61616262".
aabbTid :: TokenId
aabbTid = TokenId (AssetName (SBS.toShort "aabb"))

-- | Stub the verification snapshot and the UTxO
-- witness machinery and seed a checkpoint so the
-- proof-bearing handler can assemble its envelope.
withSnapshot
    :: BS.ByteString
    -> BS.ByteString
    -> BS.ByteString
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

-- | Seed an indexed token state for a token id so
-- the proof-bearing handler can find a state UTxO
-- to anchor against.
seedTokenState :: Context IO -> TokenId -> IO ()
seedTokenState ctx tid = do
    ts <- mkDummyTokenState
    txIn <- generate genTxIn
    St.putToken
        (St.tokens (state ctx))
        tid
        (LocatedTokenState txIn ts)

spec :: Spec
spec = describe "GET /tokens/:id/requests" $ do
    it "returns empty list when no requests exist" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        seedTokenState ctx cafeTid
        resp <- getRequests ctx "63616665"
        simpleStatus resp `shouldBe` status200
        assertEnvelope resp 0

    it
        "returns a single witnessed request after \
        \insertion"
        $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot
                    "root"
                    "tx-out"
                    "proof"
                    ctx0
            seedTokenState ctx cafeTid
            txIn <- generate genTxIn
            req <- generate (genRequest cafeTid)
            St.putRequest
                (St.requests (state ctx))
                (LocatedRequest txIn req)
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status200
            assertEnvelope resp 1
            assertWitnessedRequestFields resp

    it
        "returns multiple witnessed requests for same \
        \token"
        $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot
                    "root"
                    "tx-out"
                    "proof"
                    ctx0
            seedTokenState ctx cafeTid
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
            assertEnvelope resp 2

    it "filters by token — other tokens excluded" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        seedTokenState ctx cafeTid
        seedTokenState ctx aabbTid
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
        assertEnvelope resp1 1
        resp2 <- getRequests ctx "61616262"
        simpleStatus resp2 `shouldBe` status200
        assertEnvelope resp2 1

    it "returns 404 for unknown token" $ do
        ctx0 <- mkTestContext
        ctx <-
            withSnapshot "root" "tx-out" "proof" ctx0
        resp <- getRequests ctx "63616665"
        simpleStatus resp `shouldBe` status404

    it "returns 503 when snapshot not yet available"
        $ do
            ctx <- mkTestContext
            seedTokenState ctx cafeTid
            resp <- getRequests ctx "63616665"
            simpleStatus resp `shouldBe` status503

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

-- | Assert the response body is a 'RequestsResponse'
-- envelope with exactly @n@ witnessed requests.
assertEnvelope :: SResponse -> Int -> IO ()
assertEnvelope resp n =
    case decode (simpleBody resp) of
        Just (Object obj) -> do
            KM.member "snapshot" obj `shouldBe` True
            case KM.lookup "requests" obj of
                Just (Array arr) ->
                    length arr `shouldBe` n
                _ ->
                    expectationFailure
                        "requests is not an array"
        _ ->
            expectationFailure
                "Expected JSON object"

-- | Assert the first witnessed request has both the
-- @utxo@ witness and the decoded @request@ payload.
assertWitnessedRequestFields :: SResponse -> IO ()
assertWitnessedRequestFields resp =
    case decode (simpleBody resp) of
        Just (Object obj) ->
            case KM.lookup "requests" obj of
                Just (Array arr)
                    | not (V.null arr) ->
                        case V.head arr of
                            Object wreq -> do
                                KM.member "utxo" wreq
                                    `shouldBe` True
                                case KM.lookup
                                    "request"
                                    wreq of
                                    Just (Object r) -> do
                                        KM.member "token" r
                                            `shouldBe` True
                                        KM.member "owner" r
                                            `shouldBe` True
                                        KM.member "key" r
                                            `shouldBe` True
                                        KM.member
                                            "operation"
                                            r
                                            `shouldBe` True
                                        KM.member "fee" r
                                            `shouldBe` True
                                        KM.member
                                            "submitted_at"
                                            r
                                            `shouldBe` True
                                    _ ->
                                        expectationFailure
                                            "request \
                                            \is not an \
                                            \object"
                            _ ->
                                expectationFailure
                                    "witnessed \
                                    \request is not \
                                    \an object"
                _ ->
                    expectationFailure
                        "requests is empty or not \
                        \an array"
        _ ->
            expectationFailure
                "Expected JSON object"
