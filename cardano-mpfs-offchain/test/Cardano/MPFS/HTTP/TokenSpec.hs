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
import Data.Either (isRight)
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

import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Read (verifyTokenState)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( LocatedTokenState (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.AtomicReadFixture
    ( sampleStateOutBytes
    , staleRoot
    , withProofIndexer
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.HTTP.Types
    ( TokenResponse (..)
    , VerificationSnapshot (..)
    )
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

    it
        "returns 503 when snapshot not yet available"
        $ do
            ctx <- mkTestContext
            let ctxNotReady =
                    ctx{indexerProofsReady = pure False}
            ts <- mkDummyTokenState
            txIn <- generate genTxIn
            let tid =
                    TokenId
                        ( AssetName
                            (SBS.toShort "cafe")
                        )
            St.putToken
                (St.tokens (state ctxNotReady))
                tid
                (LocatedTokenState txIn ts)
            resp <-
                runSession
                    ( request
                        ( setPath
                            defaultRequest
                            "/tokens/63616665"
                        )
                    )
                    (mkApp ctxNotReady)
            simpleStatus resp `shouldBe` status503

    it
        "returns proof-bearing envelope for a known \
        \token"
        $ do
            ctx0 <- mkTestContext
            txIn <- generate genTxIn
            let txOut = sampleStateOutBytes 0
            withProofIndexer
                (Just staleRoot)
                [(txIn, txOut)]
                ctx0
                $ \_txRoot ctx -> do
                    ts <- mkDummyTokenState
                    let tid =
                            TokenId
                                ( AssetName
                                    (SBS.toShort "cafe")
                                )
                    St.putToken
                        (St.tokens (state ctx))
                        tid
                        (LocatedTokenState txIn ts)
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
                            KM.member "snapshot" obj
                                `shouldBe` True
                            KM.member "state" obj
                                `shouldBe` True
                            case KM.lookup "snapshot" obj of
                                Just (Object sObj) -> do
                                    KM.member "utxo_root" sObj
                                        `shouldBe` True
                                    KM.member "chainpoint" sObj
                                        `shouldBe` True
                                _ ->
                                    expectationFailure
                                        "snapshot is not an \
                                        \object"
                            case KM.lookup "state" obj of
                                Just (Object stateObj) -> do
                                    KM.member "utxo" stateObj
                                        `shouldBe` True
                                    KM.member "state" stateObj
                                        `shouldBe` True
                                _ ->
                                    expectationFailure
                                        "state is not an \
                                        \object"
                        _ ->
                            expectationFailure
                                "Expected JSON object"
                    assertTokenResponseVerifies resp

assertTokenResponseVerifies :: SResponse -> IO ()
assertTokenResponseVerifies resp =
    case decode (simpleBody resp) of
        Just body@TokenResponse{trSnapshot} ->
            verifyTokenState
                (TrustedRoot (vsUtxoRoot trSnapshot))
                body
                `shouldSatisfy` isRight
        Nothing ->
            expectationFailure
                "Expected TokenResponse JSON"
