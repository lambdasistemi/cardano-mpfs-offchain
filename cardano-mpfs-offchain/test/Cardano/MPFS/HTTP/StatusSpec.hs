{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.StatusSpec
-- Description : Tests for GET /status endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.StatusSpec
    ( spec
    , mkTestContext
    ) where

import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
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

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Mock.State (mkMockState)
import Cardano.MPFS.Mock.Submitter (mkMockSubmitter)
import Cardano.MPFS.Mock.TxBuilder (mkMockTxBuilder)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )

-- | Build a mock context where unused fields
-- are lazy stubs (avoiding StrictData + error).
mkTestContext :: IO (Context IO)
mkTestContext = do
    tm <- mkPureTrieManager
    st <- mkMockState
    pure
        Context
            { provider =
                Provider
                    { queryUTxOs = \_ -> pure []
                    , queryProtocolParams =
                        pure
                            ( error
                                "unused: queryProtocolParams"
                            )
                    , evaluateTx = \_ ->
                        error "unused: evaluateTx"
                    , posixMsToSlot = \_ ->
                        error "unused: posixMsToSlot"
                    , posixMsCeilSlot = \_ ->
                        error
                            "unused: posixMsCeilSlot"
                    }
            , trieManager = tm
            , state = st
            , submitter = mkMockSubmitter
            , txBuilder = mkMockTxBuilder
            , utxoExists = \_ -> pure False
            , resolveUtxo = \_ -> pure Nothing
            , awaitUtxo = \_ _ -> pure Nothing
            , utxoRoot = pure Nothing
            , utxoProof = \_ -> pure Nothing
            , readMetrics = pure Nothing
            }

getStatus :: Context IO -> IO SResponse
getStatus ctx =
    runSession
        ( request
            (setPath defaultRequest "/status")
        )
        (mkApp ctx)

getUtxoRoot :: Context IO -> IO SResponse
getUtxoRoot ctx =
    runSession
        ( request
            (setPath defaultRequest "/utxo/root")
        )
        (mkApp ctx)

spec :: Spec
spec = describe "GET /status" $ do
    it "returns 200 with expected fields" $ do
        ctx <- mkTestContext
        resp <- getStatus ctx
        simpleStatus resp `shouldBe` status200
        case decode (simpleBody resp) of
            Just (Object obj) -> do
                KM.member "tip_slot" obj
                    `shouldBe` True
                KM.member "tip_block_id" obj
                    `shouldBe` True
                KM.member "checkpoint_slot" obj
                    `shouldBe` True
                KM.member "checkpoint_block_id" obj
                    `shouldBe` True
                KM.member "utxo_root" obj
                    `shouldBe` True
            _ ->
                expectationFailure
                    "Expected JSON object"

    it "returns tip_slot 0 when no metrics" $ do
        ctx <- mkTestContext
        resp <- getStatus ctx
        case decode (simpleBody resp) of
            Just (Object obj) ->
                KM.lookup "tip_slot" obj
                    `shouldBe` Just (Number 0)
            _ ->
                expectationFailure
                    "Expected JSON object"

    it "returns null checkpoint on empty state" $ do
        ctx <- mkTestContext
        resp <- getStatus ctx
        case decode (simpleBody resp) of
            Just (Object obj) -> do
                KM.lookup "checkpoint_slot" obj
                    `shouldBe` Just Null
                KM.lookup "checkpoint_block_id" obj
                    `shouldBe` Just Null
            _ ->
                expectationFailure
                    "Expected JSON object"

    it "returns null utxo_root when CSMT unavailable" $ do
        ctx <- mkTestContext
        resp <- getStatus ctx
        case decode (simpleBody resp) of
            Just (Object obj) ->
                KM.lookup "utxo_root" obj
                    `shouldBe` Just Null
            _ ->
                expectationFailure
                    "Expected JSON object"

    it "agrees with /utxo/root when CSMT available" $ do
        let rootBs = "\x01\x02\x03"
            ctxWith =
                fmap
                    ( \c ->
                        c{utxoRoot = pure (Just rootBs)}
                    )
                    mkTestContext
        ctx <- ctxWith
        statusResp <- getStatus ctx
        utxoRootResp <- getUtxoRoot ctx
        simpleStatus utxoRootResp `shouldBe` status200
        let statusRoot = case decode (simpleBody statusResp) of
                Just (Object obj) ->
                    KM.lookup "utxo_root" obj
                _ -> Nothing
            directRoot = decode (simpleBody utxoRootResp)
        case (statusRoot, directRoot) of
            (Just rootVal, Just directVal) ->
                rootVal `shouldBe` directVal
            _ ->
                expectationFailure
                    "Expected both endpoints to return a \
                    \utxo root"
