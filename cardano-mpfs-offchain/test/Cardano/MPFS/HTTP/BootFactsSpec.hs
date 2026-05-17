{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.BootFactsSpec
-- Description : Tests for POST /facts/boot
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.BootFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , eitherDecode
    )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString.Lazy.Char8 qualified as BL
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status400
    )
import Network.Wai (Request (..))
import Network.Wai.Test
    ( SRequest (..)
    , SResponse (..)
    , defaultRequest
    , runSession
    , setPath
    , srequest
    )
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    )
import Test.QuickCheck (generate)

import Cardano.Ledger.Api.PParams (emptyPParams)

import Cardano.MPFS.Context (Context)
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.Server
    ( mkApp
    , mkBootFacts
    )
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Swagger (renderSwaggerJSON)
import Cardano.MPFS.TxBuilder (BundleSnapshot (..))

spec :: Spec
spec = describe "POST /facts/boot" $ do
    it "is routed and rejects malformed addresses with 400" $ do
        ctx <- mkTestContext
        resp <- postJson ctx "/facts/boot" badBootRequest
        simpleStatus resp `shouldBe` status400

    it "documents facts route and omits legacy boot route"
        $ case eitherDecode renderSwaggerJSON of
            Right (Object swagger) ->
                case KM.lookup "paths" swagger of
                    Just (Object paths) -> do
                        KM.member "/facts/boot" paths
                            `shouldBe` True
                        KM.member "/tx/boot" paths
                            `shouldBe` False
                    _ ->
                        expectationFailure
                            "Swagger paths are not an object"
            Right _ ->
                expectationFailure
                    "Swagger document is not an object"
            Left err ->
                expectationFailure
                    $ "Could not decode Swagger JSON: "
                        <> err

    it
        "packages wallet facts and protocol parameters \
        \without unsigned tx cbor"
        $ do
            txIn <- generate genTxIn
            let facts =
                    mkBootFacts
                        BundleSnapshot
                            { snapshotUtxoRoot = "root"
                            , snapshotSlot = SlotNo 42
                            , snapshotBlockId = BlockId "block-id"
                            }
                        [(txIn, "tx-out", "proof")]
                        emptyPParams
            case toJSON facts of
                Object obj -> do
                    KM.member "unsigned_tx_cbor" obj
                        `shouldBe` False
                    KM.member "snapshot" obj
                        `shouldBe` True
                    KM.member "wallet_utxos" obj
                        `shouldBe` True
                    KM.member "protocol_parameters" obj
                        `shouldBe` True
                    case KM.lookup "protocol_parameters" obj of
                        Just (Object pp) -> do
                            KM.lookup "verified" pp
                                `shouldBe` Just (Bool False)
                            KM.member "cbor" pp
                                `shouldBe` True
                        _ ->
                            expectationFailure
                                "protocol_parameters \
                                \is not an object"
                _ ->
                    expectationFailure
                        "Expected BootFacts JSON object"

-- | Deliberately malformed serialized address.
badBootRequest :: String
badBootRequest = "{\"address\":\"00\"}"

postJson
    :: Context IO
    -> ByteString
    -> String
    -> IO SResponse
postJson ctx path body =
    runSession
        ( srequest
            SRequest
                { simpleRequest =
                    (setPath defaultRequest path)
                        { requestMethod = methodPost
                        , requestHeaders =
                            [
                                ( hContentType
                                , "application/json"
                                )
                            ]
                        }
                , simpleRequestBody =
                    BL.pack body
                }
        )
        (mkApp ctx)
