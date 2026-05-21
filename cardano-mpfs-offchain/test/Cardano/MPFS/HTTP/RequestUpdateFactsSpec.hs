{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RequestUpdateFactsSpec
-- Description : Tests for POST /facts/request/update
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RequestUpdateFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , eitherDecode
    )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.ByteString.Short qualified as SBS
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status400
    )
import Network.Wai
    ( Request (..)
    )
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
import Test.QuickCheck
    ( generate
    )

import Cardano.Ledger.Api.PParams
    ( emptyPParams
    )
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    )
import Cardano.MPFS.Context
    ( Context
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators
    ( genTxIn
    )
import Cardano.MPFS.HTTP.Server
    ( mkApp
    )
import Cardano.MPFS.HTTP.StatusSpec
    ( mkTestContext
    )
import Cardano.MPFS.HTTP.Swagger
    ( renderSwaggerJSON
    )
import Cardano.MPFS.HTTP.Types.Facts
    ( mkRequestUpdateFacts
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    )

spec :: Spec
spec = describe "POST /facts/request/update" $ do
    it "is routed and rejects malformed addresses with 400" $ do
        ctx <- mkTestContext
        resp <- postJson ctx "/facts/request/update" badRequest
        simpleStatus resp `shouldBe` status400

    it "documents facts route and drops legacy tx route"
        $ case eitherDecode renderSwaggerJSON of
            Right (Object swagger) ->
                case KM.lookup "paths" swagger of
                    Just (Object paths) -> do
                        KM.member "/facts/request/update" paths
                            `shouldBe` True
                        KM.member "/tx/request/update" paths
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

    it "packages request-update facts without unsigned tx cbor" $ do
        walletTxIn <- generate genTxIn
        let facts =
                mkRequestUpdateFacts
                    BundleSnapshot
                        { snapshotUtxoRoot = "root"
                        , snapshotSlot = SlotNo 42
                        , snapshotBlockId = BlockId "block-id"
                        }
                    sampleToken
                    "mykey"
                    "oldvalue"
                    "newvalue"
                    "addr"
                    1_700_000_000_000
                    [(walletTxIn, "wallet-tx-out", "wallet-proof")]
                    emptyPParams
        case toJSON facts of
            Object obj -> do
                KM.member "unsigned_tx_cbor" obj
                    `shouldBe` False
                KM.member "tx" obj
                    `shouldBe` False
                KM.member "snapshot" obj
                    `shouldBe` True
                KM.member "token" obj
                    `shouldBe` True
                KM.member "key" obj
                    `shouldBe` True
                KM.member "old_value" obj
                    `shouldBe` True
                KM.member "new_value" obj
                    `shouldBe` True
                KM.member "address" obj
                    `shouldBe` True
                KM.member "submitted_at" obj
                    `shouldBe` True
                KM.member "wallet_utxos" obj
                    `shouldBe` True
                KM.member "protocol_parameters" obj
                    `shouldBe` True
            _ ->
                expectationFailure
                    "Expected RequestUpdateFacts JSON object"

-- | Deliberately malformed serialized address with valid
-- token/key/value fields, so handler-level address validation
-- determines the status.
badRequest :: String
badRequest =
    "{\"token\":\"63616665\",\"key\":\"6b\",\"old_value\":\"6f\",\"new_value\":\"6e\",\"address\":\"00\"}"

sampleToken :: TokenId
sampleToken = TokenId (AssetName (SBS.toShort "cafe"))

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
