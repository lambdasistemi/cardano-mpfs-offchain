{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RetractFactsSpec
-- Description : Tests for POST /facts/retract
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RetractFactsSpec (spec) where

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
    , mkRetractFacts
    )
import Cardano.MPFS.HTTP.StatusSpec
    ( mkTestContext
    )
import Cardano.MPFS.HTTP.Swagger
    ( renderSwaggerJSON
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    )

spec :: Spec
spec = describe "POST /facts/retract" $ do
    it "is routed and rejects malformed addresses with 400" $ do
        ctx <- mkTestContext
        resp <- postJson ctx "/facts/retract" badRequest
        simpleStatus resp `shouldBe` status400

    it "documents facts route in swagger"
        $ case eitherDecode renderSwaggerJSON of
            Right (Object swagger) ->
                case KM.lookup "paths" swagger of
                    Just (Object paths) ->
                        KM.member "/facts/retract" paths
                            `shouldBe` True
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

    it "packages retract facts without unsigned tx cbor" $ do
        requestTxIn <- generate genTxIn
        stateTxIn <- generate genTxIn
        walletTxIn <- generate genTxIn
        let facts =
                mkRetractFacts
                    BundleSnapshot
                        { snapshotUtxoRoot = "root"
                        , snapshotSlot = SlotNo 42
                        , snapshotBlockId = BlockId "block-id"
                        }
                    sampleToken
                    (requestTxIn, "request-tx-out", "request-proof")
                    (stateTxIn, "state-tx-out", "state-proof")
                    [(walletTxIn, "wallet-tx-out", "wallet-proof")]
                    100
                    200
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
                KM.member "request_utxo" obj
                    `shouldBe` True
                KM.member "state_utxo" obj
                    `shouldBe` True
                KM.member "wallet_utxos" obj
                    `shouldBe` True
                KM.member "validity_start_slot" obj
                    `shouldBe` True
                KM.member "validity_end_slot" obj
                    `shouldBe` True
                KM.member "protocol_parameters" obj
                    `shouldBe` True
            _ ->
                expectationFailure
                    "Expected RetractFacts JSON object"

-- | Deliberately malformed serialized address with valid utxo
-- field, so handler-level address validation determines the status.
badRequest :: String
badRequest =
    "{\"utxo\":\"00#0\",\"address\":\"00\"}"

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
