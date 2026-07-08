{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.BootFactsSpec
-- Description : Tests for POST /facts/boot
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.BootFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , decode
    , eitherDecode
    , encode
    , object
    , (.=)
    )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.Either (isRight)
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    , status400
    , status503
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
    , shouldSatisfy
    )
import Test.QuickCheck (generate, suchThat)

import Cardano.Ledger.Address (Addr, serialiseAddr)
import Cardano.Ledger.Api.PParams (emptyPParams)
import Cardano.Ledger.Api.Tx.Out (TxOut, mkBasicTxOut)
import Cardano.Ledger.BaseTypes (Inject (..))
import Cardano.Ledger.Binary (natVersion, serialize')

import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify (verifyBootFacts)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , SlotNo (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.AtomicReadFixture (withProofIndexer)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server
    ( mkApp
    , mkBootFacts
    )
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Swagger (renderSwaggerJSON)
import Cardano.MPFS.HTTP.Types
    ( BootFacts (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Indexer.TxFixtures
    ( testCageAddr
    , testWalletAddr
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder (BundleSnapshot (..))

spec :: Spec
spec = describe "POST /facts/boot" $ do
    it "is routed and rejects malformed addresses with 400" $ do
        ctx <- mkTestContext
        resp <- postJson ctx "/facts/boot" badBootRequest
        simpleStatus resp `shouldBe` status400

    it "returns 503 while proof reads are not ready" $ do
        ctx <-
            fmap
                ( \c ->
                    c
                        { indexerProofsReady = pure False
                        , runIndexerTx =
                            \_ ->
                                error
                                    "runIndexerTx must not be \
                                    \called while proof reads \
                                    \are gated"
                        }
                )
                mkTestContext
        resp <- postJson ctx "/facts/boot" validBootRequest
        simpleStatus resp `shouldBe` status503

    it
        "returns the union of wallet UTxOs for unique addresses \
        \against one snapshot root"
        $ do
            txInA <- generate genTxIn
            txInB <- generate (genTxIn `suchThat` (/= txInA))
            ctx0 <- mkBootTestContext
            let outA = walletTxOutBytes testWalletAddr
                outB = walletTxOutBytes testCageAddr
            withProofIndexer
                Nothing
                [(txInA, outA), (txInB, outB)]
                ctx0
                $ \root ctx -> do
                    resp <-
                        postJson
                            ctx
                            "/facts/boot"
                            ( multiAddressBootRequest
                                [ testWalletAddr
                                , testCageAddr
                                , testWalletAddr
                                ]
                            )
                    simpleStatus resp `shouldBe` status200
                    case decode (simpleBody resp) of
                        Just body@BootFacts{..} -> do
                            vsUtxoRoot bfSnapshot
                                `shouldBe` Hex root
                            length bfWalletUtxos `shouldBe` 2
                            verifyBootFacts
                                (TrustedRoot (vsUtxoRoot bfSnapshot))
                                body
                                `shouldSatisfy` isRight
                        Nothing ->
                            expectationFailure
                                "Expected BootFacts JSON"

    it "keeps accepting the legacy single address request body" $ do
        txIn <- generate genTxIn
        ctx0 <- mkBootTestContext
        withProofIndexer
            Nothing
            [(txIn, walletTxOutBytes testWalletAddr)]
            ctx0
            $ \root ctx -> do
                resp <-
                    postJson
                        ctx
                        "/facts/boot"
                        (singleAddressBootRequest testWalletAddr)
                simpleStatus resp `shouldBe` status200
                case decode (simpleBody resp) of
                    Just body@BootFacts{..} -> do
                        vsUtxoRoot bfSnapshot `shouldBe` Hex root
                        length bfWalletUtxos `shouldBe` 1
                        verifyBootFacts
                            (TrustedRoot (vsUtxoRoot bfSnapshot))
                            body
                            `shouldSatisfy` isRight
                    Nothing ->
                        expectationFailure
                            "Expected BootFacts JSON"

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

validBootRequest :: String
validBootRequest =
    singleAddressBootRequest testCageAddr

singleAddressBootRequest :: Addr -> String
singleAddressBootRequest addr =
    BL.unpack
        $ encode
        $ object
            ["address" .= Hex (serialiseAddr addr)]

multiAddressBootRequest :: [Addr] -> String
multiAddressBootRequest addrs =
    BL.unpack
        $ encode
        $ object
            [ "addresses"
                .= fmap
                    (Hex . serialiseAddr)
                    addrs
            ]

walletTxOutBytes :: Addr -> ByteString
walletTxOutBytes addr =
    serialize'
        (natVersion @11)
        ( mkBasicTxOut
            addr
            (inject (Coin 2_000_000))
            :: TxOut ConwayEra
        )

mkBootTestContext :: IO (Context IO)
mkBootTestContext =
    fmap
        ( \ctx ->
            ctx
                { provider =
                    (provider ctx)
                        { queryProtocolParams =
                            pure emptyPParams
                        }
                }
        )
        mkTestContext

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
