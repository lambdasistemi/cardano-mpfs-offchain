{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RetractFactsSpec
-- Description : Tests for POST /facts/retract
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RetractFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , Value (..)
    , decode
    , eitherDecode
    , encode
    , object
    , (.=)
    )
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    , status400
    , status409
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
    , shouldSatisfy
    )
import Test.QuickCheck
    ( generate
    )

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address
    ( Addr (..)
    , serialiseAddr
    )
import Cardano.Ledger.Api.PParams
    ( emptyPParams
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize
    , serialize'
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MaryValue (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    , CSMTOps (..)
    , mkCSMTOps
    )
import Cardano.UTxOCSMT.Application.Run.Config (context)
import ChainFollower.Rollbacks.Store qualified as CFStore
import Control.Lens ((&), (.~))
import Control.Monad (forM_)
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( RunTransaction (..)
    , mapColumns
    , newRunTransaction
    )
import Database.RocksDB
    ( DB (..)
    , withDBCF
    )
import System.Directory (doesFileExist)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.Unsafe (unsafePerformIO)

import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.API.Types.Facts
    ( RetractFacts (..)
    )
import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    , unifiedCodecs
    )
import Cardano.MPFS.Context
    ( Context (..)
    )
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , LocatedRequest (..)
    , Operation (..)
    , Request (..)
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators
    ( genKeyHash
    , genTxIn
    )
import Cardano.MPFS.HTTP.Encoding
    ( Hex (..)
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
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx (..)
    , readMerkleRoot
    )
import Cardano.MPFS.Indexer.TxFixtures (testScriptHash)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    )
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( addrKeyHashBytes
    , cageAddrFromCfg
    , cagePolicyIdFromCfg
    , currentPosixMs
    , mkInlineDatum
    , mkRequestDatum
    , requestAddrFromCfg
    , toPlcData
    )

spec :: Spec
spec = describe "POST /facts/retract" $ do
    it "is routed and rejects malformed addresses with 400" $ do
        ctx <- mkTestContext
        resp <- postJson ctx "/facts/retract" badRequest
        simpleStatus resp `shouldBe` status400

    it "documents facts route and drops legacy tx route"
        $ case eitherDecode renderSwaggerJSON of
            Right (Object swagger) ->
                case KM.lookup "paths" swagger of
                    Just (Object paths) -> do
                        KM.member "/facts/retract" paths
                            `shouldBe` True
                        KM.member "/tx/retract" paths
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

    it "returns 409 JSON while the request is still in process_time" $ do
        nowMs <- currentPosixMs
        withRetractContext
            (nonRetractableProvider "process-time")
            300_000
            600_000
            (nowMs - 1_000)
            $ \ctx reqIn ownerAddr -> do
                resp <-
                    postJsonValue
                        ctx
                        "/facts/retract"
                        (retractRequestBody reqIn ownerAddr)
                simpleStatus resp `shouldBe` status409
                assertJsonError
                    resp
                    "request_not_retractable"
                    "process_time"

    it
        "returns 409 JSON when the retract window is too close to \
        \its end"
        $ do
            nowMs <- currentPosixMs
            withRetractContext
                (nonRetractableProvider "too-close")
                1_000
                15_000
                (nowMs - 10_000)
                $ \ctx reqIn ownerAddr -> do
                    resp <-
                        postJsonValue
                            ctx
                            "/facts/retract"
                            (retractRequestBody reqIn ownerAddr)
                    simpleStatus resp `shouldBe` status409
                    assertJsonError
                        resp
                        "request_not_retractable"
                        "safe validity interval"

    it "returns retract facts inside the retractable window" $ do
        nowMs <- currentPosixMs
        withRetractContext
            happyPathProvider
            1_000
            120_000
            (nowMs - 30_000)
            $ \ctx reqIn ownerAddr -> do
                resp <-
                    postJsonValue
                        ctx
                        "/facts/retract"
                        (retractRequestBody reqIn ownerAddr)
                simpleStatus resp `shouldBe` status200
                assertRetractFactsResponse resp

-- | Deliberately malformed serialized address with valid utxo
-- field, so handler-level address validation determines the status.
badRequest :: String
badRequest =
    "{\"utxo\":\"00#0\",\"address\":\"00\"}"

sampleToken :: TokenId
sampleToken = TokenId (AssetName (SBS.toShort "cafe"))

testCageConfig :: CageConfig
testCageConfig =
    CageConfig
        { cageScriptBytes = SBS.toShort "dummy"
        , requestScriptBytes = testRequestScriptBytes
        , cfgScriptHash = testScriptHash
        , defaultProcessTime = 60_000
        , defaultRetractTime = 30_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        , cfgStakeScript = Nothing
        }

testRequestScriptBytes :: SBS.ShortByteString
testRequestScriptBytes =
    unsafePerformIO loadTestRequestScriptBytes
{-# NOINLINE testRequestScriptBytes #-}

loadTestRequestScriptBytes :: IO SBS.ShortByteString
loadTestRequestScriptBytes = do
    hex <- tryRead candidatePaths
    let trimmed =
            BS.takeWhile
                (\b -> b /= 10 && b /= 13)
                hex
    case B16.decode trimmed of
        Right bs -> pure (SBS.toShort bs)
        Left err ->
            error
                $ "loadTestRequestScriptBytes: "
                    <> err
  where
    candidatePaths =
        [ "test-data/request.uplc.hex"
        , "cardano-mpfs-offchain/test-data/request.uplc.hex"
        ]
    tryRead [] =
        error
            "loadTestRequestScriptBytes: \
            \test-data/request.uplc.hex not found \
            \in any of the candidate paths"
    tryRead (p : ps) = do
        exists <- doesFileExist p
        if exists
            then BS.readFile p
            else tryRead ps

withRetractContext
    :: (Provider IO -> Provider IO)
    -> Integer
    -> Integer
    -> Integer
    -> (Context IO -> TxIn -> Addr -> IO a)
    -> IO a
withRetractContext providerPatch processTime retractTime submittedAt action = do
    ctx0 <- mkTestContext
    reqIn <- generate genTxIn
    stateIn <- generate genTxIn
    walletIn <- generate genTxIn
    ownerKh <- generate genKeyHash
    let ownerAddr =
            Addr Testnet (KeyHashObj ownerKh) StakeRefNull
        request =
            Request
                { requestToken = sampleToken
                , requestOwner = ownerKh
                , requestKey = "key"
                , requestValue = Insert "value"
                , requestFee = Coin 100_000
                , requestSubmittedAt = submittedAt
                }
        ctx =
            ctx0
                { cfgCage = testCageConfig
                , provider =
                    providerPatch (provider ctx0)
                }
    St.putRequest
        (St.requests (state ctx))
        (LocatedRequest reqIn request)
    withIndexedUtxos
        [ (reqIn, requestTxOutBytes ownerAddr submittedAt)
        , (stateIn, stateTxOutBytes ownerAddr processTime retractTime)
        , (walletIn, walletTxOutBytes ownerAddr)
        ]
        ctx
        $ \ctxWithIndexer ->
            action ctxWithIndexer reqIn ownerAddr

withIndexedUtxos
    :: [(TxIn, ByteString)]
    -> Context IO
    -> (Context IO -> IO a)
    -> IO a
withIndexedUtxos entries ctx action =
    withSystemTempDirectory "retract-indexer-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db -> do
                    let database =
                            mkRocksDBDatabase
                                db
                                ( mkColumns
                                    (columnFamilies db)
                                    unifiedCodecs
                                )
                        CSMTContext{fromKV, hashing} = context
                        csmtOps = mkCSMTOps fromKV hashing
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    runTransaction
                        $ CFStore.armageddonSetup
                            InRollbacks
                            (SlotNo 42)
                            (Just (BlockId "block-id-bytes"))
                    runTransaction
                        $ mapColumns InUtxo
                        $ forM_ entries
                        $ \(txIn, txOutBytes) ->
                            csmtInsert
                                csmtOps
                                (serialize (natVersion @11) txIn)
                                (BSL.fromStrict txOutBytes)
                    let IndexerTx readRoot = readMerkleRoot
                    mRoot <- runTransaction readRoot
                    action
                        ctx
                            { runIndexerTx =
                                \(IndexerTx body) ->
                                    runTransaction body
                            , utxoRoot = pure mRoot
                            }

requestTxOutBytes :: Addr -> Integer -> ByteString
requestTxOutBytes ownerAddr submittedAt =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    reqAddr =
        requestAddrFromCfg
            testCageConfig
            sampleToken
            Testnet
    txOut =
        mkBasicTxOut reqAddr (inject (Coin 2_000_000))
            & datumTxOutL
                .~ mkInlineDatum
                    ( mkRequestDatum
                        sampleToken
                        ownerAddr
                        "key"
                        (OpInsert "value")
                        100_000
                        submittedAt
                    )

stateTxOutBytes
    :: Addr -> Integer -> Integer -> ByteString
stateTxOutBytes ownerAddr processTime retractTime =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    tokenMA =
        MultiAsset
            $ Map.singleton
                (cagePolicyIdFromCfg testCageConfig)
            $ Map.singleton
                (unTokenId sampleToken)
                1
    val = MaryValue (Coin 2_000_000) tokenMA
    datum =
        StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString
                        (addrKeyHashBytes ownerAddr)
                , stateRoot =
                    OnChainRoot (BS.replicate 32 0)
                , stateMaxFee = 1_000_000
                , stateProcessTime = processTime
                , stateRetractTime = retractTime
                , stateStakeScript = Nothing
                }
    txOut =
        mkBasicTxOut
            (cageAddrFromCfg testCageConfig Testnet)
            val
            & datumTxOutL
                .~ mkInlineDatum (toPlcData datum)

walletTxOutBytes :: Addr -> ByteString
walletTxOutBytes ownerAddr =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    txOut =
        mkBasicTxOut
            ownerAddr
            (inject (Coin 5_000_000))

nonRetractableProvider :: String -> Provider IO -> Provider IO
nonRetractableProvider label prov =
    prov
        { queryProtocolParams =
            fail
                ( label
                    <> ": queryProtocolParams should not be called"
                )
        , posixMsCeilSlot =
            \_ ->
                error
                    $ label
                        <> ": posixMsCeilSlot should not be called"
        }

happyPathProvider :: Provider IO -> Provider IO
happyPathProvider prov =
    prov
        { queryProtocolParams = pure emptyPParams
        , posixMsCeilSlot =
            \ms ->
                pure
                    ( SlotNo
                        (fromInteger (ms `div` 1000))
                    )
        }

retractRequestBody :: TxIn -> Addr -> Value
retractRequestBody txIn addr =
    object
        [ "utxo" .= txInRefText txIn
        , "address" .= Hex (serialiseAddr addr)
        ]

txInRefText :: TxIn -> T.Text
txInRefText (TxIn (TxId sh) (TxIx ix)) =
    TE.decodeUtf8
        ( B16.encode
            $ Crypto.hashToBytes
            $ extractHash sh
        )
        <> "#"
        <> T.pack (show ix)

assertJsonError :: SResponse -> T.Text -> T.Text -> IO ()
assertJsonError resp expectedError detailNeedle =
    case decode (simpleBody resp) of
        Just (Object obj) -> do
            KM.lookup "error" obj
                `shouldBe` Just (String expectedError)
            case KM.lookup "detail" obj of
                Just (String detail) ->
                    detail
                        `shouldSatisfy` T.isInfixOf detailNeedle
                _ ->
                    expectationFailure
                        "Expected JSON error detail string"
        _ ->
            expectationFailure
                "Expected JSON error body"

assertRetractFactsResponse :: SResponse -> IO ()
assertRetractFactsResponse resp =
    case decode (simpleBody resp) of
        Just facts@RetractFacts{} ->
            rfValidityEndSlot facts
                `shouldSatisfy` (> rfValidityStartSlot facts)
        Nothing ->
            expectationFailure
                "Expected RetractFacts response"

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

postJsonValue
    :: Context IO
    -> ByteString
    -> Value
    -> IO SResponse
postJsonValue ctx path body =
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
                    encode body
                }
        )
        (mkApp ctx)
