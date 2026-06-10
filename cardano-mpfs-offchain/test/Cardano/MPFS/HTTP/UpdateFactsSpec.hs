{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.UpdateFactsSpec
-- Description : Tests for update facts wire and read foundation
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.UpdateFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , Value (..)
    , eitherDecode
    , encode
    , object
    , (.=)
    )
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.ByteString.Short qualified as SBS
import Data.Foldable (traverse_)
import Data.List qualified as List
import Data.Map.Strict qualified as Map
import Data.Proxy (Proxy (..))
import Data.Swagger qualified as Swagger
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    , status400
    , status500
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
    , shouldContain
    , shouldSatisfy
    )
import Test.QuickCheck (generate)

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address
    ( Addr (..)
    , serialiseAddr
    )
import Cardano.Ledger.Api.PParams (emptyPParams)
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
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Facts
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , TrieFact (..)
    , UnverifiedPParams (..)
    , UpdateFacts (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.OnChain qualified as OnChain
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , PParams
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.Generators
    ( genKeyHash
    , genTxIn
    )
import Cardano.MPFS.HTTP.AtomicReadFixture
    ( cafeTid
    , cafeTokenJSON
    , insertFacts
    , testCageConfig
    , withProofIndexer
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Swagger (renderSwaggerJSON)
import Cardano.MPFS.HTTP.Types.Facts (mkUpdateFacts)
import Cardano.MPFS.Indexer.Reads
    ( IndexerReadError
    , IndexerTx
    , readRequestUtxosAt
    , readTrieFact
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    )
import Cardano.MPFS.TxBuilder qualified as Tx
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
import Control.Lens ((&), (.~))
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

spec :: Spec
spec = do
    describe "POST " $ do
        describe "facts" $ do
            describe "update|update facts wire" $ do
                it "is routed and rejects malformed addresses with 400" $ do
                    ctx <- mkTestContext
                    resp <- postJson ctx "/facts/update" badUpdateRequest
                    simpleStatus resp `shouldBe` status400

                it "returns update facts for an exact request subset" $ do
                    nowMs <- currentPosixMs
                    ctx0 <- mkTestContext
                    reqIn <- generate genTxIn
                    stateIn <- generate genTxIn
                    walletIn <- generate genTxIn
                    ownerKh <- generate genKeyHash
                    let ownerAddr =
                            Addr
                                Testnet
                                (KeyHashObj ownerKh)
                                StakeRefNull
                        submittedAt = nowMs - 1_000
                        entries =
                            [ (stateIn, stateTxOutBytes ownerAddr)
                            ,
                                ( reqIn
                                , requestTxOutBytes
                                    ownerAddr
                                    submittedAt
                                )
                            , (walletIn, walletTxOutBytes ownerAddr)
                            ]
                    withProofIndexer Nothing entries ctx0
                        $ \_txRoot ctxWithIndexer -> do
                            _ <- insertFacts ctxWithIndexer cafeTid []
                            let ctx =
                                    ctxWithIndexer
                                        { provider =
                                            happyUpdateProvider
                                                (provider ctxWithIndexer)
                                        }
                                body =
                                    object
                                        [ "token" .= cafeTokenJSON
                                        , "address"
                                            .= Hex
                                                ( serialiseAddr
                                                    ownerAddr
                                                )
                                        , "requests"
                                            .= [txInRefText reqIn]
                                        ]
                            resp <-
                                postJsonValue
                                    ctx
                                    "/facts/update"
                                    body
                            simpleStatus resp `shouldBe` status200

                it
                    "returns a typed failure when trie proof data is missing"
                    $ do
                        nowMs <- currentPosixMs
                        ctx0 <- mkTestContext
                        reqIn <- generate genTxIn
                        stateIn <- generate genTxIn
                        walletIn <- generate genTxIn
                        ownerKh <- generate genKeyHash
                        let ownerAddr =
                                Addr
                                    Testnet
                                    (KeyHashObj ownerKh)
                                    StakeRefNull
                            submittedAt = nowMs - 1_000
                            entries =
                                [ (stateIn, stateTxOutBytes ownerAddr)
                                ,
                                    ( reqIn
                                    , requestTxOutBytesWithOp
                                        ownerAddr
                                        submittedAt
                                        (OnChain.OpDelete "missing-value")
                                    )
                                , (walletIn, walletTxOutBytes ownerAddr)
                                ]
                        withProofIndexer Nothing entries ctx0
                            $ \_txRoot ctxWithIndexer -> do
                                _ <- insertFacts ctxWithIndexer cafeTid []
                                let ctx =
                                        ctxWithIndexer
                                            { provider =
                                                happyUpdateProvider
                                                    (provider ctxWithIndexer)
                                            }
                                    body =
                                        object
                                            [ "token" .= cafeTokenJSON
                                            , "address"
                                                .= Hex
                                                    ( serialiseAddr
                                                        ownerAddr
                                                    )
                                            , "requests"
                                                .= [txInRefText reqIn]
                                            ]
                                resp <-
                                    postJsonValue
                                        ctx
                                        "/facts/update"
                                        body
                                simpleStatus resp `shouldBe` status500
                                BL.unpack (simpleBody resp)
                                    `shouldContain` "no MPF inclusion proof for key"
                                BL.unpack (simpleBody resp)
                                    `shouldContain` "7375627365742d6b6579"

                it "documents facts route and drops legacy tx route"
                    $ case eitherDecode renderSwaggerJSON of
                        Right (Object swagger) ->
                            case KM.lookup "paths" swagger of
                                Just (Object paths) -> do
                                    KM.member "/facts/update" paths
                                        `shouldBe` True
                                    KM.member "/tx/update" paths
                                        `shouldBe` False
                                    case KM.lookup "/facts/update" paths of
                                        Just route ->
                                            BL.unpack (encode route)
                                                `shouldSatisfy` List.isInfixOf
                                                    "UpdateFacts"
                                        Nothing ->
                                            expectationFailure
                                                "Swagger missing /facts/update"
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

    it "encodes the shared TrieFact shape with nullable values" $ do
        let fact =
                TrieFact
                    { tfKey = Hex "key"
                    , tfValue = Nothing
                    , tfMpfProof = Hex "proof"
                    }
        assertJSONKeys
            ["key", "value", "mpf_proof"]
            (toJSON fact)
        case toJSON fact of
            Object obj ->
                KM.lookup "value" obj `shouldBe` Just Null
            _ ->
                expectationFailure
                    "Expected TrieFact JSON object"

    it "encodes UpdateFacts without unsigned transaction CBOR" $ do
        let facts =
                sampleUpdateFacts
        assertJSONKeys
            [ "snapshot"
            , "token"
            , "state_utxo"
            , "request_utxos"
            , "wallet_utxos"
            , "trie_root"
            , "trie_facts"
            , "validity_upper_slot"
            , "protocol_parameters"
            ]
            (toJSON facts)
        case toJSON facts of
            Object obj -> do
                KM.member "tx" obj `shouldBe` False
                KM.member "unsigned_tx_cbor" obj
                    `shouldBe` False
            _ ->
                expectationFailure
                    "Expected UpdateFacts JSON object"

    it "round-trips UpdateFacts through JSON"
        $ eitherDecode (encode sampleUpdateFacts)
        `shouldBe` Right sampleUpdateFacts

    it "has Swagger schema instances" $ do
        let _updateSchema =
                Swagger.toSchema (Proxy @UpdateFacts)
            _trieFactSchema =
                Swagger.toSchema (Proxy @TrieFact)
        _updateSchema `seq`
            _trieFactSchema `seq`
                (pure () :: IO ())

    it "provides server conversion from update inputs" $ do
        txIn <- generate genTxIn
        let facts =
                mkUpdateFacts
                    sampleSnapshot
                    sampleToken
                    (sampleUtxoInput txIn)
                    [sampleUtxoInput txIn]
                    [sampleUtxoInput txIn]
                    "trie-root"
                    [sampleBuilderTrieFact]
                    123
                    samplePParams
        assertJSONKeys
            [ "snapshot"
            , "token"
            , "state_utxo"
            , "request_utxos"
            , "wallet_utxos"
            , "trie_root"
            , "trie_facts"
            , "validity_upper_slot"
            , "protocol_parameters"
            ]
            (toJSON facts)

    it "exports update-focused atomic read helpers" $ do
        let _readRequestUtxosAt
                :: Addr
                -> IndexerTx
                    ( Either
                        IndexerReadError
                        [ResolvedWalletInput]
                    )
            _readRequestUtxosAt = readRequestUtxosAt
            _readTrieFact
                :: TokenId
                -> ByteString
                -> IndexerTx
                    (Either IndexerReadError Tx.TrieFact)
            _readTrieFact = readTrieFact
        _readRequestUtxosAt `seq`
            _readTrieFact `seq`
                (pure () :: IO ())

assertJSONKeys :: [String] -> Value -> IO ()
assertJSONKeys keys = \case
    Object obj ->
        traverse_
            ( \key ->
                KM.member (Key.fromString key) obj
                    `shouldBe` True
            )
            keys
    _ ->
        expectationFailure "Expected JSON object"

-- | Deliberately malformed serialized address with a valid token
-- field, so handler-level address validation determines the status.
badUpdateRequest :: String
badUpdateRequest =
    "{\"token\":\"63616665\",\"address\":\"00\"}"

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
    postJson ctx path (BL.unpack (encode body))

stateTxOutBytes :: Addr -> ByteString
stateTxOutBytes ownerAddr =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    tokenMA =
        MultiAsset
            $ Map.singleton
                (cagePolicyIdFromCfg testCageConfig)
            $ Map.singleton
                (unTokenId cafeTid)
                1
    val = MaryValue (Coin 2_000_000) tokenMA
    datum =
        StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString
                        (addrKeyHashBytes ownerAddr)
                , stateRoot =
                    OnChainRoot
                        (BS.replicate 32 0)
                , stateMaxFee = 1_000_000
                , stateProcessTime = 300_000
                , stateRetractTime = 60_000
                }
    txOut =
        mkBasicTxOut
            (cageAddrFromCfg testCageConfig Testnet)
            val
            & datumTxOutL .~ mkInlineDatum (toPlcData datum)

requestTxOutBytes :: Addr -> Integer -> ByteString
requestTxOutBytes ownerAddr submittedAt =
    requestTxOutBytesWithOp
        ownerAddr
        submittedAt
        (OnChain.OpInsert "subset-value")

requestTxOutBytesWithOp
    :: Addr
    -> Integer
    -> OnChain.OnChainOperation
    -> ByteString
requestTxOutBytesWithOp ownerAddr submittedAt op =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    txOut =
        mkBasicTxOut
            (requestAddrFromCfg testCageConfig cafeTid Testnet)
            (inject (Coin 2_000_000))
            & datumTxOutL
                .~ mkInlineDatum
                    ( mkRequestDatum
                        cafeTid
                        ownerAddr
                        "subset-key"
                        op
                        100_000
                        submittedAt
                    )

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

happyUpdateProvider :: Provider IO -> Provider IO
happyUpdateProvider prov =
    prov
        { queryProtocolParams = pure emptyPParams
        , posixMsToSlot =
            \ms ->
                pure
                    $ SlotNo
                    $ fromInteger
                    $ ms `div` 1000
        }

txInRefText :: TxIn -> Text
txInRefText (TxIn (TxId sh) (TxIx ix)) =
    TE.decodeUtf8
        ( B16.encode
            $ Crypto.hashToBytes
            $ extractHash sh
        )
        <> "#"
        <> T.pack (show ix)

sampleUpdateFacts :: UpdateFacts
sampleUpdateFacts =
    UpdateFacts
        { ufSnapshot = sampleVerificationSnapshot
        , ufToken = TokenIdJSON "cafe"
        , ufStateUtxo = sampleUtxoEntry
        , ufRequestUtxos = [sampleUtxoEntry]
        , ufWalletUtxos = [sampleUtxoEntry]
        , ufTrieRoot = Hex "trie-root"
        , ufTrieFacts =
            [ TrieFact
                { tfKey = Hex "key"
                , tfValue = Just (Hex "value")
                , tfMpfProof = Hex "proof"
                }
            ]
        , ufValidityUpperSlot = 123
        , ufProtocolParameters = sampleUnverifiedPParams
        }

sampleVerificationSnapshot :: VerificationSnapshot
sampleVerificationSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex "root"
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 42
                , cpBlockId = Hex "block-id"
                }
        }

sampleUtxoEntry :: UtxoEntry
sampleUtxoEntry =
    UtxoEntry
        { ueRef =
            UtxoRef
                { urTxId = Hex "tx-id"
                , urTxIx = 0
                }
        , ueTxOutCbor = Hex "tx-out"
        , ueInclusionProof = Hex "utxo-proof"
        }

sampleUnverifiedPParams :: UnverifiedPParams
sampleUnverifiedPParams =
    UnverifiedPParams
        { uppVerified = False
        , uppCbor = Hex "pparams"
        }

sampleBuilderTrieFact :: Tx.TrieFact
sampleBuilderTrieFact =
    Tx.TrieFact
        { Tx.factKey = "key"
        , Tx.factValue = Just "value"
        , Tx.factMpfProof = "proof"
        }

sampleSnapshot :: BundleSnapshot
sampleSnapshot =
    BundleSnapshot
        { snapshotUtxoRoot = "root"
        , snapshotSlot = SlotNo 42
        , snapshotBlockId = BlockId "block-id"
        }

sampleToken :: TokenId
sampleToken = TokenId (AssetName (SBS.toShort "cafe"))

sampleUtxoInput :: TxIn -> ResolvedWalletInput
sampleUtxoInput txIn =
    (txIn, "tx-out", "utxo-proof")

samplePParams :: PParams ConwayEra
samplePParams = emptyPParams
