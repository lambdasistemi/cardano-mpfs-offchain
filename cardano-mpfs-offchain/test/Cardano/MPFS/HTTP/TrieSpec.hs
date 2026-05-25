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

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Either (isRight)
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

import CSMT.Core.CBOR (renderProof)
import CSMT.Core.Hash
    ( byteStringToKey
    , renderHash
    )
import CSMT.Hashes
    ( hashHashing
    , mkHash
    )
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , identityFromKV
    , insertMHash
    , proofM
    )
import Cardano.MPFS.Application (dbConfig)
import Cardano.MPFS.Client.Facts
    ( FactAbsentFacts (..)
    , FactPresentFacts (..)
    , verifyFactAbsentFacts
    , verifyFactPresentFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , LocatedTokenState (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.HTTP.Types
    ( FactResponse (..)
    , FactWitness (..)
    , ProofResponse (..)
    , TxInJSON (..)
    , VerificationSnapshot (..)
    , txInToJSON
    )
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.Trie.Persistent
    ( mkPersistentTrieManager
    )
import Database.RocksDB
    ( DB (..)
    , withDBCF
    )
import MPF.Verify
    ( verifyAikenExclusionProof
    )
import System.IO.Temp (withSystemTempDirectory)

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

seedTokenStateWithRoot :: Context IO -> TxIn -> Root -> IO ()
seedTokenStateWithRoot ctx txIn trieRoot = do
    ts <- mkDummyTokenState
    St.putToken
        (St.tokens (state ctx))
        cafeTid
        (LocatedTokenState txIn ts{root = trieRoot})

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

        it
            "returns a persistent fact value that replays \
            \against its MPF proof"
            $ withPersistentFactContext
            $ \ctx -> do
                resp <-
                    getFact
                        ctx
                        "63616665"
                        (hex "hello")
                simpleStatus resp `shouldBe` status200
                case decode (simpleBody resp) of
                    Just factResp@FactResponse{frSnapshot} ->
                        verifyFactPresentFacts
                            (TrustedRoot (vsUtxoRoot frSnapshot))
                            FactPresentFacts
                                { fpfKey = Hex "hello"
                                , fpfResponse = factResp
                                }
                            `shouldSatisfy` isRight
                    _ ->
                        expectationFailure
                            "Expected fact response JSON"

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

        it "returns verifiable exclusion proof for missing key" $ do
            ctx0 <- mkTestContext
            ctx <-
                withSnapshot "root" "tx-out" "proof" ctx0
            Trie.createTrie (trieManager ctx) cafeTid
            trieRoot <-
                Trie.withTrie
                    (trieManager ctx)
                    cafeTid
                    Trie.getRoot
            txIn <- generate genTxIn
            seedTokenStateWithRoot ctx txIn trieRoot
            resp <-
                getProof
                    ctx
                    "63616665"
                    (hex "absent")
            simpleStatus resp `shouldBe` status200
            case decode (simpleBody resp) of
                Just ProofResponse{prFact} ->
                    verifyAikenExclusionProof
                        (unRoot trieRoot)
                        "absent"
                        (unHex (fwMpfProof prFact))
                        `shouldBe` True
                _ ->
                    expectationFailure
                        "Expected proof response JSON"

        it
            "returns a persistent absence proof that \
            \verifies after delete"
            $ withPersistentFactContextWithTrie
                ( \trie -> do
                    _ <- Trie.insert trie "deleted" "gone"
                    Trie.delete trie "deleted"
                )
            $ \ctx -> do
                resp <-
                    getProof
                        ctx
                        "63616665"
                        (hex "deleted")
                simpleStatus resp `shouldBe` status200
                case decode (simpleBody resp) of
                    Just proofResp@ProofResponse{prSnapshot} ->
                        verifyFactAbsentFacts
                            (TrustedRoot (vsUtxoRoot prSnapshot))
                            FactAbsentFacts
                                { fafKey = Hex "deleted"
                                , fafResponse = proofResp
                                }
                            `shouldSatisfy` isRight
                    _ ->
                        expectationFailure
                            "Expected proof response JSON"

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

withPersistentFactContext :: (Context IO -> IO a) -> IO a
withPersistentFactContext =
    withPersistentFactContextWithTrie
        ( \trie -> do
            _ <- Trie.insert trie "hello" "world"
            Trie.getRoot trie
        )

withPersistentFactContextWithTrie
    :: (Trie.Trie IO -> IO Root)
    -> (Context IO -> IO a)
    -> IO a
withPersistentFactContextWithTrie setupTrie action =
    withSystemTempDirectory "persistent-fact-http" $ \dir ->
        withDBCF
            dir
            dbConfig
            [ ("nodes", dbConfig)
            , ("kv", dbConfig)
            , ("meta", dbConfig)
            , ("raw", dbConfig)
            ]
            $ \db@DB{columnFamilies} ->
                case columnFamilies of
                    [nodesCF, kvCF, metaCF, _rawCF] -> do
                        tm <-
                            mkPersistentTrieManager
                                db
                                nodesCF
                                kvCF
                                metaCF
                        ctx0 <- mkTestContext
                        stateTxIn <- generate genTxIn
                        let stateTxOut = "state-tx-out"
                            CsmtWitness
                                { cwRoot
                                , cwTxOut
                                , cwProof
                                } =
                                    singleUtxoWitness
                                        stateTxIn
                                        stateTxOut
                        ctx <-
                            withSnapshot
                                cwRoot
                                cwTxOut
                                cwProof
                                ctx0{trieManager = tm}
                        Trie.createTrie (trieManager ctx) cafeTid
                        trieRoot <-
                            Trie.withTrie
                                (trieManager ctx)
                                cafeTid
                                setupTrie
                        seedTokenStateWithRoot
                            ctx
                            stateTxIn
                            trieRoot
                        action ctx
                    _ ->
                        expectationFailure
                            "expected four test column families"
                            >> error "unreachable"

data CsmtWitness = CsmtWitness
    { cwRoot :: ByteString
    , cwTxOut :: ByteString
    , cwProof :: ByteString
    }

singleUtxoWitness :: TxIn -> ByteString -> CsmtWitness
singleUtxoWitness txIn txOutBs =
    evalPureFromEmptyDB $ do
        let key = byteStringToKey (encodeTxInForCsmt txIn)
        insertMHash key (mkHash txOutBs)
        mProof <- proofM hashCodecs identityFromKV hashHashing key
        mRoot <- getRootHashM
        pure
            CsmtWitness
                { cwRoot = maybe BS.empty renderHash mRoot
                , cwTxOut = txOutBs
                , cwProof =
                    case mProof of
                        Just (_, proof) -> renderProof proof
                        Nothing -> BS.empty
                }

encodeTxInForCsmt :: TxIn -> ByteString
encodeTxInForCsmt txIn =
    let TxInJSON
            { tjTxId = Hex txIdBs
            , tjTxIx
            } = txInToJSON txIn
    in  CBOR.toStrictByteString
            $ mconcat
                [ CBOR.encodeListLen 2
                , CBOR.encodeBytes txIdBs
                , CBOR.encodeWord64 tjTxIx
                ]
