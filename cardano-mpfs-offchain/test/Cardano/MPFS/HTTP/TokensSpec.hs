{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokensSpec
-- Description : Tests for GET /tokens endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokensSpec
    ( spec
    , mkDummyTokenState
    ) where

import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Vector qualified as V
import Network.HTTP.Types
    ( status200
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

import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Mary.Value (AssetName (..))

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , LocatedTokenState (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Generators (genKeyHash, genTxIn)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Types
    ( TokenIdJSON (..)
    , TokenSetWitness (..)
    , TokenUtxoEntry (..)
    , TokensResponse (..)
    )
import Cardano.MPFS.Indexer.TxFixtures (testScriptHash)
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Test.QuickCheck (generate)
import Unsafe.Coerce (unsafeCoerce)

getTokens :: Context IO -> IO SResponse
getTokens ctx =
    runSession
        ( request
            (setPath defaultRequest "/tokens")
        )
        (mkApp ctx)

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
            , cfgCage = testCageConfig
            , runIndexerTx =
                \_ -> pure (unsafeCoerce emptyTokenSet)
            }
  where
    emptyTokenSet :: ([(TxIn, ByteString)], ByteString)
    emptyTokenSet = ([], proofBs)

withTokenSet
    :: [(TxIn, ByteString)]
    -> ByteString
    -> Context IO
    -> Context IO
withTokenSet entries proofBs ctx =
    ctx
        { runIndexerTx =
            \_ -> pure (unsafeCoerce tokenSet)
        }
  where
    tokenSet :: ([(TxIn, ByteString)], ByteString)
    tokenSet = (entries, proofBs)

testCageConfig :: CageConfig
testCageConfig =
    CageConfig
        { cageScriptBytes = SBS.toShort "dummy"
        , requestScriptBytes = SBS.toShort "dummy"
        , cfgScriptHash = testScriptHash
        , defaultProcessTime = 60_000
        , defaultRetractTime = 30_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }

-- | A dummy token state for testing.
mkDummyTokenState :: IO TokenState
mkDummyTokenState = do
    kh <- generate genKeyHash
    pure
        TokenState
            { owner = kh
            , root = Root (BS.replicate 32 0)
            , tip = Coin 1_000_000
            , processTime = 60_000
            , retractTime = 30_000
            }

spec :: Spec
spec = describe "GET /tokens" $ do
    it "returns empty proof-bearing token set on fresh state" $ do
        ctx0 <- mkTestContext
        ctx <- withSnapshot "root" "tx-out" "proof" ctx0
        resp <- getTokens ctx
        simpleStatus resp `shouldBe` status200
        assertEnvelope resp 0

    it "returns proof-bearing witness for all token UTxOs" $ do
        ctx0 <- mkTestContext
        ctx <- withSnapshot "root" "tx-out" "proof" ctx0
        ts <- mkDummyTokenState
        txIn1 <- generate genTxIn
        txIn2 <- generate genTxIn
        let tid1 =
                TokenId (AssetName (SBS.toShort "deadbeef"))
            tid2 =
                TokenId (AssetName (SBS.toShort "cafebabe"))
        St.putToken
            (St.tokens (state ctx))
            tid1
            (LocatedTokenState txIn1 ts)
        St.putToken
            (St.tokens (state ctx))
            tid2
            (LocatedTokenState txIn2 ts)
        let ctxWithSet =
                withTokenSet
                    [(txIn1, "tx-out-1"), (txIn2, "tx-out-2")]
                    "proof"
                    ctx
        resp <- getTokens ctxWithSet
        simpleStatus resp `shouldBe` status200
        assertEnvelope resp 2
        assertTokenEntries
            resp
            [ ("deadbeef", "tx-out-1")
            , ("cafebabe", "tx-out-2")
            ]

    it "returns 503 when snapshot not yet available" $ do
        ctx <- mkTestContext
        resp <- getTokens ctx
        simpleStatus resp `shouldBe` status503

assertEnvelope :: SResponse -> Int -> IO ()
assertEnvelope resp n =
    case decode (simpleBody resp) of
        Just (Object obj) -> do
            KM.member "snapshot" obj `shouldBe` True
            case KM.lookup "tokens" obj of
                Just (Object tokensObj) -> do
                    case KM.lookup "entries" tokensObj of
                        Just (Array entries) ->
                            V.length entries `shouldBe` n
                        _ ->
                            expectationFailure
                                "tokens.entries is not an array"
                    case KM.lookup
                        "completeness_proof"
                        tokensObj of
                        Just (String proof) ->
                            proof `shouldBe` "70726f6f66"
                        _ ->
                            expectationFailure
                                "tokens.completeness_proof \
                                \is not a string"
                _ ->
                    expectationFailure
                        "tokens is not an object"
        _ ->
            expectationFailure
                "Expected JSON object"

assertTokenEntries :: SResponse -> [(ByteString, ByteString)] -> IO ()
assertTokenEntries resp expected =
    case decode (simpleBody resp) of
        Just
            TokensResponse
                { trsTokens = TokenSetWitness{tswEntries}
                } ->
            map
                ( \TokenUtxoEntry
                    { tueTokenId = TokenIdJSON tokenId
                    , tueTxOutCbor = Hex txOut
                    } -> (tokenId, txOut)
                )
                tswEntries
                `shouldBe` expected
        _ ->
            expectationFailure
                "Expected TokensResponse"
