{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokenFactsSpec
-- Description : Tests for GET /tokens/:id/facts endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokenFactsSpec (spec) where

import Control.Monad (forM_)
import Data.Aeson (decode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.List (sort)
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

import Cardano.Ledger.Mary.Value (AssetName (..))
import Test.QuickCheck (generate)

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , LocatedTokenState (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.HTTP.Types
    ( FactEntry (..)
    , FactsResponse (..)
    , VerificationSnapshot (..)
    , WitnessedTokenState (..)
    , tokenStateToJSON
    )
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Trie qualified as Trie

-- | "cafe" token — hex "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

-- | Stub the verification snapshot and the UTxO
-- witness machinery so proof-bearing handlers can
-- assemble their envelopes.
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

spec :: Spec
spec =
    describe "GET /tokens/:id/facts"
        $ it
            "returns snapshot, witnessed state, and all \
            \enumerated facts"
        $ do
            ctx0 <- mkTestContext
            ctx <- withSnapshot "root" "tx-out" "proof" ctx0
            Trie.createTrie (trieManager ctx) cafeTid
            trieRoot <-
                Trie.withTrie
                    (trieManager ctx)
                    cafeTid
                    $ \trie -> do
                        forM_ facts
                            $ uncurry (Trie.insert trie)
                        Trie.getRoot trie
            ts0 <- mkDummyTokenState
            txIn <- generate genTxIn
            let ts = ts0{root = trieRoot}
            St.putToken
                (St.tokens (state ctx))
                cafeTid
                (LocatedTokenState txIn ts)

            resp <- getFacts ctx "63616665"

            simpleStatus resp `shouldBe` status200
            case decode (simpleBody resp) of
                Just FactsResponse{..} -> do
                    vsUtxoRoot frsSnapshot
                        `shouldBe` Hex "root"
                    wtsState frsState
                        `shouldBe` tokenStateToJSON ts
                    factPairs frsFacts
                        `shouldBe` sort facts
                _ ->
                    expectationFailure
                        "Expected facts response JSON"

facts :: [(ByteString, ByteString)]
facts =
    [ ("alpha", "one")
    , ("bravo", "two")
    , ("charlie", "three")
    ]

factPairs :: [FactEntry] -> [(ByteString, ByteString)]
factPairs entries =
    sort
        [ (unHex feKey, unHex feValue)
        | FactEntry{..} <- entries
        ]

-- | @GET \/tokens\/:id\/facts@
getFacts
    :: Context IO -> ByteString -> IO SResponse
getFacts ctx tokenHex =
    runSession
        ( request
            ( setPath
                defaultRequest
                ("/tokens/" <> tokenHex <> "/facts")
            )
        )
        (mkApp ctx)
