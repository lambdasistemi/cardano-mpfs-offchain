{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokenFactsSpec
-- Description : Tests for GET /tokens/:id/facts endpoint
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.TokenFactsSpec (spec) where

import Data.Aeson (decode)
import Data.ByteString (ByteString)
import Data.ByteString.Short qualified as SBS
import Data.Either (isRight)
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
    , shouldSatisfy
    )

import Cardano.Ledger.Mary.Value (AssetName (..))
import Test.QuickCheck (generate)

import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Read (verifyTokenFacts)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( LocatedTokenState (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.AtomicReadFixture
    ( insertFacts
    , staleRoot
    , stateTxOutBytesWithRoot
    , withProofIndexer
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.TokensSpec (mkDummyTokenState)
import Cardano.MPFS.HTTP.Types
    ( FactEntry (..)
    , FactsResponse (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.State qualified as St

-- | "cafe" token — hex "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

spec :: Spec
spec =
    describe "GET /tokens/:id/facts"
        $ it
            "returns snapshot, witnessed state, and all \
            \enumerated facts"
        $ do
            ctx0 <- mkTestContext
            txIn <- generate genTxIn
            -- Precompute the deterministic trie root so the indexed
            -- state UTxO can encode it in its inline datum. The read
            -- verifier decodes the on-chain root from that provable
            -- @TxOut@ - there is no server-side projection to trust.
            Root trieRootBytes <- insertFacts ctx0 cafeTid facts
            withProofIndexer
                (Just staleRoot)
                [(txIn, stateTxOutBytesWithRoot trieRootBytes)]
                ctx0
                $ \_txRoot ctx -> do
                    trieRoot <- insertFacts ctx cafeTid facts
                    ts0 <- mkDummyTokenState
                    let ts = ts0{root = trieRoot}
                    St.putToken
                        (St.tokens (state ctx))
                        cafeTid
                        (LocatedTokenState txIn ts)

                    resp <- getFacts ctx "63616665"

                    simpleStatus resp `shouldBe` status200
                    case decode (simpleBody resp) of
                        Just body@FactsResponse{..} -> do
                            factPairs frsFacts
                                `shouldBe` sort facts
                            verifyTokenFacts
                                (TrustedRoot (vsUtxoRoot frsSnapshot))
                                body
                                `shouldSatisfy` isRight
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
