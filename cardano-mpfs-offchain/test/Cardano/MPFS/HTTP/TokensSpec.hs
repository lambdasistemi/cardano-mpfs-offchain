{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.TokensSpec
-- Description : Tests for GET /tokens endpoint
-- License     : Apache-2.0
--
-- The endpoint returns a 'TokensListResponse' carrying a
-- 'VerificationSnapshot' and a 'UtxoSetWitness' (the proof-bearing
-- shape introduced in 243-tokens-list). The unit tests below
-- exercise the response *shape* — not cryptographic validity of the
-- completeness proof, which is the e2e suite's job.
module Cardano.MPFS.HTTP.TokensSpec
    ( spec
    , mkDummyTokenState
    ) where

import Data.Aeson (Value (..), decode)
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString qualified as BS
import Data.ByteString.Lazy (ByteString)
import Data.ByteString.Lazy qualified as BSL
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

import Cardano.Ledger.Binary (natVersion, serialize)
import Cardano.Ledger.TxIn (TxIn)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , Root (..)
    , SlotNo (..)
    , TokenState (..)
    )
import Cardano.MPFS.Generators (genKeyHash, genTxIn)
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.State qualified as St
import Test.QuickCheck (generate)

-- | Hit @GET /tokens@ on the supplied context.
getTokens :: Context IO -> IO SResponse
getTokens ctx =
    runSession
        ( request
            (setPath defaultRequest "/tokens")
        )
        (mkApp ctx)

-- | A dummy token state for callers that still need one.
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

-- | Seed the snapshot precondition: the handler 503s unless the
-- mock context has a CSMT root and a chain checkpoint. Returns a
-- context with both filled in plus a caller-supplied
-- @utxoSetWitness@ stub.
withSnapshot
    :: ( BS.ByteString
         -> IO
                ( Maybe
                    ([(BS.ByteString, BS.ByteString)], BS.ByteString)
                )
       )
    -> IO (Context IO)
withSnapshot stubWitness = do
    ctx <- mkTestContext
    let rootBs = BS.replicate 32 0
        ctx' =
            ctx
                { utxoRoot = pure (Just rootBs)
                , utxoSetWitness = stubWitness
                }
    St.putCheckpoint
        (St.checkpoints (state ctx'))
        (SlotNo 1)
        (BlockId (BS.replicate 32 0))
    pure ctx'

-- | Decode the response body and look up @tokens.entries@.
tokensEntries :: ByteString -> Maybe Value
tokensEntries body = do
    Object root <- decode body
    Object tokens <- KM.lookup "tokens" root
    KM.lookup "entries" tokens

spec :: Spec
spec = describe "GET /tokens" $ do
    it "returns 200 with empty entries when the witness is empty"
        $ do
            ctx <-
                withSnapshot
                    ( \_ ->
                        pure (Just ([], BS.empty))
                    )
            resp <- getTokens ctx
            simpleStatus resp `shouldBe` status200
            case tokensEntries (simpleBody resp) of
                Just (Array arr) ->
                    length arr `shouldBe` 0
                _ ->
                    expectationFailure
                        "Expected JSON object with \
                        \tokens.entries array"

    it "returns 200 with one entry when the witness has one"
        $ do
            txIn <- generate genTxIn
            let txInBs =
                    BSL.toStrict
                        ( serialize
                            (natVersion @11)
                            (txIn :: TxIn)
                        )
                txOutBs = BS.replicate 16 0xbb
            ctx <-
                withSnapshot
                    ( \_ ->
                        pure
                            ( Just
                                ([(txInBs, txOutBs)], BS.empty)
                            )
                    )
            resp <- getTokens ctx
            simpleStatus resp `shouldBe` status200
            case tokensEntries (simpleBody resp) of
                Just (Array arr) ->
                    length arr `shouldBe` 1
                _ ->
                    expectationFailure
                        "Expected JSON object with \
                        \tokens.entries array"
