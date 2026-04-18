-- |
-- Module      : Cardano.MPFS.Indexer.EventSpec
-- Description : Tests for CageEvent inverse operations
-- License     : Apache-2.0
module Cardano.MPFS.Indexer.EventSpec
    ( spec
    ) where

import Data.Map.Strict qualified as Map
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )
import Test.QuickCheck
    ( Gen
    , forAll
    , listOf1
    )

import Cardano.MPFS.Core.Types
    ( Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Generators
    ( genRequest
    , genRoot
    , genTokenId
    , genTokenState
    , genTxIn
    )
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp (..)
    , inversesOf
    )

spec :: Spec
spec = describe "CageEvent" $ do
    describe "inversesOf" $ do
        it "boot produces InvRemoveToken"
            $ forAll genBoot
            $ \(tid, bootTxIn, ts) ->
                inversesOf
                    noTokens
                    noReqs
                    (CageBoot tid bootTxIn ts)
                    `shouldBe` [InvRemoveToken tid]

        it "request produces InvRemoveRequest"
            $ forAll genReqEvent
            $ \(txIn, req) ->
                inversesOf noTokens noReqs (CageRequest txIn req)
                    `shouldBe` [InvRemoveRequest txIn]

        it "retract with known request produces InvRestoreRequest"
            $ forAll genRetractKnown
            $ \(txIn, req) ->
                let reqs = Map.singleton txIn req
                in  inversesOf
                        noTokens
                        (`Map.lookup` reqs)
                        (CageRetract txIn)
                        `shouldBe` [InvRestoreRequest txIn req]

        it "retract with unknown request produces empty"
            $ forAll genTxIn
            $ \txIn ->
                inversesOf noTokens noReqs (CageRetract txIn)
                    `shouldBe` []

        it "burn with known token produces InvRestoreToken"
            $ forAll genBurnKnown
            $ \(tid, stateTxIn, ts) ->
                let tokens =
                        Map.singleton tid (stateTxIn, ts)
                in  inversesOf
                        (`Map.lookup` tokens)
                        noReqs
                        (CageBurn tid)
                        `shouldBe` [ InvRestoreToken
                                        tid
                                        stateTxIn
                                        ts
                                   ]

        it "burn with unknown token produces empty"
            $ forAll genTokenId
            $ \tid ->
                inversesOf noTokens noReqs (CageBurn tid)
                    `shouldBe` []

        it
            "update with known token+requests produces InvRestoreRoot + InvRestoreRequest"
            $ forAll genUpdateKnownFull
            $ \( tid
                , oldStateTxIn
                , newStateTxIn
                , ts
                , newRoot
                , txIn
                , req
                ) ->
                    let tokMap =
                            Map.singleton
                                tid
                                (oldStateTxIn, ts)
                        reqMap = Map.singleton txIn req
                        result =
                            inversesOf
                                (`Map.lookup` tokMap)
                                (`Map.lookup` reqMap)
                                ( CageUpdate
                                    tid
                                    newStateTxIn
                                    newRoot
                                    [txIn]
                                )
                    in  result
                            `shouldBe` [ InvRestoreRoot
                                            tid
                                            oldStateTxIn
                                            (root ts)
                                       , InvRestoreRequest
                                            txIn
                                            req
                                       ]

        it
            "update with unknown token and no requests produces empty"
            $ forAll genUpdateUnknown
            $ \(tid, newStateTxIn, newRoot, consumed) ->
                inversesOf
                    noTokens
                    noReqs
                    ( CageUpdate
                        tid
                        newStateTxIn
                        newRoot
                        consumed
                    )
                    `shouldBe` []

-- Helpers

noTokens :: TokenId -> Maybe (TxIn, TokenState)
noTokens = const Nothing

noReqs :: TxIn -> Maybe Request
noReqs = const Nothing

-- Generators

genBoot :: Gen (TokenId, TxIn, TokenState)
genBoot =
    (,,) <$> genTokenId <*> genTxIn <*> genTokenState

genReqEvent :: Gen (TxIn, Request)
genReqEvent = do
    tid <- genTokenId
    txIn <- genTxIn
    req <- genRequest tid
    pure (txIn, req)

genBurnKnown :: Gen (TokenId, TxIn, TokenState)
genBurnKnown =
    (,,) <$> genTokenId <*> genTxIn <*> genTokenState

genRetractKnown :: Gen (TxIn, Request)
genRetractKnown = do
    tid <- genTokenId
    txIn <- genTxIn
    req <- genRequest tid
    pure (txIn, req)

genUpdateKnownFull
    :: Gen
        ( TokenId
        , TxIn
        , TxIn
        , TokenState
        , Root
        , TxIn
        , Request
        )
genUpdateKnownFull = do
    tid <- genTokenId
    oldStateTxIn <- genTxIn
    newStateTxIn <- genTxIn
    ts <- genTokenState
    newRoot <- genRoot
    txIn <- genTxIn
    req <- genRequest tid
    pure
        ( tid
        , oldStateTxIn
        , newStateTxIn
        , ts
        , newRoot
        , txIn
        , req
        )

genUpdateUnknown :: Gen (TokenId, TxIn, Root, [TxIn])
genUpdateUnknown =
    (,,,)
        <$> genTokenId
        <*> genTxIn
        <*> genRoot
        <*> listOf1 genTxIn
