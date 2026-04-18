-- |
-- Module      : Cardano.MPFS.StateSpec
-- Description : Property tests for State interfaces
-- License     : Apache-2.0
module Cardano.MPFS.StateSpec (spec) where

import Data.Maybe (isNothing)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldReturn
    )
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (forAll, (==>))
import Test.QuickCheck.Monadic (assert, monadicIO, run)

import Cardano.MPFS.Core.Types
    ( LocatedRequest (..)
    , LocatedTokenState (..)
    )
import Cardano.MPFS.Generators
    ( genBlockId
    , genRequest
    , genSlotNo
    , genTokenId
    , genTokenState
    , genTxIn
    )
import Cardano.MPFS.State
    ( Checkpoints (..)
    , Requests (..)
    , Tokens (..)
    )

-- -----------------------------------------------------------------
-- Specs
-- -----------------------------------------------------------------

spec
    :: IO (Tokens IO)
    -> IO (Requests IO)
    -> IO (Checkpoints IO)
    -> Spec
spec newTokens newRequests newCheckpoints = do
    describe "Tokens" $ tokensSpec newTokens
    describe "Requests" $ requestsSpec newRequests
    describe "Checkpoints"
        $ checkpointsSpec newCheckpoints

tokensSpec :: IO (Tokens IO) -> Spec
tokensSpec newTokens = do
    prop "get on empty returns Nothing"
        $ forAll genTokenId
        $ \tid ->
            monadicIO $ do
                tok <- run newTokens
                r <- run $ getToken tok tid
                assert (isNothing r)

    prop "put/get round-trip"
        $ forAll genTokenId
        $ \tid ->
            forAll genTxIn $ \ref ->
                forAll genTokenState $ \ts ->
                    let lts = LocatedTokenState ref ts
                    in  monadicIO $ do
                            tok <- run newTokens
                            run $ putToken tok tid lts
                            r <- run $ getToken tok tid
                            assert (r == Just lts)

    prop "put/remove/get returns Nothing"
        $ forAll genTokenId
        $ \tid ->
            forAll genTxIn $ \ref ->
                forAll genTokenState $ \ts ->
                    monadicIO $ do
                        tok <- run newTokens
                        run
                            $ putToken
                                tok
                                tid
                                (LocatedTokenState ref ts)
                        run $ removeToken tok tid
                        r <- run $ getToken tok tid
                        assert (isNothing r)

    prop "put appears in listTokens"
        $ forAll genTokenId
        $ \tid ->
            forAll genTxIn $ \ref ->
                forAll genTokenState $ \ts ->
                    monadicIO $ do
                        tok <- run newTokens
                        run
                            $ putToken
                                tok
                                tid
                                (LocatedTokenState ref ts)
                        ids <- run $ listTokens tok
                        assert (tid `elem` ids)

    prop "put overwrites previous"
        $ forAll genTokenId
        $ \tid ->
            forAll genTxIn $ \ref1 ->
                forAll genTxIn $ \ref2 ->
                    forAll genTokenState $ \ts1 ->
                        forAll genTokenState $ \ts2 ->
                            let lts2 = LocatedTokenState ref2 ts2
                            in  monadicIO $ do
                                    tok <- run newTokens
                                    run
                                        $ putToken
                                            tok
                                            tid
                                            (LocatedTokenState ref1 ts1)
                                    run $ putToken tok tid lts2
                                    r <- run $ getToken tok tid
                                    assert (r == Just lts2)

    prop "remove on empty doesn't crash"
        $ forAll genTokenId
        $ \tid ->
            monadicIO $ do
                tok <- run newTokens
                run $ removeToken tok tid
                assert True

requestsSpec :: IO (Requests IO) -> Spec
requestsSpec newRequests = do
    prop "put/get round-trip"
        $ forAll genTxIn
        $ \txin ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    let lr = LocatedRequest txin req
                    in  monadicIO $ do
                            rs <- run newRequests
                            run $ putRequest rs lr
                            r <- run $ getRequest rs txin
                            assert (r == Just lr)

    prop "put/remove/get returns Nothing"
        $ forAll genTxIn
        $ \txin ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    monadicIO $ do
                        rs <- run newRequests
                        run
                            $ putRequest
                                rs
                                (LocatedRequest txin req)
                        run $ removeRequest rs txin
                        r <- run $ getRequest rs txin
                        assert (isNothing r)

    prop "requestsByToken filters correctly"
        $ forAll genTokenId
        $ \tid1 ->
            forAll genTokenId $ \tid2 ->
                tid1 /= tid2 ==>
                    forAll genTxIn $ \txin1 ->
                        forAll genTxIn $ \txin2 ->
                            txin1 /= txin2 ==>
                                forAll (genRequest tid1) $ \req1 ->
                                    forAll (genRequest tid2) $ \req2 ->
                                        let lr1 = LocatedRequest txin1 req1
                                            lr2 = LocatedRequest txin2 req2
                                        in  monadicIO $ do
                                                rs <- run newRequests
                                                run $ putRequest rs lr1
                                                run $ putRequest rs lr2
                                                r <- run $ requestsByToken rs tid1
                                                assert (r == [lr1])

    prop "requestsByToken on empty returns []"
        $ forAll genTokenId
        $ \tid ->
            monadicIO $ do
                rs <- run newRequests
                r <- run $ requestsByToken rs tid
                assert (null r)

checkpointsSpec :: IO (Checkpoints IO) -> Spec
checkpointsSpec newCheckpoints = do
    it "get on empty returns Nothing" $ do
        cp <- newCheckpoints
        getCheckpoint cp `shouldReturn` Nothing

    prop "put/get round-trip"
        $ forAll genSlotNo
        $ \s ->
            forAll genBlockId $ \b ->
                monadicIO $ do
                    cp <- run newCheckpoints
                    run $ putCheckpoint cp s b
                    r <- run $ getCheckpoint cp
                    assert (r == Just (s, b))

    prop "put overwrites previous"
        $ forAll genSlotNo
        $ \s1 ->
            forAll genBlockId $ \b1 ->
                forAll genSlotNo $ \s2 ->
                    forAll genBlockId $ \b2 ->
                        monadicIO $ do
                            cp <- run newCheckpoints
                            run
                                $ putCheckpoint
                                    cp
                                    s1
                                    b1
                            run
                                $ putCheckpoint
                                    cp
                                    s2
                                    b2
                            r <- run $ getCheckpoint cp
                            assert
                                (r == Just (s2, b2))
