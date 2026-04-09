{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Indexer.InverseSpec
-- Description : QC properties for cage inverse operations
-- License     : Apache-2.0
--
-- Tests mirroring the Lean theorems in
-- @Phase4.Properties@ and @Phase4.Theorems@.
-- Validates 'inversesOf' correctness using
-- 'Mock.State' as the execution environment.
module Cardano.MPFS.Indexer.InverseSpec (spec) where

import Data.Maybe (isJust, isNothing)

import Test.Hspec (Spec, describe)
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (forAll)
import Test.QuickCheck.Monadic (assert, monadicIO, run)

import Cardano.MPFS.Core.Types
    ( TokenState (..)
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
import Cardano.MPFS.Mock.State
    ( mkMockState
    )
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )

-- | Apply a cage event to the state interface.
applyCageEvent :: State IO -> CageEvent -> IO ()
applyCageEvent State{..} = \case
    CageBoot tid ts ->
        putToken tokens tid ts
    CageRequest txIn req ->
        putRequest requests txIn req
    CageUpdate tid newRoot consumed -> do
        mts <- getToken tokens tid
        case mts of
            Just ts ->
                putToken
                    tokens
                    tid
                    ts{root = newRoot}
            Nothing -> pure ()
        mapM_ (removeRequest requests) consumed
    CageReject _tid consumed ->
        mapM_ (removeRequest requests) consumed
    CageRetract txIn ->
        removeRequest requests txIn
    CageBurn tid ->
        removeToken tokens tid

-- | Apply an inverse operation to the state.
applyInverseOp :: State IO -> CageInverseOp -> IO ()
applyInverseOp State{..} = \case
    InvRestoreToken tid ts ->
        putToken tokens tid ts
    InvRemoveToken tid ->
        removeToken tokens tid
    InvRestoreRequest txIn req ->
        putRequest requests txIn req
    InvRemoveRequest txIn ->
        removeRequest requests txIn
    InvRestoreRoot tid newRoot -> do
        mts <- getToken tokens tid
        case mts of
            Just ts ->
                putToken
                    tokens
                    tid
                    ts{root = newRoot}
            Nothing -> pure ()
    InvTrieInsert{} -> pure ()
    InvTrieDelete{} -> pure ()

spec :: Spec
spec = describe "Inverse operations" $ do
    -- -----------------------------------------------
    -- Lean: boot_mem_tokens
    -- -----------------------------------------------
    describe "boot_mem_tokens"
        $ prop "token is present after boot"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                monadicIO $ do
                    st <- run mkMockState
                    run
                        $ applyCageEvent
                            st
                            (CageBoot tid ts)
                    r <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (r == Just ts)

    -- -----------------------------------------------
    -- Lean: request_mem_requests
    -- -----------------------------------------------
    describe "request_mem_requests"
        $ prop "request is present after submit"
        $ forAll genTxIn
        $ \txIn ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    monadicIO $ do
                        st <- run mkMockState
                        run
                            $ applyCageEvent
                                st
                                (CageRequest txIn req)
                        r <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (r == Just req)

    -- -----------------------------------------------
    -- Lean: boot_preserves_requests
    -- -----------------------------------------------
    describe "boot_preserves_requests"
        $ prop "boot does not affect requests"
        $ forAll genTxIn
        $ \txIn ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    forAll genTokenState $ \ts ->
                        monadicIO $ do
                            st <- run mkMockState
                            run
                                $ putRequest
                                    (requests st)
                                    txIn
                                    req
                            run
                                $ applyCageEvent
                                    st
                                    (CageBoot tid ts)
                            r <-
                                run
                                    $ getRequest
                                        (requests st)
                                        txIn
                            assert (r == Just req)

    -- -----------------------------------------------
    -- Lean: retract_preserves_tokens
    -- -----------------------------------------------
    describe "retract_preserves_tokens"
        $ prop "retract does not affect tokens"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                forAll genTxIn $ \txIn ->
                    monadicIO $ do
                        st <- run mkMockState
                        run
                            $ putToken
                                (tokens st)
                                tid
                                ts
                        run
                            $ applyCageEvent
                                st
                                (CageRetract txIn)
                        r <-
                            run
                                $ getToken
                                    (tokens st)
                                    tid
                        assert (r == Just ts)

    -- -----------------------------------------------
    -- Lean: prop_inverseRoundTrip (boot)
    -- -----------------------------------------------
    describe "boot inverse round-trip"
        $ prop "boot then inverse removes token"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                monadicIO $ do
                    st <- run mkMockState
                    let invs =
                            inversesOf
                                (const Nothing)
                                (const Nothing)
                                (CageBoot tid ts)
                    run
                        $ applyCageEvent
                            st
                            (CageBoot tid ts)
                    r1 <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (isJust r1)
                    run
                        $ mapM_
                            (applyInverseOp st)
                            invs
                    r2 <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (isNothing r2)

    -- -----------------------------------------------
    -- Lean: prop_inverseRoundTrip (request)
    -- -----------------------------------------------
    describe "request inverse round-trip"
        $ prop "request then inverse removes request"
        $ forAll genTxIn
        $ \txIn ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    monadicIO $ do
                        st <- run mkMockState
                        let invs =
                                inversesOf
                                    (const Nothing)
                                    (const Nothing)
                                    ( CageRequest
                                        txIn
                                        req
                                    )
                        run
                            $ applyCageEvent
                                st
                                (CageRequest txIn req)
                        r1 <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (isJust r1)
                        run
                            $ mapM_
                                (applyInverseOp st)
                                invs
                        r2 <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (isNothing r2)

    -- -----------------------------------------------
    -- Lean: prop_inverseRoundTrip (burn)
    -- -----------------------------------------------
    describe "burn inverse round-trip"
        $ prop "burn then inverse restores token"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                monadicIO $ do
                    st <- run mkMockState
                    run
                        $ putToken
                            (tokens st)
                            tid
                            ts
                    let lookupT t =
                            if t == tid
                                then Just ts
                                else Nothing
                        invs =
                            inversesOf
                                lookupT
                                (const Nothing)
                                (CageBurn tid)
                    run
                        $ applyCageEvent
                            st
                            (CageBurn tid)
                    r1 <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (isNothing r1)
                    run
                        $ mapM_
                            (applyInverseOp st)
                            invs
                    r2 <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (r2 == Just ts)

    -- -----------------------------------------------
    -- Lean: prop_inverseRoundTrip (retract)
    -- Fixed bug: was producing InvRemoveRequest,
    -- now correctly produces InvRestoreRequest.
    -- -----------------------------------------------
    describe "retract inverse round-trip"
        $ prop "retract then inverse restores request"
        $ forAll genTxIn
        $ \txIn ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    monadicIO $ do
                        st <- run mkMockState
                        run
                            $ putRequest
                                (requests st)
                                txIn
                                req
                        let lookupR t =
                                if t == txIn
                                    then Just req
                                    else Nothing
                            invs =
                                inversesOf
                                    (const Nothing)
                                    lookupR
                                    (CageRetract txIn)
                        run
                            $ applyCageEvent
                                st
                                (CageRetract txIn)
                        r1 <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (isNothing r1)
                        run
                            $ mapM_
                                (applyInverseOp st)
                                invs
                        r2 <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (r2 == Just req)

    -- -----------------------------------------------
    -- Lean: prop_inverseRoundTrip (update)
    -- Fixed bug: consumed requests now correctly
    -- produce InvRestoreRequest.
    -- -----------------------------------------------
    describe "update inverse round-trip"
        $ prop
            "update then inverse restores root and requests"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                forAll genRoot $ \newRoot ->
                    forAll genTxIn $ \txIn ->
                        forAll (genRequest tid)
                            $ \req ->
                                monadicIO $ do
                                    st <-
                                        run mkMockState
                                    run
                                        $ putToken
                                            (tokens st)
                                            tid
                                            ts
                                    run
                                        $ putRequest
                                            (requests st)
                                            txIn
                                            req
                                    let lookupT t =
                                            if t == tid
                                                then Just ts
                                                else Nothing
                                        lookupR t =
                                            if t == txIn
                                                then Just req
                                                else Nothing
                                        invs =
                                            inversesOf
                                                lookupT
                                                lookupR
                                                ( CageUpdate
                                                    tid
                                                    newRoot
                                                    [txIn]
                                                )
                                    run
                                        $ applyCageEvent
                                            st
                                            ( CageUpdate
                                                tid
                                                newRoot
                                                [txIn]
                                            )
                                    run
                                        $ mapM_
                                            (applyInverseOp st)
                                            invs
                                    r1 <-
                                        run
                                            $ getToken
                                                (tokens st)
                                                tid
                                    assert
                                        ( r1
                                            == Just ts
                                        )
                                    r2 <-
                                        run
                                            $ getRequest
                                                (requests st)
                                                txIn
                                    assert
                                        ( r2
                                            == Just req
                                        )

    -- -----------------------------------------------
    -- Lean: prop_bootBurnRoundTrip
    -- -----------------------------------------------
    describe "boot/burn symmetry"
        $ prop
            "boot then burn restores empty state"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                monadicIO $ do
                    st <- run mkMockState
                    run
                        $ applyCageEvent
                            st
                            (CageBoot tid ts)
                    run
                        $ applyCageEvent
                            st
                            (CageBurn tid)
                    r <-
                        run
                            $ getToken
                                (tokens st)
                                tid
                    assert (isNothing r)

    -- -----------------------------------------------
    -- Lean: prop_requestRetractRoundTrip
    -- -----------------------------------------------
    describe "request/retract symmetry"
        $ prop
            "request then retract restores empty state"
        $ forAll genTxIn
        $ \txIn ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    monadicIO $ do
                        st <- run mkMockState
                        run
                            $ applyCageEvent
                                st
                                (CageRequest txIn req)
                        run
                            $ applyCageEvent
                                st
                                (CageRetract txIn)
                        r <-
                            run
                                $ getRequest
                                    (requests st)
                                    txIn
                        assert (isNothing r)
