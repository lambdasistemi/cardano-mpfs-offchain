{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Indexer.FollowerSpec
-- Description : Tests for cage block processor
-- License     : Apache-2.0
--
-- Tests for 'applyCageEvent', 'computeInverse',
-- 'applyCageInverses', 'applyCageBlockEvents'
-- (pipeline), and 'detectCageEvents' / 'detectFromTx'
-- (event detection from real transactions).
module Cardano.MPFS.Indexer.FollowerSpec
    ( spec
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Lens.Micro ((&), (.~))

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldReturn
    , shouldSatisfy
    )
import Test.QuickCheck
    ( forAll
    , property
    , (==>)
    )

import Cardano.Crypto.Hash (hashToBytes)
import Cardano.Ledger.Api.Tx.Out
    ( datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes (Inject (..))
import Cardano.Ledger.Keys (KeyHash (..))
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , ConwayEra
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    , TxOut
    )
import Cardano.MPFS.Generators
    ( genRequest
    , genRoot
    , genTokenId
    , genTokenState
    , genTxIn
    , genValidEventSequence
    )
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp (..)
    , detectCageEvents
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageBlockEvents
    , applyCageEvent
    , applyCageInverses
    , computeInverse
    )
import Cardano.MPFS.Indexer.TxFixtures
    ( mkBootRequestTx
    , mkBootTx
    , mkBurnTx
    , mkPlainTx
    , mkRequestTx
    , mkRetractTx
    , mkUpdateTx
    , testCageAddr
    , testPolicyId
    , testScriptHash
    , wrongScriptHash
    )
import Cardano.MPFS.Mock.State (mkMockState)
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Trie ()
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( mkInlineDatum
    , toPlcData
    )

spec :: Spec
spec = describe "CageFollower" $ do
    unitTests
    pipelineTests
    detectionTests

-- ---------------------------------------------------------
-- Existing unit tests
-- ---------------------------------------------------------

unitTests :: Spec
unitTests = do
    describe "applyCageEvent" $ do
        it "boot inserts token and creates trie"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                getToken (tokens st) tid
                    `shouldReturn` Just ts

        it "request inserts into requests"
            $ property
            $ forAll
                ( do
                    tid <- genTokenId
                    txIn <- genTxIn
                    req <- genRequest tid
                    pure (txIn, req)
                )
            $ \(txIn, req) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageRequest txIn req)
                getRequest (requests st) txIn
                    `shouldReturn` Just req

        it "retract removes request"
            $ property
            $ forAll
                ( do
                    tid <- genTokenId
                    txIn <- genTxIn
                    req <- genRequest tid
                    pure (txIn, req)
                )
            $ \(txIn, req) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageRequest txIn req)
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageRetract txIn)
                getRequest (requests st) txIn
                    `shouldReturn` Nothing

        it "burn removes token and hides trie"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBurn tid)
                getToken (tokens st) tid
                    `shouldReturn` Nothing

        it "update changes token root"
            $ property
            $ forAll
                ( (,,)
                    <$> genTokenId
                    <*> genTokenState
                    <*> genRoot
                )
            $ \(tid, ts, newRoot) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                _ <-
                    applyCageEvent
                        st
                        tm
                        ( CageUpdate
                            tid
                            newRoot
                            []
                        )
                mTs <- getToken (tokens st) tid
                fmap root mTs
                    `shouldBe` Just newRoot

    describe "computeInverse" $ do
        it "boot inverse is InvRemoveToken"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                inv <-
                    computeInverse
                        st
                        (CageBoot tid ts)
                inv `shouldBe` [InvRemoveToken tid]

        it
            "update inverse includes InvRestoreRoot\
            \ when token exists"
            $ property
            $ forAll
                ( (,,)
                    <$> genTokenId
                    <*> genTokenState
                    <*> genRoot
                )
            $ \(tid, ts, newRoot) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                inv <-
                    computeInverse
                        st
                        ( CageUpdate
                            tid
                            newRoot
                            []
                        )
                inv
                    `shouldBe` [ InvRestoreRoot
                                    tid
                                    (root ts)
                               ]

        it
            "burn inverse is InvRestoreToken when\
            \ token exists"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                inv <-
                    computeInverse st (CageBurn tid)
                inv
                    `shouldBe` [InvRestoreToken tid ts]

    describe "applyCageInverses" $ do
        it
            "InvRestoreToken after burn restores\
            \ original state"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBurn tid)
                applyCageInverses
                    st
                    tm
                    [InvRestoreToken tid ts]
                getToken (tokens st) tid
                    `shouldReturn` Just ts

        it "InvRemoveToken undoes a boot"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                applyCageInverses
                    st
                    tm
                    [InvRemoveToken tid]
                getToken (tokens st) tid
                    `shouldReturn` Nothing

        it "InvRestoreRoot restores previous root"
            $ property
            $ forAll
                ( (,,)
                    <$> genTokenId
                    <*> genTokenState
                    <*> genRoot
                )
            $ \(tid, ts, newRoot) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                let origRoot = root ts
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                _ <-
                    applyCageEvent
                        st
                        tm
                        ( CageUpdate
                            tid
                            newRoot
                            []
                        )
                applyCageInverses
                    st
                    tm
                    [InvRestoreRoot tid origRoot]
                mTs <- getToken (tokens st) tid
                fmap root mTs
                    `shouldBe` Just origRoot

        it "full boot → inverse roundtrip"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                inv <-
                    computeInverse
                        st
                        (CageBoot tid ts)
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageBoot tid ts)
                applyCageInverses st tm inv
                getToken (tokens st) tid
                    `shouldReturn` Nothing

        it "full request → inverse roundtrip"
            $ property
            $ forAll
                ( do
                    tid <- genTokenId
                    txIn <- genTxIn
                    req <- genRequest tid
                    pure (txIn, req)
                )
            $ \(txIn, req) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                inv <-
                    computeInverse
                        st
                        (CageRequest txIn req)
                _ <-
                    applyCageEvent
                        st
                        tm
                        (CageRequest txIn req)
                applyCageInverses st tm inv
                getRequest (requests st) txIn
                    `shouldReturn` Nothing

    describe "extractConwayTxs" $ do
        it
            "returns empty for non-Conway blocks"
            (pure () :: IO ())

-- ---------------------------------------------------------
-- Layer 1: applyCageBlockEvents pipeline tests
-- ---------------------------------------------------------

pipelineTests :: Spec
pipelineTests =
    describe "applyCageBlockEvents (pipeline)" $ do
        it
            "inverses from valid sequence restore\
            \ empty state"
            $ property
            $ forAll genValidEventSequence
            $ \events -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                invs <-
                    applyCageBlockEvents
                        st
                        tm
                        events
                applyCageInverses
                    st
                    tm
                    (reverse invs)
                toks <- listTokens (tokens st)
                toks `shouldBe` []

        it
            "each event produces at least one\
            \ inverse op"
            $ property
            $ forAll genValidEventSequence
            $ \events -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                invs <-
                    applyCageBlockEvents
                        st
                        tm
                        events
                length invs
                    `shouldSatisfy` (>= length events)

        it
            "booted token present if not burned"
            $ property
            $ forAll
                ( (,,)
                    <$> genTokenId
                    <*> genTokenState
                    <*> genRoot
                )
            $ \(tid, ts, newRoot) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                let events =
                        [ CageBoot tid ts
                        , CageUpdate
                            tid
                            newRoot
                            []
                        ]
                _ <-
                    applyCageBlockEvents
                        st
                        tm
                        events
                mTs <- getToken (tokens st) tid
                fmap root mTs
                    `shouldBe` Just newRoot

        it "burned token is absent"
            $ property
            $ forAll
                ( (,)
                    <$> genTokenId
                    <*> genTokenState
                )
            $ \(tid, ts) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageBlockEvents
                        st
                        tm
                        [ CageBoot tid ts
                        , CageBurn tid
                        ]
                getToken (tokens st) tid
                    `shouldReturn` Nothing

        it "independent tokens don't interfere"
            $ property
            $ forAll
                ( (,,,)
                    <$> genTokenId
                    <*> genTokenState
                    <*> genTokenId
                    <*> genTokenState
                )
            $ \(tidA, tsA, tidB, tsB) ->
                tidA /= tidB ==> do
                    st <- mkMockState
                    tm <- mkPureTrieManager
                    _ <-
                        applyCageBlockEvents
                            st
                            tm
                            [ CageBoot tidA tsA
                            , CageBoot tidB tsB
                            , CageBurn tidA
                            ]
                    getToken (tokens st) tidA
                        `shouldReturn` Nothing
                    getToken (tokens st) tidB
                        `shouldReturn` Just tsB

        it "request present after boot+request"
            $ property
            $ forAll
                ( do
                    tid <- genTokenId
                    ts <- genTokenState
                    txIn <- genTxIn
                    req <- genRequest tid
                    pure (tid, ts, txIn, req)
                )
            $ \(tid, ts, txIn, req) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageBlockEvents
                        st
                        tm
                        [ CageBoot tid ts
                        , CageRequest txIn req
                        ]
                getRequest (requests st) txIn
                    `shouldReturn` Just req

        it "retracted request is absent"
            $ property
            $ forAll
                ( do
                    tid <- genTokenId
                    ts <- genTokenState
                    txIn <- genTxIn
                    req <- genRequest tid
                    pure (tid, ts, txIn, req)
                )
            $ \(tid, ts, txIn, req) -> do
                st <- mkMockState
                tm <- mkPureTrieManager
                _ <-
                    applyCageBlockEvents
                        st
                        tm
                        [ CageBoot tid ts
                        , CageRequest txIn req
                        , CageRetract txIn
                        ]
                getRequest (requests st) txIn
                    `shouldReturn` Nothing

-- ---------------------------------------------------------
-- Layer 2: detectCageEvents from real transactions
-- ---------------------------------------------------------

detectionTests :: Spec
detectionTests =
    describe
        "detectCageEvents (from transactions)"
        $ do
            it "detects CageBoot from a boot tx"
                $ property
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, seedIn) -> do
                    let tx =
                            mkBootTx tid ts seedIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events
                        `shouldSatisfy` any
                            (isBoot tid)

            it "detects CageBurn from a burn tx"
                $ property
                $ forAll
                    ((,) <$> genTokenId <*> genTxIn)
                $ \(tid, dummyIn) -> do
                    let tx = mkBurnTx tid dummyIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events
                        `shouldBe` [CageBurn tid]

            it
                "detects CageRequest from a\
                \ request tx"
                $ property
                $ forAll
                    ( do
                        tid <- genTokenId
                        txIn <- genTxIn
                        req <- genRequest tid
                        pure (txIn, req)
                    )
                $ \(txIn, req) -> do
                    let tx = mkRequestTx req txIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events
                        `shouldSatisfy` any
                            isRequestEvt

            it
                "detects CageRetract from a\
                \ retract tx"
                $ property
                $ forAll
                    ((,) <$> genTxIn <*> genTxIn)
                $ \(reqIn, extraIn) ->
                    reqIn /= extraIn ==> do
                        let tx =
                                mkRetractTx
                                    reqIn
                                    extraIn
                            reqOut =
                                mkDummyRequestOut
                            resolved =
                                [(reqIn, reqOut)]
                            events =
                                detectCageEvents
                                    testScriptHash
                                    resolved
                                    tx
                        events
                            `shouldBe` [CageRetract reqIn]

            it
                "detects CageUpdate from an\
                \ update tx"
                $ property
                $ forAll
                    ( (,,,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genRoot
                        <*> genTxIn
                        <*> genTxIn
                    )
                $ \(tid, ts, newRoot, stIn, reqIn) ->
                    stIn /= reqIn ==> do
                        let tx =
                                mkUpdateTx
                                    tid
                                    ts
                                    newRoot
                                    [reqIn]
                                    stIn
                            stOut =
                                mkStateOut tid ts
                            reqOut =
                                mkRequestOutFor
                                    tid
                            resolved =
                                [ (stIn, stOut)
                                , (reqIn, reqOut)
                                ]
                            events =
                                detectCageEvents
                                    testScriptHash
                                    resolved
                                    tx
                        events
                            `shouldSatisfy` any
                                (isUpdate tid)

            -- Edge cases and field extraction

            it
                "returns [] for a tx with no\
                \ cage content"
                $ property
                $ forAll genTxIn
                $ \dummyIn -> do
                    let tx = mkPlainTx dummyIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events `shouldBe` []

            it
                "returns [] when ScriptHash\
                \ does not match"
                $ property
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, seedIn) -> do
                    let tx =
                            mkBootTx tid ts seedIn
                        events =
                            detectCageEvents
                                wrongScriptHash
                                []
                                tx
                    events `shouldBe` []

            it
                "boot roundtrips TokenId and\
                \ TokenState fields"
                $ property
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, seedIn) -> do
                    let tx =
                            mkBootTx tid ts seedIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events
                        `shouldBe` [CageBoot tid ts]

            it
                "request roundtrips all Request\
                \ fields"
                $ property
                $ forAll
                    ( do
                        tid <- genTokenId
                        txIn <- genTxIn
                        req <- genRequest tid
                        pure (txIn, req)
                    )
                $ \(txIn, req) -> do
                    let tx = mkRequestTx req txIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    length events
                        `shouldBe` 1
                    case events of
                        [CageRequest _ req'] ->
                            req' `shouldBe` req
                        other ->
                            fail
                                $ "expected [CageRequest\
                                  \ ...], got "
                                    ++ show other

            it
                "detects multiple events in\
                \ one tx (boot + request)"
                $ property
                $ forAll
                    ( do
                        tid <- genTokenId
                        ts <- genTokenState
                        req <- genRequest tid
                        seedIn <- genTxIn
                        pure (tid, ts, req, seedIn)
                    )
                $ \(tid, ts, req, seedIn) -> do
                    let tx =
                            mkBootRequestTx
                                tid
                                ts
                                req
                                seedIn
                        events =
                            detectCageEvents
                                testScriptHash
                                []
                                tx
                    events
                        `shouldSatisfy` any
                            (isBoot tid)
                    events
                        `shouldSatisfy` any
                            isRequestEvt
                    length events
                        `shouldSatisfy` (>= 2)

-- ---------------------------------------------------------
-- Event classifiers
-- ---------------------------------------------------------

isBoot :: TokenId -> CageEvent -> Bool
isBoot tid (CageBoot tid' _) = tid == tid'
isBoot _ _ = False

isRequestEvt :: CageEvent -> Bool
isRequestEvt (CageRequest _ _) = True
isRequestEvt _ = False

isUpdate :: TokenId -> CageEvent -> Bool
isUpdate tid (CageUpdate tid' _ _) = tid == tid'
isUpdate _ _ = False

-- ---------------------------------------------------------
-- TxOut builders for resolved inputs
-- ---------------------------------------------------------

-- | Dummy 28-byte owner bytes.
dummyOwnerBytes :: ByteString
dummyOwnerBytes = BS.replicate 28 0

-- | Minimal request TxOut for retract tests.
mkDummyRequestOut :: TxOut ConwayEra
mkDummyRequestOut =
    mkBasicTxOut
        testCageAddr
        (inject (Coin 2_000_000))
        & datumTxOutL
            .~ mkInlineDatum
                (toPlcData dummyReqDatum)
  where
    dummyReqDatum =
        RequestDatum
            OnChainRequest
                { requestToken =
                    OnChainTokenId
                        (BuiltinByteString "tok")
                , requestOwner =
                    BuiltinByteString
                        dummyOwnerBytes
                , requestKey = "key"
                , requestValue = OpInsert "val"
                , requestFee = 0
                , requestSubmittedAt = 0
                }

-- | Request TxOut for a specific token.
mkRequestOutFor :: TokenId -> TxOut ConwayEra
mkRequestOutFor (TokenId (AssetName sbs)) =
    mkBasicTxOut
        testCageAddr
        (inject (Coin 2_000_000))
        & datumTxOutL
            .~ mkInlineDatum
                (toPlcData reqDatum)
  where
    reqDatum =
        RequestDatum
            OnChainRequest
                { requestToken =
                    OnChainTokenId
                        $ BuiltinByteString
                        $ SBS.fromShort sbs
                , requestOwner =
                    BuiltinByteString
                        dummyOwnerBytes
                , requestKey = "key"
                , requestValue = OpInsert "val"
                , requestFee = 0
                , requestSubmittedAt = 0
                }

-- | State TxOut for a specific token.
mkStateOut
    :: TokenId -> TokenState -> TxOut ConwayEra
mkStateOut tid ts =
    mkBasicTxOut testCageAddr outValue
        & datumTxOutL
            .~ mkInlineDatum
                (toPlcData stateDatum)
  where
    assetName = unTokenId tid
    tokenMA =
        MultiAsset
            $ Map.singleton testPolicyId
            $ Map.singleton assetName 1
    outValue =
        MaryValue (Coin 2_000_000) tokenMA
    KeyHash ownerH = owner ts
    stateDatum =
        StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString
                        (hashToBytes ownerH)
                , stateRoot =
                    OnChainRoot
                        (unRoot (root ts))
                , stateMaxFee =
                    let Coin c = maxFee ts in c
                , stateProcessTime =
                    processTime ts
                , stateRetractTime =
                    retractTime ts
                }
