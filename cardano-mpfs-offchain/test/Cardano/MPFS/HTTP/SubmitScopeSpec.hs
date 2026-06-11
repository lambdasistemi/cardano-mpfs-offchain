-- |
-- Module      : Cardano.MPFS.HTTP.SubmitScopeSpec
-- Description : Tests for the /tx/submit Level-1 scope gate
-- License     : Apache-2.0
--
-- Exercises 'txTouchesMpfs' directly on the cage-event
-- transaction fixtures. It proves the gate admits EVERY
-- real MPFS operation the system can submit — including
-- the spend-only ones (retract, sweep) that leave no cage
-- mint or output and are recognised only by their spent
-- request UTxO — while still rejecting a plain value
-- transfer whose inputs all resolve to non-cage UTxOs.
-- It also pins the conservative stale-input policy: an
-- unresolved spent input is never grounds for a
-- false-reject, and pins that recognition stays keyed to
-- the configured cage (wrong cage / wrong request address
-- are rejected).
module Cardano.MPFS.HTTP.SubmitScopeSpec
    ( spec
    ) where

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )
import Test.QuickCheck (forAll, suchThat)

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.BaseTypes (Network (Testnet))
import Cardano.Ledger.Credential
    ( Credential (ScriptHashObj)
    , StakeReference (StakeRefNull)
    )

import Cardano.MPFS.Generators
    ( genRequest
    , genRoot
    , genTokenId
    , genTokenState
    , genTxIn
    )
import Cardano.MPFS.HTTP.AtomicReadFixture (testCageConfig)
import Cardano.MPFS.HTTP.SubmitScope
    ( SpentInput (..)
    , txTouchesMpfs
    )
import Cardano.MPFS.Indexer.TxFixtures
    ( mkBootTx
    , mkBurnTx
    , mkPlainTx
    , mkRequestTxAt
    , mkRequestTxOutAt
    , mkRetractTx
    , mkStateTxOut
    , mkUpdateTx
    , mkWalletTxOut
    , wrongScriptHash
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (cfgScriptHash)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( requestAddrFromCfg
    )

-- | A cage config for a DIFFERENT cage, used to prove
-- the gate is keyed to the configured cage identity. It
-- reuses the valid request-validator bytes from
-- 'testCageConfig' but overrides the state script hash.
otherCfg :: CageConfig
otherCfg =
    testCageConfig{cfgScriptHash = wrongScriptHash}

-- | A wrong script address that is neither the cage
-- state address nor any per-token request address.
wrongScriptAddr :: Addr
wrongScriptAddr =
    Addr
        Testnet
        (ScriptHashObj wrongScriptHash)
        StakeRefNull

-- | A single resolved non-cage wallet input — the kind
-- every transaction carries to pay fees. Present in the
-- positive structural cases to prove mint\/output
-- recognition admits a tx even when its inputs are
-- non-cage.
walletSpent :: [SpentInput]
walletSpent = [ResolvedSpent mkWalletTxOut]

spec :: Spec
spec =
    describe
        "txTouchesMpfs (/tx/submit Level-1 scope gate)"
        $ do
            it "accepts a boot tx (mints the state-token policy)"
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, seed) ->
                    txTouchesMpfs
                        testCageConfig
                        walletSpent
                        (mkBootTx tid ts seed)
                        `shouldBe` True

            it "accepts an end tx (burns the state-token policy)"
                $ forAll
                    ((,) <$> genTokenId <*> genTxIn)
                $ \(tid, dummy) ->
                    txTouchesMpfs
                        testCageConfig
                        walletSpent
                        (mkBurnTx tid dummy)
                        `shouldBe` True

            it "accepts an update tx (output at the cage address)"
                $ forAll
                    ( (,,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genRoot
                        <*> genTxIn
                    )
                $ \(tid, ts, r, stateIn) ->
                    txTouchesMpfs
                        testCageConfig
                        walletSpent
                        (mkUpdateTx tid ts r [] stateIn)
                        `shouldBe` True

            it
                "accepts a request-create tx at this cage's \
                \request address"
                $ forAll genReqAndInput
                $ \(tid, req, dummy) ->
                    let reqAddr =
                            requestAddrFromCfg
                                testCageConfig
                                tid
                                Testnet
                    in  txTouchesMpfs
                            testCageConfig
                            walletSpent
                            (mkRequestTxAt reqAddr req dummy)
                            `shouldBe` True

            it
                "accepts a retract tx (spends a request UTxO; \
                \no cage mint or output)"
                $ forAll genReqAndTwoInputs
                $ \(tid, req, reqIn, extraIn) ->
                    let reqAddr =
                            requestAddrFromCfg
                                testCageConfig
                                tid
                                Testnet
                        spent =
                            [ ResolvedSpent
                                (mkRequestTxOutAt reqAddr req)
                            , ResolvedSpent mkWalletTxOut
                            ]
                    in  txTouchesMpfs
                            testCageConfig
                            spent
                            (mkRetractTx reqIn extraIn)
                            `shouldBe` True

            it
                "accepts a sweep tx (spends a request UTxO; \
                \no cage mint or output)"
                $ forAll genReqAndTwoInputs
                $ \(tid, req, reqIn, extraIn) ->
                    let reqAddr =
                            requestAddrFromCfg
                                testCageConfig
                                tid
                                Testnet
                        spent =
                            [ ResolvedSpent mkWalletTxOut
                            , ResolvedSpent
                                (mkRequestTxOutAt reqAddr req)
                            ]
                    in  -- Sweep is structurally a request-UTxO
                        -- spend like retract; the gate ignores
                        -- the redeemer, so 'mkRetractTx' models
                        -- both spend-only shapes.
                        txTouchesMpfs
                            testCageConfig
                            spent
                            (mkRetractTx reqIn extraIn)
                            `shouldBe` True

            it
                "accepts a spend-only op that spends the cage \
                \state UTxO (no cage output)"
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, dummy) ->
                    let spent =
                            [ ResolvedSpent (mkStateTxOut tid ts)
                            , ResolvedSpent mkWalletTxOut
                            ]
                    in  txTouchesMpfs
                            testCageConfig
                            spent
                            (mkPlainTx dummy)
                            `shouldBe` True

            it
                "admits a tx with an unresolved spent input \
                \(conservative: never false-reject on a \
                \lagging view)"
                $ forAll genTxIn
                $ \dummy ->
                    txTouchesMpfs
                        testCageConfig
                        [UnresolvedSpent]
                        (mkPlainTx dummy)
                        `shouldBe` True

            it
                "rejects a plain value-transfer tx (inputs \
                \resolve to non-cage UTxOs)"
                $ forAll genTxIn
                $ \dummy ->
                    txTouchesMpfs
                        testCageConfig
                        walletSpent
                        (mkPlainTx dummy)
                        `shouldBe` False

            it "rejects a boot tx keyed to a different cage"
                $ forAll
                    ( (,,)
                        <$> genTokenId
                        <*> genTokenState
                        <*> genTxIn
                    )
                $ \(tid, ts, seed) ->
                    txTouchesMpfs
                        otherCfg
                        walletSpent
                        (mkBootTx tid ts seed)
                        `shouldBe` False

            it
                "rejects a RequestDatum at a wrong script \
                \address"
                $ forAll genReqAndInput
                $ \(_tid, req, dummy) ->
                    txTouchesMpfs
                        testCageConfig
                        walletSpent
                        ( mkRequestTxAt
                            wrongScriptAddr
                            req
                            dummy
                        )
                        `shouldBe` False

            it
                "rejects a spent RequestDatum at a wrong \
                \script address"
                $ forAll genReqAndInput
                $ \(_tid, req, dummy) ->
                    let spent =
                            [ ResolvedSpent
                                ( mkRequestTxOutAt
                                    wrongScriptAddr
                                    req
                                )
                            , ResolvedSpent mkWalletTxOut
                            ]
                    in  txTouchesMpfs
                            testCageConfig
                            spent
                            (mkPlainTx dummy)
                            `shouldBe` False

            it
                "rejects a RequestDatum at another token's \
                \request address"
                $ forAll genReqAndOtherToken
                $ \(req, other, dummy) ->
                    let wrongReqAddr =
                            requestAddrFromCfg
                                testCageConfig
                                other
                                Testnet
                    in  txTouchesMpfs
                            testCageConfig
                            walletSpent
                            ( mkRequestTxAt
                                wrongReqAddr
                                req
                                dummy
                            )
                            `shouldBe` False
  where
    genReqAndInput = do
        tid <- genTokenId
        req <- genRequest tid
        dummy <- genTxIn
        pure (tid, req, dummy)
    genReqAndTwoInputs = do
        tid <- genTokenId
        req <- genRequest tid
        reqIn <- genTxIn
        extraIn <- genTxIn `suchThat` (/= reqIn)
        pure (tid, req, reqIn, extraIn)
    genReqAndOtherToken = do
        tid <- genTokenId
        other <- genTokenId `suchThat` (/= tid)
        req <- genRequest tid
        dummy <- genTxIn
        pure (req, other, dummy)
