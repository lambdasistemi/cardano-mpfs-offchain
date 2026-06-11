-- |
-- Module      : Cardano.MPFS.HTTP.SubmitScopeSpec
-- Description : Tests for the /tx/submit Level-1 scope gate
-- License     : Apache-2.0
--
-- Exercises 'txTouchesMpfs' directly on the cage-event
-- transaction fixtures: every MPFS op shape (boot, end,
-- update, request) is admitted, a plain value transfer
-- is rejected, recognition is keyed to the configured
-- cage, and a request output is bound to this cage's
-- per-token request validator address — a crafted
-- 'RequestDatum' at any other script address is rejected.
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
import Cardano.MPFS.HTTP.SubmitScope (txTouchesMpfs)
import Cardano.MPFS.Indexer.TxFixtures
    ( mkBootTx
    , mkBurnTx
    , mkPlainTx
    , mkRequestTxAt
    , mkUpdateTx
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
                        (mkBootTx tid ts seed)
                        `shouldBe` True

            it "accepts an end tx (burns the state-token policy)"
                $ forAll
                    ((,) <$> genTokenId <*> genTxIn)
                $ \(tid, dummy) ->
                    txTouchesMpfs
                        testCageConfig
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
                            (mkRequestTxAt reqAddr req dummy)
                            `shouldBe` True

            it "rejects a plain value-transfer tx (no MPFS surface)"
                $ forAll genTxIn
                $ \dummy ->
                    txTouchesMpfs
                        testCageConfig
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
                        (mkBootTx tid ts seed)
                        `shouldBe` False

            it
                "rejects a RequestDatum at a wrong script \
                \address"
                $ forAll genReqAndInput
                $ \(_tid, req, dummy) ->
                    txTouchesMpfs
                        testCageConfig
                        ( mkRequestTxAt
                            wrongScriptAddr
                            req
                            dummy
                        )
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
    genReqAndOtherToken = do
        tid <- genTokenId
        other <- genTokenId `suchThat` (/= tid)
        req <- genRequest tid
        dummy <- genTxIn
        pure (req, other, dummy)
