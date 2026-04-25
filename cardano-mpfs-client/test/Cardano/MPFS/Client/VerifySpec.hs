-- |
-- Module      : Cardano.MPFS.Client.VerifySpec
-- Description : Positive and negative cryptographic replay
--               corpus, wired through the operational
--               free-monad forgery DSL.
--
-- Exercises every 'Cardano.MPFS.Client.Verify' verifier on
-- honest fixtures built with the pure CSMT \/ MPF backends
-- ('Cardano.MPFS.Client.Fixtures') — each scenario must
-- 'shouldAccept' — and on forged variants produced by a
-- 'CsmtForge' \/ 'TrieForge' program. Each scenario reads as
-- tutorial prose: build a program, run it with the endpoint's
-- runner, assert a matching @shouldRejectWith@.
module Cardano.MPFS.Client.VerifySpec (spec) where

import Test.Hspec (Spec, describe, it)

import Cardano.MPFS.Client
    ( BootTxResponse (..)
    , Hex (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , WitnessedUtxo (..)
    , csmtReplayFailedAt
    , dropToExclusion
    , flipProof
    , flipSnapshotRoot
    , flipTrieRoot
    , flipTrieValue
    , flipTxOut
    , mpfReplayFailedAt
    , runForgeBoot
    , runForgeEnd
    , runForgeReject
    , runForgeRequest
    , runForgeRetract
    , runForgeUpdate
    , runForgeUpdateTrie
    , shouldAccept
    , shouldRejectWith
    , txBindingFailedAt
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyUpdateTxResponse
    , withReason
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootResponse
    , honestEndResponse
    , honestRejectResponse
    , honestRequestResponse
    , honestRetractResponse
    , honestUpdateResponse
    , honestUpdateResponseEmptyTrie
    , honestUpdateResponseMixedTrie
    , txCborFromTxIns
    )

spec :: Spec
spec = do
    describe "positive path — every honest response shouldAccept" $ do
        it "boot"
            $ honestBootResponse
                `shouldAccept` verifyBootTxResponse

        it "request"
            $ honestRequestResponse
                `shouldAccept` verifyRequestTxResponse

        it "retract"
            $ honestRetractResponse
                `shouldAccept` verifyRetractTxResponse

        it "reject"
            $ honestRejectResponse
                `shouldAccept` verifyRejectTxResponse

        it "end"
            $ honestEndResponse
                `shouldAccept` verifyEndTxResponse

        it "update — single inclusion trie_read"
            $ honestUpdateResponse
                `shouldAccept` verifyUpdateTxResponse

    describe "UpdateProof trie_read edge cases" $ do
        it "accepts mixed inclusion and exclusion trie reads"
            $ honestUpdateResponseMixedTrie
                `shouldAccept` verifyUpdateTxResponse

        it "accepts an empty trie_read"
            $ honestUpdateResponseEmptyTrie
                `shouldAccept` verifyUpdateTxResponse

    describe "tx/proof binding corpus" $ do
        it "rejects a boot tx whose inputs differ from funding proofs"
            $ replaceBootTx
                (txCborFromTxIns [foreignTxIn] [])
                honestBootResponse
                `shouldRejectWith` verifyBootTxResponse
            $ txBindingFailedAt "boot.tx.inputs"

        it "rejects a retract tx missing the state reference input"
            $ retractWithoutReference honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ txBindingFailedAt "retract.tx.reference_inputs"

        it "rejects an update tx missing a request input"
            $ updateWithoutRequestInput honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ txBindingFailedAt "update.tx.inputs"

    describe "CSMT forgery corpus (free-monad DSL)" $ do
        it "rejects a boot funding utxo_proof with a flipped byte"
            $ runForgeBoot
                (flipProof "funding[0]")
                honestBootResponse
                `shouldRejectWith` verifyBootTxResponse
            $ csmtReplayFailedAt
                "boot.funding[0].utxo_proof"

        it "rejects a retract rooted at a wrong snapshot root"
            $ runForgeRetract
                flipSnapshotRoot
                honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ csmtReplayFailedAt
                "retract.request_in.utxo_proof"
                `withReason` "root mismatch"

        it "rejects a retract state_ref with a tampered tx_out"
            $ runForgeRetract
                (flipTxOut "state_ref")
                honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ csmtReplayFailedAt
                "retract.state_ref.utxo_proof"
                `withReason` "value binding mismatch"

        it "rejects an update with a tampered state tx_out"
            $ runForgeUpdate
                (flipTxOut "state")
                honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ csmtReplayFailedAt
                "update.state.utxo_proof"
                `withReason` "value binding mismatch"

        it "rejects a request funding tx_out with a flipped byte"
            $ runForgeRequest
                (flipTxOut "funding[0]")
                honestRequestResponse
                `shouldRejectWith` verifyRequestTxResponse
            $ csmtReplayFailedAt
                "request.funding[0].utxo_proof"
                `withReason` "value binding mismatch"

        it "rejects a reject state utxo_proof with a flipped byte"
            $ runForgeReject
                (flipProof "state")
                honestRejectResponse
                `shouldRejectWith` verifyRejectTxResponse
            -- proof-byte flip may produce "malformed proof CBOR"
            -- or "root mismatch" depending on the byte changed;
            -- mirror the boot test and leave the reason unconstrained.
            $ csmtReplayFailedAt
                "reject.state.utxo_proof"

        it "rejects an end state tx_out with a flipped byte"
            $ runForgeEnd
                (flipTxOut "state")
                honestEndResponse
                `shouldRejectWith` verifyEndTxResponse
            $ csmtReplayFailedAt
                "end.state.utxo_proof"
                `withReason` "value binding mismatch"

    describe "MPF forgery corpus (free-monad DSL)" $ do
        it "rejects an update trie_read value flipped by one byte"
            $ runForgeUpdateTrie
                (flipTrieValue 0)
                honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"

        it "rejects an inclusion proof under an absence claim"
            $ runForgeUpdateTrie
                (dropToExclusion 0)
                honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"
                `withReason` "root mismatch"

        it "rejects an update rooted at a wrong trie_root"
            $ runForgeUpdateTrie
                flipTrieRoot
                honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"
                `withReason` "root mismatch"

foreignTxIn :: TxIn
foreignTxIn =
    TxIn
        { txId =
            Hex
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        , txIx = 99
        }

replaceBootTx :: Hex -> BootTxResponse -> BootTxResponse
replaceBootTx tx' (BootTxResponse _ s p) =
    BootTxResponse tx' s p

retractWithoutReference :: RetractTxResponse -> RetractTxResponse
retractWithoutReference
    (RetractTxResponse _ s p@(RetractProof req _st funding)) =
        RetractTxResponse
            (txCborFromTxIns (map txIn (req : funding)) [])
            s
            p

updateWithoutRequestInput :: UpdateTxResponse -> UpdateTxResponse
updateWithoutRequestInput
    (UpdateTxResponse _ s p@(UpdateProof st _reqs funding _tr _tread)) =
        UpdateTxResponse
            (txCborFromTxIns (map txIn (st : funding)) [])
            s
            p
