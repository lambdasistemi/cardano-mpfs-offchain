-- |
-- Module      : Cardano.MPFS.Client.VerifySpec
-- Description : Positive and negative cryptographic replay
--               corpus.
--
-- Exercises every 'Cardano.MPFS.Client.Verify' verifier on
-- honest fixtures built with the pure CSMT / MPF backends
-- ('Cardano.MPFS.Client.Fixtures') — each scenario must
-- 'shouldAccept' — and on forged variants that flip exactly
-- one field so the rejection surfaces at the expected dotted
-- field path and reason. Every scenario reads as tutorial
-- prose: endpoint → expected outcome → which forgery (if any).
module Cardano.MPFS.Client.VerifySpec (spec) where

import Data.ByteString qualified as BS
import Test.Hspec (Spec, describe, it)

import Cardano.MPFS.Client
    ( BootProof (..)
    , BootTxResponse (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , VerificationSnapshot (..)
    , csmtReplayFailedAt
    , dropTrieFactToExclusion
    , flipByteInHex
    , forgeTrieFactValue
    , forgeWitnessedUtxoProof
    , forgeWitnessedUtxoTxOut
    , mpfReplayFailedAt
    , promoteTrieFactToInclusion
    , shouldAccept
    , shouldRejectWith
    , swapHexTo
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
    , toHex
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

    describe "CSMT forgery corpus" $ do
        it "rejects a boot funding utxo_proof with a flipped byte"
            $ forgeBootFunding honestBootResponse
                `shouldRejectWith` verifyBootTxResponse
            $ csmtReplayFailedAt
                "boot.funding[0].utxo_proof"

        it "rejects a retract state_ref rooted at a wrong root"
            $ forgeRetractRoot honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ csmtReplayFailedAt
                "retract.request_in.utxo_proof"
                `withReason` "root mismatch"

        it "rejects a retract state_ref with a tampered tx_out"
            $ forgeRetractStateTxOut honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ csmtReplayFailedAt
                "retract.state_ref.utxo_proof"
                `withReason` "value binding mismatch"

        it "rejects an update with a tampered state tx_out"
            $ forgeUpdateStateTxOut honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ csmtReplayFailedAt
                "update.state.utxo_proof"
                `withReason` "value binding mismatch"

    describe "MPF forgery corpus" $ do
        it "rejects an update trie_read value flipped by one byte"
            $ forgeUpdateTrieValue honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"

        it "rejects an inclusion proof under an absence claim"
            $ forgeUpdateTrieDrop honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"
                `withReason` "root mismatch"

        it "rejects an exclusion proof under an inclusion claim"
            $ forgeUpdateTriePromote
                honestUpdateResponseMixedTrie
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[1].mpf_proof"
                `withReason` "root mismatch"

        it "rejects an update rooted at a wrong trie_root"
            $ forgeUpdateTrieRoot honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ mpfReplayFailedAt
                "update.trie_read[0].mpf_proof"
                `withReason` "root mismatch"

-- ---------------------------------------------------------------
-- One-field forgeries on the shared honest fixtures
-- ---------------------------------------------------------------

forgeBootFunding :: BootTxResponse -> BootTxResponse
forgeBootFunding (BootTxResponse tx sn (BootProof fs)) =
    BootTxResponse
        tx
        sn
        (BootProof (forgeList forgeWitnessedUtxoProof fs))

forgeRetractRoot :: RetractTxResponse -> RetractTxResponse
forgeRetractRoot (RetractTxResponse tx sn p) =
    RetractTxResponse tx (swapSnapRoot sn) p

forgeRetractStateTxOut
    :: RetractTxResponse -> RetractTxResponse
forgeRetractStateTxOut (RetractTxResponse tx sn (RetractProof ri sr fs)) =
    RetractTxResponse
        tx
        sn
        (RetractProof ri (forgeWitnessedUtxoTxOut sr) fs)

forgeUpdateStateTxOut :: UpdateTxResponse -> UpdateTxResponse
forgeUpdateStateTxOut
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            ( UpdateProof
                (forgeWitnessedUtxoTxOut st)
                rs
                fs
                tr
                tread
            )

forgeUpdateTrieValue :: UpdateTxResponse -> UpdateTxResponse
forgeUpdateTrieValue
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            ( UpdateProof
                st
                rs
                fs
                tr
                (forgeList forgeTrieFactValue tread)
            )

forgeUpdateTrieDrop :: UpdateTxResponse -> UpdateTxResponse
forgeUpdateTrieDrop
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            ( UpdateProof
                st
                rs
                fs
                tr
                (forgeList dropTrieFactToExclusion tread)
            )

-- | Promote the *second* trie_read entry (which is an
-- exclusion claim in the mixed fixture) to an inclusion claim
-- by attaching an arbitrary value. The proof is still an
-- exclusion proof, so the verifier surfaces \"exclusion proof
-- for inclusion claim\".
forgeUpdateTriePromote :: UpdateTxResponse -> UpdateTxResponse
forgeUpdateTriePromote
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            ( UpdateProof
                st
                rs
                fs
                tr
                ( zipWith
                    ( \i fact ->
                        if i == 1
                            then
                                promoteTrieFactToInclusion
                                    (toHex "forged")
                                    fact
                            else fact
                    )
                    [0 :: Int ..]
                    tread
                )
            )

forgeUpdateTrieRoot :: UpdateTxResponse -> UpdateTxResponse
forgeUpdateTrieRoot
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            (UpdateProof st rs fs (flipByteInHex tr) tread)

-- | Replace the snapshot's @utxo_root@ with a random 32-byte
-- value so every witness in the response replays against the
-- wrong root.
swapSnapRoot :: VerificationSnapshot -> VerificationSnapshot
swapSnapRoot (VerificationSnapshot _ cp) =
    VerificationSnapshot
        (swapHexTo (BS.replicate 32 0xff) (toHex mempty))
        cp

-- | Apply a forgery to the first element of a list and leave
-- the rest untouched.
forgeList :: (a -> a) -> [a] -> [a]
forgeList _ [] = []
forgeList f (x : xs) = f x : xs
