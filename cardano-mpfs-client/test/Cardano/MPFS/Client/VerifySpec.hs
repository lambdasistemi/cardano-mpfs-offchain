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

import Control.Monad (void)
import Data.Bits (xor)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text.Encoding qualified as T
import Test.Hspec (Spec, describe, it)

import Cardano.MPFS.API.Encoding qualified as Api
import Cardano.MPFS.API.Types qualified as Api
import Cardano.MPFS.Client
    ( BootProof (..)
    , BootTxResponse (..)
    , EndProof (..)
    , EndTxResponse (..)
    , FactAbsentFacts (..)
    , FactPresentFacts (..)
    , Hex (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TrieFact (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , VerifyError
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
    , trustedRootMismatchAt
    , txBindingFailedAt
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyFactAbsentFacts
    , verifyFactPresentFacts
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyUpdateTxResponse
    , withReason
    )
import Cardano.MPFS.Client.Fixtures
    ( CsmtBundle
    , TxRedeemerFixture (..)
    , buildBundleWithStateRoot
    , bundleRoot
    , bundleState
    , honestBootResponse
    , honestEndResponse
    , honestRejectResponse
    , honestRequestResponse
    , honestRetractResponse
    , honestTrieExclusion
    , honestTrieInclusion
    , honestUpdateResponse
    , honestUpdateResponseEmptyTrie
    , honestUpdateResponseMixedTrie
    , sampleStateAsset
    , spendContributeRedeemerTerm
    , spendEndRedeemerTerm
    , spendModifyRedeemerTerm
    , spendRedeemerFixture
    , txCborFromTxIns
    , txCborFromTxParts
    , txCborFromTxPartsWithRedeemers
    , txOutTerm
    , updateActionTermFromProof
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))

spec :: Spec
spec = do
    describe "Facts read verifiers" $ do
        it
            "accepts a present fact with a real state witness, value, and MPF proof"
            $ honestFactPresentFacts
                `shouldAccept` verifyPresentUnit
                    honestFactTrustedRoot

        it "accepts an absent fact with a real exclusion proof"
            $ honestFactAbsentFacts
                `shouldAccept` verifyAbsentUnit
                    honestFactTrustedRoot

        it "rejects a present fact with a tampered value"
            $ tamperPresentValue honestFactPresentFacts
                `shouldRejectWith` verifyPresentUnit
                    honestFactTrustedRoot
            $ mpfReplayFailedAt "fact_present.fact.mpf_proof"

        it "rejects a present fact with a tampered proof"
            $ tamperPresentProof honestFactPresentFacts
                `shouldRejectWith` verifyPresentUnit
                    honestFactTrustedRoot
            $ mpfReplayFailedAt "fact_present.fact.mpf_proof"

        it "rejects a present fact with a mismatched trusted root"
            $ honestFactPresentFacts
                `shouldRejectWith` verifyPresentUnit
                    (TrustedRoot (Api.Hex (BS.replicate 32 0x99)))
            $ trustedRootMismatchAt "fact_present.snapshot.utxo_root"

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

        it "rejects a boot tx without a continuing state output"
            $ bootWithoutStateOutput honestBootResponse
                `shouldRejectWith` verifyBootTxResponse
            $ txBindingFailedAt "boot.tx.state_outputs"

        it "rejects an end tx that burns the wrong state token quantity"
            $ endBurnsWrongQuantity honestEndResponse
                `shouldRejectWith` verifyEndTxResponse
            $ txBindingFailedAt "end.tx.mint"

        it "rejects an update tx without a continuing state output"
            $ updateWithoutStateOutput honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ txBindingFailedAt "update.tx.state_outputs"

        it "rejects a boot tx without its minting redeemer"
            $ bootWithoutRedeemer honestBootResponse
                `shouldRejectWith` verifyBootTxResponse
            $ txBindingFailedAt "boot.tx.redeemers"

        it "rejects a retract tx with the wrong spending redeemer"
            $ retractWithEndRedeemer honestRetractResponse
                `shouldRejectWith` verifyRetractTxResponse
            $ txBindingFailedAt "retract.tx.redeemers"

        it "rejects an update tx with mismatched MPF proof payload"
            $ updateWithMismatchedRedeemerProof honestUpdateResponse
                `shouldRejectWith` verifyUpdateTxResponse
            $ txBindingFailedAt "update.tx.redeemers"

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

honestFactTrustedRoot :: TrustedRoot
honestFactTrustedRoot =
    TrustedRoot (Api.Hex (bundleRoot factsBundle))

verifyPresentUnit
    :: TrustedRoot -> FactPresentFacts -> Either VerifyError ()
verifyPresentUnit trusted =
    void . verifyFactPresentFacts trusted

verifyAbsentUnit
    :: TrustedRoot -> FactAbsentFacts -> Either VerifyError ()
verifyAbsentUnit trusted =
    void . verifyFactAbsentFacts trusted

honestFactPresentFacts :: FactPresentFacts
honestFactPresentFacts =
    let (_, trieFact) = honestTrieInclusion
    in  FactPresentFacts
            { fpfKey = toApiHex (key trieFact)
            , fpfResponse =
                Api.FactResponse
                    { Api.frSnapshot = factSnapshot
                    , Api.frValue =
                        case value trieFact of
                            Just v -> toApiHex v
                            Nothing ->
                                error
                                    "VerifySpec: expected inclusion value"
                    , Api.frFact = factWitness trieFact
                    }
            }

honestFactAbsentFacts :: FactAbsentFacts
honestFactAbsentFacts =
    let (_, trieFact) = honestTrieExclusion
    in  FactAbsentFacts
            { fafKey = toApiHex (key trieFact)
            , fafResponse =
                Api.ProofResponse
                    { Api.prSnapshot = factSnapshot
                    , Api.prFact = factWitness trieFact
                    }
            }

-- | The single-fact verifiers decode the on-chain trie root from the
-- witnessed state @TxOut@ inline datum, so the witness must encode the
-- same root the MPF proof targets. The inclusion and exclusion
-- fixtures are built over the same entries, so they share one root.
factsTrieRoot :: BS.ByteString
factsTrieRoot = fst honestTrieInclusion

-- | A CSMT bundle whose state UTxO datum carries 'factsTrieRoot'.
factsBundle :: CsmtBundle
factsBundle = buildBundleWithStateRoot factsTrieRoot

factWitness :: TrieFact -> Api.FactWitness
factWitness trieFact =
    Api.FactWitness
        { Api.fwState =
            Api.WitnessedTokenState
                { Api.wtsUtxo =
                    toApiWitnessedUtxo (bundleState factsBundle)
                }
        , Api.fwMpfProof = toApiHex (mpfProof trieFact)
        }

factSnapshot :: Api.VerificationSnapshot
factSnapshot =
    Api.VerificationSnapshot
        { Api.vsUtxoRoot = Api.Hex (bundleRoot factsBundle)
        , Api.vsChainPoint =
            Api.ChainPointJSON
                { Api.cpSlot = 42
                , Api.cpBlockId = Api.Hex (BS.replicate 32 0x11)
                }
        }

toApiWitnessedUtxo :: WitnessedUtxo -> Api.WitnessedUtxo
toApiWitnessedUtxo
    WitnessedUtxo
        { txIn = TxIn{txId = txId', txIx = txIx'}
        , txOut
        , utxoProof
        } =
        Api.WitnessedUtxo
            { Api.wuTxIn =
                Api.TxInJSON
                    { Api.tjTxId = toApiHex txId'
                    , Api.tjTxIx = txIx'
                    }
            , Api.wuTxOut = toApiHex txOut
            , Api.wuProof = toApiHex utxoProof
            }

tamperPresentValue :: FactPresentFacts -> FactPresentFacts
tamperPresentValue facts@FactPresentFacts{fpfResponse} =
    facts
        { fpfResponse =
            fpfResponse
                { Api.frValue = flipLastByte (Api.frValue fpfResponse)
                }
        }

tamperPresentProof :: FactPresentFacts -> FactPresentFacts
tamperPresentProof facts@FactPresentFacts{fpfResponse} =
    facts
        { fpfResponse =
            fpfResponse
                { Api.frFact =
                    (Api.frFact fpfResponse)
                        { Api.fwMpfProof =
                            flipLastByte
                                (Api.fwMpfProof (Api.frFact fpfResponse))
                        }
                }
        }

flipLastByte :: Api.Hex -> Api.Hex
flipLastByte (Api.Hex bs)
    | BS.null bs = error "VerifySpec.flipLastByte: empty bytes"
    | otherwise =
        Api.Hex
            ( BS.init bs
                <> BS.singleton (BS.last bs `xor` 0x01)
            )

toApiHex :: Hex -> Api.Hex
toApiHex (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Api.Hex bs
        Left err -> error ("VerifySpec.toApiHex: " <> err)

replaceBootTx :: Hex -> BootTxResponse -> BootTxResponse
replaceBootTx tx' (BootTxResponse _ s p) =
    BootTxResponse tx' s p

bootWithoutStateOutput :: BootTxResponse -> BootTxResponse
bootWithoutStateOutput (BootTxResponse _ s p@(BootProof funding)) =
    BootTxResponse
        ( txCborFromTxParts
            (map txIn funding)
            []
            [sampleStateAsset 1]
            []
        )
        s
        p

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

endBurnsWrongQuantity :: EndTxResponse -> EndTxResponse
endBurnsWrongQuantity (EndTxResponse _ s p@(EndProof st funding)) =
    EndTxResponse
        ( txCborFromTxParts
            (map txIn (st : funding))
            []
            [sampleStateAsset (-2)]
            []
        )
        s
        p

updateWithoutStateOutput :: UpdateTxResponse -> UpdateTxResponse
updateWithoutStateOutput
    (UpdateTxResponse _ s p@(UpdateProof st reqs funding _tr _tread)) =
        UpdateTxResponse
            ( txCborFromTxParts
                (map txIn (st : reqs <> funding))
                []
                []
                []
            )
            s
            p

bootWithoutRedeemer :: BootTxResponse -> BootTxResponse
bootWithoutRedeemer (BootTxResponse _ s p@(BootProof funding)) =
    BootTxResponse
        ( txCborFromTxParts
            (map txIn funding)
            []
            [sampleStateAsset 1]
            [txOutTerm True [sampleStateAsset 1]]
        )
        s
        p

retractWithEndRedeemer :: RetractTxResponse -> RetractTxResponse
retractWithEndRedeemer
    (RetractTxResponse _ s p@(RetractProof req st funding)) =
        RetractTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn (req : funding))
                [txIn st]
                []
                [txOutTerm False []]
                [spendRedeemerFixture 0 spendEndRedeemerTerm]
            )
            s
            p

updateWithMismatchedRedeemerProof
    :: UpdateTxResponse -> UpdateTxResponse
updateWithMismatchedRedeemerProof
    (UpdateTxResponse _ s p@(UpdateProof st reqs funding _tr _tread)) =
        let (_, exclusionFact) = honestTrieExclusion
        in  UpdateTxResponse
                ( txCborFromTxPartsWithRedeemers
                    (map txIn (st : reqs <> funding))
                    []
                    []
                    [txOutTerm True [sampleStateAsset 1]]
                    ( updateRedeemers
                        (txIn st)
                        (length reqs)
                        exclusionFact
                    )
                )
                s
                p

updateRedeemers :: TxIn -> Int -> TrieFact -> [TxRedeemerFixture]
updateRedeemers stateIn requestCount trieFact =
    spendRedeemerFixture
        0
        (spendModifyRedeemerTerm [updateActionTermFromProof trieFact])
        : [ spendRedeemerFixture
                (fromIntegral ix)
                (spendContributeRedeemerTerm stateIn)
          | ix <- [1 .. requestCount]
          ]
