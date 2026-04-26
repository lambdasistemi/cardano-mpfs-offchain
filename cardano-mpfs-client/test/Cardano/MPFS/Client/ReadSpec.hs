-- |
-- Module      : Cardano.MPFS.Client.ReadSpec
-- Description : Positive and negative replay corpus for the
--               proof-bearing read responses.
--
-- Exercises every 'Cardano.MPFS.Client.Verify' read verifier on
-- honest fixtures built with the pure CSMT \/ MPF backends
-- ('Cardano.MPFS.Client.Fixtures') — each scenario must
-- 'shouldAccept' — and on forged variants produced by the
-- existing forgery helpers. Each scenario reads as tutorial
-- prose: tweak one role, run the verifier, assert the matching
-- @shouldRejectWith@ rejection.
module Cardano.MPFS.Client.ReadSpec (spec) where

import Data.Aeson qualified as Aeson
import Test.Hspec (Spec, describe, it, shouldBe)

import Cardano.MPFS.Client
    ( FactResponse (..)
    , FactWitness (..)
    , Hex (..)
    , ProofResponse (..)
    , RequestsResponse (..)
    , TokenResponse (..)
    , TokenState (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo
    , csmtReplayFailedAt
    , flipByteInHex
    , forgeWitnessedUtxoProof
    , forgeWitnessedUtxoTxOut
    , malformedHexAt
    , mpfReplayFailedAt
    , shouldAccept
    , shouldRejectWith
    , verifyFactResponse
    , verifyProofResponse
    , verifyRequestsResponse
    , verifyTokenResponse
    , withReason
    , wrongHexLengthAt
    )
import Cardano.MPFS.Client.Fixtures
    ( honestExclusionKey
    , honestFactKey
    , honestFactResponse
    , honestProofResponseExclusion
    , honestProofResponseInclusion
    , honestRequestsResponse
    , honestRequestsResponseEmpty
    , honestTokenResponse
    )

spec :: Spec
spec = do
    describe "positive path — every honest read response shouldAccept" $ do
        it "GET /tokens/:id"
            $ honestTokenResponse
                `shouldAccept` verifyTokenResponse

        it "GET /tokens/:id/facts/:key"
            $ honestFactResponse
                `shouldAccept` verifyFactResponse honestFactKey

        it "GET /tokens/:id/proofs/:key — inclusion claim"
            $ honestProofResponseInclusion
                `shouldAccept` verifyProofResponse
                    honestFactKey
                    (Just honestFactValueFromInclusion)

        it "GET /tokens/:id/proofs/:key — exclusion claim"
            $ honestProofResponseExclusion
                `shouldAccept` verifyProofResponse
                    honestExclusionKey
                    Nothing

        it "GET /tokens/:id/requests"
            $ honestRequestsResponse
                `shouldAccept` verifyRequestsResponse

        it "GET /tokens/:id/requests — empty list"
            $ honestRequestsResponseEmpty
                `shouldAccept` verifyRequestsResponse

    describe "GET /tokens/:id forgeries" $ do
        it "rejects a tampered state_ref utxo_proof"
            $ forgeStateProof honestTokenResponse
                `shouldRejectWith` verifyTokenResponse
            $ csmtReplayFailedAt "token.state.utxo_proof"

        it "rejects a tampered state_ref tx_out"
            $ forgeStateTxOut honestTokenResponse
                `shouldRejectWith` verifyTokenResponse
            $ csmtReplayFailedAt "token.state.utxo_proof"
                `withReason` "value binding mismatch"

        it "rejects a flipped snapshot utxo_root"
            $ flipSnapshotUtxoRoot honestTokenResponse
                `shouldRejectWith` verifyTokenResponse
            $ csmtReplayFailedAt "token.state.utxo_proof"
                `withReason` "root mismatch"

        it "rejects a malformed state.root hex"
            $ withTokenStateRoot
                (Hex "not-hex")
                honestTokenResponse
                `shouldRejectWith` verifyTokenResponse
            $ malformedHexAt "token.state.state.root"

        it "rejects a wrong-length state.root hex"
            $ withTokenStateRoot
                (Hex "deadbeef")
                honestTokenResponse
                `shouldRejectWith` verifyTokenResponse
            $ wrongHexLengthAt "token.state.state.root"

    describe "GET /tokens/:id/facts/:key forgeries" $ do
        it "rejects a tampered state utxo_proof"
            $ forgeFactStateProof honestFactResponse
                `shouldRejectWith` verifyFactResponse honestFactKey
            $ csmtReplayFailedAt "fact.state.utxo_proof"

        it "rejects a flipped MPF proof"
            $ forgeFactMpfProof honestFactResponse
                `shouldRejectWith` verifyFactResponse honestFactKey
            $ mpfReplayFailedAt "fact.mpf_proof"
                `withReason` "root mismatch"

        it "rejects a flipped advertised value"
            $ forgeFactValue honestFactResponse
                `shouldRejectWith` verifyFactResponse honestFactKey
            $ mpfReplayFailedAt "fact.mpf_proof"
                `withReason` "root mismatch"

        it "rejects a queried key with a different last byte"
            $ honestFactResponse
                `shouldRejectWith` verifyFactResponse
                    (flipByteInHex honestFactKey)
            $ mpfReplayFailedAt "fact.mpf_proof"
                `withReason` "root mismatch"

        it "rejects a malformed queried key"
            $ honestFactResponse
                `shouldRejectWith` verifyFactResponse (Hex "not-hex")
            $ malformedHexAt "fact.key"

    describe "GET /tokens/:id/proofs/:key forgeries" $ do
        it "rejects a tampered state utxo_proof"
            $ forgeProofStateProof honestProofResponseInclusion
                `shouldRejectWith` verifyProofResponse
                    honestFactKey
                    (Just honestFactValueFromInclusion)
            $ csmtReplayFailedAt "proof.state.utxo_proof"

        it "rejects a flipped MPF proof on inclusion"
            $ forgeProofMpfProof honestProofResponseInclusion
                `shouldRejectWith` verifyProofResponse
                    honestFactKey
                    (Just honestFactValueFromInclusion)
            $ mpfReplayFailedAt "proof.mpf_proof"
                `withReason` "root mismatch"

        it "rejects an exclusion proof claimed as inclusion"
            $ honestProofResponseExclusion
                `shouldRejectWith` verifyProofResponse
                    honestExclusionKey
                    (Just (Hex "deadbeef"))
            $ mpfReplayFailedAt "proof.mpf_proof"

        it "rejects an inclusion proof claimed as absence"
            $ honestProofResponseInclusion
                `shouldRejectWith` verifyProofResponse
                    honestFactKey
                    Nothing
            $ mpfReplayFailedAt "proof.mpf_proof"

    describe "GET /tokens/:id/requests forgeries" $ do
        it "rejects a tampered first-request utxo_proof"
            $ forgeRequestProof 0 honestRequestsResponse
                `shouldRejectWith` verifyRequestsResponse
            $ csmtReplayFailedAt
                "requests.requests[0].utxo_proof"

        it "rejects a tampered second-request tx_out"
            $ forgeRequestTxOut 1 honestRequestsResponse
                `shouldRejectWith` verifyRequestsResponse
            $ csmtReplayFailedAt
                "requests.requests[1].utxo_proof"
                `withReason` "value binding mismatch"

    describe "JSON round-trip — read fixtures" $ do
        it "TokenResponse" $ roundTripJson honestTokenResponse
        it "FactResponse" $ roundTripJson honestFactResponse
        it "ProofResponse inclusion"
            $ roundTripJson honestProofResponseInclusion
        it "ProofResponse exclusion"
            $ roundTripJson honestProofResponseExclusion
        it "RequestsResponse" $ roundTripJson honestRequestsResponse

-- ---------------------------------------------------------------
-- Test helpers
-- ---------------------------------------------------------------

honestFactValueFromInclusion :: Hex
honestFactValueFromInclusion =
    case honestFactResponse of
        FactResponse _ v _ -> v

forgeStateProof :: TokenResponse -> TokenResponse
forgeStateProof (TokenResponse s wts) =
    TokenResponse s (mapWtsUtxo forgeWitnessedUtxoProof wts)

forgeStateTxOut :: TokenResponse -> TokenResponse
forgeStateTxOut (TokenResponse s wts) =
    TokenResponse s (mapWtsUtxo forgeWitnessedUtxoTxOut wts)

flipSnapshotUtxoRoot :: TokenResponse -> TokenResponse
flipSnapshotUtxoRoot (TokenResponse s wts) =
    TokenResponse
        s{utxoRoot = flipByteInHex (utxoRoot s)}
        wts

withTokenStateRoot :: Hex -> TokenResponse -> TokenResponse
withTokenStateRoot newRoot (TokenResponse s wts) =
    let WitnessedTokenState{utxo = u, state = ts} = wts
    in  TokenResponse
            s
            WitnessedTokenState{utxo = u, state = ts{root = newRoot}}

forgeFactStateProof :: FactResponse -> FactResponse
forgeFactStateProof (FactResponse s v fw) =
    FactResponse s v (mapFwState (mapWtsUtxo forgeWitnessedUtxoProof) fw)

forgeFactMpfProof :: FactResponse -> FactResponse
forgeFactMpfProof (FactResponse s v fw) =
    FactResponse s v (mapFwProof flipByteInHex fw)

forgeFactValue :: FactResponse -> FactResponse
forgeFactValue (FactResponse s v fw) =
    FactResponse s (flipByteInHex v) fw

forgeProofStateProof :: ProofResponse -> ProofResponse
forgeProofStateProof (ProofResponse s fw) =
    ProofResponse s (mapFwState (mapWtsUtxo forgeWitnessedUtxoProof) fw)

forgeProofMpfProof :: ProofResponse -> ProofResponse
forgeProofMpfProof (ProofResponse s fw) =
    ProofResponse s (mapFwProof flipByteInHex fw)

forgeRequestProof :: Int -> RequestsResponse -> RequestsResponse
forgeRequestProof i (RequestsResponse s rs) =
    RequestsResponse s (mapAt i (mapWrUtxo forgeWitnessedUtxoProof) rs)

forgeRequestTxOut :: Int -> RequestsResponse -> RequestsResponse
forgeRequestTxOut i (RequestsResponse s rs) =
    RequestsResponse s (mapAt i (mapWrUtxo forgeWitnessedUtxoTxOut) rs)

mapWtsUtxo
    :: (WUtxo -> WUtxo)
    -> WitnessedTokenState
    -> WitnessedTokenState
mapWtsUtxo f (WitnessedTokenState u ts) =
    WitnessedTokenState (f u) ts

mapFwState
    :: (WitnessedTokenState -> WitnessedTokenState)
    -> FactWitness
    -> FactWitness
mapFwState f (FactWitness wts pf) = FactWitness (f wts) pf

mapFwProof :: (Hex -> Hex) -> FactWitness -> FactWitness
mapFwProof f (FactWitness wts pf) = FactWitness wts (f pf)

mapWrUtxo
    :: (WUtxo -> WUtxo) -> WitnessedRequest -> WitnessedRequest
mapWrUtxo f (WitnessedRequest u req) = WitnessedRequest (f u) req

type WUtxo = WitnessedUtxo

mapAt :: Int -> (a -> a) -> [a] -> [a]
mapAt _ _ [] = []
mapAt 0 f (x : xs) = f x : xs
mapAt n f (x : xs) = x : mapAt (n - 1) f xs

roundTripJson
    :: ( Eq a
       , Show a
       , Aeson.ToJSON a
       , Aeson.FromJSON a
       )
    => a
    -> IO ()
roundTripJson a =
    Aeson.eitherDecode (Aeson.encode a) `shouldBe` Right a
