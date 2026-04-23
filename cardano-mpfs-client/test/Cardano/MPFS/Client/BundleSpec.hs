module Cardano.MPFS.Client.BundleSpec (spec) where

import Data.Aeson qualified as Aeson
import Data.Text qualified as T
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)

import Cardano.MPFS.Client
    ( BootProof (..)
    , BootTxResponse (..)
    , ChainPoint (..)
    , EndProof (..)
    , EndTxResponse (..)
    , Hex (..)
    , RejectProof (..)
    , RejectTxResponse (..)
    , RequestProof (..)
    , RequestTxResponse (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TrieFact (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , VerificationSnapshot (..)
    , VerifyError (..)
    , WitnessedUtxo (..)
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyUpdateTxResponse
    )

spec :: Spec
spec = do
    describe "BootTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = bootResponse [okUtxo]
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy funding witness at the CSMT replay"
            $ verifyBootTxResponse (bootResponse [okUtxo])
            `shouldSatisfy` isCsmtReplayFailure
                "boot.funding[0].utxo_proof"

        it "rejects a funding witness with malformed tx_id"
            $ verifyBootTxResponse (bootResponse [badTxIdUtxo])
            `shouldSatisfy` isMalformed
                "boot.funding[0].tx_in.tx_id"

        it "rejects a malformed tx CBOR"
            $ verifyBootTxResponse
                (BootTxResponse (Hex "zz") snapshot32 (BootProof []))
            `shouldBe` Left (MalformedTxCbor "boot.tx")

    describe "RequestTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = requestResponse
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy funding witness at the CSMT replay"
            $ verifyRequestTxResponse requestResponse
            `shouldSatisfy` isCsmtReplayFailure
                "request.funding[0].utxo_proof"

    describe "RetractTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = retractResponse
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy request_in witness at the CSMT replay"
            $ verifyRetractTxResponse retractResponse
            `shouldSatisfy` isCsmtReplayFailure
                "retract.request_in.utxo_proof"

        it "rejects an empty tx_out on state_ref"
            $ verifyRetractTxResponse
                ( RetractTxResponse
                    txCbor
                    snapshot32
                    (RetractProof okUtxo badTxOutUtxo [])
                )
            `shouldSatisfy` isWrongLength "retract.state_ref.tx_out"

    describe "RejectTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = rejectResponse
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy state witness at the CSMT replay"
            $ verifyRejectTxResponse rejectResponse
            `shouldSatisfy` isCsmtReplayFailure
                "reject.state.utxo_proof"

    describe "EndTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = endResponse
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy state witness at the CSMT replay"
            $ verifyEndTxResponse endResponse
            `shouldSatisfy` isCsmtReplayFailure
                "end.state.utxo_proof"

    describe "UpdateTxResponse" $ do
        it "round-trips via aeson" $ do
            let r = updateResponse
            Aeson.eitherDecode (Aeson.encode r) `shouldBe` Right r

        it "rejects a dummy state witness at the CSMT replay"
            $ verifyUpdateTxResponse updateResponse
            `shouldSatisfy` isCsmtReplayFailure
                "update.state.utxo_proof"

        it "rejects a non-32-byte trie_root"
            $ verifyUpdateTxResponse (mkUpdate (Hex shortHex) [okTrie])
            `shouldSatisfy` isWrongLength "update.trie_root"

        it "rejects an empty mpf_proof on a trie fact"
            $ verifyUpdateTxResponse
                (mkUpdate validRoot [emptyProofTrie])
            `shouldSatisfy` isWrongLength
                "update.trie_read[0].mpf_proof"

txCbor :: Hex
txCbor = Hex "deadbeef"

validRoot :: Hex
validRoot = Hex (T.replicate 64 "d")

shortHex :: T.Text
shortHex = T.replicate 10 "a"

snapshot32 :: VerificationSnapshot
snapshot32 =
    VerificationSnapshot
        { utxoRoot = Hex (T.replicate 64 "a")
        , chainpoint =
            ChainPoint
                { slot = 42
                , blockId = Hex (T.replicate 64 "b")
                }
        }

okTxIn :: TxIn
okTxIn = TxIn{txId = Hex (T.replicate 64 "c"), txIx = 0}

okUtxo :: WitnessedUtxo
okUtxo =
    WitnessedUtxo
        { txIn = okTxIn
        , txOut = Hex "aa"
        , utxoProof = Hex "bb"
        }

badTxIdUtxo :: WitnessedUtxo
badTxIdUtxo =
    WitnessedUtxo
        { txIn = TxIn{txId = Hex "zz", txIx = 0}
        , txOut = Hex "aa"
        , utxoProof = Hex "bb"
        }

badTxOutUtxo :: WitnessedUtxo
badTxOutUtxo =
    WitnessedUtxo
        { txIn = okTxIn
        , txOut = Hex ""
        , utxoProof = Hex "bb"
        }

okTrie :: TrieFact
okTrie = TrieFact (Hex "ab") (Just (Hex "cd")) (Hex "ef")

emptyProofTrie :: TrieFact
emptyProofTrie = TrieFact (Hex "ab") Nothing (Hex "")

bootResponse :: [WitnessedUtxo] -> BootTxResponse
bootResponse fs = BootTxResponse txCbor snapshot32 (BootProof fs)

requestResponse :: RequestTxResponse
requestResponse =
    RequestTxResponse txCbor snapshot32 (RequestProof [okUtxo])

retractResponse :: RetractTxResponse
retractResponse =
    RetractTxResponse
        txCbor
        snapshot32
        (RetractProof okUtxo okUtxo [okUtxo])

rejectResponse :: RejectTxResponse
rejectResponse =
    RejectTxResponse
        txCbor
        snapshot32
        (RejectProof okUtxo [okUtxo] [okUtxo])

endResponse :: EndTxResponse
endResponse =
    EndTxResponse txCbor snapshot32 (EndProof okUtxo [okUtxo])

updateResponse :: UpdateTxResponse
updateResponse = mkUpdate validRoot [okTrie]

mkUpdate :: Hex -> [TrieFact] -> UpdateTxResponse
mkUpdate tr facts =
    UpdateTxResponse
        txCbor
        snapshot32
        ( UpdateProof
            okUtxo
            [okUtxo]
            [okUtxo]
            tr
            facts
        )

isMalformed :: T.Text -> Either VerifyError () -> Bool
isMalformed fld (Left (MalformedHex f _)) = f == fld
isMalformed _ _ = False

isWrongLength :: T.Text -> Either VerifyError () -> Bool
isWrongLength fld (Left (WrongHexLength f _ _)) = f == fld
isWrongLength _ _ = False

isCsmtReplayFailure :: T.Text -> Either VerifyError () -> Bool
isCsmtReplayFailure fld (Left (CsmtReplayFailed f _)) = f == fld
isCsmtReplayFailure _ _ = False
