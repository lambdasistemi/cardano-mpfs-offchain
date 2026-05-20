-- |
-- Module      : Cardano.MPFS.Client.RetractFactsSpec
-- Description : Unit tests for retract facts verification.
module Cardano.MPFS.Client.RetractFactsSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.Aeson
    ( Value
    , decode
    , encode
    , object
    , (.=)
    )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Either (isRight)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import CSMT.Core.CBOR
    ( renderProof
    )
import CSMT.Core.Hash
    ( byteStringToKey
    , renderHash
    )
import CSMT.Hashes
    ( hashHashing
    , mkHash
    )
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , identityFromKV
    , insertMHash
    , proofM
    )
import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RetractFacts (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRetractFacts
    , verifiedRetractFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyRetractFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( flipApiHexMidByte
    )

spec :: Spec
spec = describe "verifyRetractFacts" $ do
    it "round-trips the retract facts JSON shape" $ do
        let RetractFixture{facts} = honestRetractFixture
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (retractFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
        verifyRetractFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
        verified <- expectVerified trustedRoot facts
        verifiedRetractFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
            forged =
                facts
                    { rfSnapshot =
                        (rfSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyRetractFacts trustedRoot forged
            `shouldBe` Left
                ( WrongHexLength
                    "retract.snapshot.utxo_root"
                    32
                    1
                )

    it "rejects a trusted-root mismatch" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyRetractFacts forged facts
            `shouldBe` Left
                ( TrustedRootMismatch
                    "retract.snapshot.utxo_root"
                )

    it "rejects a tampered request inclusion proof" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
            forgedRequest =
                (rfRequestUtxo facts)
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rfRequestUtxo = forgedRequest}
        verifyRetractFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "retract.request_utxo.inclusion_proof"
                    "malformed proof CBOR"
                )

    it "rejects a tampered state inclusion proof" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
            forgedState =
                (rfStateUtxo facts)
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rfStateUtxo = forgedState}
        verifyRetractFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "retract.state_utxo.inclusion_proof"
                    "malformed proof CBOR"
                )

    it "rejects a tampered wallet inclusion proof" $ do
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture
            entry = soleWalletUtxo facts
            forgedEntry =
                entry
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rfWalletUtxos = [forgedEntry]}
        verifyRetractFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "retract.wallet_utxos[0].inclusion_proof"
                    "malformed proof CBOR"
                )

data RetractFixture = RetractFixture
    { trustedRoot :: TrustedRoot
    , facts :: RetractFacts
    }

honestRetractFixture :: RetractFixture
honestRetractFixture =
    let (root, requestEntry, stateEntry, walletEntry) =
            csmtThreeRow
                requestTxId
                requestTxOut
                stateTxId
                stateTxOut
                walletTxId
                walletTxOut
        retract =
            RetractFacts
                { rfSnapshot = snapshotWithRoot root
                , rfToken = sampleToken
                , rfRequestUtxo = requestEntry
                , rfStateUtxo = stateEntry
                , rfWalletUtxos = [walletEntry]
                , rfValidityStartSlot = 100
                , rfValidityEndSlot = 200
                , rfProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  RetractFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = retract
            }

csmtThreeRow
    :: ByteString
    -> ByteString
    -> ByteString
    -> ByteString
    -> ByteString
    -> ByteString
    -> (ByteString, UtxoEntry, UtxoEntry, UtxoEntry)
csmtThreeRow
    reqIdBytes
    reqOutBytes
    stIdBytes
    stOutBytes
    walIdBytes
    walOutBytes =
        evalPureFromEmptyDB $ do
            let reqKey =
                    byteStringToKey
                        (encodeTxIn reqIdBytes 0)
                stKey =
                    byteStringToKey
                        (encodeTxIn stIdBytes 0)
                walKey =
                    byteStringToKey
                        (encodeTxIn walIdBytes 0)
            insertMHash reqKey (mkHash reqOutBytes)
            insertMHash stKey (mkHash stOutBytes)
            insertMHash walKey (mkHash walOutBytes)
            reqProof <- proofBytesM reqKey
            stProof <- proofBytesM stKey
            walProof <- proofBytesM walKey
            root <-
                maybe BS.empty renderHash
                    <$> getRootHashM
            pure
                ( root
                , mkEntry reqIdBytes reqOutBytes reqProof
                , mkEntry stIdBytes stOutBytes stProof
                , mkEntry walIdBytes walOutBytes walProof
                )
      where
        proofBytesM key = do
            mProof <-
                proofM
                    hashCodecs
                    identityFromKV
                    hashHashing
                    key
            pure $ case mProof of
                Just (_, proof) -> renderProof proof
                Nothing -> BS.empty
        mkEntry idBytes outBytes proofBytes =
            UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex idBytes
                        , urTxIx = 0
                        }
                , ueTxOutCbor = Hex outBytes
                , ueInclusionProof = Hex proofBytes
                }

expectVerified
    :: TrustedRoot
    -> RetractFacts
    -> IO VerifiedRetractFacts
expectVerified trusted facts =
    case verifyRetractFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRetractFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

soleWalletUtxo :: RetractFacts -> UtxoEntry
soleWalletUtxo RetractFacts{rfWalletUtxos = [entry]} = entry
soleWalletUtxo RetractFacts{rfWalletUtxos = entries} =
    error
        ( "RetractFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )

retractFactsJson :: RetractFacts -> Value
retractFactsJson RetractFacts{..} =
    object
        [ "snapshot" .= rfSnapshot
        , "token" .= rfToken
        , "request_utxo" .= rfRequestUtxo
        , "state_utxo" .= rfStateUtxo
        , "wallet_utxos" .= rfWalletUtxos
        , "validity_start_slot" .= rfValidityStartSlot
        , "validity_end_slot" .= rfValidityEndSlot
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified rfProtocolParameters
                , "cbor" .= uppCbor rfProtocolParameters
                ]
        ]

snapshotWithRoot :: ByteString -> VerificationSnapshot
snapshotWithRoot root =
    VerificationSnapshot
        { vsUtxoRoot = Hex root
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 0
                , cpBlockId = Hex (BS.replicate 32 0)
                }
        }

encodeTxIn :: ByteString -> Word -> ByteString
encodeTxIn txIdBytes txIx =
    CBOR.toStrictByteString
        $ mconcat
            [ CBOR.encodeListLen 2
            , CBOR.encodeBytes txIdBytes
            , CBOR.encodeWord64 (fromIntegral txIx)
            ]

requestTxId, requestTxOut :: ByteString
requestTxId = BS.replicate 32 0xA1
requestTxOut = "retract-request-txout"

stateTxId, stateTxOut :: ByteString
stateTxId = BS.replicate 32 0xB2
stateTxOut = "retract-state-txout"

walletTxId, walletTxOut :: ByteString
walletTxId = BS.replicate 32 0xC3
walletTxOut = "retract-wallet-txout"

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)
