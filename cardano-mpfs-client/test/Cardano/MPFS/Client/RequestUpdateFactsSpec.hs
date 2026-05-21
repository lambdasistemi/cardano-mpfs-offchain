-- |
-- Module      : Cardano.MPFS.Client.RequestUpdateFactsSpec
-- Description : Unit tests for request-update facts verification.
module Cardano.MPFS.Client.RequestUpdateFactsSpec
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
    ( RequestUpdateFacts (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRequestUpdateFacts
    , verifiedRequestUpdateFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyRequestUpdateFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( flipApiHexMidByte
    )

spec :: Spec
spec = describe "verifyRequestUpdateFacts" $ do
    it "round-trips the request-update facts JSON shape" $ do
        let RequestUpdateFixture{facts} = honestRequestUpdateFixture
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (requestUpdateFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        let RequestUpdateFixture{trustedRoot, facts} =
                honestRequestUpdateFixture
        verifyRequestUpdateFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let RequestUpdateFixture{trustedRoot, facts} =
                honestRequestUpdateFixture
        verified <- expectVerified trustedRoot facts
        verifiedRequestUpdateFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let RequestUpdateFixture{trustedRoot, facts} =
                honestRequestUpdateFixture
            forged =
                facts
                    { rufSnapshot =
                        (rufSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyRequestUpdateFacts trustedRoot forged
            `shouldBe` Left
                ( WrongHexLength
                    "request_update.snapshot.utxo_root"
                    32
                    1
                )

    it "rejects a trusted-root mismatch" $ do
        let RequestUpdateFixture{trustedRoot, facts} =
                honestRequestUpdateFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyRequestUpdateFacts forged facts
            `shouldBe` Left
                (TrustedRootMismatch "request_update.snapshot.utxo_root")

    it "rejects a tampered wallet inclusion proof" $ do
        let RequestUpdateFixture{trustedRoot, facts} =
                honestRequestUpdateFixture
            entry = soleWalletUtxo facts
            forgedEntry =
                entry
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rufWalletUtxos = [forgedEntry]}
        verifyRequestUpdateFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "request_update.wallet_utxos[0].inclusion_proof"
                    "malformed proof CBOR"
                )

data RequestUpdateFixture = RequestUpdateFixture
    { trustedRoot :: TrustedRoot
    , facts :: RequestUpdateFacts
    }

honestRequestUpdateFixture :: RequestUpdateFixture
honestRequestUpdateFixture =
    let (root, walletEntry) = csmtWalletRow walletTxId walletTxOut
        updateFacts =
            RequestUpdateFacts
                { rufSnapshot = snapshotWithRoot root
                , rufToken = sampleToken
                , rufKey = Hex "mykey"
                , rufOldValue = Hex "oldvalue"
                , rufNewValue = Hex "newvalue"
                , rufAddress = Hex "addr"
                , rufSubmittedAt = 1_700_000_000_000
                , rufWalletUtxos = [walletEntry]
                , rufProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  RequestUpdateFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = updateFacts
            }

csmtWalletRow :: ByteString -> ByteString -> (ByteString, UtxoEntry)
csmtWalletRow txIdBytes txOutBytes =
    evalPureFromEmptyDB $ do
        let key = byteStringToKey (encodeTxIn txIdBytes 0)
        insertMHash key (mkHash txOutBytes)
        proofBytes <- do
            mProof <-
                proofM
                    hashCodecs
                    identityFromKV
                    hashHashing
                    key
            pure $ case mProof of
                Just (_, proof) -> renderProof proof
                Nothing -> BS.empty
        root <- maybe BS.empty renderHash <$> getRootHashM
        pure
            ( root
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex txIdBytes
                        , urTxIx = 0
                        }
                , ueTxOutCbor = Hex txOutBytes
                , ueInclusionProof = Hex proofBytes
                }
            )

expectVerified
    :: TrustedRoot
    -> RequestUpdateFacts
    -> IO VerifiedRequestUpdateFacts
expectVerified trusted facts =
    case verifyRequestUpdateFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestUpdateFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

soleWalletUtxo :: RequestUpdateFacts -> UtxoEntry
soleWalletUtxo RequestUpdateFacts{rufWalletUtxos = [entry]} = entry
soleWalletUtxo RequestUpdateFacts{rufWalletUtxos = entries} =
    error
        ( "RequestUpdateFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )

requestUpdateFactsJson :: RequestUpdateFacts -> Value
requestUpdateFactsJson RequestUpdateFacts{..} =
    object
        [ "snapshot" .= rufSnapshot
        , "token" .= rufToken
        , "key" .= rufKey
        , "old_value" .= rufOldValue
        , "new_value" .= rufNewValue
        , "address" .= rufAddress
        , "submitted_at" .= rufSubmittedAt
        , "wallet_utxos" .= rufWalletUtxos
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified rufProtocolParameters
                , "cbor" .= uppCbor rufProtocolParameters
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

walletTxId, walletTxOut :: ByteString
walletTxId = BS.replicate 32 0xC2
walletTxOut = "wallet-txout"

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)
