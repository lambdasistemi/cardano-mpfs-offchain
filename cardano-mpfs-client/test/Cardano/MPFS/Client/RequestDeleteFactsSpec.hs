-- |
-- Module      : Cardano.MPFS.Client.RequestDeleteFactsSpec
-- Description : Unit tests for request-delete facts verification.
module Cardano.MPFS.Client.RequestDeleteFactsSpec
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
    ( RequestDeleteFacts (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRequestDeleteFacts
    , verifiedRequestDeleteFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyRequestDeleteFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( flipApiHexMidByte
    )

spec :: Spec
spec = describe "verifyRequestDeleteFacts" $ do
    it "round-trips the request-delete facts JSON shape" $ do
        let RequestDeleteFixture{facts} = honestRequestDeleteFixture
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (requestDeleteFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        let RequestDeleteFixture{trustedRoot, facts} =
                honestRequestDeleteFixture
        verifyRequestDeleteFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let RequestDeleteFixture{trustedRoot, facts} =
                honestRequestDeleteFixture
        verified <- expectVerified trustedRoot facts
        verifiedRequestDeleteFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let RequestDeleteFixture{trustedRoot, facts} =
                honestRequestDeleteFixture
            forged =
                facts
                    { rdfSnapshot =
                        (rdfSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyRequestDeleteFacts trustedRoot forged
            `shouldBe` Left
                ( WrongHexLength
                    "request_delete.snapshot.utxo_root"
                    32
                    1
                )

    it "rejects a trusted-root mismatch" $ do
        let RequestDeleteFixture{trustedRoot, facts} =
                honestRequestDeleteFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyRequestDeleteFacts forged facts
            `shouldBe` Left
                (TrustedRootMismatch "request_delete.snapshot.utxo_root")

    it "rejects a tampered wallet inclusion proof" $ do
        let RequestDeleteFixture{trustedRoot, facts} =
                honestRequestDeleteFixture
            entry = soleWalletUtxo facts
            forgedEntry =
                entry
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rdfWalletUtxos = [forgedEntry]}
        verifyRequestDeleteFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "request_delete.wallet_utxos[0].inclusion_proof"
                    "malformed proof CBOR"
                )

data RequestDeleteFixture = RequestDeleteFixture
    { trustedRoot :: TrustedRoot
    , facts :: RequestDeleteFacts
    }

honestRequestDeleteFixture :: RequestDeleteFixture
honestRequestDeleteFixture =
    let (root, walletEntry) = csmtWalletRow walletTxId walletTxOut
        deleteFacts =
            RequestDeleteFacts
                { rdfSnapshot = snapshotWithRoot root
                , rdfToken = sampleToken
                , rdfKey = Hex "mykey"
                , rdfValue = Hex "myvalue"
                , rdfAddress = Hex "addr"
                , rdfSubmittedAt = 1_700_000_000_000
                , rdfWalletUtxos = [walletEntry]
                , rdfProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  RequestDeleteFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = deleteFacts
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
    -> RequestDeleteFacts
    -> IO VerifiedRequestDeleteFacts
expectVerified trusted facts =
    case verifyRequestDeleteFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestDeleteFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

soleWalletUtxo :: RequestDeleteFacts -> UtxoEntry
soleWalletUtxo RequestDeleteFacts{rdfWalletUtxos = [entry]} = entry
soleWalletUtxo RequestDeleteFacts{rdfWalletUtxos = entries} =
    error
        ( "RequestDeleteFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )

requestDeleteFactsJson :: RequestDeleteFacts -> Value
requestDeleteFactsJson RequestDeleteFacts{..} =
    object
        [ "snapshot" .= rdfSnapshot
        , "token" .= rdfToken
        , "key" .= rdfKey
        , "value" .= rdfValue
        , "address" .= rdfAddress
        , "submitted_at" .= rdfSubmittedAt
        , "wallet_utxos" .= rdfWalletUtxos
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified rdfProtocolParameters
                , "cbor" .= uppCbor rdfProtocolParameters
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
