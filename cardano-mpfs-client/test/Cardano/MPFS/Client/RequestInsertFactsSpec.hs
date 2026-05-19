-- |
-- Module      : Cardano.MPFS.Client.RequestInsertFactsSpec
-- Description : Unit tests for request-insert facts verification.
module Cardano.MPFS.Client.RequestInsertFactsSpec
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
    ( RequestInsertFacts (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRequestInsertFacts
    , verifiedRequestInsertFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyRequestInsertFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( flipApiHexMidByte
    )

spec :: Spec
spec = describe "verifyRequestInsertFacts" $ do
    it "round-trips the request-insert facts JSON shape" $ do
        let RequestInsertFixture{facts} = honestRequestInsertFixture
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (requestInsertFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        let RequestInsertFixture{trustedRoot, facts} =
                honestRequestInsertFixture
        verifyRequestInsertFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let RequestInsertFixture{trustedRoot, facts} =
                honestRequestInsertFixture
        verified <- expectVerified trustedRoot facts
        verifiedRequestInsertFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let RequestInsertFixture{trustedRoot, facts} =
                honestRequestInsertFixture
            forged =
                facts
                    { rifSnapshot =
                        (rifSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyRequestInsertFacts trustedRoot forged
            `shouldBe` Left
                ( WrongHexLength
                    "request_insert.snapshot.utxo_root"
                    32
                    1
                )

    it "rejects a trusted-root mismatch" $ do
        let RequestInsertFixture{trustedRoot, facts} =
                honestRequestInsertFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyRequestInsertFacts forged facts
            `shouldBe` Left
                (TrustedRootMismatch "request_insert.snapshot.utxo_root")

    it "rejects a tampered wallet inclusion proof" $ do
        let RequestInsertFixture{trustedRoot, facts} =
                honestRequestInsertFixture
            entry = soleWalletUtxo facts
            forgedEntry =
                entry
                    { ueInclusionProof = Hex "\x00"
                    }
            forged = facts{rifWalletUtxos = [forgedEntry]}
        verifyRequestInsertFacts trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "request_insert.wallet_utxos[0].inclusion_proof"
                    "malformed proof CBOR"
                )

data RequestInsertFixture = RequestInsertFixture
    { trustedRoot :: TrustedRoot
    , facts :: RequestInsertFacts
    }

honestRequestInsertFixture :: RequestInsertFixture
honestRequestInsertFixture =
    let (root, walletEntry) = csmtWalletRow walletTxId walletTxOut
        requestFacts =
            RequestInsertFacts
                { rifSnapshot = snapshotWithRoot root
                , rifToken = sampleToken
                , rifKey = Hex "mykey"
                , rifValue = Hex "myvalue"
                , rifAddress = Hex "addr"
                , rifSubmittedAt = 1_700_000_000_000
                , rifWalletUtxos = [walletEntry]
                , rifProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  RequestInsertFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = requestFacts
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
    -> RequestInsertFacts
    -> IO VerifiedRequestInsertFacts
expectVerified trusted facts =
    case verifyRequestInsertFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestInsertFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

soleWalletUtxo :: RequestInsertFacts -> UtxoEntry
soleWalletUtxo RequestInsertFacts{rifWalletUtxos = [entry]} = entry
soleWalletUtxo RequestInsertFacts{rifWalletUtxos = entries} =
    error
        ( "RequestInsertFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )

requestInsertFactsJson :: RequestInsertFacts -> Value
requestInsertFactsJson RequestInsertFacts{..} =
    object
        [ "snapshot" .= rifSnapshot
        , "token" .= rifToken
        , "key" .= rifKey
        , "value" .= rifValue
        , "address" .= rifAddress
        , "submitted_at" .= rifSubmittedAt
        , "wallet_utxos" .= rifWalletUtxos
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified rifProtocolParameters
                , "cbor" .= uppCbor rifProtocolParameters
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
