{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Client.EndFactsSpec
-- Description : Unit tests for end facts JSON and verification.
module Cardano.MPFS.Client.EndFactsSpec
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
import System.Environment (getEnv)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import CSMT
    ( Direction
    , Standalone (StandaloneCSMTCol)
    )
import CSMT.Backend.Pure
    ( runPureTransaction
    )
import CSMT.Core.CBOR
    ( renderCompletenessProof
    , renderProof
    )
import CSMT.Core.Hash
    ( Hash
    , byteStringToKey
    , renderHash
    )
import CSMT.Hashes
    ( hashHashing
    , mkHash
    )
import CSMT.Proof.Completeness
    ( CompletenessProof
    , generateProof
    )
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , identityFromKV
    , insertMHash
    , proofM
    )
import Cardano.Ledger.BaseTypes
    ( Network (Testnet)
    )
import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Ledger
    ( Coin (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , applyPreviousPolicies
    , computeScriptHash
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestSetPrefixFromCfg
    )
import Cardano.MPFS.Client.Facts
    ( EndFacts (..)
    , VerifiedEndFacts
    , verifiedEndFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyEndFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( flipApiHexMidByte
    )

spec :: Spec
spec = describe "verifyEndFacts" $ do
    it "round-trips the end facts JSON shape" $ do
        cfg <- testCageConfig
        let EndFixture{facts} = honestEndFixture cfg
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (endFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
        verifyEndFacts cfg trustedRoot facts `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
        verified <- expectVerified cfg trustedRoot facts
        verifiedEndFacts verified `shouldBe` facts

    it "rejects a malformed trusted root before replay" $ do
        cfg <- testCageConfig
        let EndFixture{facts} = honestEndFixture cfg
        verifyEndFacts cfg (TrustedRoot (Hex "\x01")) facts
            `shouldBe` Left (WrongHexLength "end.trusted_root" 32 1)

    it "rejects a malformed snapshot root before replay" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            forged =
                facts
                    { efSnapshot =
                        (efSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyEndFacts cfg trustedRoot forged
            `shouldBe` Left
                (WrongHexLength "end.snapshot.utxo_root" 32 1)

    it "rejects a trusted-root mismatch" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyEndFacts cfg forged facts
            `shouldBe` Left
                (TrustedRootMismatch "end.snapshot.utxo_root")

    it "rejects a tampered state inclusion proof" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            entry = efStateUtxo facts
            forged =
                facts
                    { efStateUtxo =
                        entry
                            { ueInclusionProof =
                                Hex "\x00"
                            }
                    }
        verifyEndFacts cfg trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "end.state_utxo.inclusion_proof"
                    "malformed proof CBOR"
                )

    it "rejects a tampered wallet inclusion proof" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            entry = soleWalletUtxo facts
            forgedEntry =
                entry
                    { ueInclusionProof =
                        Hex "\x00"
                    }
            forged = facts{efWalletUtxos = [forgedEntry]}
        verifyEndFacts cfg trustedRoot forged
            `shouldBe` Left
                ( CsmtReplayFailed
                    "end.wallet_utxos[0].inclusion_proof"
                    "malformed proof CBOR"
                )

    it "rejects non-empty request-set entries before completeness replay"
        $ do
            cfg <- testCageConfig
            let EndFixture{trustedRoot, facts, requestEntry} =
                    honestEndFixture cfg
                forged =
                    facts
                        { efRequestSet =
                            (efRequestSet facts)
                                { uswEntries = [requestEntry]
                                }
                        }
            verifyEndFacts cfg trustedRoot forged
                `shouldBe` Left
                    ( CompletenessExtraLeaf
                        "end.request_set.entries[0]"
                        (uerRef requestEntry)
                    )

    it "rejects a tampered request-set completeness proof" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            requestSet = efRequestSet facts
            forged =
                facts
                    { efRequestSet =
                        requestSet
                            { uswCompletenessProof =
                                flipApiHexMidByte
                                    (uswCompletenessProof requestSet)
                            }
                    }
        verifyEndFacts cfg trustedRoot forged
            `shouldBe` Left
                ( CompletenessProofInvalid
                    "end.request_set.completeness_proof"
                )

data EndFixture = EndFixture
    { trustedRoot :: TrustedRoot
    , facts :: EndFacts
    , requestEntry :: UtxoEntryRefOnly
    }

honestEndFixture :: CageConfig -> EndFixture
honestEndFixture cfg =
    let requestPrefix = requestSetPrefixFromCfg cfg sampleToken
        (root, stateEntry, walletEntry, requestOnly, proofBs) =
            csmtEndRows requestPrefix
        endFacts =
            EndFacts
                { efSnapshot = snapshotWithRoot root
                , efToken = sampleToken
                , efStateUtxo = stateEntry
                , efWalletUtxos = [walletEntry]
                , efRequestSet =
                    UtxoSetWitness
                        { uswEntries = []
                        , uswCompletenessProof = Hex proofBs
                        }
                , efProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  EndFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = endFacts
            , requestEntry = requestOnly
            }

csmtEndRows
    :: [Direction]
    -> (ByteString, UtxoEntry, UtxoEntry, UtxoEntryRefOnly, ByteString)
csmtEndRows requestPrefix = evalPureFromEmptyDB $ do
    let stateKey =
            byteStringToKey (encodeTxIn stateTxId 0)
        walletKey =
            byteStringToKey (encodeTxIn walletTxId 1)
        rows =
            [ (stateKey, stateTxOut)
            , (walletKey, walletTxOut)
            ]
    mapM_ (\(key, txOut) -> insertMHash key (mkHash txOut)) rows
    stateProof <- proofBytes stateKey
    walletProof <- proofBytes walletKey
    completenessProof <-
        runPureTransaction hashCodecs
            $ generateProof StandaloneCSMTCol [] requestPrefix
    root <- maybe BS.empty renderHash <$> getRootHashM
    pure
        ( root
        , UtxoEntry
            { ueRef =
                UtxoRef
                    { urTxId = Hex stateTxId
                    , urTxIx = 0
                    }
            , ueTxOutCbor = Hex stateTxOut
            , ueInclusionProof = Hex stateProof
            }
        , UtxoEntry
            { ueRef =
                UtxoRef
                    { urTxId = Hex walletTxId
                    , urTxIx = 1
                    }
            , ueTxOutCbor = Hex walletTxOut
            , ueInclusionProof = Hex walletProof
            }
        , UtxoEntryRefOnly
            { uerRef =
                UtxoRef
                    { urTxId = Hex requestTxId
                    , urTxIx = 2
                    }
            , uerTxOutCbor = Hex requestTxOut
            }
        , case completenessProof of
            Just proof ->
                renderCompletenessProof
                    (proof :: CompletenessProof Hash)
            Nothing ->
                error "expected request-set completeness proof"
        )
  where
    proofBytes key = do
        mProof <-
            proofM
                hashCodecs
                identityFromKV
                hashHashing
                key
        pure $ case mProof of
            Just (_, proof) -> renderProof proof
            Nothing -> BS.empty

expectVerified
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> IO VerifiedEndFacts
expectVerified cfg trusted facts =
    case verifyEndFacts cfg trusted facts of
        Left err ->
            expectationFailure ("verifyEndFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

soleWalletUtxo :: EndFacts -> UtxoEntry
soleWalletUtxo EndFacts{efWalletUtxos = [entry]} = entry
soleWalletUtxo EndFacts{efWalletUtxos = entries} =
    error
        ( "EndFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )

endFactsJson :: EndFacts -> Value
endFactsJson EndFacts{..} =
    object
        [ "snapshot" .= efSnapshot
        , "token" .= efToken
        , "state_utxo" .= efStateUtxo
        , "wallet_utxos" .= efWalletUtxos
        , "request_set" .= efRequestSet
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified efProtocolParameters
                , "cbor" .= uppCbor efProtocolParameters
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

testCageConfig :: IO CageConfig
testCageConfig = do
    blueprintPath <- getEnv "MPFS_BLUEPRINT"
    eBlueprint <- loadBlueprint blueprintPath
    blueprint <- case eBlueprint of
        Left err ->
            expectationFailure
                ("loadBlueprint failed: " <> err)
                *> error "unreachable"
        Right bp -> pure bp
    scriptBytes <-
        case extractCompiledCode "state." blueprint of
            Just bytes -> pure bytes
            Nothing ->
                expectationFailure
                    "state script not found in MPFS_BLUEPRINT"
                    *> error "unreachable"
    requestBytes <-
        case extractCompiledCode "request." blueprint of
            Just bytes -> pure bytes
            Nothing ->
                expectationFailure
                    "request script not found in MPFS_BLUEPRINT"
                    *> error "unreachable"
    let appliedStateBytes =
            applyPreviousPolicies [] scriptBytes
    pure
        CageConfig
            { cageScriptBytes = appliedStateBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = computeScriptHash appliedStateBytes
            , defaultProcessTime = 60_000
            , defaultRetractTime = 30_000
            , defaultTip = Coin 1_000_000
            , network = Testnet
            }

stateTxId, walletTxId, requestTxId :: ByteString
stateTxId = BS.replicate 32 0xA0
walletTxId = BS.replicate 32 0xC2
requestTxId = BS.replicate 32 0xB1

stateTxOut, walletTxOut, requestTxOut :: ByteString
stateTxOut = "state-txout"
walletTxOut = "wallet-txout"
requestTxOut = "request-txout"

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)
