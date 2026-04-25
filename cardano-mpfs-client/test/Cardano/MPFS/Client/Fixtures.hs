-- |
-- Module      : Cardano.MPFS.Client.Fixtures
-- Description : Honest CSMT + MPF fixture generator for unit tests.
--
-- Uses the pure backends from @mts:csmt-test-lib@ and
-- @mts:mpf-test-lib@ to build real, cryptographically valid
-- proofs against advertised roots, then wraps them into the
-- per-endpoint response envelopes. The resulting fixtures flow
-- through "Cardano.MPFS.Client.Verify.Replay" byte-for-byte the
-- same as a genuine server response — 'shouldAccept' must
-- succeed on every one of them.
module Cardano.MPFS.Client.Fixtures
    ( -- * Per-endpoint honest responses
      honestBootResponse
    , honestRequestResponse
    , honestRetractResponse
    , honestRejectResponse
    , honestEndResponse
    , honestUpdateResponse
    , honestUpdateResponseMixedTrie
    , honestUpdateResponseEmptyTrie

      -- * Underlying primitives
    , honestWitness
    , honestTrieInclusion
    , honestTrieExclusion

      -- * Hex utilities
    , toHex
    , txCborFromTxIns
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Term qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text.Encoding qualified as T
import Data.Word (Word64)

import CSMT.Core.CBOR (renderProof)
import CSMT.Core.Hash
    ( byteStringToKey
    , renderHash
    )
import CSMT.Hashes (hashHashing, mkHash)
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , identityFromKV
    , insertMHash
    , proofM
    )
import MPF.Hashes (mkMPFHash, renderMPFHash)
import MPF.Hashes.Aiken (renderAikenProof)
import MPF.Interface (byteStringToHexKey)
import MPF.Proof.Exclusion (mpfExclusionProofSteps)
import MPF.Proof.Insertion (MPFProof (..))
import MPF.Test.Lib
    ( insertByteStringM
    , proofExcludeMPFM
    , proofMPFM
    , runMPFPure'
    )
import MPF.Test.Lib qualified as MPFTest (getRootHashM)

import Cardano.MPFS.Client.Bundle
    ( BootProof (..)
    , BootTxResponse (..)
    , EndProof (..)
    , EndTxResponse (..)
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
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    )

-- ---------------------------------------------------------------
-- Shared sample inputs
-- ---------------------------------------------------------------

sampleSlot :: Word64
sampleSlot = 42

sampleBlockId :: ByteString
sampleBlockId = BS.replicate 32 0x11

stateTxIn, requestTxIn, fundingTxIn :: (ByteString, Word64)
stateTxIn = (BS.replicate 32 0xA0, 0)
requestTxIn = (BS.replicate 32 0xB1, 1)
fundingTxIn = (BS.replicate 32 0xC2, 2)

stateTxOut, requestTxOut, fundingTxOut :: ByteString
stateTxOut = "state-tx-out-bytes"
requestTxOut = "request-tx-out-bytes"
fundingTxOut = "funding-tx-out-bytes"

txCborFromTxIns :: [TxIn] -> [TxIn] -> Hex
txCborFromTxIns inputs refs =
    toHex
        $ CBOR.toStrictByteString
        $ CBOR.encodeTerm
        $ CBOR.TList
            [ CBOR.TMap bodyFields
            , CBOR.TMap []
            , CBOR.TBool True
            , CBOR.TNull
            ]
  where
    bodyFields =
        [(CBOR.TInt 0, setTerm inputs)]
            <> if null refs
                then []
                else [(CBOR.TInt 18, setTerm refs)]

setTerm :: [TxIn] -> CBOR.Term
setTerm xs =
    CBOR.TTagged 258
        $ CBOR.TList
        $ map txInTerm xs

txInTerm :: TxIn -> CBOR.Term
txInTerm TxIn{txId = Hex tid, txIx} =
    CBOR.TList
        [ CBOR.TBytes (decodeFixtureHex tid)
        , CBOR.TInteger (fromIntegral txIx)
        ]

decodeFixtureHex :: Text -> ByteString
decodeFixtureHex txt =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> bs
        Left err -> error ("invalid fixture hex: " <> show err)

txFor :: [WitnessedUtxo] -> [WitnessedUtxo] -> Hex
txFor inputs refs =
    txCborFromTxIns
        (map txIn inputs)
        (map txIn refs)

-- ---------------------------------------------------------------
-- CSMT primitives
-- ---------------------------------------------------------------

-- | Canonical CBOR of a @(txId, txIx)@ pair — matches
-- @cborEncode (Shelley.TxIn ...)@ in shape (2-element list of
-- @[bytestring, uint]@ with minimal integer encoding), which is
-- what the client's 'replayWitnessedUtxo' suffix-matches on.
encodeTxIn :: ByteString -> Word64 -> ByteString
encodeTxIn txIdBs txIxWord =
    CBOR.toStrictByteString
        $ mconcat
            [ CBOR.encodeListLen 2
            , CBOR.encodeBytes txIdBs
            , CBOR.encodeWord64 txIxWord
            ]

-- | A CSMT built over the three sample UTxOs, shared across
-- every per-endpoint fixture so they all advertise the same
-- @utxo_root@ and every witness in a given response replays
-- against it.
data CsmtBundle = CsmtBundle
    { bundleRoot :: ByteString
    , bundleState :: WitnessedUtxo
    , bundleRequest :: WitnessedUtxo
    , bundleFunding :: WitnessedUtxo
    }

buildBundle :: CsmtBundle
buildBundle = evalPureFromEmptyDB $ do
    insertUtxo stateTxIn stateTxOut
    insertUtxo requestTxIn requestTxOut
    insertUtxo fundingTxIn fundingTxOut
    stateWitness <- mkWitness stateTxIn stateTxOut
    requestWitness <- mkWitness requestTxIn requestTxOut
    fundingWitness <- mkWitness fundingTxIn fundingTxOut
    mRoot <- getRootHashM
    let rootBytes = maybe BS.empty renderHash mRoot
    pure
        CsmtBundle
            { bundleRoot = rootBytes
            , bundleState = stateWitness
            , bundleRequest = requestWitness
            , bundleFunding = fundingWitness
            }
  where
    insertUtxo (txIdBs, txIxWord) txOutBs =
        insertMHash
            (byteStringToKey (encodeTxIn txIdBs txIxWord))
            (mkHash txOutBs)

    mkWitness (txIdBs, txIxWord) txOutBs = do
        let k = byteStringToKey (encodeTxIn txIdBs txIxWord)
        mProof <- proofM hashCodecs identityFromKV hashHashing k
        let proofBytes = case mProof of
                Just (_, p) -> renderProof p
                Nothing -> BS.empty
        pure
            WitnessedUtxo
                { txIn =
                    TxIn
                        { txId = toHex txIdBs
                        , txIx = txIxWord
                        }
                , txOut = toHex txOutBs
                , utxoProof = toHex proofBytes
                }

-- | Top-level CSMT fixture: advertised root + three witnesses.
honestWitness :: CsmtBundle
honestWitness = buildBundle

sampleSnapshot :: ByteString -> VerificationSnapshot
sampleSnapshot rootBs =
    VerificationSnapshot
        { utxoRoot = toHex rootBs
        , chainpoint =
            ChainPoint
                { slot = sampleSlot
                , blockId = toHex sampleBlockId
                }
        }

-- ---------------------------------------------------------------
-- MPF primitives
-- ---------------------------------------------------------------

mpfInclusionEntries :: [(ByteString, ByteString)]
mpfInclusionEntries = allEntries
  where
    allEntries =
        [ ("apple", "fruit")
        , ("banana", "yellow")
        , ("cherry", "red")
        ]

primaryKey, primaryValue :: ByteString
primaryKey = "apple"
primaryValue = "fruit"

-- | Real MPF over three inclusion entries + one @TrieFact@
-- asserting inclusion for @"apple"@.
honestTrieInclusion :: (ByteString, TrieFact)
honestTrieInclusion =
    let (rootBytes, proofBytes) = fst $ runMPFPure' $ do
            mapM_ (uncurry insertByteStringM) mpfInclusionEntries
            mRoot <- MPFTest.getRootHashM
            mProof <-
                proofMPFM
                    ( byteStringToHexKey
                        (renderMPFHash (mkMPFHash primaryKey))
                    )
            pure
                ( maybe BS.empty renderMPFHash mRoot
                , case mProof of
                    Just p ->
                        renderAikenProof (mpfProofSteps p)
                    Nothing -> BS.empty
                )
    in  ( rootBytes
        , TrieFact
            { key = toHex primaryKey
            , value = Just (toHex primaryValue)
            , mpfProof = toHex proofBytes
            }
        )

-- | Real MPF over the same three entries + one @TrieFact@
-- asserting absence of @"durian"@.
honestTrieExclusion :: (ByteString, TrieFact)
honestTrieExclusion =
    let absentKey = "durian"
        (rootBytes, proofBytes) = fst $ runMPFPure' $ do
            mapM_ (uncurry insertByteStringM) mpfInclusionEntries
            mRoot <- MPFTest.getRootHashM
            mProof <-
                proofExcludeMPFM
                    ( byteStringToHexKey
                        (renderMPFHash (mkMPFHash absentKey))
                    )
            pure
                ( maybe BS.empty renderMPFHash mRoot
                , case mProof of
                    Just p ->
                        renderAikenProof
                            (mpfExclusionProofSteps p)
                    Nothing -> BS.empty
                )
    in  ( rootBytes
        , TrieFact
            { key = toHex absentKey
            , value = Nothing
            , mpfProof = toHex proofBytes
            }
        )

-- ---------------------------------------------------------------
-- Per-endpoint fixtures
-- ---------------------------------------------------------------

honestBootResponse :: BootTxResponse
honestBootResponse =
    BootTxResponse
        (txFor [bundleFunding honestWitness] [])
        (sampleSnapshot (bundleRoot honestWitness))
        (BootProof [bundleFunding honestWitness])

honestRequestResponse :: RequestTxResponse
honestRequestResponse =
    RequestTxResponse
        (txFor [bundleFunding honestWitness] [])
        (sampleSnapshot (bundleRoot honestWitness))
        (RequestProof [bundleFunding honestWitness])

honestRetractResponse :: RetractTxResponse
honestRetractResponse =
    RetractTxResponse
        ( txFor
            [ bundleRequest honestWitness
            , bundleFunding honestWitness
            ]
            [bundleState honestWitness]
        )
        (sampleSnapshot (bundleRoot honestWitness))
        ( RetractProof
            (bundleRequest honestWitness)
            (bundleState honestWitness)
            [bundleFunding honestWitness]
        )

honestRejectResponse :: RejectTxResponse
honestRejectResponse =
    RejectTxResponse
        ( txFor
            [ bundleState honestWitness
            , bundleRequest honestWitness
            , bundleFunding honestWitness
            ]
            []
        )
        (sampleSnapshot (bundleRoot honestWitness))
        ( RejectProof
            (bundleState honestWitness)
            [bundleRequest honestWitness]
            [bundleFunding honestWitness]
        )

honestEndResponse :: EndTxResponse
honestEndResponse =
    EndTxResponse
        ( txFor
            [ bundleState honestWitness
            , bundleFunding honestWitness
            ]
            []
        )
        (sampleSnapshot (bundleRoot honestWitness))
        ( EndProof
            (bundleState honestWitness)
            [bundleFunding honestWitness]
        )

honestUpdateResponse :: UpdateTxResponse
honestUpdateResponse =
    let (trieRoot, trieFact) = honestTrieInclusion
    in  UpdateTxResponse
            ( txFor
                [ bundleState honestWitness
                , bundleRequest honestWitness
                , bundleFunding honestWitness
                ]
                []
            )
            (sampleSnapshot (bundleRoot honestWitness))
            ( UpdateProof
                (bundleState honestWitness)
                [bundleRequest honestWitness]
                [bundleFunding honestWitness]
                (toHex trieRoot)
                [trieFact]
            )

-- | Mixed inclusion + exclusion TrieFacts against a shared
-- MPF root.
honestUpdateResponseMixedTrie :: UpdateTxResponse
honestUpdateResponseMixedTrie =
    let (trieRoot, inclusionFact) = honestTrieInclusion
        (_, exclusionFact) = honestTrieExclusion
    in  UpdateTxResponse
            ( txFor
                [ bundleState honestWitness
                , bundleRequest honestWitness
                , bundleFunding honestWitness
                ]
                []
            )
            (sampleSnapshot (bundleRoot honestWitness))
            ( UpdateProof
                (bundleState honestWitness)
                [bundleRequest honestWitness]
                [bundleFunding honestWitness]
                (toHex trieRoot)
                [inclusionFact, exclusionFact]
            )

-- | Update response with an empty @trie_read@ list — the MPF
-- pass is a no-op; only the CSMT witnesses replay.
honestUpdateResponseEmptyTrie :: UpdateTxResponse
honestUpdateResponseEmptyTrie =
    let (trieRoot, _) = honestTrieInclusion
    in  UpdateTxResponse
            ( txFor
                [ bundleState honestWitness
                , bundleRequest honestWitness
                , bundleFunding honestWitness
                ]
                []
            )
            (sampleSnapshot (bundleRoot honestWitness))
            ( UpdateProof
                (bundleState honestWitness)
                [bundleRequest honestWitness]
                [bundleFunding honestWitness]
                (toHex trieRoot)
                []
            )

-- ---------------------------------------------------------------
-- Hex helpers
-- ---------------------------------------------------------------

-- | Encode raw bytes as the hex 'Hex' wrapper used across the
-- response envelopes.
toHex :: ByteString -> Hex
toHex = Hex . T.decodeUtf8 . Base16.encode
