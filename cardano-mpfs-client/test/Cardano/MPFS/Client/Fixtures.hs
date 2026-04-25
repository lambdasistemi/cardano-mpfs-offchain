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

      -- * Transaction fixture builders
    , TxRedeemerFixture (..)
    , burningRedeemerTerm
    , mintRedeemerFixture
    , mintingRedeemerTerm
    , rejectedActionTerm
    , spendContributeRedeemerTerm
    , spendEndRedeemerTerm
    , spendModifyRedeemerTerm
    , spendRedeemerFixture
    , spendRetractRedeemerTerm
    , txCborFromTxPartsWithRedeemers
    , txOutTerm
    , updateActionTermFromProof

      -- * Hex utilities
    , toHex
    , sampleStateAsset
    , txCborFromTxIns
    , txCborFromTxParts
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Term qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.ByteString.Lazy qualified as BSL
import Data.List (nub)
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
import Cardano.MPFS.Client.Verify.TxView
    ( TxAsset (..)
    )

-- ---------------------------------------------------------------
-- Shared sample inputs
-- ---------------------------------------------------------------

sampleSlot :: Word64
sampleSlot = 42

sampleBlockId :: ByteString
sampleBlockId = BS.replicate 32 0x11

stateTxIn
    , requestTxIn
    , requestTxIn2
    , fundingTxIn
        :: (ByteString, Word64)
stateTxIn = (BS.replicate 32 0xA0, 0)
requestTxIn = (BS.replicate 32 0xB1, 1)
requestTxIn2 = (BS.replicate 32 0xB2, 3)
fundingTxIn = (BS.replicate 32 0xC2, 2)

samplePolicyId, sampleAssetName :: ByteString
samplePolicyId = BS.replicate 28 0xD3
sampleAssetName = BS.replicate 32 0xE4

sampleStateRoot :: ByteString
sampleStateRoot = BS.replicate 32 0x00

sampleStateAsset :: Integer -> TxAsset
sampleStateAsset quantity =
    TxAsset
        { assetPolicy = toHex samplePolicyId
        , assetName = toHex sampleAssetName
        , assetQuantity = quantity
        }

requestTxOut, fundingTxOut :: ByteString
requestTxOut = txOutCbor True []
fundingTxOut = txOutCbor False []

stateTxOutWithRoot :: ByteString -> ByteString
stateTxOutWithRoot root =
    txOutCborWithDatum
        [sampleStateAsset 1]
        (Just (stateDatumTerm root))

txCborFromTxIns :: [TxIn] -> [TxIn] -> Hex
txCborFromTxIns inputs refs =
    txCborFromTxParts inputs refs [] []

txCborFromTxParts
    :: [TxIn] -> [TxIn] -> [TxAsset] -> [CBOR.Term] -> Hex
txCborFromTxParts inputs refs mint outputs =
    txCborFromTxPartsWithRedeemers inputs refs mint outputs []

txCborFromTxPartsWithRedeemers
    :: [TxIn]
    -> [TxIn]
    -> [TxAsset]
    -> [CBOR.Term]
    -> [TxRedeemerFixture]
    -> Hex
txCborFromTxPartsWithRedeemers inputs refs mint outputs redeemers =
    toHex
        $ CBOR.toStrictByteString
        $ CBOR.encodeTerm
        $ CBOR.TList
            [ CBOR.TMap bodyFields
            , witnessSetTerm redeemers
            , CBOR.TBool True
            , CBOR.TNull
            ]
  where
    bodyFields =
        [ (CBOR.TInt 0, setTerm inputs)
        , (CBOR.TInt 1, CBOR.TList outputs)
        ]
            <> [(CBOR.TInt 18, setTerm refs) | not (null refs)]
            <> [(CBOR.TInt 9, multiAssetTerm mint) | not (null mint)]

data TxRedeemerFixture = TxRedeemerFixture
    { redeemerFixtureTag :: Integer
    , redeemerFixtureIndex :: Word64
    , redeemerFixtureData :: CBOR.Term
    }
    deriving stock (Eq, Show)

spendRedeemerFixture :: Word64 -> CBOR.Term -> TxRedeemerFixture
spendRedeemerFixture = TxRedeemerFixture 0

mintRedeemerFixture :: Word64 -> CBOR.Term -> TxRedeemerFixture
mintRedeemerFixture = TxRedeemerFixture 1

witnessSetTerm :: [TxRedeemerFixture] -> CBOR.Term
witnessSetTerm [] = CBOR.TMap []
witnessSetTerm redeemers =
    CBOR.TMap [(CBOR.TInt 5, redeemerMapTerm redeemers)]

redeemerMapTerm :: [TxRedeemerFixture] -> CBOR.Term
redeemerMapTerm redeemers =
    CBOR.TMap
        [ ( CBOR.TList
                [ CBOR.TInteger redeemerFixtureTag
                , CBOR.TInteger (fromIntegral redeemerFixtureIndex)
                ]
          , CBOR.TList
                [ redeemerFixtureData
                , CBOR.TList [CBOR.TInteger 0, CBOR.TInteger 0]
                ]
          )
        | TxRedeemerFixture{..} <- redeemers
        ]

txOutCbor :: Bool -> [TxAsset] -> ByteString
txOutCbor hasInlineDatum assets =
    CBOR.toStrictByteString
        $ CBOR.encodeTerm
        $ txOutTerm hasInlineDatum assets

txOutCborWithDatum :: [TxAsset] -> Maybe CBOR.Term -> ByteString
txOutCborWithDatum assets datum =
    CBOR.toStrictByteString
        $ CBOR.encodeTerm
        $ txOutTermWithDatum assets datum

txOutTerm :: Bool -> [TxAsset] -> CBOR.Term
txOutTerm hasInlineDatum assets =
    txOutTermWithDatum
        assets
        (if hasInlineDatum then Just inlineDatumDataTerm else Nothing)

txOutTermWithDatum :: [TxAsset] -> Maybe CBOR.Term -> CBOR.Term
txOutTermWithDatum assets datum =
    CBOR.TMap
        $ [ (CBOR.TInt 0, CBOR.TBytes "addr")
          , (CBOR.TInt 1, valueTerm assets)
          ]
            <> maybe
                []
                (\d -> [(CBOR.TInt 2, inlineDatumOption d)])
                datum

inlineDatumOption :: CBOR.Term -> CBOR.Term
inlineDatumOption datum =
    CBOR.TList
        [ CBOR.TInt 1
        , CBOR.TTagged
            24
            ( CBOR.TBytes
                ( CBOR.toStrictByteString
                    (CBOR.encodeTerm datum)
                )
            )
        ]

inlineDatumDataTerm :: CBOR.Term
inlineDatumDataTerm =
    CBOR.TTagged 121 (CBOR.TList [])

stateDatumTerm :: ByteString -> CBOR.Term
stateDatumTerm root =
    constr
        1
        [ constr
            0
            [ CBOR.TBytes (BS.replicate 28 0xAA)
            , CBOR.TBytes root
            , CBOR.TInteger 1_000_000
            , CBOR.TInteger 60_000
            , CBOR.TInteger 30_000
            ]
        ]

mintingRedeemerTerm :: TxIn -> CBOR.Term
mintingRedeemerTerm seedRef =
    constr 0 [constr 0 [txOutRefTerm seedRef]]

burningRedeemerTerm :: CBOR.Term
burningRedeemerTerm = constr 2 []

spendEndRedeemerTerm :: CBOR.Term
spendEndRedeemerTerm = constr 0 []

spendContributeRedeemerTerm :: TxIn -> CBOR.Term
spendContributeRedeemerTerm stateRef =
    constr 1 [txOutRefTerm stateRef]

spendModifyRedeemerTerm :: [CBOR.Term] -> CBOR.Term
spendModifyRedeemerTerm actions =
    constr 2 [CBOR.TList actions]

spendRetractRedeemerTerm :: TxIn -> CBOR.Term
spendRetractRedeemerTerm stateRef =
    constr 3 [txOutRefTerm stateRef]

rejectedActionTerm :: CBOR.Term
rejectedActionTerm = constr 1 []

updateActionTermFromProof :: TrieFact -> CBOR.Term
updateActionTermFromProof TrieFact{mpfProof} =
    constr 0 [decodeProofTerm mpfProof]

txOutRefTerm :: TxIn -> CBOR.Term
txOutRefTerm TxIn{txId = Hex tid, txIx} =
    constr
        0
        [ CBOR.TBytes (decodeFixtureHex tid)
        , CBOR.TInteger (fromIntegral txIx)
        ]

constr :: Integer -> [CBOR.Term] -> CBOR.Term
constr n fields
    | n >= 0 && n <= 6 =
        CBOR.TTagged (fromInteger (121 + n)) (CBOR.TList fields)
    | n >= 7 && n <= 127 =
        CBOR.TTagged (fromInteger (1280 + n - 7)) (CBOR.TList fields)
    | otherwise =
        error ("unsupported fixture constructor: " <> show n)

decodeProofTerm :: Hex -> CBOR.Term
decodeProofTerm proofHex =
    let proofBytes = decodeFixtureHex (unHex proofHex)
    in  case CBOR.deserialiseFromBytes
            CBOR.decodeTerm
            (BSL.fromStrict proofBytes) of
            Right (remaining, term)
                | BSL.null remaining -> term
            _ -> error "invalid fixture proof CBOR"

valueTerm :: [TxAsset] -> CBOR.Term
valueTerm [] = CBOR.TInteger 2_000_000
valueTerm assets =
    CBOR.TList
        [ CBOR.TInteger 2_000_000
        , multiAssetTerm assets
        ]

multiAssetTerm :: [TxAsset] -> CBOR.Term
multiAssetTerm assets =
    CBOR.TMap
        [ ( CBOR.TBytes (decodeFixtureHex (unHex policy))
          , CBOR.TMap
                [ ( CBOR.TBytes (decodeFixtureHex (unHex assetName))
                  , CBOR.TInteger assetQuantity
                  )
                | TxAsset{..} <- assets
                , assetPolicy == policy
                ]
          )
        | policy <- nub (map assetPolicy assets)
        ]

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
    , bundleRequest2 :: WitnessedUtxo
    , bundleFunding :: WitnessedUtxo
    }

buildBundle :: CsmtBundle
buildBundle = buildBundleWithStateRoot sampleStateRoot

buildBundleWithStateRoot :: ByteString -> CsmtBundle
buildBundleWithStateRoot stateRoot = evalPureFromEmptyDB $ do
    let rootedStateTxOut = stateTxOutWithRoot stateRoot
    insertUtxo stateTxIn rootedStateTxOut
    insertUtxo requestTxIn requestTxOut
    insertUtxo requestTxIn2 requestTxOut
    insertUtxo fundingTxIn fundingTxOut
    stateWitness <- mkWitness stateTxIn rootedStateTxOut
    requestWitness <- mkWitness requestTxIn requestTxOut
    requestWitness2 <- mkWitness requestTxIn2 requestTxOut
    fundingWitness <- mkWitness fundingTxIn fundingTxOut
    mRoot <- getRootHashM
    let rootBytes = maybe BS.empty renderHash mRoot
    pure
        CsmtBundle
            { bundleRoot = rootBytes
            , bundleState = stateWitness
            , bundleRequest = requestWitness
            , bundleRequest2 = requestWitness2
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
    let funding = bundleFunding honestWitness
    in  BootTxResponse
            ( txCborFromTxPartsWithRedeemers
                [txIn funding]
                []
                [sampleStateAsset 1]
                [txOutTerm True [sampleStateAsset 1]]
                [ mintRedeemerFixture
                    0
                    (mintingRedeemerTerm (txIn funding))
                ]
            )
            (sampleSnapshot (bundleRoot honestWitness))
            (BootProof [funding])

honestRequestResponse :: RequestTxResponse
honestRequestResponse =
    RequestTxResponse
        ( txCborFromTxParts
            [txIn (bundleFunding honestWitness)]
            []
            []
            [txOutTerm True []]
        )
        (sampleSnapshot (bundleRoot honestWitness))
        (RequestProof [bundleFunding honestWitness])

honestRetractResponse :: RetractTxResponse
honestRetractResponse =
    let state = bundleState honestWitness
        request = bundleRequest honestWitness
        funding = bundleFunding honestWitness
    in  RetractTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [request, funding])
                [txIn state]
                []
                [txOutTerm False []]
                [ spendRedeemerFixture
                    0
                    (spendRetractRedeemerTerm (txIn state))
                ]
            )
            (sampleSnapshot (bundleRoot honestWitness))
            (RetractProof request state [funding])

honestRejectResponse :: RejectTxResponse
honestRejectResponse =
    let state = bundleState honestWitness
        request = bundleRequest honestWitness
        funding = bundleFunding honestWitness
    in  RejectTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [state, request, funding])
                []
                []
                [txOutTerm True [sampleStateAsset 1]]
                [ spendRedeemerFixture
                    0
                    (spendModifyRedeemerTerm [rejectedActionTerm])
                , spendRedeemerFixture
                    1
                    (spendContributeRedeemerTerm (txIn state))
                ]
            )
            (sampleSnapshot (bundleRoot honestWitness))
            (RejectProof state [request] [funding])

honestEndResponse :: EndTxResponse
honestEndResponse =
    let state = bundleState honestWitness
        funding = bundleFunding honestWitness
    in  EndTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [state, funding])
                []
                [sampleStateAsset (-1)]
                [txOutTerm False []]
                [ spendRedeemerFixture 0 spendEndRedeemerTerm
                , mintRedeemerFixture 0 burningRedeemerTerm
                ]
            )
            (sampleSnapshot (bundleRoot honestWitness))
            (EndProof state [funding])

honestUpdateResponse :: UpdateTxResponse
honestUpdateResponse =
    let (trieRoot, trieFact) = honestTrieInclusion
        witness = buildBundleWithStateRoot trieRoot
        state = bundleState witness
        request = bundleRequest witness
        funding = bundleFunding witness
    in  UpdateTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [state, request, funding])
                []
                []
                [txOutTerm True [sampleStateAsset 1]]
                [ spendRedeemerFixture
                    0
                    ( spendModifyRedeemerTerm
                        [updateActionTermFromProof trieFact]
                    )
                , spendRedeemerFixture
                    1
                    (spendContributeRedeemerTerm (txIn state))
                ]
            )
            (sampleSnapshot (bundleRoot witness))
            ( UpdateProof
                state
                [request]
                [funding]
                (toHex trieRoot)
                [trieFact]
            )

-- | Mixed inclusion + exclusion TrieFacts against a shared
-- MPF root.
honestUpdateResponseMixedTrie :: UpdateTxResponse
honestUpdateResponseMixedTrie =
    let (trieRoot, inclusionFact) = honestTrieInclusion
        (_, exclusionFact) = honestTrieExclusion
        witness = buildBundleWithStateRoot trieRoot
        state = bundleState witness
        request = bundleRequest witness
        request2 = bundleRequest2 witness
        funding = bundleFunding witness
    in  UpdateTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [state, request, request2, funding])
                []
                []
                [txOutTerm True [sampleStateAsset 1]]
                [ spendRedeemerFixture
                    0
                    ( spendModifyRedeemerTerm
                        [ updateActionTermFromProof inclusionFact
                        , updateActionTermFromProof exclusionFact
                        ]
                    )
                , spendRedeemerFixture
                    1
                    (spendContributeRedeemerTerm (txIn state))
                , spendRedeemerFixture
                    2
                    (spendContributeRedeemerTerm (txIn state))
                ]
            )
            (sampleSnapshot (bundleRoot witness))
            ( UpdateProof
                state
                [request, request2]
                [funding]
                (toHex trieRoot)
                [inclusionFact, exclusionFact]
            )

-- | Update response with an empty @trie_read@ list — the MPF
-- pass is a no-op; only the CSMT witnesses replay.
honestUpdateResponseEmptyTrie :: UpdateTxResponse
honestUpdateResponseEmptyTrie =
    let (trieRoot, _) = honestTrieInclusion
        witness = buildBundleWithStateRoot trieRoot
        state = bundleState witness
        request = bundleRequest witness
        funding = bundleFunding witness
    in  UpdateTxResponse
            ( txCborFromTxPartsWithRedeemers
                (map txIn [state, request, funding])
                []
                []
                [txOutTerm True [sampleStateAsset 1]]
                [ spendRedeemerFixture
                    0
                    (spendModifyRedeemerTerm [])
                , spendRedeemerFixture
                    1
                    (spendContributeRedeemerTerm (txIn state))
                ]
            )
            (sampleSnapshot (bundleRoot witness))
            ( UpdateProof
                state
                [request]
                [funding]
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
