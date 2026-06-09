-- |
-- Module      : Cardano.MPFS.Client.Verify.ReadSpec
-- Description : Honest + forged corpus for the read-side verifiers.
--
-- Exercises 'verifyTokenState' (GET \/tokens\/:id) and
-- 'verifyTokenFacts' (GET \/tokens\/:id\/facts) on honest fixtures
-- built from the pure CSMT \/ MPF backends — each must accept — and
-- on hand-forged variants that must reject with a matching
-- 'VerifyError'.
module Cardano.MPFS.Client.Verify.ReadSpec (spec) where

import Control.Monad (void)
import Data.Bits (xor)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text.Encoding qualified as T
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
    ( Direction (..)
    , Standalone (StandaloneCSMTCol)
    )
import CSMT.Backend.Pure (runPureTransaction)
import CSMT.Core.CBOR (renderCompletenessProof)
import CSMT.Core.Hash
    ( Hash
    , byteStringToKey
    , renderHash
    )
import CSMT.Hashes (mkHash)
import CSMT.Proof.Completeness
    ( CompletenessProof
    , generateProof
    )
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , insertMHash
    )
import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import MPF.Hashes (renderMPFHash)
import MPF.Test.Lib (insertByteStringM, runMPFPure')
import MPF.Test.Lib qualified as MPFTest (getRootHashM)

import Cardano.Ledger.BaseTypes (Network (Testnet))
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( ChainPointJSON (..)
    , FactEntry (..)
    , FactsResponse (..)
    , RequestJSON (..)
    , RequestsResponse (..)
    , TokenIdJSON (..)
    , TokenResponse (..)
    , TokenSetWitness (..)
    , TokenStateJSON (..)
    , TokenUtxoEntry (..)
    , TokensResponse (..)
    , TxInJSON (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Ledger (Coin (..))
import Cardano.MPFS.Client.Bundle qualified as Bundle
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , computeScriptHash
    )
import Cardano.MPFS.Client.Cage.Identity
    ( cageSetPrefixFromCfg
    , requestSetPrefixFromCfg
    )
import Cardano.MPFS.Client.Fixtures
    ( bundleFunding
    , bundleRoot
    , honestWitness
    )
import Cardano.MPFS.Client.Snapshot qualified as Snap
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Read
    ( verifyTokenFacts
    , verifyTokenRequests
    , verifyTokenState
    , verifyTokens
    )
import Cardano.MPFS.Client.Verify.Replay (VerifyError (..))

spec :: Spec
spec = describe "Read-side verifiers" $ do
    describe "verifyTokens" $ do
        it "accepts an honest token listing with a complete state set" $ do
            cfg <- testCageConfig
            let TokenListFixture{tokenListTrustedRoot, tokenListResponse} =
                    honestTokenListFixture cfg
            verifyTokensUnit cfg tokenListTrustedRoot tokenListResponse
                `shouldBe` Right ()
        it "rejects a tampered token-set completeness proof" $ do
            cfg <- testCageConfig
            let TokenListFixture{tokenListTrustedRoot, tokenListResponse} =
                    honestTokenListFixture cfg
            verifyTokensUnit
                cfg
                tokenListTrustedRoot
                (tamperTokenSetProof tokenListResponse)
                `shouldSatisfy` isCompletenessProofInvalid
        it "rejects a dropped token-set entry" $ do
            cfg <- testCageConfig
            let TokenListFixture{tokenListTrustedRoot, tokenListResponse} =
                    honestTokenListFixture cfg
            verifyTokensUnit
                cfg
                tokenListTrustedRoot
                (dropFirstTokenSetEntry tokenListResponse)
                `shouldSatisfy` isCompletenessProofInvalid
    describe "verifyTokenState" $ do
        it "accepts an honest token response"
            $ verifyTokenStateUnit honestTrustedRoot honestTokenResponse
            `shouldBe` Right ()
        it "rejects a snapshot root that is not the trusted root"
            $ verifyTokenStateUnit foreignTrustedRoot honestTokenResponse
            `shouldSatisfy` isTrustedRootMismatch
        it "rejects a tampered state tx_out (broken UTxO proof)"
            $ verifyTokenStateUnit
                honestTrustedRoot
                (tamperStateTxOut honestTokenResponse)
            `shouldSatisfy` isCsmtReplayFailed
        it "rejects a tampered state inclusion proof"
            $ verifyTokenStateUnit
                honestTrustedRoot
                (tamperStateProof honestTokenResponse)
            `shouldSatisfy` isCsmtReplayFailed
    describe "verifyTokenFacts" $ do
        it "accepts an honest facts response with a complete fact set"
            $ verifyTokenFactsUnit honestTrustedRoot honestFactsResponse
            `shouldBe` Right ()
        it "rejects a snapshot root that is not the trusted root"
            $ verifyTokenFactsUnit foreignTrustedRoot honestFactsResponse
            `shouldSatisfy` isTrustedRootMismatch
        it "rejects a dropped fact (incomplete set)"
            $ verifyTokenFactsUnit
                honestTrustedRoot
                (dropFirstFact honestFactsResponse)
            `shouldSatisfy` isMpfReplayFailed
        it "rejects an added spurious fact"
            $ verifyTokenFactsUnit
                honestTrustedRoot
                (addSpuriousFact honestFactsResponse)
            `shouldSatisfy` isMpfReplayFailed
        it "rejects a tampered fact value"
            $ verifyTokenFactsUnit
                honestTrustedRoot
                (tamperFirstFactValue honestFactsResponse)
            `shouldSatisfy` isMpfReplayFailed
    describe "verifyTokenRequests" $ do
        it "accepts an honest requests response with a complete request set"
            $ do
                cfg <- testCageConfig
                let RequestFixture{requestTrustedRoot, requestResponse} =
                        honestRequestFixture cfg
                verifyTokenRequestsUnit
                    cfg
                    sampleToken
                    requestTrustedRoot
                    requestResponse
                    `shouldBe` Right ()
        it "rejects a tampered request-set completeness proof" $ do
            cfg <- testCageConfig
            let RequestFixture{requestTrustedRoot, requestResponse} =
                    honestRequestFixture cfg
            verifyTokenRequestsUnit
                cfg
                sampleToken
                requestTrustedRoot
                (tamperRequestSetProof requestResponse)
                `shouldSatisfy` isCompletenessProofInvalid
        it "rejects a dropped request-set entry" $ do
            cfg <- testCageConfig
            let RequestFixture{requestTrustedRoot, requestResponse} =
                    honestRequestFixture cfg
            verifyTokenRequestsUnit
                cfg
                sampleToken
                requestTrustedRoot
                (dropFirstRequestSetEntry requestResponse)
                `shouldSatisfy` isCompletenessProofInvalid

-- | Discard the opaque witness so we can assert on @Right ()@.
verifyTokensUnit
    :: CageConfig
    -> TrustedRoot
    -> TokensResponse
    -> Either VerifyError ()
verifyTokensUnit cfg trusted =
    void . verifyTokens cfg trusted

verifyTokenStateUnit
    :: TrustedRoot -> TokenResponse -> Either VerifyError ()
verifyTokenStateUnit trusted =
    void . verifyTokenState trusted

verifyTokenFactsUnit
    :: TrustedRoot -> FactsResponse -> Either VerifyError ()
verifyTokenFactsUnit trusted =
    void . verifyTokenFacts trusted

verifyTokenRequestsUnit
    :: CageConfig
    -> TokenIdJSON
    -> TrustedRoot
    -> RequestsResponse
    -> Either VerifyError ()
verifyTokenRequestsUnit cfg token trusted =
    void . verifyTokenRequests cfg token trusted

-- | The complete fact set for the honest token. The on-chain trie
-- root is derived from these via the real MPF write backend, so the
-- verifier's independent reconstruction must reproduce it.
factEntries :: [(BS.ByteString, BS.ByteString)]
factEntries =
    [ ("apple", "fruit")
    , ("banana", "yellow")
    , ("cherry", "red")
    ]

-- | The MPF trie root of 'factEntries', computed by the genuine write
-- backend (not the verifier's reconstruction path).
honestFactsRoot :: BS.ByteString
honestFactsRoot = fst $ runMPFPure' $ do
    mapM_ (uncurry insertByteStringM) factEntries
    maybe BS.empty renderMPFHash <$> MPFTest.getRootHashM

honestFactsResponse :: FactsResponse
honestFactsResponse =
    FactsResponse
        { frsSnapshot = honestSnapshot
        , frsState =
            WitnessedTokenState
                { wtsUtxo =
                    toApiWitnessedUtxo (bundleFunding honestWitness)
                , wtsState =
                    TokenStateJSON
                        { owner = "owner"
                        , root = Hex honestFactsRoot
                        , tip = 1000000
                        , processTime = 60000
                        , retractTime = 30000
                        }
                }
        , frsFacts =
            [FactEntry{feKey = Hex k, feValue = Hex v} | (k, v) <- factEntries]
        }

-- | Drop a fact so the enumerated set is incomplete.
dropFirstFact :: FactsResponse -> FactsResponse
dropFirstFact resp = resp{frsFacts = drop 1 (frsFacts resp)}

-- | Add a fact that is not in the on-chain trie.
addSpuriousFact :: FactsResponse -> FactsResponse
addSpuriousFact resp =
    resp
        { frsFacts =
            FactEntry{feKey = Hex "durian", feValue = Hex "spiky"}
                : frsFacts resp
        }

-- | Tamper the value of the first fact, so its trie leaf — and thus
-- the reconstructed root — diverges.
tamperFirstFactValue :: FactsResponse -> FactsResponse
tamperFirstFactValue resp =
    resp{frsFacts = overFirst bumpValue (frsFacts resp)}
  where
    bumpValue entry = entry{feValue = flipHexByte (feValue entry)}
    overFirst _ [] = []
    overFirst f (x : xs) = f x : xs

data RequestFixture = RequestFixture
    { requestTrustedRoot :: TrustedRoot
    , requestResponse :: RequestsResponse
    }

honestRequestFixture :: CageConfig -> RequestFixture
honestRequestFixture cfg =
    let requestPrefix = requestSetPrefixFromCfg cfg sampleToken
        (rootBytes, requestSet) = csmtRequestRows requestPrefix
        response =
            RequestsResponse
                { rrSnapshot = snapshotWithRoot rootBytes
                , rrRequestSet = requestSet
                , rrRequests =
                    [ WitnessedRequest
                        { wrUtxo =
                            WitnessedUtxo
                                { wuTxIn =
                                    TxInJSON
                                        { tjTxId = Hex requestTxId
                                        , tjTxIx = 0
                                        }
                                , wuTxOut = Hex requestTxOut
                                , wuProof = Hex "\x82\x01\x02"
                                }
                        , wrRequest =
                            RequestJSON
                                { rjToken = sampleToken
                                , rjOwner = "owner"
                                , rjKey = Hex "apple"
                                , rjOperation = "insert"
                                , rjValue = Just (Hex "fruit")
                                , rjFee = 1000000
                                , rjSubmittedAt = 42
                                }
                        }
                    ]
                }
    in  RequestFixture
            { requestTrustedRoot = TrustedRoot (Hex rootBytes)
            , requestResponse = response
            }

csmtRequestRows :: [Direction] -> (ByteString, UtxoSetWitness)
csmtRequestRows requestPrefix = evalPureFromEmptyDB $ do
    let requestKey =
            requestPrefix
                <> byteStringToKey
                    (encodeTxIn requestTxId 0)
        siblingRows =
            [ (divergingKey 0 "root-sibling", "root-sibling-txout")
            , (divergingKey 127 "mid-sibling", "mid-sibling-txout")
            ]
    insertMHash requestKey (mkHash requestTxOut)
    mapM_ (\(key, txOut) -> insertMHash key (mkHash txOut)) siblingRows
    completenessProof <-
        runPureTransaction hashCodecs
            $ generateProof StandaloneCSMTCol [] requestPrefix
    rootBytes <- maybe BS.empty renderHash <$> getRootHashM
    pure
        ( rootBytes
        , UtxoSetWitness
            { uswEntries =
                [ UtxoEntryRefOnly
                    { uerRef =
                        UtxoRef
                            { urTxId = Hex requestTxId
                            , urTxIx = 0
                            }
                    , uerTxOutCbor = Hex requestTxOut
                    }
                ]
            , uswCompletenessProof =
                Hex
                    $ case completenessProof of
                        Just proof ->
                            renderCompletenessProof
                                (proof :: CompletenessProof Hash)
                        Nothing ->
                            error
                                "expected request-set completeness proof"
            }
        )
  where
    divergingKey n suffix =
        case splitAt n requestPrefix of
            (before, direction : _) ->
                before <> [otherDirection direction] <> byteStringToKey suffix
            _ -> byteStringToKey suffix

    otherDirection L = R
    otherDirection R = L

tamperRequestSetProof :: RequestsResponse -> RequestsResponse
tamperRequestSetProof resp =
    resp
        { rrRequestSet =
            (rrRequestSet resp)
                { uswCompletenessProof =
                    flipLastHexByte
                        (uswCompletenessProof (rrRequestSet resp))
                }
        }

dropFirstRequestSetEntry :: RequestsResponse -> RequestsResponse
dropFirstRequestSetEntry resp =
    resp
        { rrRequestSet =
            (rrRequestSet resp)
                { uswEntries =
                    drop 1 (uswEntries (rrRequestSet resp))
                }
        }

data TokenListFixture = TokenListFixture
    { tokenListTrustedRoot :: TrustedRoot
    , tokenListResponse :: TokensResponse
    }

honestTokenListFixture :: CageConfig -> TokenListFixture
honestTokenListFixture cfg =
    let (rootBytes, tokenSet) = csmtRequestRows (cageSetPrefixFromCfg cfg)
        response =
            TokensResponse
                { trsSnapshot = snapshotWithRoot rootBytes
                , trsTokens =
                    TokenSetWitness
                        { tswEntries =
                            [ TokenUtxoEntry
                                { tueTokenId = sampleToken
                                , tueRef =
                                    UtxoRef
                                        { urTxId = Hex requestTxId
                                        , urTxIx = 0
                                        }
                                , tueTxOutCbor = Hex requestTxOut
                                }
                            ]
                        , tswCompletenessProof =
                            uswCompletenessProof tokenSet
                        }
                }
    in  TokenListFixture
            { tokenListTrustedRoot = TrustedRoot (Hex rootBytes)
            , tokenListResponse = response
            }

tamperTokenSetProof :: TokensResponse -> TokensResponse
tamperTokenSetProof resp =
    resp
        { trsTokens =
            (trsTokens resp)
                { tswCompletenessProof =
                    flipLastHexByte
                        (tswCompletenessProof (trsTokens resp))
                }
        }

dropFirstTokenSetEntry :: TokensResponse -> TokensResponse
dropFirstTokenSetEntry resp =
    resp
        { trsTokens =
            (trsTokens resp)
                { tswEntries =
                    drop 1 (tswEntries (trsTokens resp))
                }
        }

honestTrustedRoot :: TrustedRoot
honestTrustedRoot = TrustedRoot (Hex (bundleRoot honestWitness))

foreignTrustedRoot :: TrustedRoot
foreignTrustedRoot = TrustedRoot (Hex (BS.replicate 32 0x2a))

honestTokenResponse :: TokenResponse
honestTokenResponse =
    TokenResponse
        { trSnapshot = honestSnapshot
        , trState =
            WitnessedTokenState
                { wtsUtxo =
                    toApiWitnessedUtxo (bundleFunding honestWitness)
                , wtsState =
                    TokenStateJSON
                        { owner = "owner"
                        , root = Hex (BS.replicate 32 0x00)
                        , tip = 1000000
                        , processTime = 60000
                        , retractTime = 30000
                        }
                }
        }

honestSnapshot :: VerificationSnapshot
honestSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex (bundleRoot honestWitness)
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 42
                , cpBlockId = Hex (BS.replicate 32 0x11)
                }
        }

snapshotWithRoot :: ByteString -> VerificationSnapshot
snapshotWithRoot rootBytes =
    VerificationSnapshot
        { vsUtxoRoot = Hex rootBytes
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 42
                , cpBlockId = Hex (BS.replicate 32 0x11)
                }
        }

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON "cafe"

requestTxId :: ByteString
requestTxId = BS.replicate 32 0x33

requestTxOut :: ByteString
requestTxOut = "request-tx-out-cbor"

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
    pure
        CageConfig
            { cageScriptBytes = scriptBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = computeScriptHash scriptBytes
            , defaultProcessTime = 60_000
            , defaultRetractTime = 30_000
            , defaultTip = Coin 1_000_000
            , network = Testnet
            }

-- | Flip a byte in the state UTxO's @tx_out@ so the advertised value
-- no longer matches the value bound into the CSMT inclusion proof.
tamperStateTxOut :: TokenResponse -> TokenResponse
tamperStateTxOut = overStateUtxo $ \u ->
    u{wuTxOut = flipHexByte (wuTxOut u)}

-- | Flip the last byte of the state UTxO's inclusion proof. The
-- trailing bytes carry sibling-hash material, so the recomputed root
-- no longer matches the trusted root (flipping the leading CBOR header
-- byte would not).
tamperStateProof :: TokenResponse -> TokenResponse
tamperStateProof = overStateUtxo $ \u ->
    u{wuProof = flipLastHexByte (wuProof u)}

overStateUtxo
    :: (WitnessedUtxo -> WitnessedUtxo) -> TokenResponse -> TokenResponse
overStateUtxo f resp =
    resp
        { trState =
            (trState resp)
                { wtsUtxo = f (wtsUtxo (trState resp))
                }
        }

isTrustedRootMismatch :: Either VerifyError () -> Bool
isTrustedRootMismatch (Left (TrustedRootMismatch _)) = True
isTrustedRootMismatch _ = False

isCsmtReplayFailed :: Either VerifyError () -> Bool
isCsmtReplayFailed (Left (CsmtReplayFailed _ _)) = True
isCsmtReplayFailed _ = False

isMpfReplayFailed :: Either VerifyError () -> Bool
isMpfReplayFailed (Left (MpfReplayFailed _ _)) = True
isMpfReplayFailed _ = False

isCompletenessProofInvalid :: Either VerifyError () -> Bool
isCompletenessProofInvalid (Left (CompletenessProofInvalid _)) = True
isCompletenessProofInvalid _ = False

-- | Flip the first byte of a hex-wrapped bytestring.
flipHexByte :: Hex -> Hex
flipHexByte (Hex bs) =
    case BS.uncons bs of
        Just (b, rest) -> Hex (BS.cons (b `xor` 0x01) rest)
        Nothing -> Hex bs

-- | Flip the last byte of a hex-wrapped bytestring.
flipLastHexByte :: Hex -> Hex
flipLastHexByte (Hex bs) =
    case BS.unsnoc bs of
        Just (initBs, b) -> Hex (BS.snoc initBs (b `xor` 0x01))
        Nothing -> Hex bs

toApiWitnessedUtxo :: Bundle.WitnessedUtxo -> WitnessedUtxo
toApiWitnessedUtxo
    Bundle.WitnessedUtxo
        { Bundle.txIn =
            Bundle.TxIn{Bundle.txId = txId', Bundle.txIx = txIx'}
        , Bundle.txOut = txOut'
        , Bundle.utxoProof = utxoProof'
        } =
        WitnessedUtxo
            { wuTxIn =
                TxInJSON
                    { tjTxId = clientToApiHex txId'
                    , tjTxIx = txIx'
                    }
            , wuTxOut = clientToApiHex txOut'
            , wuProof = clientToApiHex utxoProof'
            }

-- | The client 'Bundle' types carry hex as 'Text'; the API wire types
-- carry it as raw bytes. Decode across the boundary.
clientToApiHex :: Snap.Hex -> Hex
clientToApiHex (Snap.Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Hex bs
        Left err -> error ("ReadSpec.clientToApiHex: " <> err)
