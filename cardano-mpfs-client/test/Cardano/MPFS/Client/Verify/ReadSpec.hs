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
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text.Encoding qualified as T
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)

import MPF.Hashes (renderMPFHash)
import MPF.Test.Lib (insertByteStringM, runMPFPure')
import MPF.Test.Lib qualified as MPFTest (getRootHashM)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( ChainPointJSON (..)
    , FactEntry (..)
    , FactsResponse (..)
    , TokenResponse (..)
    , TokenStateJSON (..)
    , TxInJSON (..)
    , VerificationSnapshot (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Bundle qualified as Bundle
import Cardano.MPFS.Client.Fixtures
    ( bundleFunding
    , bundleRoot
    , honestWitness
    )
import Cardano.MPFS.Client.Snapshot qualified as Snap
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Read
    ( verifyTokenFacts
    , verifyTokenState
    )
import Cardano.MPFS.Client.Verify.Replay (VerifyError (..))

spec :: Spec
spec = describe "Read-side verifiers" $ do
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

-- | Discard the opaque witness so we can assert on @Right ()@.
verifyTokenStateUnit
    :: TrustedRoot -> TokenResponse -> Either VerifyError ()
verifyTokenStateUnit trusted =
    void . verifyTokenState trusted

verifyTokenFactsUnit
    :: TrustedRoot -> FactsResponse -> Either VerifyError ()
verifyTokenFactsUnit trusted =
    void . verifyTokenFacts trusted

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
