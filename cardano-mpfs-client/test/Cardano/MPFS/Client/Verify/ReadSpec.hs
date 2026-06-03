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

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( ChainPointJSON (..)
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
    ( verifyTokenState
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

-- | Discard the opaque witness so we can assert on @Right ()@.
verifyTokenStateUnit
    :: TrustedRoot -> TokenResponse -> Either VerifyError ()
verifyTokenStateUnit trusted =
    void . verifyTokenState trusted

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
