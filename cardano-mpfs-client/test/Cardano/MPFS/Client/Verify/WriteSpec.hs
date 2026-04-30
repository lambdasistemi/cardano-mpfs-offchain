-- |
-- Module      : Cardano.MPFS.Client.Verify.WriteSpec
-- Description : Unit tests for the uniform write-response verifier.
--
-- Exercises 'verifyUnsignedTxResponse' against the same CSMT fixture
-- bundle the legacy fixtures use, repackaged into the post-split
-- wire shape ('UtxoEntry' rather than 'WitnessedUtxo').
module Cardano.MPFS.Client.Verify.WriteSpec
    ( spec
    ) where

import Data.Bits (xor)
import Data.ByteString qualified as BS
import Data.Text qualified as T
import Test.Hspec (Spec, describe, it, shouldBe)

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types
    ( UnsignedTxResponse (..)
    , UtxoEntry (..)
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootTrustedRoot
    , honestUnsignedBootResponse
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Replay (VerifyError (..))
import Cardano.MPFS.Client.Verify.Write (verifyUnsignedTxResponse)

-- | Flip every bit of the byte half-way through the payload.
-- Targets the body of a CBOR proof rather than its outer
-- list-length tag, so a shallow CBOR parser still accepts the
-- structure but the hash chain breaks.
flipMidByte :: Wire.Hex -> Wire.Hex
flipMidByte (Wire.Hex bs)
    | BS.null bs = Wire.Hex bs
    | otherwise =
        let n = BS.length bs `div` 2
        in  Wire.Hex
                ( BS.take n bs
                    <> BS.singleton
                        (BS.index bs n `xor` 0xFF)
                    <> BS.drop (n + 1) bs
                )

spec :: Spec
spec = describe "verifyUnsignedTxResponse" $ do
    it "accepts the honest fixture" $ do
        verifyUnsignedTxResponse
            "boot"
            honestBootTrustedRoot
            honestUnsignedBootResponse
            `shouldBe` Right ()

    it "rejects a wrong trusted root with TrustedRootMismatch" $ do
        let TrustedRoot rootHex = honestBootTrustedRoot
            forged = TrustedRoot (flipMidByte rootHex)
        verifyUnsignedTxResponse
            "boot"
            forged
            honestUnsignedBootResponse
            `shouldBe` Left
                (TrustedRootMismatch "boot.snapshot.utxo_root")

    it "rejects a tampered inclusion proof" $ do
        let entry = soleInput honestUnsignedBootResponse
            tampered =
                entry
                    { ueInclusionProof =
                        flipMidByte (ueInclusionProof entry)
                    }
            response =
                honestUnsignedBootResponse{utrInputs = [tampered]}
        case verifyUnsignedTxResponse
            "boot"
            honestBootTrustedRoot
            response of
            Left (CsmtReplayFailed path _) ->
                path
                    `shouldBe` "boot.inputs[0].inclusion_proof"
            other ->
                error ("expected CsmtReplayFailed, got " <> show other)

    it "rejects a tampered txout_cbor" $ do
        let entry = soleInput honestUnsignedBootResponse
            tampered =
                entry
                    { ueTxOutCbor =
                        flipMidByte (ueTxOutCbor entry)
                    }
            response =
                honestUnsignedBootResponse{utrInputs = [tampered]}
        case verifyUnsignedTxResponse
            "boot"
            honestBootTrustedRoot
            response of
            Left (CsmtReplayFailed path reason) -> do
                path
                    `shouldBe` "boot.inputs[0].inclusion_proof"
                reason
                    `shouldBe` T.pack "value binding mismatch"
            other ->
                error ("expected CsmtReplayFailed, got " <> show other)
  where
    soleInput resp = case utrInputs resp of
        [e] -> e
        other ->
            error
                ( "WriteSpec: expected one input, got "
                    <> show (length other)
                )
