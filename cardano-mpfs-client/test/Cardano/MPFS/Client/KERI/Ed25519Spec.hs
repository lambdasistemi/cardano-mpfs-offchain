-- |
-- Module      : Cardano.MPFS.Client.KERI.Ed25519Spec
-- Description : Spike (#365) — portable Ed25519 verify + CESR round-trip.
--
-- Fixed test vectors prove byte-identical results across native, WASM,
-- and GHC-JS without any libsodium FFI.  The public key and signature
-- are generated deterministically so the vector is fully reproducible.
module Cardano.MPFS.Client.KERI.Ed25519Spec
    ( spec
    ) where

import Cardano.Crypto.DSIGN
    ( Ed25519DSIGN
    , SigDSIGN
    , SignKeyDSIGN
    , deriveVerKeyDSIGN
    , genKeyDSIGN
    , rawSerialiseSigDSIGN
    , rawSerialiseVerKeyDSIGN
    , signDSIGN
    )
import Cardano.Crypto.Seed (mkSeedFromBytes)
import Cardano.MPFS.Client.KERI.CESR
    ( Primitive (..)
    , parsePrimitive
    )
import Cardano.MPFS.Client.KERI.Ed25519 (verifyEd25519)
import Data.ByteArray.Encoding
    ( Base (Base64URLUnpadded)
    , convertToBase
    )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Test.Hspec

-- Deterministic key for the spike vector (seed = 0x01 * 32).
testSK :: SignKeyDSIGN Ed25519DSIGN
testSK = genKeyDSIGN (mkSeedFromBytes (BS.replicate 32 0x01))

testPKBytes :: ByteString
testPKBytes = rawSerialiseVerKeyDSIGN (deriveVerKeyDSIGN testSK)

testMsg :: ByteString
testMsg = "KERI spike #365 fixed vector"

testSigBytes :: ByteString
testSigBytes =
    rawSerialiseSigDSIGN
        (signDSIGN () testMsg testSK :: SigDSIGN Ed25519DSIGN)

spec :: Spec
spec = do
    describe "KERI.Ed25519 verifyEd25519" $ do
        it "accepts the spike test vector"
            $ verifyEd25519 testPKBytes testMsg testSigBytes
            `shouldBe` True
        it "rejects a mutated message"
            $ verifyEd25519 testPKBytes (testMsg <> "x") testSigBytes
            `shouldBe` False
        it "rejects a mutated signature"
            $ let bad = BS.cons 0x00 (BS.drop 1 testSigBytes)
              in  verifyEd25519 testPKBytes testMsg bad
                    `shouldBe` False
        it "rejects a 31-byte (malformed) public key"
            $ verifyEd25519 (BS.take 31 testPKBytes) testMsg testSigBytes
            `shouldBe` False

    describe "KERI.CESR parsePrimitive" $ do
        it "round-trips an Ed25519 public key" $ do
            let encoded = encodePubKey testPKBytes
            case parsePrimitive encoded of
                Right (Ed25519PublicKey bs, "") -> bs `shouldBe` testPKBytes
                other -> expectationFailure ("unexpected: " <> show other)

        it "round-trips an Ed25519 signature" $ do
            let encoded = encodeSig testSigBytes
            case parsePrimitive encoded of
                Right (Ed25519Signature bs, "") -> bs `shouldBe` testSigBytes
                other -> expectationFailure ("unexpected: " <> show other)

        it "returns the remainder" $ do
            let encoded = encodePubKey testPKBytes <> "EXTRA"
            case parsePrimitive encoded of
                Right (Ed25519PublicKey _, rest) -> rest `shouldBe` "EXTRA"
                other -> expectationFailure ("unexpected: " <> show other)

-- Encode 32 raw bytes as a CESR Ed25519 public key (code 'B', 44 chars).
encodePubKey :: ByteString -> ByteString
encodePubKey raw =
    let padded = BS.cons 0x00 raw
        b64 = encodeB64Url padded
    in  BS.cons 0x42 (BS.tail b64)

-- Encode 64 raw bytes as a CESR Ed25519 signature (code '0B', 88 chars).
encodeSig :: ByteString -> ByteString
encodeSig raw =
    let padded = BS.pack [0x00, 0x00] <> raw
        b64 = encodeB64Url padded
    in  BS.pack [0x30, 0x42] <> BS.drop 2 b64

encodeB64Url :: ByteString -> ByteString
encodeB64Url bs = convertToBase Base64URLUnpadded (bs :: ByteString) :: ByteString
