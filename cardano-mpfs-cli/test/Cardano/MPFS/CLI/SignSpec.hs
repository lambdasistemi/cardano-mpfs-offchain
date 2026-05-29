{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.CLI.SignSpec
-- Description : Bech32 key loading + local signing tests.
module Cardano.MPFS.CLI.SignSpec
    ( spec
    ) where

import Cardano.Crypto.DSIGN
    ( Ed25519DSIGN
    , SignKeyDSIGN
    , deriveVerKeyDSIGN
    , genKeyDSIGN
    , rawSerialiseSignKeyDSIGN
    , rawSerialiseVerKeyDSIGN
    )
import Cardano.Crypto.Seed (mkSeedFromBytes)
import Cardano.Ledger.Api.Tx (addrTxWitsL, mkBasicTx, witsTxL)
import Cardano.Ledger.Api.Tx.Body (mkBasicTxBody)
import Cardano.Ledger.Binary
    ( decodeFull'
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Keys (VKey (..), WitVKey (..))
import Cardano.MPFS.CLI.Key
    ( KeyError (..)
    , decodeSigningKey
    )
import Cardano.MPFS.CLI.Sign (ConwayTx, signTx)
import Codec.Binary.Bech32
    ( dataPartFromBytes
    , encodeLenient
    , humanReadablePartFromText
    )
import Data.ByteString qualified as BS
import Data.Set qualified as Set
import Data.Text (Text)
import Lens.Micro ((^.))
import Test.Hspec

-- | A deterministic key for tests.
testKey :: SignKeyDSIGN Ed25519DSIGN
testKey = genKeyDSIGN (mkSeedFromBytes (BS.replicate 32 7))

-- | Encode a signing key as a CIP-5 @ed25519_sk1…@ Bech32 string.
encodeKey :: SignKeyDSIGN Ed25519DSIGN -> Text
encodeKey sk =
    case humanReadablePartFromText "ed25519_sk" of
        Left err -> error ("bad hrp: " <> show err)
        Right hrp ->
            encodeLenient
                hrp
                (dataPartFromBytes (rawSerialiseSignKeyDSIGN sk))

-- | An empty unsigned Conway transaction, serialized.
unsignedTx :: BS.ByteString
unsignedTx =
    serialize' (natVersion @11) (mkBasicTx mkBasicTxBody :: ConwayTx)

spec :: Spec
spec = do
    describe "decodeSigningKey" $ do
        it "round-trips a Bech32-encoded ed25519 key"
            $ (rawSerialiseSignKeyDSIGN <$> decodeSigningKey (encodeKey testKey))
            `shouldBe` Right (rawSerialiseSignKeyDSIGN testKey)
        it "rejects empty content"
            $ decodeSigningKey "   "
            `shouldBe` Left KeyFileEmpty
        it "rejects non-Bech32 content"
            $ case decodeSigningKey "this is not bech32" of
                Left (KeyBech32Error _) -> pure ()
                other -> expectationFailure ("expected KeyBech32Error, got " <> show other)

    describe "signTx" $ do
        it "adds a vkey witness for the signing key"
            $ case signTx testKey unsignedTx of
                Left err -> expectationFailure ("signTx failed: " <> show err)
                Right signedCbor ->
                    case decodeFull' (natVersion @11) signedCbor of
                        Left err ->
                            expectationFailure ("re-decode failed: " <> show err)
                        Right (signedTx :: ConwayTx) ->
                            case Set.toList (signedTx ^. witsTxL . addrTxWitsL) of
                                [WitVKey (VKey vk) _] ->
                                    rawSerialiseVerKeyDSIGN vk
                                        `shouldBe` rawSerialiseVerKeyDSIGN
                                            (deriveVerKeyDSIGN testKey)
                                wits ->
                                    expectationFailure
                                        ( "expected exactly one witness, got "
                                            <> show (length wits)
                                        )
        it "fails to sign undecodable CBOR"
            $ case signTx testKey (BS.pack [0xff, 0x00, 0x13]) of
                Left _ -> pure ()
                Right _ -> expectationFailure "expected a decode failure"
