-- |
-- Module      : Cardano.MPFS.Client.BootFactsSpec
-- Description : Unit tests for boot facts JSON and verification.
module Cardano.MPFS.Client.BootFactsSpec
    ( spec
    ) where

import Data.Aeson
    ( Value
    , decode
    , encode
    , object
    , (.=)
    )
import Data.Either (isRight)
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( UnsignedTxResponse (..)
    , UtxoEntry (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , UnverifiedPParams (..)
    )
import Cardano.MPFS.Client.Fixtures
    ( honestBootTrustedRoot
    , honestUnsignedBootResponse
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyBootFacts
    )
import Cardano.MPFS.Client.Verify.DSL (flipApiHexMidByte)

spec :: Spec
spec = describe "verifyBootFacts" $ do
    it "round-trips the boot facts JSON shape" $ do
        let facts = honestBootFacts
            encoded = encode facts
        decode encoded `shouldBe` Just facts
        decode encoded `shouldBe` Just (bootFactsJson facts)

    it "accepts honest facts with a matching trusted root" $ do
        verifyBootFacts honestBootTrustedRoot honestBootFacts
            `shouldSatisfy` isRight

    it "rejects a malformed snapshot root before construction" $ do
        let facts =
                honestBootFacts
                    { bfSnapshot =
                        (bfSnapshot honestBootFacts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyBootFacts honestBootTrustedRoot facts
            `shouldBe` Left
                (WrongHexLength "boot.snapshot.utxo_root" 32 1)

    it "rejects a trusted-root mismatch" $ do
        let TrustedRoot rootHex = honestBootTrustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        verifyBootFacts forged honestBootFacts
            `shouldBe` Left
                (TrustedRootMismatch "boot.snapshot.utxo_root")

    it "rejects a tampered wallet UTxO proof" $ do
        let entry = soleWalletUtxo honestBootFacts
            tampered =
                entry
                    { ueInclusionProof =
                        flipApiHexMidByte (ueInclusionProof entry)
                    }
            facts =
                honestBootFacts{bfWalletUtxos = [tampered]}
        case verifyBootFacts honestBootTrustedRoot facts of
            Left (CsmtReplayFailed path _) ->
                path
                    `shouldBe` "boot.wallet_utxos[0].inclusion_proof"
            other ->
                error ("expected CsmtReplayFailed, got " <> show other)

honestBootFacts :: BootFacts
honestBootFacts =
    BootFacts
        { bfSnapshot = utrSnapshot honestUnsignedBootResponse
        , bfWalletUtxos = utrInputs honestUnsignedBootResponse
        , bfProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor = Hex "\x82\x01\x02"
                }
        }

bootFactsJson :: BootFacts -> Value
bootFactsJson BootFacts{..} =
    object
        [ "snapshot" .= bfSnapshot
        , "wallet_utxos" .= bfWalletUtxos
        , "protocol_parameters"
            .= object
                [ "verified" .= uppVerified bfProtocolParameters
                , "cbor" .= uppCbor bfProtocolParameters
                ]
        ]

soleWalletUtxo :: BootFacts -> UtxoEntry
soleWalletUtxo BootFacts{bfWalletUtxos = [entry]} = entry
soleWalletUtxo BootFacts{bfWalletUtxos = entries} =
    error
        ( "BootFactsSpec: expected one wallet UTxO, got "
            <> show (length entries)
        )
