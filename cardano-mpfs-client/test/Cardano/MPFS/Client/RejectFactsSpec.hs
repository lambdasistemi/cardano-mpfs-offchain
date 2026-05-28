-- |
-- Module      : Cardano.MPFS.Client.RejectFactsSpec
-- Description : Unit tests for the reject facts verifier.
module Cardano.MPFS.Client.RejectFactsSpec
    ( spec
    ) where

import Control.Monad (void)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Either (isRight)
import Data.Text.Encoding qualified as Text
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.MPFS.API.Encoding
    ( Hex (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RejectFacts (..)
    )
import Cardano.MPFS.Client.Bundle qualified as ClientWire
import Cardano.MPFS.Client.Facts
    ( VerifiedRejectFacts
    , verifiedRejectFacts
    )
import Cardano.MPFS.Client.Fixtures
    ( honestRejectResponse
    )
import Cardano.MPFS.Client.Snapshot qualified as ClientSnapshot
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyRejectFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( csmtReplayFailedAt
    , flipApiHexMidByte
    , flipProof
    , runForgeRejectFacts
    , shouldRejectWith
    , trustedRootMismatchAt
    )

spec :: Spec
spec = describe "verifyRejectFacts" $ do
    it "accepts honest reject facts with matching roots" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
        verifyRejectFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
        verified <- expectVerified trustedRoot facts
        verifiedRejectFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged =
                facts
                    { rfSnapshot =
                        (rfSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyRejectFacts trustedRoot forged
            `shouldBe` Left
                (WrongHexLength "reject.snapshot.utxo_root" 32 1)

    it "rejects a trusted-root mismatch" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        facts
            `shouldRejectWith` verifyRejectUnit forged
            $ trustedRootMismatchAt "reject.snapshot.utxo_root"

    it "rejects an empty request batch" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged = facts{rfRequestUtxos = []}
        verifyRejectFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "reject.request_utxos"
                    "must not be empty"
                )

    it "rejects a stale validity lower slot" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            snapSlot =
                fromIntegral
                    $ cpSlot
                    $ vsChainPoint
                    $ rfSnapshot facts
            forged = facts{rfValidityLowerSlot = snapSlot}
        verifyRejectFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "reject.validity_lower_slot"
                    "must be greater than the snapshot slot"
                )

    it "rejects upper slot at or below lower slot" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged =
                facts
                    { rfValidityUpperSlot =
                        rfValidityLowerSlot facts
                    }
        verifyRejectFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "reject.validity_upper_slot"
                    "must be greater than the lower slot"
                )

    it "rejects an out-of-horizon validity upper slot" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            snapSlot =
                fromIntegral
                    $ cpSlot
                    $ vsChainPoint
                    $ rfSnapshot facts
            -- horizon is 60 + 600 = 660 slots
            forged =
                facts
                    { rfValidityUpperSlot =
                        snapSlot + 660 + 1
                    }
        verifyRejectFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "reject.validity_upper_slot"
                    "too far beyond the snapshot slot"
                )

    it "rejects a tampered state UTxO inclusion proof" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged =
                runForgeRejectFacts
                    (flipProof "state_utxo")
                    facts
        forged
            `shouldRejectWith` verifyRejectUnit trustedRoot
            $ csmtReplayFailedAt
                "reject.state_utxo.inclusion_proof"

    it "rejects a tampered request UTxO inclusion proof" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged =
                runForgeRejectFacts
                    (flipProof "request_utxos[0]")
                    facts
        forged
            `shouldRejectWith` verifyRejectUnit trustedRoot
            $ csmtReplayFailedAt
                "reject.request_utxos[0].inclusion_proof"

    it "rejects a tampered wallet UTxO inclusion proof" $ do
        let RejectFactsFixture{trustedRoot, facts} =
                honestRejectFactsFixture
            forged =
                runForgeRejectFacts
                    (flipProof "wallet_utxos[0]")
                    facts
        forged
            `shouldRejectWith` verifyRejectUnit trustedRoot
            $ csmtReplayFailedAt
                "reject.wallet_utxos[0].inclusion_proof"

-- ---------------------------------------------------------------
-- Fixture
-- ---------------------------------------------------------------

data RejectFactsFixture = RejectFactsFixture
    { trustedRoot :: TrustedRoot
    , facts :: RejectFacts
    }

honestRejectFactsFixture :: RejectFactsFixture
honestRejectFactsFixture =
    let ClientWire.RejectTxResponse
            _
            snapshot
            (ClientWire.RejectProof st rs fs) =
                honestRejectResponse
        apiSnapshot = toApiSnapshot snapshot
        snapSlot =
            fromIntegral
                $ ClientSnapshot.slot
                $ ClientSnapshot.chainpoint snapshot
        rejectFacts =
            RejectFacts
                { rfSnapshot = apiSnapshot
                , rfToken = sampleToken
                , rfStateUtxo = toApiUtxoEntry st
                , rfRequestUtxos = toApiUtxoEntry <$> rs
                , rfWalletUtxos = toApiUtxoEntry <$> fs
                , rfValidityLowerSlot = snapSlot + 50
                , rfValidityUpperSlot = snapSlot + 150
                , rfProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  RejectFactsFixture
            { trustedRoot = TrustedRoot (vsUtxoRoot apiSnapshot)
            , facts = rejectFacts
            }

expectVerified
    :: TrustedRoot
    -> RejectFacts
    -> IO VerifiedRejectFacts
expectVerified trusted facts =
    case verifyRejectFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRejectFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

verifyRejectUnit
    :: TrustedRoot -> RejectFacts -> Either VerifyError ()
verifyRejectUnit trusted =
    void . verifyRejectFacts trusted

toApiSnapshot
    :: ClientSnapshot.VerificationSnapshot -> VerificationSnapshot
toApiSnapshot snapshot =
    let chainPoint = ClientSnapshot.chainpoint snapshot
    in  VerificationSnapshot
            { vsUtxoRoot =
                toApiHex (ClientSnapshot.utxoRoot snapshot)
            , vsChainPoint =
                ChainPointJSON
                    { cpSlot = ClientSnapshot.slot chainPoint
                    , cpBlockId =
                        toApiHex (ClientSnapshot.blockId chainPoint)
                    }
            }

toApiUtxoEntry :: ClientWire.WitnessedUtxo -> UtxoEntry
toApiUtxoEntry witness =
    let txIn = ClientWire.txIn witness
    in  UtxoEntry
            { ueRef =
                UtxoRef
                    { urTxId = toApiHex (ClientWire.txId txIn)
                    , urTxIx = ClientWire.txIx txIn
                    }
            , ueTxOutCbor = toApiHex (ClientWire.txOut witness)
            , ueInclusionProof =
                toApiHex (ClientWire.utxoProof witness)
            }

toApiHex :: ClientSnapshot.Hex -> Hex
toApiHex (ClientSnapshot.Hex txt) =
    case Base16.decode (Text.encodeUtf8 txt) of
        Right bs -> Hex bs
        Left err ->
            error
                ( "RejectFactsSpec.toApiHex: malformed fixture hex: "
                    <> err
                )

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)
