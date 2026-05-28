-- |
-- Module      : Cardano.MPFS.Client.UpdateFactsSpec
-- Description : Unit tests for update facts verification.
module Cardano.MPFS.Client.UpdateFactsSpec
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
    ( TrieFact (..)
    , UpdateFacts (..)
    )
import Cardano.MPFS.Client.Bundle qualified as ClientWire
import Cardano.MPFS.Client.Facts
    ( VerifiedUpdateFacts
    , verifiedUpdateFacts
    )
import Cardano.MPFS.Client.Fixtures
    ( honestUpdateResponseMixedTrie
    )
import Cardano.MPFS.Client.Snapshot qualified as ClientSnapshot
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyUpdateFacts
    )
import Cardano.MPFS.Client.Verify.DSL
    ( csmtReplayFailedAt
    , flipApiHexMidByte
    , flipProof
    , flipTrieProof
    , flipTrieValue
    , mpfReplayFailedAt
    , runForgeUpdateFacts
    , runForgeUpdateFactsTrie
    , shouldRejectWith
    , trustedRootMismatchAt
    )

spec :: Spec
spec = describe "verifyUpdateFacts" $ do
    it "accepts honest update facts with matching roots" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
        verifyUpdateFacts trustedRoot facts
            `shouldSatisfy` isRight

    it "returns an opaque witness with an accessor" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
        verified <- expectVerified trustedRoot facts
        verifiedUpdateFacts verified `shouldBe` facts

    it "rejects a malformed snapshot root before replay" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                facts
                    { ufSnapshot =
                        (ufSnapshot facts)
                            { vsUtxoRoot = Hex "\x01"
                            }
                    }
        verifyUpdateFacts trustedRoot forged
            `shouldBe` Left
                (WrongHexLength "update.snapshot.utxo_root" 32 1)

    it "rejects a trusted-root mismatch" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            TrustedRoot rootHex = trustedRoot
            forged = TrustedRoot (flipApiHexMidByte rootHex)
        facts
            `shouldRejectWith` verifyUpdateUnit forged
            $ trustedRootMismatchAt "update.snapshot.utxo_root"

    it "rejects a malformed trie root before MPF replay" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged = facts{ufTrieRoot = Hex "\x01"}
        verifyUpdateFacts trustedRoot forged
            `shouldBe` Left
                (WrongHexLength "update.trie_root" 32 1)

    it "rejects an empty update request list" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged = facts{ufRequestUtxos = []}
        verifyUpdateFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "update.request_utxos"
                    "must not be empty"
                )

    it "rejects a stale validity upper slot" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                facts
                    { ufValidityUpperSlot =
                        fromIntegral
                            $ cpSlot
                            $ vsChainPoint
                            $ ufSnapshot facts
                    }
        verifyUpdateFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "update.validity_upper_slot"
                    "must be greater than the snapshot slot"
                )

    it "rejects an out-of-horizon validity upper slot" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            snapshotSlot =
                fromIntegral
                    $ cpSlot
                    $ vsChainPoint
                    $ ufSnapshot facts
            computedHorizon = 60 + 600
            forged =
                facts
                    { ufValidityUpperSlot =
                        snapshotSlot + computedHorizon + 1
                    }
        verifyUpdateFacts trustedRoot forged
            `shouldBe` Left
                ( TxBindingFailed
                    "update.validity_upper_slot"
                    "too far beyond the snapshot slot"
                )

    it "rejects a tampered state UTxO inclusion proof" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                runForgeUpdateFacts
                    (flipProof "state_utxo")
                    facts
        forged
            `shouldRejectWith` verifyUpdateUnit trustedRoot
            $ csmtReplayFailedAt
                "update.state_utxo.inclusion_proof"

    it "rejects a tampered request UTxO inclusion proof" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                runForgeUpdateFacts
                    (flipProof "request_utxos[1]")
                    facts
        forged
            `shouldRejectWith` verifyUpdateUnit trustedRoot
            $ csmtReplayFailedAt
                "update.request_utxos[1].inclusion_proof"

    it "rejects a tampered wallet UTxO inclusion proof" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                runForgeUpdateFacts
                    (flipProof "wallet_utxos[0]")
                    facts
        forged
            `shouldRejectWith` verifyUpdateUnit trustedRoot
            $ csmtReplayFailedAt
                "update.wallet_utxos[0].inclusion_proof"

    it "rejects a tampered MPF proof" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                runForgeUpdateFactsTrie
                    (flipTrieProof 1)
                    facts
        forged
            `shouldRejectWith` verifyUpdateUnit trustedRoot
            $ mpfReplayFailedAt
                "update.trie_facts[1].mpf_proof"

    it "rejects a tampered trie-fact value" $ do
        let UpdateFactsFixture{trustedRoot, facts} =
                honestUpdateFactsFixture
            forged =
                runForgeUpdateFactsTrie
                    (flipTrieValue 0)
                    facts
        forged
            `shouldRejectWith` verifyUpdateUnit trustedRoot
            $ mpfReplayFailedAt
                "update.trie_facts[0].mpf_proof"

data UpdateFactsFixture = UpdateFactsFixture
    { trustedRoot :: TrustedRoot
    , facts :: UpdateFacts
    }

honestUpdateFactsFixture :: UpdateFactsFixture
honestUpdateFactsFixture =
    let ClientWire.UpdateTxResponse
            _
            snapshot
            (ClientWire.UpdateProof st rs fs trieRoot trieFacts) =
                honestUpdateResponseMixedTrie
        apiSnapshot = toApiSnapshot snapshot
        updateFacts =
            UpdateFacts
                { ufSnapshot = apiSnapshot
                , ufToken = sampleToken
                , ufStateUtxo = toApiUtxoEntry st
                , ufRequestUtxos = toApiUtxoEntry <$> rs
                , ufWalletUtxos = toApiUtxoEntry <$> fs
                , ufTrieRoot = toApiHex trieRoot
                , ufTrieFacts = toApiTrieFact <$> trieFacts
                , ufValidityUpperSlot =
                    fromIntegral
                        $ ClientSnapshot.slot
                            (ClientSnapshot.chainpoint snapshot)
                            + 100
                , ufProtocolParameters =
                    UnverifiedPParams
                        { uppVerified = False
                        , uppCbor = Hex "\x82\x01\x02"
                        }
                }
    in  UpdateFactsFixture
            { trustedRoot = TrustedRoot (vsUtxoRoot apiSnapshot)
            , facts = updateFacts
            }

expectVerified
    :: TrustedRoot
    -> UpdateFacts
    -> IO VerifiedUpdateFacts
expectVerified trusted facts =
    case verifyUpdateFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyUpdateFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

verifyUpdateUnit
    :: TrustedRoot -> UpdateFacts -> Either VerifyError ()
verifyUpdateUnit trusted =
    void . verifyUpdateFacts trusted

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

toApiTrieFact :: ClientWire.TrieFact -> TrieFact
toApiTrieFact trieFact =
    TrieFact
        { tfKey = toApiHex (ClientWire.key trieFact)
        , tfValue = toApiHex <$> ClientWire.value trieFact
        , tfMpfProof = toApiHex (ClientWire.mpfProof trieFact)
        }

toApiHex :: ClientSnapshot.Hex -> Hex
toApiHex (ClientSnapshot.Hex txt) =
    case Base16.decode (Text.encodeUtf8 txt) of
        Right bs -> Hex bs
        Left err ->
            error
                ( "UpdateFactsSpec.toApiHex: malformed fixture hex: "
                    <> err
                )

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)
