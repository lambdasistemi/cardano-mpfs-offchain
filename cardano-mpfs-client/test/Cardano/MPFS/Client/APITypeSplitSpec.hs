-- |
-- Module      : Cardano.MPFS.Client.APITypeSplitSpec
-- Description : Import smoke test for focused API DTO modules.
module Cardano.MPFS.Client.APITypeSplitSpec
    ( spec
    ) where

import Data.Aeson (decode, encode)
import Data.ByteString qualified as BS
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( BootFacts (..)
    )

spec :: Spec
spec = describe "API type split" $ do
    it "constructs facts DTOs from focused API modules" $ do
        decode (encode bootFacts) `shouldBe` Just bootFacts
        decode (encode utxoSetWitness) `shouldBe` Just utxoSetWitness
        decode (encode tokenId) `shouldBe` Just tokenId

tokenId :: TokenIdJSON
tokenId = TokenIdJSON (BS.replicate 32 0x05)

bootFacts :: BootFacts
bootFacts =
    BootFacts
        { bfSnapshot =
            VerificationSnapshot
                { vsUtxoRoot = Hex (BS.replicate 32 0x01)
                , vsChainPoint =
                    ChainPointJSON
                        { cpSlot = 42
                        , cpBlockId = Hex (BS.replicate 32 0x02)
                        }
                }
        , bfWalletUtxos = [utxoEntry]
        , bfProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor = Hex "\x82\x01\x02"
                }
        }

utxoEntry :: UtxoEntry
utxoEntry =
    UtxoEntry
        { ueRef = utxoRef
        , ueTxOutCbor = Hex "\x82\x01\x02"
        , ueInclusionProof = Hex "\x83\x03\x04"
        }

utxoSetWitness :: UtxoSetWitness
utxoSetWitness =
    UtxoSetWitness
        { uswEntries =
            [ UtxoEntryRefOnly
                { uerRef = utxoRef
                , uerTxOutCbor = Hex "\x82\x01\x02"
                }
            ]
        , uswCompletenessProof = Hex "\x84\x05\x06"
        }

utxoRef :: UtxoRef
utxoRef =
    UtxoRef
        { urTxId = Hex (BS.replicate 32 0x03)
        , urTxIx = 1
        }
