{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.EvalSpec
-- Description : Unit tests for pure cage ex-unit evaluation context.
module Cardano.MPFS.Client.Cage.EvalSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Codec.Serialise qualified as Serialise
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    )

import Cardano.Ledger.Api.PParams (emptyPParams)
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Common
    ( EvalContext (..)
    , UnverifiedPParams (..)
    )
import Cardano.MPFS.Client.Cage.Eval
    ( DecodedEvalContext (..)
    , decodeEvalContext
    )
import Cardano.MPFS.Client.Cage.TestEvalContext
    ( testEvalPParams
    )
import Cardano.Slotting.EpochInfo qualified as EpochInfo
import Cardano.Slotting.Slot
    ( EpochNo (..)
    , SlotNo (..)
    )
import Cardano.Slotting.Time
    ( RelativeTime (..)
    , SystemStart (..)
    , slotLengthFromMillisec
    )

spec :: Spec
spec = describe "decodeEvalContext" $ do
    it "uses the live era history for slot-to-time conversion" $ do
        ctx <-
            case decodeEvalContext evalContextWithEraOffset of
                Left err ->
                    expectationFailure
                        ("decodeEvalContext failed: " <> show err)
                        *> error "unreachable"
                Right decoded -> pure decoded

        EpochInfo.epochInfoSlotToRelativeTime
            (evalEpochInfo ctx)
            (SlotNo 12)
            `shouldBe` Right (RelativeTime 104)
        EpochInfo.epochInfoEpoch
            (evalEpochInfo ctx)
            (SlotNo 12)
            `shouldBe` Right (EpochNo 1)
        EpochInfo.epochInfoFirst
            (evalEpochInfo ctx)
            (EpochNo 1)
            `shouldBe` Right (SlotNo 10)

evalContextWithEraOffset :: EvalContext
evalContextWithEraOffset =
    EvalContext
        { ecProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor =
                    Hex
                        $ serialize'
                            (natVersion @11)
                            (testEvalPParams emptyPParams)
                }
        , ecSystemStartCbor =
            Hex
                $ BSL.toStrict
                $ Serialise.serialise
                $ SystemStart (posixSecondsToUTCTime 0)
        , ecEpochSize = 432_000
        , ecSlotLengthMs = 1_000
        , ecEraHistoryCbor = Hex eraHistoryWithOffsetCbor
        , ecTrusted = True
        , ecTrustAssumption = "test"
        }

eraHistoryWithOffsetCbor :: ByteString
eraHistoryWithOffsetCbor =
    CBOR.toStrictByteString
        $ CBOR.encodeListLen 2
            <> eraSummary
                (RelativeTime 0)
                (SlotNo 0)
                (EpochNo 0)
                (Just (RelativeTime 10, SlotNo 10, EpochNo 1))
                10
                1_000
            <> eraSummary
                (RelativeTime 100)
                (SlotNo 10)
                (EpochNo 1)
                Nothing
                10
                2_000

eraSummary
    :: RelativeTime
    -> SlotNo
    -> EpochNo
    -> Maybe (RelativeTime, SlotNo, EpochNo)
    -> Word
    -> Integer
    -> CBOR.Encoding
eraSummary startTime startSlot startEpoch end epochSize slotLengthMs =
    CBOR.encodeListLen 3
        <> eraBound startTime startSlot startEpoch
        <> maybe
            CBOR.encodeNull
            ( \(endTime, endSlot, endEpoch) ->
                eraBound endTime endSlot endEpoch
            )
            end
        <> eraParams epochSize slotLengthMs

eraBound
    :: RelativeTime
    -> SlotNo
    -> EpochNo
    -> CBOR.Encoding
eraBound relativeTime slot epoch =
    CBOR.encodeListLen 3
        <> Serialise.encode relativeTime
        <> Serialise.encode slot
        <> Serialise.encode epoch

eraParams :: Word -> Integer -> CBOR.Encoding
eraParams epochSize slotLengthMs =
    CBOR.encodeListLen 4
        <> CBOR.encodeWord64 (fromIntegral epochSize)
        <> Serialise.encode (slotLengthFromMillisec slotLengthMs)
        <> (CBOR.encodeListLen 1 <> CBOR.encodeWord8 1)
        <> CBOR.encodeWord64 0
