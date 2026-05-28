{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.RejectFactsSpec
-- Description : Tests for the RejectFacts wire type and the
--               server-side reject helpers.
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.RejectFactsSpec (spec) where

import Data.Aeson
    ( ToJSON (toJSON)
    , Value (..)
    , eitherDecode
    , encode
    )
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Foldable (traverse_)
import Data.Proxy (Proxy (..))
import Data.Swagger qualified as Swagger
import Lens.Micro ((&), (.~))
import PlutusTx.Builtins.Internal (BuiltinByteString (..))
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck (generate)

import Cardano.Ledger.Api.PParams (emptyPParams)
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes (Inject (..))
import Cardano.Ledger.Binary (natVersion)
import Cardano.Ledger.Binary qualified as L
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Mary.Value (AssetName (..))
import Servant (Handler, runHandler)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Facts
    ( ChainPointJSON (..)
    , RejectFacts (..)
    , TokenIdJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , ConwayEra
    , SlotNo (..)
    , TokenId (..)
    , TxIn
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.HTTP.Server
    ( rejectValiditySlots
    , rejectableRequestUtxos
    )
import Cardano.MPFS.HTTP.StatusSpec (mkTestContext)
import Cardano.MPFS.HTTP.Types.Facts (mkRejectFacts)
import Cardano.MPFS.Indexer.TxFixtures (testCageAddr)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( currentPosixMs
    , mkInlineDatum
    , toPlcData
    )

spec :: Spec
spec = describe "RejectFacts" $ do
    it "round-trips RejectFacts through JSON"
        $ eitherDecode (encode sampleRejectFacts)
        `shouldBe` Right sampleRejectFacts

    it "encodes RejectFacts without unsigned transaction CBOR" $ do
        let facts = sampleRejectFacts
        assertJSONKeys
            [ "snapshot"
            , "token"
            , "state_utxo"
            , "request_utxos"
            , "wallet_utxos"
            , "validity_lower_slot"
            , "validity_upper_slot"
            , "protocol_parameters"
            ]
            (toJSON facts)
        case toJSON facts of
            Object obj -> do
                KM.member "tx" obj `shouldBe` False
                KM.member "unsigned_tx_cbor" obj
                    `shouldBe` False
                KM.member "trie_root" obj `shouldBe` False
                KM.member "trie_facts" obj `shouldBe` False
            _ ->
                expectationFailure
                    "Expected RejectFacts JSON object"

    it "has a Swagger schema instance" $ do
        let _rejectSchema = Swagger.toSchema (Proxy @RejectFacts)
        _rejectSchema `seq` (pure () :: IO ())

    it "provides server conversion from reject inputs" $ do
        txIn <- generate genTxIn
        let facts =
                mkRejectFacts
                    sampleSnapshot
                    sampleToken
                    (sampleUtxoInput txIn)
                    [sampleUtxoInput txIn]
                    [sampleUtxoInput txIn]
                    100
                    200
                    emptyPParams
        assertJSONKeys
            [ "snapshot"
            , "token"
            , "state_utxo"
            , "request_utxos"
            , "wallet_utxos"
            , "validity_lower_slot"
            , "validity_upper_slot"
            , "protocol_parameters"
            ]
            (toJSON facts)

    describe "rejectableRequestUtxos" $ do
        it "returns [] when no request is past deadline" $ do
            nowMs <- currentPosixMs
            txIn <- generate genTxIn
            let stateBs = sampleStateOutBytes 5_000 5_000
                presentReqs =
                    [ sampleRequestInput txIn (nowMs - 1_000)
                    , sampleRequestInput txIn (nowMs - 500)
                    ]
            result <-
                runHandler'
                    (rejectableRequestUtxos stateBs presentReqs)
            result `shouldBe` []

        it "returns the full set when every request is rejectable"
            $ do
                txIn <- generate genTxIn
                let stateBs = sampleStateOutBytes 5_000 5_000
                    pastReqs =
                        [ sampleRequestInput txIn 0
                        , sampleRequestInput txIn 1_000
                        , sampleRequestInput txIn 2_000
                        ]
                result <-
                    runHandler'
                        (rejectableRequestUtxos stateBs pastReqs)
                length result `shouldBe` length pastReqs

    describe "rejectValiditySlots" $ do
        it "returns (lower, upper) with upper > lower" $ do
            txIn <- generate genTxIn
            ctx <- mkRejectTestContext
            let stateBs = sampleStateOutBytes 5_000 5_000
                rejectableReqs =
                    [ sampleRequestInput txIn 1_000
                    , sampleRequestInput txIn 2_000
                    ]
            (lower, upper) <-
                runHandler'
                    ( rejectValiditySlots
                        ctx
                        stateBs
                        rejectableReqs
                    )
            -- pt + rt = 10000 ms, latest deadline = 2000 + 10000
            --                                    = 12000 ms.
            -- Mock posixMsCeilSlot maps ms → ms / 1000:
            -- lower = 12, upper = (12000 + 600000) / 1000 = 612.
            lower `shouldBe` 12
            upper `shouldBe` 612
            upper `shouldSatisfy` (> lower)

-- ---------------------------------------------------------------
-- JSON helpers
-- ---------------------------------------------------------------

assertJSONKeys :: [String] -> Value -> IO ()
assertJSONKeys keys value = case value of
    Object obj ->
        traverse_
            ( \k ->
                KM.member (Key.fromString k) obj
                    `shouldBe` True
            )
            keys
    _ ->
        expectationFailure
            "Expected JSON object"

-- ---------------------------------------------------------------
-- Handler driver
-- ---------------------------------------------------------------

runHandler' :: Handler a -> IO a
runHandler' h = do
    r <- runHandler h
    case r of
        Right a -> pure a
        Left err ->
            expectationFailure
                ("Handler failed: " <> show err)
                *> error "unreachable"

-- ---------------------------------------------------------------
-- Sample wire facts (JSON / schema)
-- ---------------------------------------------------------------

sampleRejectFacts :: RejectFacts
sampleRejectFacts =
    RejectFacts
        { rfSnapshot = sampleVerificationSnapshot
        , rfToken = TokenIdJSON "cafe"
        , rfStateUtxo = sampleUtxoEntry
        , rfRequestUtxos = [sampleUtxoEntry]
        , rfWalletUtxos = [sampleUtxoEntry]
        , rfValidityLowerSlot = 100
        , rfValidityUpperSlot = 200
        , rfProtocolParameters = sampleUnverifiedPParams
        }

sampleVerificationSnapshot :: VerificationSnapshot
sampleVerificationSnapshot =
    VerificationSnapshot
        { vsUtxoRoot = Hex "root"
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 42
                , cpBlockId = Hex "block-id"
                }
        }

sampleUtxoEntry :: UtxoEntry
sampleUtxoEntry =
    UtxoEntry
        { ueRef =
            UtxoRef
                { urTxId = Hex "tx-id"
                , urTxIx = 0
                }
        , ueTxOutCbor = Hex "tx-out"
        , ueInclusionProof = Hex "utxo-proof"
        }

sampleUnverifiedPParams :: UnverifiedPParams
sampleUnverifiedPParams =
    UnverifiedPParams
        { uppVerified = False
        , uppCbor = Hex "pparams"
        }

sampleSnapshot :: BundleSnapshot
sampleSnapshot =
    BundleSnapshot
        { snapshotUtxoRoot = "root"
        , snapshotSlot = SlotNo 42
        , snapshotBlockId = BlockId "block-id"
        }

sampleToken :: TokenId
sampleToken = TokenId (AssetName (SBS.toShort "cafe"))

sampleUtxoInput :: TxIn -> ResolvedWalletInput
sampleUtxoInput txIn =
    (txIn, "tx-out", "utxo-proof")

-- ---------------------------------------------------------------
-- Datum + TxOut byte fixtures (helper tests)
-- ---------------------------------------------------------------

-- | 28 stub bytes for owner / token / root payloads.
stubBytes28 :: ByteString
stubBytes28 = BS.replicate 28 0xAA

-- | Build a serialized 'TxOut' carrying a 'StateDatum' inline.
sampleStateOutBytes :: Integer -> Integer -> ByteString
sampleStateOutBytes pt rt =
    let stateDatum =
            StateDatum
                OnChainTokenState
                    { stateOwner = BuiltinByteString stubBytes28
                    , stateRoot = OnChainRoot stubBytes28
                    , stateMaxFee = 1_000_000
                    , stateProcessTime = pt
                    , stateRetractTime = rt
                    }
        txOut =
            mkBasicTxOut testCageAddr (inject (Coin 2_000_000))
                & datumTxOutL
                    .~ mkInlineDatum (toPlcData stateDatum)
    in  L.serialize'
            (natVersion @11)
            (txOut :: TxOut ConwayEra)

-- | Build a serialized 'TxOut' carrying a 'RequestDatum'
-- inline, with the given submitted-at timestamp (POSIX ms).
sampleRequestOutBytes :: Integer -> ByteString
sampleRequestOutBytes submittedAt =
    let reqDatum =
            RequestDatum
                OnChainRequest
                    { requestToken =
                        OnChainTokenId
                            (BuiltinByteString "cafe")
                    , requestOwner =
                        BuiltinByteString stubBytes28
                    , requestKey = "k"
                    , requestValue = OpInsert "v"
                    , requestFee = 100_000
                    , requestSubmittedAt = submittedAt
                    }
        txOut =
            mkBasicTxOut testCageAddr (inject (Coin 2_000_000))
                & datumTxOutL
                    .~ mkInlineDatum (toPlcData reqDatum)
    in  L.serialize'
            (natVersion @11)
            (txOut :: TxOut ConwayEra)

sampleRequestInput :: TxIn -> Integer -> ResolvedWalletInput
sampleRequestInput txIn submittedAt =
    (txIn, sampleRequestOutBytes submittedAt, "request-proof")

-- ---------------------------------------------------------------
-- Custom context with a deterministic posixMsCeilSlot
-- ---------------------------------------------------------------

-- | Wrap 'mkTestContext' overriding 'posixMsCeilSlot' to a
-- deterministic 1-slot-per-second conversion.
mkRejectTestContext :: IO (Context IO)
mkRejectTestContext = do
    ctx <- mkTestContext
    let prov = provider ctx
        prov' =
            prov
                { posixMsCeilSlot = \ms ->
                    pure
                        ( SlotNo
                            (fromInteger (ms `div` 1000))
                        )
                }
    pure ctx{provider = prov'}
