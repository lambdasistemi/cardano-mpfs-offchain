{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.RejectSpec
-- Description : Unit tests for local reject cage construction.
module Cardano.MPFS.Client.Cage.RejectSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Coerce (coerce)
import Data.Foldable (toList)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust)
import Data.Ratio ((%))
import Data.Set qualified as Set
import Lens.Micro ((&), (.~), (^.))
import System.Environment (getEnv)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import CSMT.Core.CBOR (renderProof)
import CSMT.Core.Hash
    ( byteStringToKey
    , renderHash
    )
import CSMT.Hashes (hashHashing, mkHash)
import CSMT.Test.Lib
    ( evalPureFromEmptyDB
    , getRootHashM
    , hashCodecs
    , identityFromKV
    , insertMHash
    , proofM
    )
import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromBytes
    , hashFromStringAsHex
    , hashToBytes
    )
import Cardano.Ledger.Address
    ( Addr (..)
    )
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.TxBody
    ( reqSignerHashesTxBodyL
    , scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( CoinPerByte (..)
    , emptyPParams
    , ppCoinsPerUTxOByteL
    , ppMaxTxExUnitsL
    , ppPricesL
    , ppTxFeeFixedL
    , ppTxFeePerByteL
    )
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , binaryDataToData
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    , referenceInputsTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , TxDats (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( BoundedRational (..)
    , Inject (..)
    , Network (..)
    , NonNegativeInterval
    , StrictMaybe (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )
import Cardano.Ledger.Coin
    ( Coin (..)
    , compactCoinOrError
    )
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Core
    ( PParams
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes
    ( unsafeMakeSafeHash
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    , Prices (..)
    , txscriptfee
    )
import Cardano.Ledger.Plutus.Language
    ( Language (..)
    )
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
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
import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Ledger
    ( AssetName
    , ConwayEra
    , TokenId (..)
    )
import Cardano.MPFS.Cage.Types
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , cagePolicyIdFromCfg
    , computeScriptHash
    )
import Cardano.MPFS.Client.Cage.Identity
    ( onChainTokenId
    , requestAddrFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Cage.Reject
    ( rejectCageTxWithEval
    )
import Cardano.MPFS.Client.Cage.TestEvalContext
    ( testEvalContext
    , testEvalPParams
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRejectFacts
    , verifyRejectFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import Cardano.Tx.Balance
    ( computeScriptIntegrity
    )
import Cardano.Tx.Ledger (ConwayTx)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )

spec :: Spec
spec = describe "rejectCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let RejectFixture{trustedRoot, facts} =
                honestRejectFixture cfg
            emptyFunding = facts{rfWalletUtxos = []}
        verified <- expectVerified trustedRoot emptyFunding
        rejectCageTxWithEval
            (testEvalContext realisticPParams)
            cfg
            permissiveWalletPolicy
            verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let RejectFixture{trustedRoot, facts} =
                honestRejectFixture cfg
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <- expectVerified trustedRoot facts
        rejectCageTxWithEval
            (testEvalContext realisticPParams)
            cfg
            policy
            verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "rejects an empty request batch through the verifier gate"
        $ do
            cfg <- testCageConfig
            let RejectFixture{trustedRoot, facts} =
                    honestRejectFixture cfg
                emptyBatch = facts{rfRequestUtxos = []}
            -- The verifier is the gate for empty batches; the
            -- cage builder carries a redundant defense-in-depth
            -- guard but is unreachable in practice because
            -- VerifiedRejectFacts cannot be constructed without
            -- the verifier signing off first.
            verifyRejectFacts trustedRoot emptyBatch
                `shouldBe` Left
                    ( TxBindingFailed
                        "reject.request_utxos"
                        "must not be empty"
                    )

    it "builds an unsigned reject transaction for verified facts"
        $ do
            cfg <- testCageConfig
            let RejectFixture{trustedRoot, facts} =
                    honestRejectFixture cfg
            verified <- expectVerified trustedRoot facts
            case rejectCageTxWithEval
                (testEvalContext realisticPParams)
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("rejectCageTx failed: " <> show err)
                Right _ -> pure ()

    it "uses the fact-derived validity lower and upper slots" $ do
        cfg <- testCageConfig
        let RejectFixture{trustedRoot, facts} =
                honestRejectFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <- expectBuilt cfg verified
        let body = tx ^. bodyTxL
        body ^. vldtTxBodyL
            `shouldBe` ValidityInterval
                (SJust (SlotNo (fromIntegral phase3LowerSlot)))
                (SJust (SlotNo (fromIntegral phase3UpperSlot)))

    it "matches fact-derived legacy reject structure" $ do
        cfg <- testCageConfig
        let RejectFixture
                { trustedRoot
                , facts
                , stateInput
                , requestInput
                , walletInput
                } = honestRejectFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <- expectBuilt cfg verified
        let body = tx ^. bodyTxL
            inputs = body ^. inputsTxBodyL
            refs = body ^. referenceInputsTxBodyL
            collateral = body ^. collateralInputsTxBodyL
            scripts = tx ^. witsTxL . scriptTxWitsL
            redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            integrity = body ^. scriptIntegrityHashTxBodyL
            expectedIntegrity =
                computeScriptIntegrity
                    (Set.singleton PlutusV3)
                    (testEvalPParams realisticPParams)
                    redeemers
                    (TxDats mempty)
        inputs
            `shouldBe` Set.fromList
                [stateInput, requestInput, walletInput]
        refs `shouldBe` Set.empty
        collateral `shouldBe` Set.singleton walletInput
        Map.size scripts `shouldBe` 2
        Map.size rdmrs `shouldBe` 2
        body ^. mintTxBodyL `shouldBe` mempty
        body ^. reqSignerHashesTxBodyL
            `shouldBe` Set.singleton expectedOwnerWitness
        integrity `shouldBe` expectedIntegrity
        case Map.elems rdmrs of
            xs ->
                xs
                    `shouldSatisfy` all
                        ( \(_, ExUnits m s) ->
                            m > 0 && s > 0
                        )
        -- Decode redeemer datums and check action shapes.
        let datums = map (fst . snd) (Map.toList rdmrs)
            actionShapes = map decodeRedeemer datums
        actionShapes
            `shouldSatisfy` elem (Just (RedeemerModify 1))
        actionShapes
            `shouldSatisfy` any
                ( \case
                    Just RedeemerContribute -> True
                    _ -> False
                )

    it "state root is unchanged across the reject step" $ do
        cfg <- testCageConfig
        let RejectFixture{trustedRoot, facts} =
                honestRejectFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <- expectBuilt cfg verified
        let body = tx ^. bodyTxL
            outs = toList (body ^. outputsTxBodyL)
        case outs of
            (stateOut : _) ->
                case datumState stateOut of
                    Just s ->
                        stateRoot s
                            `shouldBe` OnChainRoot expectedRootBytes
                    Nothing ->
                        expectationFailure
                            "expected state datum on output[0]"
            [] ->
                expectationFailure
                    "expected at least one output"

    it "keeps the bounded request fee envelope above measured reject fee" $ do
        cfg <- testCageConfig
        let RejectFixture{trustedRoot, facts} =
                honestRejectFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <- expectBuilt cfg verified
        let Coin rejectFee = tx ^. bodyTxL . feeTxBodyL
            Coin feeBound = feeBufferUpperBound realisticPParams
        feeBound `shouldSatisfy` (>= rejectFee)
        feeBound `shouldSatisfy` (<= grossFeeBufferUpperBound)

-- ---------------------------------------------------------------
-- Redeemer decoding helpers (test-only)
-- ---------------------------------------------------------------

data RedeemerShape
    = RedeemerModify !Int
    | RedeemerContribute
    deriving stock (Eq, Show)

decodeRedeemer :: Data ConwayEra -> Maybe RedeemerShape
decodeRedeemer (Data plc) =
    case fromBuiltinData (BuiltinData plc) of
        Just (Modify actions) ->
            Just (RedeemerModify (length actions))
        Just (Contribute _) -> Just RedeemerContribute
        _ -> Nothing

datumState :: TxOut ConwayEra -> Maybe OnChainTokenState
datumState out =
    case out ^. datumTxOutL of
        Datum bd ->
            let Data plc = binaryDataToData bd
            in  case fromBuiltinData (BuiltinData plc) of
                    Just (StateDatum s) -> Just s
                    _ -> Nothing
        _ -> Nothing

-- ---------------------------------------------------------------
-- Fixture
-- ---------------------------------------------------------------

data RejectFixture = RejectFixture
    { trustedRoot :: TrustedRoot
    , facts :: RejectFacts
    , stateInput :: TxIn
    , requestInput :: TxIn
    , walletInput :: TxIn
    }

honestRejectFixture :: CageConfig -> RejectFixture
honestRejectFixture cfg =
    let token = tokenIdFromJSON sampleToken
        requestAddr =
            requestAddrFromCfg cfg token Testnet
        stateAddr =
            Addr
                Testnet
                (ScriptHashObj $ cfgScriptHash cfg)
                StakeRefNull
        asset = unTokenId token
        requestBytes =
            serialize' (natVersion @11)
                $ requestTxOut requestAddr token
        stateBytes =
            serialize' (natVersion @11)
                $ stateTxOut stateAddr cfg asset
        walletBytes = walletTxOutBytes
        (root, stateEntry, requestEntry, walletEntry) =
            csmtRejectRows stateBytes requestBytes walletBytes
        reject =
            RejectFacts
                { rfSnapshot = snapshotWithRoot root
                , rfToken = sampleToken
                , rfStateUtxo = stateEntry
                , rfRequestUtxos = [requestEntry]
                , rfWalletUtxos = [walletEntry]
                , rfValidityLowerSlot = phase3LowerSlot
                , rfValidityUpperSlot = phase3UpperSlot
                , rfProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  RejectFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = reject
            , stateInput = txInFromBytes stateTxId 0
            , requestInput = txInFromBytes requestTxId 1
            , walletInput = txInFromBytes walletTxId 2
            }

csmtRejectRows
    :: ByteString
    -> ByteString
    -> ByteString
    -> (ByteString, UtxoEntry, UtxoEntry, UtxoEntry)
csmtRejectRows stateBytes requestBytes walletBytes =
    evalPureFromEmptyDB $ do
        let stKey = byteStringToKey (encodeTxIn stateTxId 0)
            reqKey =
                byteStringToKey (encodeTxIn requestTxId 1)
            walKey =
                byteStringToKey (encodeTxIn walletTxId 2)
        insertMHash stKey (mkHash stateBytes)
        insertMHash reqKey (mkHash requestBytes)
        insertMHash walKey (mkHash walletBytes)
        stProof <- proofBytes stKey
        reqProof <- proofBytes reqKey
        walProof <- proofBytes walKey
        root <-
            maybe BS.empty renderHash <$> getRootHashM
        pure
            ( root
            , mkUtxoEntry stateTxId 0 stateBytes stProof
            , mkUtxoEntry requestTxId 1 requestBytes reqProof
            , mkUtxoEntry walletTxId 2 walletBytes walProof
            )
  where
    proofBytes key = do
        mProof <-
            proofM
                hashCodecs
                identityFromKV
                hashHashing
                key
        pure $ case mProof of
            Just (_, proof) -> renderProof proof
            Nothing -> BS.empty
    mkUtxoEntry idBytes ix outBytes p =
        UtxoEntry
            { ueRef =
                UtxoRef
                    { urTxId = Hex idBytes
                    , urTxIx = ix
                    }
            , ueTxOutCbor = Hex outBytes
            , ueInclusionProof = Hex p
            }

expectVerified
    :: TrustedRoot
    -> RejectFacts
    -> IO VerifiedRejectFacts
expectVerified trusted f =
    case verifyRejectFacts trusted f of
        Left err ->
            expectationFailure
                ("verifyRejectFacts failed: " <> show err)
                *> error "unreachable"
        Right verified -> pure verified

expectBuilt
    :: CageConfig
    -> VerifiedRejectFacts
    -> IO ConwayTx
expectBuilt cfg verified =
    case rejectCageTxWithEval
        (testEvalContext realisticPParams)
        cfg
        permissiveWalletPolicy
        verified of
        Left err ->
            expectationFailure
                ("rejectCageTx failed: " <> show err)
                *> error "unreachable"
        Right tx -> pure tx

requestTxOut :: Addr -> TokenId -> TxOut ConwayEra
requestTxOut requestAddr token =
    mkBasicTxOut requestAddr (inject (Coin 4_000_000))
        & datumTxOutL .~ mkInlineDatum requestDatum
  where
    requestDatum =
        RequestDatum
            OnChainRequest
                { requestToken = onChainTokenId token
                , requestOwner =
                    BuiltinByteString
                        $ hashToBytes
                        $ let KeyHash h = testKh in h
                , requestKey = "mykey"
                , requestValue = OpInsert "myvalue"
                , requestFee = 1_000_000
                , requestSubmittedAt = submittedAt
                }

stateTxOut :: Addr -> CageConfig -> AssetName -> TxOut ConwayEra
stateTxOut stateAddr cfg asset =
    mkBasicTxOut stateAddr stateValue
        & datumTxOutL .~ mkInlineDatum stateDatum
  where
    stateValue =
        MaryValue
            (Coin 2_000_000)
            ( MultiAsset
                $ Map.singleton
                    (cagePolicyIdFromCfg cfg)
                    (Map.singleton asset 1)
            )
    stateDatum =
        StateDatum sampleOnChainTokenState

sampleOnChainTokenState :: OnChainTokenState
sampleOnChainTokenState =
    OnChainTokenState
        { stateOwner =
            BuiltinByteString
                $ hashToBytes
                $ let KeyHash h = testKh in h
        , stateRoot = OnChainRoot expectedRootBytes
        , stateMaxFee = 1_000_000
        , stateProcessTime = 60_000
        , stateRetractTime = 30_000
        }

expectedRootBytes :: ByteString
expectedRootBytes = BS.replicate 32 0x44

walletTxOutBytes :: ByteString
walletTxOutBytes =
    serialize' (natVersion @11) walletTxOut

walletTxOut :: TxOut ConwayEra
walletTxOut =
    mkBasicTxOut
        fundingAddr
        (inject (Coin 50_000_000))

mkInlineDatum :: (ToData a) => a -> Datum ConwayEra
mkInlineDatum datum =
    let BuiltinData d = toBuiltinData datum
    in  Datum $ dataToBinaryData (Data d :: Data ConwayEra)

fundingAddr :: Addr
fundingAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

pparamsFacts :: PParams ConwayEra -> UnverifiedPParams
pparamsFacts pp =
    UnverifiedPParams
        { uppVerified = False
        , uppCbor = Hex (serialize' (natVersion @11) pp)
        }

snapshotWithRoot :: ByteString -> VerificationSnapshot
snapshotWithRoot root =
    VerificationSnapshot
        { vsUtxoRoot = Hex root
        , vsChainPoint =
            ChainPointJSON
                { cpSlot = 0
                , cpBlockId = Hex (BS.replicate 32 0)
                }
        }

encodeTxIn :: ByteString -> Word -> ByteString
encodeTxIn txIdBytes txIx =
    CBOR.toStrictByteString
        $ mconcat
            [ CBOR.encodeListLen 2
            , CBOR.encodeBytes txIdBytes
            , CBOR.encodeWord64 (fromIntegral txIx)
            ]

txInFromBytes :: ByteString -> Word -> TxIn
txInFromBytes txIdBytes txIx =
    TxIn
        ( TxId
            $ unsafeMakeSafeHash
            $ fromJust
            $ hashFromBytes @Blake2b_256 txIdBytes
        )
        (TxIx $ fromIntegral txIx)

permissiveWalletPolicy :: WalletPolicy
permissiveWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

realisticPParams :: PParams ConwayEra
realisticPParams =
    emptyPParams
        & ppTxFeePerByteL
            .~ CoinPerByte (compactCoinOrError (Coin 44))
        & ppTxFeeFixedL .~ Coin 155_381
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (compactCoinOrError (Coin 4_310))
        & ppPricesL
            .~ Prices
                (unsafeNonNegativeInterval (577 % 10_000))
                (unsafeNonNegativeInterval (721 % 10_000_000))
        & ppMaxTxExUnitsL
            .~ ExUnits 140_000_000 10_000_000_000

unsafeNonNegativeInterval :: Rational -> NonNegativeInterval
unsafeNonNegativeInterval r =
    fromJust (boundRational r)

feeBufferUpperBound :: PParams ConwayEra -> Coin
feeBufferUpperBound pp =
    let CoinPerByte minFeeACompact = pp ^. ppTxFeePerByteL
        Coin minFeeA = fromCompact minFeeACompact
        Coin minFeeB = pp ^. ppTxFeeFixedL
        Coin scriptFee =
            txscriptfee
                (pp ^. ppPricesL)
                perRequestFutureSpendExUnits
    in  Coin
            ( minFeeB
                + minFeeA * maxUpdateTxBytes
                + scriptFee
            )

perRequestFutureSpendExUnits :: ExUnits
perRequestFutureSpendExUnits =
    ExUnits 40_000_000 3_000_000_000

grossFeeBufferUpperBound :: Integer
grossFeeBufferUpperBound = 5_000_000

maxUpdateTxBytes :: Integer
maxUpdateTxBytes = 16_384

testCageConfig :: IO CageConfig
testCageConfig = do
    blueprintPath <- getEnv "MPFS_BLUEPRINT"
    eBlueprint <- loadBlueprint blueprintPath
    blueprint <- case eBlueprint of
        Left err ->
            expectationFailure
                ("loadBlueprint failed: " <> err)
                *> error "unreachable"
        Right bp -> pure bp
    scriptBytes <-
        case extractCompiledCode "state." blueprint of
            Just bytes -> pure bytes
            Nothing ->
                expectationFailure
                    "state script not found in MPFS_BLUEPRINT"
                    *> error "unreachable"
    requestBytes <-
        case extractCompiledCode "request." blueprint of
            Just bytes -> pure bytes
            Nothing ->
                expectationFailure
                    "request script not found in MPFS_BLUEPRINT"
                    *> error "unreachable"
    pure
        CageConfig
            { cageScriptBytes = scriptBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = computeScriptHash scriptBytes
            , defaultProcessTime = 60_000
            , defaultRetractTime = 30_000
            , defaultTip = Coin 1_000_000
            , network = Testnet
            }

stateTxId, requestTxId, walletTxId :: ByteString
stateTxId = BS.replicate 32 0xB2
requestTxId = BS.replicate 32 0xA1
walletTxId = BS.replicate 32 0xC3

submittedAt :: Integer
submittedAt = 9_000

phase3LowerSlot :: Integer
phase3LowerSlot = 100

phase3UpperSlot :: Integer
phase3UpperSlot = 200

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)

expectedOwnerWitness :: KeyHash Guard
expectedOwnerWitness = coerce testKh

testKh :: KeyHash Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"
