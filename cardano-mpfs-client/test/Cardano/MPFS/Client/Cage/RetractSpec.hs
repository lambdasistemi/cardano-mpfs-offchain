{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.RetractSpec
-- Description : Unit tests for local retract cage construction.
module Cardano.MPFS.Client.Cage.RetractSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Coerce (coerce)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust)
import Data.Set qualified as Set
import Lens.Micro ((&), (.~), (^.))
import System.Directory (doesFileExist)
import System.Environment (getEnv)
import System.FilePath ((</>))
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldNotBe
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
    )
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , inputsTxBodyL
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
    ( Inject (..)
    , Network (..)
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
    ( RetractFacts (..)
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
import Cardano.MPFS.Client.Cage.Retract
    ( retractCageTxWithEval
    )
import Cardano.MPFS.Client.Cage.TestEvalContext
    ( testEvalContext
    , testEvalPParams
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRetractFacts
    , verifyRetractFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import Cardano.Tx.Balance
    ( computeScriptIntegrity
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( ToData (..)
    )

spec :: Spec
spec = describe "retractCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture cfg
            emptyFunding = facts{rfWalletUtxos = []}
        verified <- expectVerified trustedRoot emptyFunding
        retractCageTxWithEval
            (testEvalContext realisticPParams)
            cfg
            permissiveWalletPolicy
            verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture cfg
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <- expectVerified trustedRoot facts
        retractCageTxWithEval
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

    it "builds an unsigned retract transaction" $ do
        cfg <- testCageConfig
        let RetractFixture
                { trustedRoot
                , facts
                , requestInput
                , stateInput
                , walletInput
                } = honestRetractFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <-
            case retractCageTxWithEval
                (testEvalContext realisticPParams)
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("retractCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        let body = tx ^. bodyTxL
            inputs = body ^. inputsTxBodyL
            refs = body ^. referenceInputsTxBodyL
            collateral = body ^. collateralInputsTxBodyL
            scripts =
                tx ^. witsTxL . scriptTxWitsL
            redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            integrity =
                body ^. scriptIntegrityHashTxBodyL
            expectedIntegrity =
                computeScriptIntegrity
                    (Set.singleton PlutusV3)
                    (testEvalPParams realisticPParams)
                    redeemers
                    (TxDats mempty)
        Set.member requestInput inputs `shouldBe` True
        Set.member stateInput refs `shouldBe` True
        Set.member walletInput collateral `shouldBe` True
        Map.size scripts `shouldBe` 1
        Map.size rdmrs `shouldBe` 1
        case Map.elems rdmrs of
            [(_, budget)] ->
                budget
                    `shouldSatisfy` ( \(ExUnits m s) ->
                                        m > 0 && s > 0
                                    )
            _ ->
                expectationFailure
                    "expected exactly one spending redeemer"
        integrity `shouldBe` expectedIntegrity
        body ^. reqSignerHashesTxBodyL
            `shouldBe` Set.singleton expectedOwnerWitness
        body ^. vldtTxBodyL
            `shouldBe` ValidityInterval
                (SJust (SlotNo (fromIntegral phase2StartSlot)))
                (SJust (SlotNo (fromIntegral phase2EndSlot)))

    it "does not reuse the placeholder-budget legacy retract vector" $ do
        cfg <- testCageConfig
        let RetractFixture{trustedRoot, facts} =
                honestRetractFixture cfg
        verified <- expectVerified trustedRoot facts
        tx <-
            case retractCageTxWithEval
                (testEvalContext realisticPParams)
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("retractCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        expected <- BS.readFile =<< legacyRetractVectorPath
        serialize' (natVersion @11) tx `shouldNotBe` expected

data RetractFixture = RetractFixture
    { trustedRoot :: TrustedRoot
    , facts :: RetractFacts
    , requestInput :: TxIn
    , stateInput :: TxIn
    , walletInput :: TxIn
    }

honestRetractFixture :: CageConfig -> RetractFixture
honestRetractFixture cfg =
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
        (root, requestEntry, stateEntry, walletEntry) =
            csmtRetractRows
                requestBytes
                stateBytes
                walletBytes
        retract =
            RetractFacts
                { rfSnapshot = snapshotWithRoot root
                , rfToken = sampleToken
                , rfRequestUtxo = requestEntry
                , rfStateUtxo = stateEntry
                , rfWalletUtxos = [walletEntry]
                , rfValidityStartSlot = phase2StartSlot
                , rfValidityEndSlot = phase2EndSlot
                , rfProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  RetractFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = retract
            , requestInput = txInFromBytes requestTxId 0
            , stateInput = txInFromBytes stateTxId 1
            , walletInput = txInFromBytes walletTxId 2
            }

csmtRetractRows
    :: ByteString
    -> ByteString
    -> ByteString
    -> (ByteString, UtxoEntry, UtxoEntry, UtxoEntry)
csmtRetractRows requestBytes stateBytes walletBytes =
    evalPureFromEmptyDB $ do
        let reqKey =
                byteStringToKey (encodeTxIn requestTxId 0)
            stKey =
                byteStringToKey (encodeTxIn stateTxId 1)
            walKey =
                byteStringToKey (encodeTxIn walletTxId 2)
        insertMHash reqKey (mkHash requestBytes)
        insertMHash stKey (mkHash stateBytes)
        insertMHash walKey (mkHash walletBytes)
        reqProof <- proofBytes reqKey
        stProof <- proofBytes stKey
        walProof <- proofBytes walKey
        root <-
            maybe BS.empty renderHash <$> getRootHashM
        pure
            ( root
            , mkUtxoEntry requestTxId 0 requestBytes reqProof
            , mkUtxoEntry stateTxId 1 stateBytes stProof
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
    -> RetractFacts
    -> IO VerifiedRetractFacts
expectVerified trusted facts =
    case verifyRetractFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRetractFacts failed: " <> show err)
                *> error "unreachable"
        Right verified -> pure verified

legacyRetractVectorPath :: IO FilePath
legacyRetractVectorPath = do
    let primary =
            "specs/267-retract-fact-provider-pivot/test-vectors/legacy-retract.cbor"
        fallback = ".." </> primary
    primaryExists <- doesFileExist primary
    if primaryExists
        then pure primary
        else do
            fallbackExists <- doesFileExist fallback
            if fallbackExists
                then pure fallback
                else
                    expectationFailure
                        ("legacy retract vector not found at " <> primary)
                        *> error "unreachable"

requestTxOut :: Addr -> TokenId -> TxOut ConwayEra
requestTxOut requestAddr token =
    mkBasicTxOut requestAddr (inject (Coin 2_500_000))
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
        StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString
                        $ hashToBytes
                        $ let KeyHash h = testKh in h
                , stateRoot = OnChainRoot (BS.replicate 32 0x44)
                , stateMaxFee = 1_000_000
                , stateProcessTime = 60_000
                , stateRetractTime = 30_000
                }

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
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (compactCoinOrError (Coin 4_310))
        & ppMaxTxExUnitsL
            .~ ExUnits 140_000_000 10_000_000_000

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

requestTxId, stateTxId, walletTxId :: ByteString
requestTxId = BS.replicate 32 0xA1
stateTxId = BS.replicate 32 0xB2
walletTxId = BS.replicate 32 0xC3

submittedAt :: Integer
submittedAt = 0

phase2StartSlot :: Integer
phase2StartSlot = 70

phase2EndSlot :: Integer
phase2EndSlot = 80

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
