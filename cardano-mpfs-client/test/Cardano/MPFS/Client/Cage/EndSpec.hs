{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.EndSpec
-- Description : Unit tests for local end cage transaction construction.
module Cardano.MPFS.Client.Cage.EndSpec
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
import System.Environment (getEnv)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )

import CSMT
    ( Direction
    , Standalone (StandaloneCSMTCol)
    )
import CSMT.Backend.Pure
    ( runPureTransaction
    )
import CSMT.Core.CBOR
    ( renderCompletenessProof
    , renderProof
    )
import CSMT.Core.Hash
    ( Hash
    , byteStringToKey
    , renderHash
    )
import CSMT.Hashes
    ( hashHashing
    , mkHash
    )
import CSMT.Proof.Completeness
    ( CompletenessProof
    , generateProof
    )
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
import Cardano.Ledger.Alonzo.TxBody
    ( reqSignerHashesTxBodyL
    , scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( emptyPParams
    , ppCoinsPerUTxOByteL
    , ppMaxTxExUnitsL
    )
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( collateralInputsTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Babbage.PParams
    ( CoinPerByte (..)
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )
import Cardano.Ledger.Coin
    ( Coin (..)
    )
import Cardano.Ledger.Core
    ( PParams
    , hashScript
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
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( EndFacts (..)
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
    , mkCageScript
    )
import Cardano.MPFS.Client.Cage.End
    ( endCageTx
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestSetPrefixFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedEndFacts
    , verifyEndFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.Node.Client.Balance
    ( computeScriptIntegrity
    , evalBudgetExUnits
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( ToData (..)
    )

spec :: Spec
spec = describe "endCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            emptyFunding = facts{efWalletUtxos = []}
        verified <- expectVerified cfg trustedRoot emptyFunding
        endCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let EndFixture{trustedRoot, facts} = honestEndFixture cfg
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <- expectVerified cfg trustedRoot facts
        endCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "selects the largest wallet UTxO as collateral" $ do
        cfg <- testCageConfig
        let EndFixture
                { trustedRoot
                , facts
                } = mixedBalanceEndFixture cfg
            expectedCollateral =
                txInFromBytes walletTxId 2
        verified <- expectVerified cfg trustedRoot facts
        tx <-
            case endCageTx cfg permissiveWalletPolicy verified of
                Left err ->
                    expectationFailure
                        ("endCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        let collateral = tx ^. bodyTxL . collateralInputsTxBodyL
        Set.member expectedCollateral collateral `shouldBe` True

    it "builds an unsigned burn transaction for the verified facts" $ do
        cfg <- testCageConfig
        let EndFixture
                { trustedRoot
                , facts
                , stateInput
                , walletInput
                , tokenAsset
                } = honestEndFixture cfg
            policyId = cagePolicyIdFromCfg cfg
        verified <- expectVerified cfg trustedRoot facts
        tx <-
            case endCageTx cfg permissiveWalletPolicy verified of
                Left err ->
                    expectationFailure
                        ("endCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        let inputs =
                tx ^. bodyTxL . inputsTxBodyL
            MultiAsset minted =
                tx ^. bodyTxL . mintTxBodyL
            scripts =
                tx ^. witsTxL . scriptTxWitsL
            redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            budgets =
                snd <$> Map.elems rdmrs
            maxTxBudget =
                realisticPParams ^. ppMaxTxExUnitsL
            integrity =
                tx ^. bodyTxL . scriptIntegrityHashTxBodyL
            expectedIntegrity =
                computeScriptIntegrity
                    PlutusV3
                    realisticPParams
                    redeemers
        Set.member stateInput inputs `shouldBe` True
        Set.member walletInput inputs `shouldBe` True
        Map.lookup policyId minted
            `shouldBe` Just (Map.singleton tokenAsset (-1))
        Map.member (hashScript (mkCageScript cfg)) scripts
            `shouldBe` True
        fundingAddr `shouldSatisfy` (/= ownerAddr)
        txOutputAddresses tx
            `shouldSatisfy` elem fundingAddr
        tx ^. bodyTxL . reqSignerHashesTxBodyL
            `shouldBe` Set.singleton expectedOwnerWitness
        Map.size rdmrs `shouldBe` 2
        budgets `shouldSatisfy` all nonZeroExUnits
        budgets
            `shouldSatisfy` all
                (`withinExUnits` evalBudgetExUnits)
        sumExUnits budgets
            `shouldSatisfy` (`withinExUnits` maxTxBudget)
        integrity `shouldBe` expectedIntegrity

data EndFixture = EndFixture
    { trustedRoot :: TrustedRoot
    , facts :: EndFacts
    , stateInput :: TxIn
    , walletInput :: TxIn
    , tokenAsset :: TokenIdAsset
    }

type TokenIdAsset = AssetName

honestEndFixture :: CageConfig -> EndFixture
honestEndFixture cfg =
    let requestPrefix = requestSetPrefixFromCfg cfg sampleToken
        token = tokenIdFromJSON sampleToken
        asset = unTokenId token
        stateBytes = stateTxOutBytes cfg asset
        walletBytes = walletTxOutBytes
        (root, stateEntry, walletEntry, proofBs) =
            csmtEndRows requestPrefix stateBytes walletBytes
        endFacts =
            EndFacts
                { efSnapshot = snapshotWithRoot root
                , efToken = sampleToken
                , efStateUtxo = stateEntry
                , efWalletUtxos = [walletEntry]
                , efRequestSet =
                    UtxoSetWitness
                        { uswEntries = []
                        , uswCompletenessProof = Hex proofBs
                        }
                , efProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  EndFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = endFacts
            , stateInput = txInFromBytes stateTxId 0
            , walletInput = txInFromBytes walletTxId 1
            , tokenAsset = asset
            }

-- | End fixture with two wallet UTxOs of clearly different
-- lovelace balances. The first entry (ix=1) carries 3 ADA;
-- the second entry (ix=2) carries 50 ADA. The end builder
-- must pick the larger row as collateral regardless of
-- CSMT walk order — this pins the Conway
-- InsufficientCollateral regression that surfaced on the
-- FactsMatrix end row.
mixedBalanceEndFixture :: CageConfig -> EndFixture
mixedBalanceEndFixture cfg =
    let requestPrefix = requestSetPrefixFromCfg cfg sampleToken
        token = tokenIdFromJSON sampleToken
        asset = unTokenId token
        stateBytes = stateTxOutBytes cfg asset
        ( root
            , stateEntry
            , smallEntry
            , largeEntry
            , proofBs
            ) =
                csmtEndRowsMixed
                    requestPrefix
                    stateBytes
                    smallWalletTxOutBytes
                    walletTxOutBytes
        endFacts =
            EndFacts
                { efSnapshot = snapshotWithRoot root
                , efToken = sampleToken
                , efStateUtxo = stateEntry
                , efWalletUtxos = [smallEntry, largeEntry]
                , efRequestSet =
                    UtxoSetWitness
                        { uswEntries = []
                        , uswCompletenessProof = Hex proofBs
                        }
                , efProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  EndFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = endFacts
            , stateInput = txInFromBytes stateTxId 0
            , walletInput = txInFromBytes walletTxId 2
            , tokenAsset = asset
            }

csmtEndRowsMixed
    :: [Direction]
    -> ByteString
    -> ByteString
    -> ByteString
    -> ( ByteString
       , UtxoEntry
       , UtxoEntry
       , UtxoEntry
       , ByteString
       )
csmtEndRowsMixed requestPrefix stateBytes smallBytes largeBytes =
    evalPureFromEmptyDB $ do
        let stateKey =
                byteStringToKey (encodeTxIn stateTxId 0)
            smallKey =
                byteStringToKey (encodeTxIn walletTxId 1)
            largeKey =
                byteStringToKey (encodeTxIn walletTxId 2)
            rows =
                [ (stateKey, stateBytes)
                , (smallKey, smallBytes)
                , (largeKey, largeBytes)
                ]
        mapM_ (\(key, txOut) -> insertMHash key (mkHash txOut)) rows
        stateProof <- proofBytes stateKey
        smallProof <- proofBytes smallKey
        largeProof <- proofBytes largeKey
        completenessProof <-
            runPureTransaction hashCodecs
                $ generateProof StandaloneCSMTCol [] requestPrefix
        root <- maybe BS.empty renderHash <$> getRootHashM
        pure
            ( root
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex stateTxId
                        , urTxIx = 0
                        }
                , ueTxOutCbor = Hex stateBytes
                , ueInclusionProof = Hex stateProof
                }
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex walletTxId
                        , urTxIx = 1
                        }
                , ueTxOutCbor = Hex smallBytes
                , ueInclusionProof = Hex smallProof
                }
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex walletTxId
                        , urTxIx = 2
                        }
                , ueTxOutCbor = Hex largeBytes
                , ueInclusionProof = Hex largeProof
                }
            , case completenessProof of
                Just proof ->
                    renderCompletenessProof
                        (proof :: CompletenessProof Hash)
                Nothing ->
                    error "expected request-set completeness proof"
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

csmtEndRows
    :: [Direction]
    -> ByteString
    -> ByteString
    -> (ByteString, UtxoEntry, UtxoEntry, ByteString)
csmtEndRows requestPrefix stateBytes walletBytes =
    evalPureFromEmptyDB $ do
        let stateKey =
                byteStringToKey (encodeTxIn stateTxId 0)
            walletKey =
                byteStringToKey (encodeTxIn walletTxId 1)
            rows =
                [ (stateKey, stateBytes)
                , (walletKey, walletBytes)
                ]
        mapM_ (\(key, txOut) -> insertMHash key (mkHash txOut)) rows
        stateProof <- proofBytes stateKey
        walletProof <- proofBytes walletKey
        completenessProof <-
            runPureTransaction hashCodecs
                $ generateProof StandaloneCSMTCol [] requestPrefix
        root <- maybe BS.empty renderHash <$> getRootHashM
        pure
            ( root
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex stateTxId
                        , urTxIx = 0
                        }
                , ueTxOutCbor = Hex stateBytes
                , ueInclusionProof = Hex stateProof
                }
            , UtxoEntry
                { ueRef =
                    UtxoRef
                        { urTxId = Hex walletTxId
                        , urTxIx = 1
                        }
                , ueTxOutCbor = Hex walletBytes
                , ueInclusionProof = Hex walletProof
                }
            , case completenessProof of
                Just proof ->
                    renderCompletenessProof
                        (proof :: CompletenessProof Hash)
                Nothing ->
                    error "expected request-set completeness proof"
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

expectVerified
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> IO VerifiedEndFacts
expectVerified cfg trusted facts =
    case verifyEndFacts cfg trusted facts of
        Left err ->
            expectationFailure ("verifyEndFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

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

stateTxOutBytes
    :: CageConfig -> TokenIdAsset -> ByteString
stateTxOutBytes cfg asset =
    serialize' (natVersion @11)
        $ stateTxOut cfg asset

stateTxOut :: CageConfig -> TokenIdAsset -> TxOut ConwayEra
stateTxOut cfg asset =
    mkBasicTxOut
        (Addr Testnet (ScriptHashObj $ cfgScriptHash cfg) StakeRefNull)
        stateValue
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

smallWalletTxOutBytes :: ByteString
smallWalletTxOutBytes =
    serialize' (natVersion @11) smallWalletTxOut

smallWalletTxOut :: TxOut ConwayEra
smallWalletTxOut =
    mkBasicTxOut
        fundingAddr
        (inject (Coin 3_000_000))

ownerAddr :: Addr
ownerAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

fundingAddr :: Addr
fundingAddr =
    Addr
        Testnet
        (KeyHashObj testKh)
        (StakeRefBase $ KeyHashObj stakeKh)

txOutputAddresses :: Tx ConwayEra -> [Addr]
txOutputAddresses tx =
    fmap (^. addrTxOutL)
        $ foldr (:) []
        $ tx ^. bodyTxL . outputsTxBodyL

mkInlineDatum :: (ToData a) => a -> Datum ConwayEra
mkInlineDatum datum =
    let BuiltinData d = toBuiltinData datum
    in  Datum $ dataToBinaryData (Data d :: Data ConwayEra)

permissiveWalletPolicy :: WalletPolicy
permissiveWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

nonZeroExUnits :: ExUnits -> Bool
nonZeroExUnits (ExUnits mem steps) =
    mem > 0 && steps > 0

sumExUnits :: [ExUnits] -> ExUnits
sumExUnits =
    foldr addExUnits (ExUnits 0 0)

addExUnits :: ExUnits -> ExUnits -> ExUnits
addExUnits (ExUnits memA stepsA) (ExUnits memB stepsB) =
    ExUnits (memA + memB) (stepsA + stepsB)

withinExUnits :: ExUnits -> ExUnits -> Bool
withinExUnits (ExUnits mem steps) (ExUnits maxMem maxSteps) =
    mem <= maxMem && steps <= maxSteps

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

realisticPParams :: PParams ConwayEra
realisticPParams =
    emptyPParams
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (Coin 4_310)
        & ppMaxTxExUnitsL
            .~ ExUnits 140_000_000 10_000_000_000

stateTxId, walletTxId :: ByteString
stateTxId = BS.replicate 32 0xA0
walletTxId = BS.replicate 32 0xC2

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)

expectedOwnerWitness :: KeyHash 'Witness
expectedOwnerWitness = coerce testKh

testKh :: KeyHash 'Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"

stakeKh :: KeyHash 'Staking
stakeKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "dddddddddddddddddddddddddddd\
            \dddddddddddddddddddddddddddd"
