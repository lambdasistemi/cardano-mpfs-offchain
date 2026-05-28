{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.RequestSpec
-- Description : Unit tests for local request cage construction.
module Cardano.MPFS.Client.Cage.RequestSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
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
    )
import Cardano.Ledger.Address
    ( Addr (..)
    , serialiseAddr
    )
import Cardano.Ledger.Api.PParams
    ( emptyPParams
    , ppCoinsPerUTxOByteL
    , ppMaxTxExUnitsL
    , ppMinFeeAL
    , ppMinFeeBL
    , ppPricesL
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( inputsTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , coinTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( rdmrsTxWitsL
    , scriptTxWitsL
    )
import Cardano.Ledger.Babbage.PParams
    ( CoinPerByte (..)
    )
import Cardano.Ledger.BaseTypes
    ( BoundedRational (..)
    , Inject (..)
    , Network (..)
    , NonNegativeInterval
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
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    , Prices (..)
    , txscriptfee
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
    ( RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , RequestUpdateFacts (..)
    )
import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    )
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , computeScriptHash
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestAddrFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedRequestDeleteFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestUpdateFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.Slotting.Slot
    ( SlotNo (..)
    )

spec :: Spec
spec = do
    insertSpec
    deleteSpec
    updateSpec

insertSpec :: Spec
insertSpec = describe "requestInsertCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let RequestInsertFixture{trustedRoot, facts} =
                deterministicRequestInsertFixture []
        verified <- expectVerified trustedRoot facts
        requestInsertCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let RequestInsertFixture{trustedRoot, facts} =
                deterministicRequestInsertFixture
                    [(walletTxId, 1, walletTxOutBytes)]
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <- expectVerified trustedRoot facts
        requestInsertCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "builds an unsigned insert request for verified facts" $ do
        cfg <- testCageConfig
        let RequestInsertFixture
                { trustedRoot
                , facts
                , walletInput
                } =
                    deterministicRequestInsertFixture
                        [(walletTxId, 1, walletTxOutBytes)]
            requestAddr =
                requestAddrFromCfg
                    cfg
                    (tokenIdFromJSON sampleToken)
                    Testnet
        verified <- expectVerified trustedRoot facts
        tx <-
            case requestInsertCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestInsertCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        Set.member walletInput (tx ^. bodyTxL . inputsTxBodyL)
            `shouldBe` True
        tx ^. bodyTxL . mintTxBodyL `shouldBe` mempty
        Map.size (tx ^. witsTxL . scriptTxWitsL) `shouldBe` 0
        tx ^. witsTxL . rdmrsTxWitsL `shouldBe` mempty
        txOutputAddresses tx `shouldSatisfy` elem requestAddr

    it "locks insert request funding from the bounded fee envelope" $ do
        cfg <- testCageConfig
        let RequestInsertFixture{trustedRoot, facts} =
                deterministicRequestInsertFixture
                    [(walletTxId, 1, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <-
            case requestInsertCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestInsertCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        assertBoundedRequestOutput cfg tx

deleteSpec :: Spec
deleteSpec = describe "requestDeleteCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let RequestDeleteFixture{deleteTrustedRoot, deleteFacts} =
                deterministicRequestDeleteFixture []
        verified <-
            expectDeleteVerified deleteTrustedRoot deleteFacts
        requestDeleteCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let RequestDeleteFixture{deleteTrustedRoot, deleteFacts} =
                deterministicRequestDeleteFixture
                    [(walletTxId, 1, walletTxOutBytes)]
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <-
            expectDeleteVerified deleteTrustedRoot deleteFacts
        requestDeleteCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "builds an unsigned delete request for verified facts" $ do
        cfg <- testCageConfig
        let RequestDeleteFixture
                { deleteTrustedRoot
                , deleteFacts
                , deleteWalletInput
                } =
                    deterministicRequestDeleteFixture
                        [(walletTxId, 1, walletTxOutBytes)]
            requestAddr =
                requestAddrFromCfg
                    cfg
                    (tokenIdFromJSON sampleToken)
                    Testnet
        verified <-
            expectDeleteVerified deleteTrustedRoot deleteFacts
        tx <-
            case requestDeleteCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestDeleteCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        Set.member
            deleteWalletInput
            (tx ^. bodyTxL . inputsTxBodyL)
            `shouldBe` True
        tx ^. bodyTxL . mintTxBodyL `shouldBe` mempty
        Map.size (tx ^. witsTxL . scriptTxWitsL) `shouldBe` 0
        tx ^. witsTxL . rdmrsTxWitsL `shouldBe` mempty
        txOutputAddresses tx `shouldSatisfy` elem requestAddr

    it "locks delete request funding from the bounded fee envelope" $ do
        cfg <- testCageConfig
        let RequestDeleteFixture{deleteTrustedRoot, deleteFacts} =
                deterministicRequestDeleteFixture
                    [(walletTxId, 1, walletTxOutBytes)]
        verified <-
            expectDeleteVerified deleteTrustedRoot deleteFacts
        tx <-
            case requestDeleteCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestDeleteCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        assertBoundedRequestOutput cfg tx

updateSpec :: Spec
updateSpec = describe "requestUpdateCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let RequestUpdateFixture{updateTrustedRoot, updateFacts} =
                deterministicRequestUpdateFixture []
        verified <-
            expectUpdateVerified updateTrustedRoot updateFacts
        requestUpdateCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let RequestUpdateFixture{updateTrustedRoot, updateFacts} =
                deterministicRequestUpdateFixture
                    [(walletTxId, 1, walletTxOutBytes)]
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <-
            expectUpdateVerified updateTrustedRoot updateFacts
        requestUpdateCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "builds an unsigned update request for verified facts" $ do
        cfg <- testCageConfig
        let RequestUpdateFixture
                { updateTrustedRoot
                , updateFacts
                , updateWalletInput
                } =
                    deterministicRequestUpdateFixture
                        [(walletTxId, 1, walletTxOutBytes)]
            requestAddr =
                requestAddrFromCfg
                    cfg
                    (tokenIdFromJSON sampleToken)
                    Testnet
        verified <-
            expectUpdateVerified updateTrustedRoot updateFacts
        tx <-
            case requestUpdateCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestUpdateCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        Set.member
            updateWalletInput
            (tx ^. bodyTxL . inputsTxBodyL)
            `shouldBe` True
        tx ^. bodyTxL . mintTxBodyL `shouldBe` mempty
        Map.size (tx ^. witsTxL . scriptTxWitsL) `shouldBe` 0
        tx ^. witsTxL . rdmrsTxWitsL `shouldBe` mempty
        txOutputAddresses tx `shouldSatisfy` elem requestAddr

    it "locks update request funding from the bounded fee envelope" $ do
        cfg <- testCageConfig
        let RequestUpdateFixture{updateTrustedRoot, updateFacts} =
                deterministicRequestUpdateFixture
                    [(walletTxId, 1, walletTxOutBytes)]
        verified <-
            expectUpdateVerified updateTrustedRoot updateFacts
        tx <-
            case requestUpdateCageTx
                cfg
                permissiveWalletPolicy
                verified of
                Left err ->
                    expectationFailure
                        ("requestUpdateCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        assertBoundedRequestOutput cfg tx

data RequestUpdateFixture = RequestUpdateFixture
    { updateTrustedRoot :: TrustedRoot
    , updateFacts :: RequestUpdateFacts
    , updateWalletInput :: TxIn
    }

deterministicRequestUpdateFixture
    :: [(ByteString, Word, ByteString)] -> RequestUpdateFixture
deterministicRequestUpdateFixture rows =
    let (root, entries) = csmtEntries rows
        input = txInFromBytes walletTxId 1
        facts =
            RequestUpdateFacts
                { rufSnapshot = snapshotWithRoot root
                , rufToken = sampleToken
                , rufKey = Hex "mykey"
                , rufOldValue = Hex "oldvalue"
                , rufNewValue = Hex "newvalue"
                , rufAddress = Hex (serialiseAddr fundingAddr)
                , rufSubmittedAt = submittedAt
                , rufWalletUtxos = entries
                , rufProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  RequestUpdateFixture
            { updateTrustedRoot = TrustedRoot (Hex root)
            , updateFacts = facts
            , updateWalletInput = input
            }

expectUpdateVerified
    :: TrustedRoot
    -> RequestUpdateFacts
    -> IO VerifiedRequestUpdateFacts
expectUpdateVerified trusted facts =
    case verifyRequestUpdateFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestUpdateFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

data RequestDeleteFixture = RequestDeleteFixture
    { deleteTrustedRoot :: TrustedRoot
    , deleteFacts :: RequestDeleteFacts
    , deleteWalletInput :: TxIn
    }

deterministicRequestDeleteFixture
    :: [(ByteString, Word, ByteString)] -> RequestDeleteFixture
deterministicRequestDeleteFixture rows =
    let (root, entries) = csmtEntries rows
        input = txInFromBytes walletTxId 1
        facts =
            RequestDeleteFacts
                { rdfSnapshot = snapshotWithRoot root
                , rdfToken = sampleToken
                , rdfKey = Hex "mykey"
                , rdfValue = Hex "myvalue"
                , rdfAddress = Hex (serialiseAddr fundingAddr)
                , rdfSubmittedAt = submittedAt
                , rdfWalletUtxos = entries
                , rdfProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  RequestDeleteFixture
            { deleteTrustedRoot = TrustedRoot (Hex root)
            , deleteFacts = facts
            , deleteWalletInput = input
            }

expectDeleteVerified
    :: TrustedRoot
    -> RequestDeleteFacts
    -> IO VerifiedRequestDeleteFacts
expectDeleteVerified trusted facts =
    case verifyRequestDeleteFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestDeleteFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

data RequestInsertFixture = RequestInsertFixture
    { trustedRoot :: TrustedRoot
    , facts :: RequestInsertFacts
    , walletInput :: TxIn
    }

deterministicRequestInsertFixture
    :: [(ByteString, Word, ByteString)] -> RequestInsertFixture
deterministicRequestInsertFixture rows =
    let (root, entries) = csmtEntries rows
        input = txInFromBytes walletTxId 1
        requestFacts =
            RequestInsertFacts
                { rifSnapshot = snapshotWithRoot root
                , rifToken = sampleToken
                , rifKey = Hex "mykey"
                , rifValue = Hex "myvalue"
                , rifAddress = Hex (serialiseAddr fundingAddr)
                , rifSubmittedAt = submittedAt
                , rifWalletUtxos = entries
                , rifProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  RequestInsertFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = requestFacts
            , walletInput = input
            }

csmtEntries
    :: [(ByteString, Word, ByteString)]
    -> (ByteString, [UtxoEntry])
csmtEntries rows = evalPureFromEmptyDB $ do
    mapM_
        ( \(txIdBytes, txIx, txOutBytes) ->
            insertMHash
                (byteStringToKey (encodeTxIn txIdBytes txIx))
                (mkHash txOutBytes)
        )
        rows
    entries <-
        traverse
            ( \(txIdBytes, txIx, txOutBytes) -> do
                mProof <-
                    proofM
                        hashCodecs
                        identityFromKV
                        hashHashing
                        (byteStringToKey (encodeTxIn txIdBytes txIx))
                let proofBytes = case mProof of
                        Just (_, proof) -> renderProof proof
                        Nothing -> BS.empty
                pure
                    UtxoEntry
                        { ueRef =
                            UtxoRef
                                { urTxId = Hex txIdBytes
                                , urTxIx = fromIntegral txIx
                                }
                        , ueTxOutCbor = Hex txOutBytes
                        , ueInclusionProof = Hex proofBytes
                        }
            )
            rows
    mRoot <- getRootHashM
    let rootBytes = maybe (BS.replicate 32 0) renderHash mRoot
    pure (rootBytes, entries)

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

walletTxOutBytes :: ByteString
walletTxOutBytes =
    serialize' (natVersion @11) walletTxOut

walletTxOut :: TxOut ConwayEra
walletTxOut =
    mkBasicTxOut
        fundingAddr
        (inject (Coin 50_000_000))

fundingAddr :: Addr
fundingAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

txOutputAddresses :: Tx ConwayEra -> [Addr]
txOutputAddresses tx =
    (^. addrTxOutL) <$> txOutputs tx

txOutputs :: Tx ConwayEra -> [TxOut ConwayEra]
txOutputs tx =
    foldr (:) [] $ tx ^. bodyTxL . outputsTxBodyL

assertBoundedRequestOutput :: CageConfig -> Tx ConwayEra -> IO ()
assertBoundedRequestOutput cfg tx =
    case requestOutputs of
        [out] ->
            out ^. coinTxOutL `shouldBe` expectedRequestCoin cfg
        outs ->
            expectationFailure
                ( "expected one request output, got "
                    <> show (length outs)
                )
  where
    requestAddr =
        requestAddrFromCfg
            cfg
            (tokenIdFromJSON sampleToken)
            Testnet
    requestOutputs =
        [ out
        | out <- txOutputs tx
        , out ^. addrTxOutL == requestAddr
        ]

expectedRequestCoin :: CageConfig -> Coin
expectedRequestCoin cfg =
    let Coin tipAmount = defaultTip cfg
        Coin refMin =
            getMinCoinTxOut
                realisticPParams
                (mkBasicTxOut fundingAddr (inject (Coin 0)))
        Coin feeBound =
            feeBufferUpperBound realisticPParams
    in  Coin (tipAmount + feeBound + refMin)

feeBufferUpperBound :: PParams ConwayEra -> Coin
feeBufferUpperBound pp =
    let Coin minFeeA = pp ^. ppMinFeeAL
        Coin minFeeB = pp ^. ppMinFeeBL
        Coin scriptFee =
            txscriptfee (pp ^. ppPricesL) (pp ^. ppMaxTxExUnitsL)
    in  Coin
            ( minFeeB
                + minFeeA * maxUpdateTxBytes
                + scriptFee
            )

maxUpdateTxBytes :: Integer
maxUpdateTxBytes = 8192

expectVerified
    :: TrustedRoot
    -> RequestInsertFacts
    -> IO VerifiedRequestInsertFacts
expectVerified trusted facts =
    case verifyRequestInsertFacts trusted facts of
        Left err ->
            expectationFailure
                ("verifyRequestInsertFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

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
        & ppMinFeeAL .~ Coin 44
        & ppMinFeeBL .~ Coin 155_381
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (Coin 4_310)
        & ppPricesL
            .~ Prices
                (unsafeNonNegativeInterval (577 % 10_000))
                (unsafeNonNegativeInterval (721 % 10_000_000))
        & ppMaxTxExUnitsL
            .~ ExUnits 140_000_000 10_000_000_000

unsafeNonNegativeInterval :: Rational -> NonNegativeInterval
unsafeNonNegativeInterval r =
    fromJust (boundRational r)

walletTxId :: ByteString
walletTxId = BS.replicate 32 0xC2

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)

submittedAt :: Integer
submittedAt = 1_700_000_000_000

testKh :: KeyHash 'Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"
