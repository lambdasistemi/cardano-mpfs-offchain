{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.UpdateSpec
-- Description : Unit tests for local update cage construction.
module Cardano.MPFS.Client.Cage.UpdateSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Coerce (coerce)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust)
import Data.Ratio ((%))
import Data.Set qualified as Set
import Data.Word (Word32)
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
    , Withdrawals (..)
    , serialiseAddr
    )
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Alonzo.Scripts
    ( AsIx (..)
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
    , dataToBinaryData
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( certsTxBodyL
    , collateralInputsTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , networkIdTxBodyL
    , outputsTxBodyL
    , referenceInputsTxBodyL
    , vldtTxBodyL
    , withdrawalsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , addrTxOutL
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , TxDats (..)
    , datsTxWitsL
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
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
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
    ( extractHash
    , unsafeMakeSafeHash
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
    ( RequestUpdateFacts (..)
    , TrieFact (..)
    , UpdateFacts (..)
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
    , OnChainTxOutRef (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
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
import Cardano.MPFS.Client.Cage.Identity
    ( onChainTokenId
    , requestAddrFromCfg
    , tokenIdFromJSON
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Cage.Request
    ( requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Update
    ( foldUpdateTrieFacts
    , updateCageTx
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedUpdateFacts
    , verifyRequestUpdateFacts
    , verifyUpdateFacts
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
import Cardano.Tx.Ledger (ConwayTx)
import MPF.Hashes
    ( mkMPFHash
    , renderMPFHash
    )
import MPF.Hashes.Aiken
    ( renderAikenProof
    )
import MPF.Interface
    ( byteStringToHexKey
    )
import MPF.Proof.Insertion
    ( MPFProof (..)
    )
import MPF.Test.Lib
    ( deleteMPFM
    , insertByteStringM
    , proofMPFM
    , runMPFPure'
    )
import MPF.Test.Lib qualified as MPFTest
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )

spec :: Spec
spec = describe "updateCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixture cfg []
        verified <- expectVerified trustedRoot facts
        updateCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixture cfg [(walletTxId, 2, walletTxOutBytes)]
            policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        verified <- expectVerified trustedRoot facts
        updateCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "rejects mismatched request and trie fact counts" $ do
        cfg <- testCageConfig
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixture
                    cfg
                    [(walletTxId, 2, walletTxOutBytes)]
            mismatchedFacts =
                facts{ufTrieFacts = []}
        verified <- expectVerified trustedRoot mismatchedFacts
        updateCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left
                ( MalformedTxOut
                    "update.trie_facts length must match \
                    \update.request_utxos"
                )

    it "builds an unsigned update transaction for verified facts" $ do
        cfg <- testCageConfig
        let UpdateFixture
                { trustedRoot
                , facts
                , stateInput
                , requestInputs
                , walletInput
                } =
                    honestUpdateFixture
                        cfg
                        [(walletTxId, 2, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <- expectUpdateTx cfg verified
        let body = tx ^. bodyTxL
            inputs = body ^. inputsTxBodyL
            collateral = body ^. collateralInputsTxBodyL
            scripts = tx ^. witsTxL . scriptTxWitsL
            redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            integrity =
                body ^. scriptIntegrityHashTxBodyL
            expectedIntegrity =
                computeScriptIntegrity
                    (Set.singleton PlutusV3)
                    realisticPParams
                    redeemers
                    (TxDats mempty)
        Set.member stateInput inputs `shouldBe` True
        mapM_
            ( \requestInput ->
                Set.member requestInput inputs `shouldBe` True
            )
            requestInputs
        Set.member walletInput inputs `shouldBe` True
        Set.member walletInput collateral `shouldBe` True
        body ^. mintTxBodyL `shouldBe` mempty
        Map.size scripts `shouldBe` 2
        Map.member (hashScript (mkCageScript cfg)) scripts
            `shouldBe` True
        Map.size rdmrs `shouldBe` 1 + length requestInputs
        integrity `shouldBe` expectedIntegrity
        body ^. reqSignerHashesTxBodyL
            `shouldBe` Set.singleton expectedOwnerWitness
        txOutputAddresses tx
            `shouldSatisfy` elem (stateAddr cfg)
        txOutputAddresses tx
            `shouldSatisfy` elem ownerAddr

    it "uses the fact-derived validity upper slot" $ do
        cfg <- testCageConfig
        let factSlot = 101
            UpdateFixture{trustedRoot, facts} =
                honestUpdateFixture
                    cfg
                    [(walletTxId, 2, walletTxOutBytes)]
            factsWithSlot =
                facts{ufValidityUpperSlot = factSlot}
        verified <- expectVerified trustedRoot factsWithSlot
        tx <- expectUpdateTx cfg verified
        tx ^. bodyTxL . vldtTxBodyL
            `shouldBe` ValidityInterval
                SNothing
                (SJust $ SlotNo $ fromIntegral factSlot)

    it "processes a request funded at the bounded request fee envelope" $ do
        cfg <- testCageConfig
        requestOut <- requestUpdateOutputFromRequestBuilder cfg
        requestOut ^. coinTxOutL
            `shouldBe` minFundedRequestCoin cfg
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixtureWithRequestOut
                    cfg
                    requestOut
                    [(walletTxId, 2, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <- expectUpdateTx cfg verified
        let refundOut = singleRequestRefundOutput tx
        refundOut ^. addrTxOutL `shouldBe` ownerAddr
        refundOut ^. coinTxOutL
            `shouldSatisfy` (>= refundMinCoin)

    it "keeps the bounded request fee envelope above measured update fee" $ do
        cfg <- testCageConfig
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixtureWithRequestCoin
                    cfg
                    (minFundedRequestCoin cfg)
                    [(walletTxId, 2, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <- expectUpdateTx cfg verified
        let Coin perReqFee = tx ^. bodyTxL . feeTxBodyL
            Coin feeBound = feeBufferUpperBound realisticPParams
        feeBound `shouldSatisfy` (>= perReqFee)

    it "keeps the refund non-negative at minimum request funding" $ do
        cfg <- testCageConfig
        let UpdateFixture{trustedRoot, facts} =
                honestUpdateFixtureWithRequestCoin
                    cfg
                    (minFundedRequestCoin cfg)
                    [(walletTxId, 2, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <- expectUpdateTx cfg verified
        let Coin refundCoin =
                singleRequestRefundOutput tx ^. coinTxOutL
        refundCoin `shouldSatisfy` (>= 0)

    it "matches fact-derived legacy update structure" $ do
        cfg <- testCageConfig
        let UpdateFixture
                { trustedRoot
                , facts
                , stateInput
                , requestInputs
                , walletInput
                , expectedNewRoot
                } =
                    honestUpdateFixture
                        cfg
                        [(walletTxId, 2, walletTxOutBytes)]
        verified <- expectVerified trustedRoot facts
        tx <- expectUpdateTx cfg verified
        let token = tokenIdFromJSON sampleToken
            body = tx ^. bodyTxL
            inputs = body ^. inputsTxBodyL
            outputs = txOutputs tx
            redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            rdmrData = fmap fst rdmrs
            requestInput = onlyRequestInput requestInputs
            stateSpend =
                ConwaySpending
                    (AsIx $ spendingIndex stateInput inputs)
            requestSpend =
                ConwaySpending
                    ( AsIx
                        $ spendingIndex
                            requestInput
                            inputs
                    )
            expectedStateOut =
                stateTxOut cfg (unTokenId token) expectedNewRoot
            Coin txFee = body ^. feeTxBodyL
            Coin requestCoin = minFundedRequestCoin cfg
            expectedRefund =
                mkBasicTxOut
                    ownerAddr
                    (inject $ Coin $ requestCoin - 1_000_000 - txFee)
        -- Q-001 now excludes only provider-runtime per-redeemer ExUnits;
        -- S4b makes the validity upper slot a verified fact.
        inputs
            `shouldBe` Set.fromList
                (stateInput : walletInput : requestInputs)
        body ^. referenceInputsTxBodyL `shouldBe` mempty
        body ^. collateralInputsTxBodyL
            `shouldBe` Set.singleton walletInput
        take 2 outputs `shouldBe` [expectedStateOut, expectedRefund]
        fmap (^. addrTxOutL) outputs
            `shouldSatisfy` elem (stateAddr cfg)
        fmap (^. addrTxOutL) outputs
            `shouldSatisfy` elem ownerAddr
        fmap (^. datumTxOutL) (take 2 outputs)
            `shouldBe` fmap
                (^. datumTxOutL)
                [expectedStateOut, expectedRefund]
        body ^. mintTxBodyL `shouldBe` mempty
        body ^. certsTxBodyL `shouldBe` mempty
        body ^. withdrawalsTxBodyL `shouldBe` Withdrawals mempty
        body ^. networkIdTxBodyL `shouldBe` SNothing
        body ^. vldtTxBodyL
            `shouldBe` ValidityInterval SNothing (SJust $ SlotNo 100)
        tx ^. witsTxL . datsTxWitsL `shouldBe` mempty
        body ^. reqSignerHashesTxBodyL
            `shouldBe` Set.singleton expectedOwnerWitness
        Map.keysSet rdmrData
            `shouldBe` Set.fromList [stateSpend, requestSpend]
        parseRedeemerData (rdmrData Map.! stateSpend)
            `shouldSatisfy` isModifyUpdateRedeemer
        parseRedeemerData (rdmrData Map.! requestSpend)
            `shouldBe` Just
                (Contribute $ txInToOnChainRef stateInput)
        body ^. scriptIntegrityHashTxBodyL
            `shouldBe` computeScriptIntegrity
                (Set.singleton PlutusV3)
                realisticPParams
                redeemers
                (TxDats mempty)

    it "folds update MPF facts to the same new root as the legacy fold" $ do
        cfg <- testCageConfig
        let UpdateFixture
                { facts = UpdateFacts{ufTrieRoot, ufTrieFacts}
                , requestDatums
                , expectedNewRoot
                } =
                    honestUpdateFixture
                        cfg
                        [(walletTxId, 2, walletTxOutBytes)]
            Hex oldRoot = ufTrieRoot
        foldUpdateTrieFacts oldRoot (zip requestDatums ufTrieFacts)
            `shouldBe` Right expectedNewRoot

data UpdateFixture = UpdateFixture
    { trustedRoot :: TrustedRoot
    , facts :: UpdateFacts
    , stateInput :: TxIn
    , requestInputs :: [TxIn]
    , walletInput :: TxIn
    , requestDatums :: [OnChainRequest]
    , expectedNewRoot :: ByteString
    }

honestUpdateFixture
    :: CageConfig
    -> [(ByteString, Word, ByteString)]
    -> UpdateFixture
honestUpdateFixture cfg =
    honestUpdateFixtureWithRequestCoin cfg (minFundedRequestCoin cfg)

honestUpdateFixtureWithRequestCoin
    :: CageConfig
    -> Coin
    -> [(ByteString, Word, ByteString)]
    -> UpdateFixture
honestUpdateFixtureWithRequestCoin cfg requestCoin =
    honestUpdateFixtureWithRequestOut
        cfg
        (requestTxOut requestCoin requestAddr requestDatum)
  where
    token = tokenIdFromJSON sampleToken
    requestAddr =
        requestAddrFromCfg cfg token Testnet
    requestDatum = updateRequestDatum token

honestUpdateFixtureWithRequestOut
    :: CageConfig
    -> TxOut ConwayEra
    -> [(ByteString, Word, ByteString)]
    -> UpdateFixture
honestUpdateFixtureWithRequestOut cfg requestOut walletRows =
    let TrieFixture{oldRoot, trieFact, newRoot} =
            deterministicTrieFixture
        token = tokenIdFromJSON sampleToken
        asset = unTokenId token
        stateBytes =
            serialize' (natVersion @11)
                $ stateTxOut cfg asset oldRoot
        requestDatum = updateRequestDatum token
        requestBytes =
            serialize' (natVersion @11) requestOut
        rows =
            [ (stateTxId, 0, stateBytes)
            , (requestTxId, 1, requestBytes)
            ]
                <> walletRows
        (root, entries) = csmtEntries rows
        (stateEntry, requestEntries, walletEntries) =
            splitFixtureEntries entries
        updateFacts =
            UpdateFacts
                { ufSnapshot = snapshotWithRoot root
                , ufToken = sampleToken
                , ufStateUtxo = stateEntry
                , ufRequestUtxos = requestEntries
                , ufWalletUtxos = walletEntries
                , ufTrieRoot = Hex oldRoot
                , ufTrieFacts = [trieFact]
                , ufValidityUpperSlot = 100
                , ufProtocolParameters =
                    pparamsFacts realisticPParams
                }
    in  UpdateFixture
            { trustedRoot = TrustedRoot (Hex root)
            , facts = updateFacts
            , stateInput = txInFromBytes stateTxId 0
            , requestInputs = [txInFromBytes requestTxId 1]
            , walletInput = txInFromBytes walletTxId 2
            , requestDatums = [requestDatum]
            , expectedNewRoot = newRoot
            }

data TrieFixture = TrieFixture
    { oldRoot :: ByteString
    , trieFact :: TrieFact
    , newRoot :: ByteString
    }

deterministicTrieFixture :: TrieFixture
deterministicTrieFixture =
    let ((rootBefore, proofBytes, rootAfter), _) =
            runMPFPure' $ do
                insertByteStringM updateKey oldValue
                old <- requireRoot "old"
                mProof <-
                    proofMPFM
                        ( byteStringToHexKey
                            (renderMPFHash (mkMPFHash updateKey))
                        )
                let proof = case mProof of
                        Just p ->
                            renderAikenProof (mpfProofSteps p)
                        Nothing -> BS.empty
                _ <-
                    deleteMPFM
                        ( byteStringToHexKey
                            (renderMPFHash (mkMPFHash updateKey))
                        )
                insertByteStringM updateKey newValue
                new <- requireRoot "new"
                pure (old, proof, new)
    in  TrieFixture
            { oldRoot = rootBefore
            , trieFact =
                TrieFact
                    { tfKey = Hex updateKey
                    , tfValue = Just (Hex oldValue)
                    , tfMpfProof = Hex proofBytes
                    }
            , newRoot = rootAfter
            }
  where
    requireRoot label = do
        mRoot <- MPFTest.getRootHashM
        pure $ case mRoot of
            Just root -> renderMPFHash root
            Nothing ->
                error
                    ( "deterministicTrieFixture: missing "
                        <> label
                        <> " root"
                    )

expectVerified
    :: TrustedRoot
    -> UpdateFacts
    -> IO VerifiedUpdateFacts
expectVerified trusted facts =
    case verifyUpdateFacts trusted facts of
        Left err ->
            expectationFailure ("verifyUpdateFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified

expectUpdateTx
    :: CageConfig -> VerifiedUpdateFacts -> IO ConwayTx
expectUpdateTx cfg verified =
    case updateCageTx cfg permissiveWalletPolicy verified of
        Left err ->
            expectationFailure ("updateCageTx failed: " <> show err)
                *> error "unreachable"
        Right tx -> pure tx

requestUpdateOutputFromRequestBuilder
    :: CageConfig -> IO (TxOut ConwayEra)
requestUpdateOutputFromRequestBuilder cfg = do
    let (root, entries) =
            csmtEntries [(walletTxId, 2, walletTxOutBytes)]
        requestFacts =
            RequestUpdateFacts
                { rufSnapshot = snapshotWithRoot root
                , rufToken = sampleToken
                , rufKey = Hex updateKey
                , rufOldValue = Hex oldValue
                , rufNewValue = Hex newValue
                , rufAddress = Hex (serialiseAddr fundingAddr)
                , rufSubmittedAt = submittedAt
                , rufWalletUtxos = entries
                , rufProtocolParameters =
                    pparamsFacts realisticPParams
                }
        trusted = TrustedRoot (Hex root)
    verified <-
        case verifyRequestUpdateFacts trusted requestFacts of
            Left err ->
                expectationFailure
                    ( "verifyRequestUpdateFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value -> pure value
    tx <-
        case requestUpdateCageTx cfg permissiveWalletPolicy verified of
            Left err ->
                expectationFailure
                    ("requestUpdateCageTx failed: " <> show err)
                    *> error "unreachable"
            Right value -> pure value
    let requestAddr =
            requestAddrFromCfg
                cfg
                (tokenIdFromJSON sampleToken)
                Testnet
        requestOutputs =
            [ out
            | out <- txOutputs tx
            , out ^. addrTxOutL == requestAddr
            ]
    case requestOutputs of
        [out] -> pure out
        outs ->
            expectationFailure
                ( "expected one request output, got "
                    <> show (length outs)
                )
                *> error "unreachable"

minFundedRequestCoin :: CageConfig -> Coin
minFundedRequestCoin cfg =
    let Coin tipAmount = defaultTip cfg
        Coin feeBound = feeBufferUpperBound realisticPParams
        Coin refMin = refundMinCoin
    in  Coin (tipAmount + feeBound + refMin)

refundMinCoin :: Coin
refundMinCoin =
    getMinCoinTxOut
        realisticPParams
        (mkBasicTxOut ownerAddr (inject (Coin 0)))

feeBufferUpperBound :: PParams ConwayEra -> Coin
feeBufferUpperBound pp =
    let CoinPerByte minFeeACompact = pp ^. ppTxFeePerByteL
        Coin minFeeA = fromCompact minFeeACompact
        Coin minFeeB = pp ^. ppTxFeeFixedL
        Coin scriptFee =
            txscriptfee (pp ^. ppPricesL) (pp ^. ppMaxTxExUnitsL)
    in  Coin
            ( minFeeB
                + minFeeA * maxUpdateTxBytes
                + scriptFee
            )

maxUpdateTxBytes :: Integer
maxUpdateTxBytes = 8192

singleRequestRefundOutput :: ConwayTx -> TxOut ConwayEra
singleRequestRefundOutput tx =
    case drop 1 (txOutputs tx) of
        out : _ -> out
        [] -> error "UpdateSpec expected a refund output"

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

stateTxOut
    :: CageConfig -> AssetName -> ByteString -> TxOut ConwayEra
stateTxOut cfg asset root =
    mkBasicTxOut (stateAddr cfg) stateValue
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
                , stateRoot = OnChainRoot root
                , stateMaxFee = 1_000_000
                , stateProcessTime = 60_000
                , stateRetractTime = 30_000
                }

requestTxOut :: Coin -> Addr -> OnChainRequest -> TxOut ConwayEra
requestTxOut requestCoin requestAddr requestDatum =
    mkBasicTxOut requestAddr (inject requestCoin)
        & datumTxOutL .~ mkInlineDatum (RequestDatum requestDatum)

updateRequestDatum :: TokenId -> OnChainRequest
updateRequestDatum token =
    OnChainRequest
        { requestToken = onChainTokenId token
        , requestOwner =
            BuiltinByteString
                $ hashToBytes
                $ let KeyHash h = testKh in h
        , requestKey = updateKey
        , requestValue = OpUpdate oldValue newValue
        , requestFee = 1_000_000
        , requestSubmittedAt = submittedAt
        }

walletTxOutBytes :: ByteString
walletTxOutBytes =
    serialize' (natVersion @11) walletTxOut

walletTxOut :: TxOut ConwayEra
walletTxOut =
    mkBasicTxOut
        fundingAddr
        (inject (Coin 50_000_000))

stateAddr :: CageConfig -> Addr
stateAddr cfg =
    Addr
        Testnet
        (ScriptHashObj $ cfgScriptHash cfg)
        StakeRefNull

ownerAddr :: Addr
ownerAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

fundingAddr :: Addr
fundingAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

txOutputAddresses :: ConwayTx -> [Addr]
txOutputAddresses tx =
    fmap (^. addrTxOutL) (txOutputs tx)

txOutputs :: ConwayTx -> [TxOut ConwayEra]
txOutputs tx =
    foldr (:) [] $ tx ^. bodyTxL . outputsTxBodyL

onlyRequestInput :: [TxIn] -> TxIn
onlyRequestInput [requestInput] = requestInput
onlyRequestInput _ =
    error "UpdateSpec expected one request input"

splitFixtureEntries
    :: [UtxoEntry] -> (UtxoEntry, [UtxoEntry], [UtxoEntry])
splitFixtureEntries (stateEntry : requestEntry : walletEntries) =
    (stateEntry, [requestEntry], walletEntries)
splitFixtureEntries _ =
    error "UpdateSpec expected state and request entries"

parseRedeemerData :: Data ConwayEra -> Maybe UpdateRedeemer
parseRedeemerData (Data plcData) =
    fromBuiltinData (BuiltinData plcData)

isModifyUpdateRedeemer :: Maybe UpdateRedeemer -> Bool
isModifyUpdateRedeemer (Just (Modify [Update _proofSteps])) = True
isModifyUpdateRedeemer _ = False

txInToOnChainRef :: TxIn -> OnChainTxOutRef
txInToOnChainRef (TxIn (TxId h) (TxIx ix)) =
    OnChainTxOutRef
        { txOutRefId =
            BuiltinByteString
                (hashToBytes (extractHash h))
        , txOutRefIdx = fromIntegral ix
        }

spendingIndex :: TxIn -> Set.Set TxIn -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "spendingIndex: TxIn not in input set"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

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
stateTxId = BS.replicate 32 0xA0
requestTxId = BS.replicate 32 0xB1
walletTxId = BS.replicate 32 0xC2

sampleToken :: TokenIdJSON
sampleToken = TokenIdJSON (BS.replicate 32 0xE4)

submittedAt :: Integer
submittedAt = 1_700_000_000_000

updateKey, oldValue, newValue :: ByteString
updateKey = "mykey"
oldValue = "oldvalue"
newValue = "newvalue"

expectedOwnerWitness :: KeyHash Guard
expectedOwnerWitness = coerce testKh

testKh :: KeyHash Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"
