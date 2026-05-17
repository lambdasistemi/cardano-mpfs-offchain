{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.BootSpec
-- Description : Unit tests for local boot cage transaction construction.
module Cardano.MPFS.Client.Cage.BootSpec
    ( spec
    ) where

import Codec.CBOR.Encoding qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Control.Monad (filterM)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust)
import Lens.Micro ((&), (.~), (^.))
import System.Directory (doesFileExist)
import System.Environment (getEnv)
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
    , hashFromStringAsHex
    )
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Alonzo.TxBody
    ( scriptIntegrityHashTxBodyL
    )
import Cardano.Ledger.Api.PParams
    ( emptyPParams
    , ppCoinsPerUTxOByteL
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Out (TxOut, mkBasicTxOut)
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.Babbage.PParams (CoinPerByte (..))
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    )
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Core (PParams)
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    , Prices (..)
    )
import Cardano.Ledger.Plutus.Language (Language (..))
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootFacts (..)
    , ChainPointJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Cage.Ledger
    ( ConwayEra
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , computeScriptHash
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Facts
    ( VerifiedBootFacts
    , verifyBootFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.Node.Client.Balance
    ( computeScriptIntegrity
    , evalBudgetExUnits
    )
import Cardano.Slotting.Slot (SlotNo (..))

spec :: Spec
spec = describe "bootCageTx" $ do
    it "rejects empty funding before building" $ do
        cfg <- testCageConfig
        verified <-
            verifiedBootFacts
                BootFacts
                    { bfSnapshot =
                        snapshotWithRoot (BS.replicate 32 0)
                    , bfWalletUtxos = []
                    , bfProtocolParameters =
                        pparamsFacts realisticPParams
                    }
        bootCageTx cfg permissiveWalletPolicy verified
            `shouldBe` Left EmptyFunding

    it "rejects wallet policy caps before signing" $ do
        cfg <- testCageConfig
        facts <- deterministicBootFacts
        verified <- verifiedBootFacts facts
        let policy =
                permissiveWalletPolicy
                    { wpMaxMinUtxoCoinPerByte = Coin 1
                    }
        bootCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( MinUtxoCoinPerByteTooHigh
                        (Coin 4_310)
                        (Coin 1)
                    )
                )

    it "rejects unbounded validity under a finite wallet cap" $ do
        cfg <- testCageConfig
        facts <- deterministicBootFacts
        verified <- verifiedBootFacts facts
        let policy =
                permissiveWalletPolicy
                    { wpMaxValidityWindow = SlotNo 1
                    }
        bootCageTx cfg policy verified
            `shouldBe` Left
                ( PolicyViolation
                    ( ValidityWindowTooWide
                        (SlotNo maxBound)
                        (SlotNo 1)
                    )
                )

    it "does not reuse the placeholder-budget legacy boot vector" $ do
        cfg <- testCageConfig
        facts <- deterministicBootFacts
        verified <- verifiedBootFacts facts
        tx <-
            case bootCageTx cfg permissiveWalletPolicy verified of
                Left err ->
                    expectationFailure
                        ("bootCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        expected <-
            BS.readFile =<< legacyBootVectorPath
        serialize' (natVersion @11) tx `shouldNotBe` expected

    it "sets a submit-valid minting budget and script integrity" $ do
        cfg <- testCageConfig
        facts <- deterministicBootFacts
        verified <- verifiedBootFacts facts
        tx <-
            case bootCageTx cfg permissiveWalletPolicy verified of
                Left err ->
                    expectationFailure
                        ("bootCageTx failed: " <> show err)
                        *> error "unreachable"
                Right tx -> pure tx
        let redeemers@(Redeemers rdmrs) =
                tx ^. witsTxL . rdmrsTxWitsL
            budgets =
                snd <$> Map.elems rdmrs
            integrity =
                tx ^. bodyTxL . scriptIntegrityHashTxBodyL
            expectedIntegrity =
                computeScriptIntegrity
                    PlutusV3
                    realisticPParams
                    redeemers
        Map.size rdmrs `shouldBe` 1
        budgets `shouldBe` [evalBudgetExUnits]
        budgets `shouldSatisfy` all nonZeroExUnits
        integrity `shouldBe` expectedIntegrity

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

verifiedBootFacts :: BootFacts -> IO VerifiedBootFacts
verifiedBootFacts facts =
    case verifyBootFacts trusted facts of
        Left err ->
            expectationFailure ("verifyBootFacts failed: " <> show err)
                *> error "unreachable"
        Right verified ->
            pure verified
  where
    trusted =
        TrustedRoot (vsUtxoRoot (bfSnapshot facts))

legacyBootVectorPath :: IO FilePath
legacyBootVectorPath = do
    let candidates =
            [ "specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor"
            , "../specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor"
            ]
    existing <- filterM doesFileExist candidates
    case existing of
        path : _ -> pure path
        [] ->
            expectationFailure
                "legacy boot vector not found from test working directory"
                *> error "unreachable"

deterministicBootFacts :: IO BootFacts
deterministicBootFacts = do
    let rows =
            [ (testTxId1Bytes, 0, walletTxOutBytes)
            , (testTxId2Bytes, 1, walletTxOutBytes)
            ]
        (root, entries) = csmtEntries rows
    pure
        BootFacts
            { bfSnapshot = snapshotWithRoot root
            , bfWalletUtxos = entries
            , bfProtocolParameters =
                pparamsFacts realisticPParams
            }

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
    let rootBytes = maybe BS.empty renderHash mRoot
    pure (rootBytes, entries)

encodeTxIn :: ByteString -> Word -> ByteString
encodeTxIn txIdBytes txIx =
    CBOR.toStrictByteString
        $ mconcat
            [ CBOR.encodeListLen 2
            , CBOR.encodeBytes txIdBytes
            , CBOR.encodeWord64 (fromIntegral txIx)
            ]

walletTxOutBytes :: ByteString
walletTxOutBytes =
    serialize' (natVersion @11) walletTxOut

walletTxOut :: TxOut ConwayEra
walletTxOut =
    mkBasicTxOut
        ownerAddr
        (inject (Coin 50_000_000))

ownerAddr :: Addr
ownerAddr = Addr Testnet (KeyHashObj testKh) StakeRefNull

testCageConfig :: IO CageConfig
testCageConfig = do
    blueprintPath <- getEnv "MPFS_BLUEPRINT"
    eBlueprint <- loadBlueprint blueprintPath
    blueprint <- case eBlueprint of
        Left err ->
            expectationFailure
                ("loadBlueprint failed: " <> err)
                *> error "unreachable"
        Right blueprint -> pure blueprint
    scriptBytes <- case extractCompiledCode "state." blueprint of
        Nothing ->
            expectationFailure "state compiled code missing"
                *> error "unreachable"
        Just scriptBytes -> pure scriptBytes
    pure
        CageConfig
            { cageScriptBytes = scriptBytes
            , requestScriptBytes = SBS.empty
            , cfgScriptHash = computeScriptHash scriptBytes
            , defaultProcessTime = 300_000
            , defaultRetractTime = 600_000
            , defaultTip = Coin 1_000_000
            , network = Testnet
            }

realisticPParams :: PParams ConwayEra
realisticPParams =
    emptyPParams
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (Coin 4_310)

testTxId1Bytes :: ByteString
testTxId1Bytes = BS.replicate 32 0x11

testTxId2Bytes :: ByteString
testTxId2Bytes = BS.replicate 32 0x22

testKh :: KeyHash 'Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"
