{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.MPFS.TxBuilderSpec (spec) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Foldable (for_)
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust, fromMaybe, isJust)
import Data.Ratio ((%))
import Data.Set qualified as Set
import Lens.Micro ((&), (.~), (^.))
import System.Directory (doesFileExist)
import System.IO.Unsafe (unsafePerformIO)

import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck
    ( forAll
    , generate
    , property
    )

import Cardano.Crypto.Hash
    ( Blake2b_224
    , hashFromBytes
    , hashFromStringAsHex
    , hashToBytes
    )
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
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
    ( collateralInputsTxBodyL
    , feeTxBodyL
    , inputsTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    , referenceInputsTxBodyL
    , reqSignerHashesTxBodyL
    , vldtTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
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
    ( BoundedRational (..)
    , Inject (..)
    , Network (..)
    , NonNegativeInterval
    , StrictMaybe (..)
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    , PolicyID (..)
    )
import Cardano.Ledger.Plutus.ExUnits
    ( ExUnits (..)
    , Prices (..)
    , txscriptfee
    )
import Cardano.Ledger.TxIn (TxIn)

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , BlockId (..)
    , Coin (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
    , Operation (..)
    , PParams
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Generators
    ( genKeyHash
    , genMaxFee
    , genTokenId
    , genTrieKey
    , genTrieValue
    , genTxIn
    )
import Cardano.MPFS.Mock.State (mkMockState)
import Cardano.MPFS.Provider (Provider (..), SlotNo (..))
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , RequestProof (..)
    , TxBuilder (..)
    )
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real
    ( computeRefund
    , extractCageDatum
    , mkInlineDatum
    , mkRealTxBuilder
    , mkRequestDatum
    , requestLockedAda
    , spendingIndex
    , toPlcData
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( requestAddrFromCfg
    )
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

-- ---------------------------------------------------------
-- Test helpers
-- ---------------------------------------------------------

-- | Synthetic 28-byte script hash used by the
-- TxBuilder unit tests as a stand-in for "the cage
-- script". The real global state validator hash is
-- carried by 'cfgScriptHash' on a 'CageConfig' built
-- from the upstream blueprint at startup; these unit
-- tests do not load a blueprint, so a fixed dummy
-- value is used instead.
cageScriptHash :: ByteString
cageScriptHash = BS.replicate 28 0xAB

cageScriptHashLedger :: ScriptHash
cageScriptHashLedger =
    ScriptHash
        $ fromJust
        $ hashFromBytes cageScriptHash

cagePolicyId :: PolicyID
cagePolicyId = PolicyID cageScriptHashLedger

cageAddr :: Network -> Addr
cageAddr net =
    Addr
        net
        (ScriptHashObj cageScriptHashLedger)
        StakeRefNull

-- | Testnet address from a payment key hash.
testAddr :: KeyHash 'Payment -> Addr
testAddr kh =
    Addr Testnet (KeyHashObj kh) StakeRefNull

-- | A fixed test key hash.
testKh :: KeyHash 'Payment
testKh =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "00000000000000000000000000\
            \00000000000000000000000000000a"

-- | A fixed 'BundleSnapshot' used by every test
-- runner. Tests do not yet assert on the snapshot
-- so any stable value works.
testSnap :: BundleSnapshot
testSnap =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

-- | Zero-fee PParams for deterministic balancing.
zeroPP :: PParams ConwayEra
zeroPP = emptyPParams

-- | Realistic PParams with mainnet coinsPerUTxOByte.
realisticPP :: PParams ConwayEra
realisticPP =
    emptyPParams
        & ppMinFeeAL .~ Coin 44
        & ppMinFeeBL .~ Coin 155_381
        & ppCoinsPerUTxOByteL
            .~ CoinPerByte (Coin 4310)
        & ppPricesL
            .~ Prices
                (unsafeNonNegativeInterval (577 % 10_000))
                (unsafeNonNegativeInterval (721 % 10_000_000))
        & ppMaxTxExUnitsL
            .~ ExUnits 140_000_000 10_000_000_000

unsafeNonNegativeInterval :: Rational -> NonNegativeInterval
unsafeNonNegativeInterval r =
    fromJust (boundRational r)

-- | Real flat-encoded UPLC of upstream's
-- @validators\/request.ak@ at the pinned PR #50 tip
-- (@cf3a8bdc@). Loaded once at module init from
-- @test-data\/request.uplc.hex@ so the synthetic
-- 'testCageConfig' can compute a consistent
-- per-cage request address through
-- 'requestAddrFromCfg'. Production code paths fill
-- this field from the runtime blueprint instead.
testRequestScriptBytes :: SBS.ShortByteString
testRequestScriptBytes =
    unsafePerformIO loadTestRequestScriptBytes
{-# NOINLINE testRequestScriptBytes #-}

loadTestRequestScriptBytes :: IO SBS.ShortByteString
loadTestRequestScriptBytes = do
    hex <- tryRead candidatePaths
    let trimmed =
            BS.takeWhile
                (\b -> b /= 10 && b /= 13)
                hex
    case B16.decode trimmed of
        Right bs -> pure (SBS.toShort bs)
        Left err ->
            error
                $ "loadTestRequestScriptBytes: "
                    <> err
  where
    candidatePaths =
        [ "test-data/request.uplc.hex"
        , "cardano-mpfs-offchain/test-data/request.uplc.hex"
        ]
    tryRead [] =
        error
            "loadTestRequestScriptBytes: \
            \test-data/request.uplc.hex not found \
            \in any of the candidate paths"
    tryRead (p : ps) = do
        exists <- doesFileExist p
        if exists
            then BS.readFile p
            else tryRead ps

-- | Cage config for testing with testnet.
-- 'cageScriptBytes' carries dummy bytes (the cage
-- script is not exercised by the synthetic-config
-- path); 'requestScriptBytes' carries the real
-- request UPLC so that 'requestAddrFromCfg' resolves
-- to a deterministic address routable by the mock
-- provider.
testCageConfig :: CageConfig
testCageConfig =
    CageConfig
        { cageScriptBytes = SBS.toShort "dummy"
        , requestScriptBytes = testRequestScriptBytes
        , cfgScriptHash = cageScriptHashLedger
        , defaultProcessTime = 300_000
        , defaultRetractTime = 600_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }

-- | Build a Provider that returns a fixed UTxO set
-- and zero-fee PParams.
mkTestProvider
    :: [(TxIn, TxOut ConwayEra)] -> Provider IO
mkTestProvider utxos =
    Provider
        { queryUTxOs = \_ -> pure utxos
        , queryProtocolParams = pure zeroPP
        , evaluateTx = \_ -> pure Map.empty
        , posixMsToSlot = \_ -> pure (SlotNo 0)
        , posixMsCeilSlot = \_ -> pure (SlotNo 0)
        }

-- | Dummy CSMT proof function that always returns
-- 'Nothing'. Test 'mkRealTxBuilder' call sites don't
-- exercise proof content; they only verify tx shape.
dummyProofFn :: TxIn -> IO (Maybe ByteString)
dummyProofFn _ = pure Nothing

-- | Dummy TrieManager that errors on use.
dummyTrieManager :: TrieManager IO
dummyTrieManager =
    TrieManager
        { withTrie = \_ _ ->
            error "dummyTrieManager: withTrie"
        , withSpeculativeTrie = \_ _ ->
            error
                "dummyTrieManager: \
                \withSpeculativeTrie"
        , createTrie = \_ ->
            error "dummyTrieManager: createTrie"
        , deleteTrie = \_ ->
            error "dummyTrieManager: deleteTrie"
        , hideTrie = \_ ->
            error "dummyTrieManager: hideTrie"
        , unhideTrie = \_ ->
            error "dummyTrieManager: unhideTrie"
        }

-- | Build a Provider that returns different UTxOs
-- for different addresses.
mkRoutingProvider
    :: [(Addr, [(TxIn, TxOut ConwayEra)])]
    -> Provider IO
mkRoutingProvider routes =
    Provider
        { queryUTxOs = \addr ->
            pure
                $ fromMaybe
                    []
                    ( Prelude.lookup
                        addr
                        routes
                    )
        , queryProtocolParams = pure zeroPP
        , evaluateTx = \_ -> pure Map.empty
        , posixMsToSlot = \_ -> pure (SlotNo 0)
        , posixMsCeilSlot = \_ -> pure (SlotNo 0)
        }

-- | Provider with realistic PParams.
mkRealisticProvider
    :: [(TxIn, TxOut ConwayEra)] -> Provider IO
mkRealisticProvider utxos =
    Provider
        { queryUTxOs = \_ -> pure utxos
        , queryProtocolParams = pure realisticPP
        , evaluateTx = \_ -> pure Map.empty
        , posixMsToSlot = \_ -> pure (SlotNo 0)
        , posixMsCeilSlot = \_ -> pure (SlotNo 0)
        }

-- | Routing provider with realistic PParams.
mkRealisticRoutingProvider
    :: [(Addr, [(TxIn, TxOut ConwayEra)])]
    -> Provider IO
mkRealisticRoutingProvider routes =
    Provider
        { queryUTxOs = \addr ->
            pure
                $ fromMaybe
                    []
                    ( Prelude.lookup
                        addr
                        routes
                    )
        , queryProtocolParams = pure realisticPP
        , evaluateTx = \_ -> pure Map.empty
        , posixMsToSlot = \_ -> pure (SlotNo 0)
        , posixMsCeilSlot = \_ -> pure (SlotNo 0)
        }

-- | Build a state TxOut with the cage token.
mkStateTxOut :: TxOut ConwayEra
mkStateTxOut =
    let tokenMA =
            MultiAsset
                $ Map.singleton
                    cagePolicyId
                $ Map.singleton
                    (unTokenId testTid)
                    1
        val = MaryValue (Coin 2_000_000) tokenMA
        datum =
            StateDatum
                OnChainTokenState
                    { stateOwner =
                        BuiltinByteString
                            ( hashToBytes
                                $ let KeyHash h = testKh
                                  in  h
                            )
                    , stateRoot =
                        OnChainRoot
                            (BS.replicate 32 0)
                    , stateMaxFee = 1_000_000
                    , stateProcessTime = 300_000
                    , stateRetractTime = 600_000
                    }
    in  mkBasicTxOut (cageAddr Testnet) val
            & datumTxOutL
                .~ mkInlineDatum (toPlcData datum)

-- | Build a request TxOut with tight locked ADA.
-- Uses 'requestLockedAda' to compute the minimum,
-- matching what a real request transaction would lock.
mkTightRequestTxOut
    :: PParams ConwayEra -> TxOut ConwayEra
mkTightRequestTxOut pp =
    let datum =
            RequestDatum
                OnChainRequest
                    { requestToken =
                        OnChainTokenId
                            $ BuiltinByteString
                            $ SBS.fromShort
                            $ let AssetName sbs =
                                    unTokenId testTid
                              in  sbs
                    , requestOwner =
                        BuiltinByteString
                            ( hashToBytes
                                $ let KeyHash h = testKh
                                  in  h
                            )
                    , requestKey = "mykey"
                    , requestValue =
                        OpInsert "myvalue"
                    , requestFee = 1_000_000
                    , requestSubmittedAt = 0
                    }
        scriptAddr = cageAddr Testnet
        feeAddr = testAddr testKh
        draftOut =
            mkBasicTxOut
                scriptAddr
                (inject (Coin 0))
                & datumTxOutL
                    .~ mkInlineDatum
                        (toPlcData datum)
        refDraft =
            mkBasicTxOut
                feeAddr
                (inject (Coin 0))
        minAda =
            requestLockedAda
                pp
                draftOut
                refDraft
                1_000_000
    in  mkBasicTxOut scriptAddr (inject minAda)
            & datumTxOutL
                .~ mkInlineDatum (toPlcData datum)

-- | Build a request TxOut.
mkRequestTxOut :: TxOut ConwayEra
mkRequestTxOut = mkTightRequestTxOut realisticPP

-- ---------------------------------------------------------
-- Spec
-- ---------------------------------------------------------

spec :: Spec
spec = describe "Cardano.MPFS.TxBuilder.Real" $ do
    cageIdentitySpec
    bundleShapeSpec
    requestInsertSpec
    requestDeleteSpec
    retractRequestSpec
    updateTokenSpec
    endTokenSpec
    rejectRequestsSpec
    requestLockedAdaProps
    refundComputationProps
    spendingIndexProps
    requestTxProps
    updateTxProps
    retractTxProps
    endTxProps

-- ---------------------------------------------------------
-- Cage identity
-- ---------------------------------------------------------

cageIdentitySpec :: Spec
cageIdentitySpec =
    describe "cage identity" $ do
        it "cageScriptHash is 28 bytes"
            $ BS.length cageScriptHash
            `shouldBe` 28

        it "cagePolicyId wraps the script hash" $ do
            let PolicyID (ScriptHash h) = cagePolicyId
            hashToBytes h `shouldBe` cageScriptHash

        it "cageAddr is a script address"
            $ case cageAddr Testnet of
                Addr net cred _stake -> do
                    net `shouldBe` Testnet
                    cred
                        `shouldSatisfy` \case
                            ScriptHashObj _ -> True
                            _ -> False
                _ ->
                    error
                        "expected Addr, got Bootstrap"

        it "cageAddr Mainnet uses Mainnet"
            $ case cageAddr Mainnet of
                Addr net _ _ ->
                    net `shouldBe` Mainnet
                _ ->
                    error
                        "expected Addr, got Bootstrap"

-- ---------------------------------------------------------
-- ProofEnvelope shape
-- ---------------------------------------------------------

-- | Shape tests for the per-endpoint proof envelope
-- plumbing. Builders return an endpoint-specific
-- 'ProofEnvelope'; 'witnessedCsmtProof' is stubbed
-- empty until the UTxO-CSMT view is wired through.
bundleShapeSpec :: Spec
bundleShapeSpec =
    describe "ProofEnvelope" $ do
        it "envelope carries the snapshot" $ do
            (_, _, builder, _) <- mkTestFixture
            let feeAddr = testAddr testKh
            env <-
                requestInsert
                    builder
                    testSnap
                    testTid
                    "mykey"
                    "myvalue"
                    feeAddr
            snapshotUtxoRoot (envSnapshot env)
                `shouldBe` snapshotUtxoRoot testSnap
            snapshotSlot (envSnapshot env)
                `shouldBe` snapshotSlot testSnap

        it "request envelope lists funding inputs" $ do
            (_, _, builder, _) <- mkTestFixture
            let feeAddr = testAddr testKh
            env <-
                requestInsert
                    builder
                    testSnap
                    testTid
                    "mykey"
                    "myvalue"
                    feeAddr
            requestFunding (envProof env)
                `shouldSatisfy` (not . null)

-- ---------------------------------------------------------
-- requestInsert
-- ---------------------------------------------------------

requestInsertSpec :: Spec
requestInsertSpec =
    describe "requestInsert" $ do
        it "builds a balanced tx" $ do
            tx <- runRequestInsert
            let outList = toOutList tx
            -- cage output + change output
            length outList
                `shouldSatisfy` (>= 2)

        it "cage output has tip with zero fee PParams" $ do
            tx <- runRequestInsert
            case toOutList tx of
                (cageOut : _) -> do
                    let outCoin = cageOut ^. coinTxOutL
                    -- zeroPP: minUTxO=0 and fee envelope=0.
                    outCoin `shouldBe` Coin 1_000_000
                [] -> expectationFailure "no outputs"

        it "has no mint field" $ do
            tx <- runRequestInsert
            let mint = tx ^. bodyTxL . mintTxBodyL
            mint `shouldBe` mempty

        it "has no script witnesses" $ do
            tx <- runRequestInsert
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 0

        it "adds fee input to tx inputs" $ do
            (tx, feeIn) <- runRequestInsertWith
            let ins = tx ^. bodyTxL . inputsTxBodyL
            Set.member feeIn ins `shouldBe` True

-- ---------------------------------------------------------
-- requestDelete
-- ---------------------------------------------------------

requestDeleteSpec :: Spec
requestDeleteSpec =
    describe "requestDelete" $ do
        it "builds a balanced tx" $ do
            tx <- runRequestDelete
            let outList = toOutList tx
            length outList
                `shouldSatisfy` (>= 2)

        it "cage output has tip with zero fee PParams" $ do
            tx <- runRequestDelete
            case toOutList tx of
                (cageOut : _) -> do
                    let outCoin = cageOut ^. coinTxOutL
                    -- zeroPP: minUTxO=0 and fee envelope=0.
                    outCoin `shouldBe` Coin 1_000_000
                [] -> expectationFailure "no outputs"

        it "has no mint field" $ do
            tx <- runRequestDelete
            let mint = tx ^. bodyTxL . mintTxBodyL
            mint `shouldBe` mempty

        it "has no script witnesses" $ do
            tx <- runRequestDelete
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 0

-- ---------------------------------------------------------
-- retractRequest
-- ---------------------------------------------------------

retractRequestSpec :: Spec
retractRequestSpec =
    describe "retractRequest" $ do
        it "builds a balanced tx" $ do
            tx <- runRetractRequest
            let outList = toOutList tx
            length outList
                `shouldSatisfy` (>= 1)

        it "has state UTxO as reference input" $ do
            (tx, _, stateIn) <-
                runRetractRequestWith
            let refs =
                    tx
                        ^. bodyTxL
                            . referenceInputsTxBodyL
            Set.member stateIn refs
                `shouldBe` True

        it "has a spending redeemer" $ do
            (tx, _, _) <- runRetractRequestWith
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            Map.size rdmrs `shouldBe` 1

        it "has cage script witness" $ do
            (tx, _, _) <- runRetractRequestWith
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 1

        it "consumes the request UTxO" $ do
            (tx, reqIn, _) <-
                runRetractRequestWith
            let ins =
                    tx ^. bodyTxL . inputsTxBodyL
            Set.member reqIn ins
                `shouldBe` True

-- ---------------------------------------------------------
-- updateToken
-- ---------------------------------------------------------

updateTokenSpec :: Spec
updateTokenSpec =
    describe "updateToken" $ do
        it "builds a balanced tx" $ do
            tx <- runUpdateToken
            let outList = toOutList tx
            length outList
                `shouldSatisfy` (>= 1)

        it "mints nothing" $ do
            tx <- runUpdateToken
            let mint =
                    tx ^. bodyTxL . mintTxBodyL
            mint `shouldBe` mempty

        it "has state + request script witnesses" $ do
            tx <- runUpdateToken
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            -- PR #50: state validator + per-cage
            -- request validator are both attached.
            Map.size scripts `shouldBe` 2

        it "consumes state and request UTxOs" $ do
            (tx, stateIn, reqIn) <-
                runUpdateTokenWith
            let ins =
                    tx ^. bodyTxL . inputsTxBodyL
            Set.member stateIn ins
                `shouldBe` True
            Set.member reqIn ins
                `shouldBe` True

        it "has redeemers for state and request" $ do
            (tx, _, _) <- runUpdateTokenWith
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            -- one Modify + one Contribute
            Map.size rdmrs `shouldBe` 2

-- ---------------------------------------------------------
-- endToken
-- ---------------------------------------------------------

endTokenSpec :: Spec
endTokenSpec =
    describe "endToken" $ do
        it "builds a balanced tx" $ do
            tx <- runEndToken
            let outList = toOutList tx
            length outList
                `shouldSatisfy` (>= 1)

        it "burns exactly one token" $ do
            tx <- runEndToken
            let MultiAsset ma =
                    tx ^. bodyTxL . mintTxBodyL
                mPolicy =
                    Map.lookup cagePolicyId ma
            mPolicy `shouldSatisfy` isJust
            let assets = fromJust mPolicy
            Map.size assets `shouldBe` 1
            case Map.elems assets of
                [qty] -> qty `shouldBe` (-1)
                _ -> expectationFailure "expected 1 asset"

        it "has a script witness" $ do
            tx <- runEndToken
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 1

        it "has spending and minting redeemers" $ do
            tx <- runEndToken
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            -- one spending (End) + one minting (Burning)
            Map.size rdmrs `shouldBe` 2

        it "consumes the state UTxO" $ do
            (tx, stateIn) <- runEndTokenWith
            let ins =
                    tx ^. bodyTxL . inputsTxBodyL
            Set.member stateIn ins
                `shouldBe` True

-- ---------------------------------------------------------
-- rejectRequests
-- ---------------------------------------------------------

rejectRequestsSpec :: Spec
rejectRequestsSpec =
    describe "rejectRequests" $ do
        it "builds a balanced tx" $ do
            tx <- runRejectRequests
            let outList = toOutList tx
            -- 1 state + 1 refund + 1 change
            length outList
                `shouldSatisfy` (>= 2)

        it "state output preserves datum" $ do
            tx <- runRejectRequests
            case toOutList tx of
                (stOut : _) -> do
                    let mDatum =
                            extractCageDatum stOut
                    case mDatum of
                        Just (StateDatum _) ->
                            pure ()
                        _ ->
                            expectationFailure
                                "first output is \
                                \not a StateDatum"
                [] ->
                    expectationFailure
                        "no outputs"

        it "has state + request script witnesses" $ do
            tx <- runRejectRequests
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            -- PR #50: state validator + per-cage
            -- request validator are both attached.
            Map.size scripts `shouldBe` 2

        it
            "has redeemers for state and request"
            $ do
                tx <- runRejectRequests
                let (Redeemers rdmrs) =
                        tx ^. witsTxL . rdmrsTxWitsL
                -- 1 Reject + 1 Contribute
                Map.size rdmrs `shouldBe` 2

-- ---------------------------------------------------------
-- Group A: requestLockedAda (pure)
-- ---------------------------------------------------------

requestLockedAdaProps :: Spec
requestLockedAdaProps =
    describe "requestLockedAda" $ do
        it "locked >= request minUTxO"
            $ property
            $ forAll genTokenId
            $ \tid ->
                forAll genKeyHash $ \kh ->
                    forAll genTrieKey $ \key ->
                        forAll genTrieValue $ \val ->
                            forAll genMaxFee
                                $ \(Coin mf) ->
                                    let addr = testAddr kh
                                        sAddr =
                                            cageAddr
                                                Testnet
                                        datum =
                                            mkRequestDatum
                                                tid
                                                addr
                                                key
                                                ( OpInsert
                                                    val
                                                )
                                                mf
                                                0
                                        draft =
                                            mkBasicTxOut
                                                sAddr
                                                ( inject
                                                    (Coin 0)
                                                )
                                                & datumTxOutL
                                                    .~ mkInlineDatum
                                                        datum
                                        refDraft =
                                            mkBasicTxOut
                                                addr
                                                ( inject
                                                    (Coin 0)
                                                )
                                        la =
                                            requestLockedAda
                                                realisticPP
                                                draft
                                                refDraft
                                                mf
                                        reqMin =
                                            getMinCoinTxOut
                                                realisticPP
                                                draft
                                    in  la >= reqMin

        it "locked >= tip + refund minUTxO"
            $ property
            $ forAll genTokenId
            $ \tid ->
                forAll genKeyHash $ \kh ->
                    forAll genTrieKey $ \key ->
                        forAll genTrieValue $ \val ->
                            forAll genMaxFee
                                $ \(Coin mf) ->
                                    let addr = testAddr kh
                                        sAddr =
                                            cageAddr
                                                Testnet
                                        datum =
                                            mkRequestDatum
                                                tid
                                                addr
                                                key
                                                ( OpInsert
                                                    val
                                                )
                                                mf
                                                0
                                        draft =
                                            mkBasicTxOut
                                                sAddr
                                                ( inject
                                                    (Coin 0)
                                                )
                                                & datumTxOutL
                                                    .~ mkInlineDatum
                                                        datum
                                        refDraft =
                                            mkBasicTxOut
                                                addr
                                                ( inject
                                                    (Coin 0)
                                                )
                                        Coin la =
                                            requestLockedAda
                                                realisticPP
                                                draft
                                                refDraft
                                                mf
                                        Coin refMin =
                                            getMinCoinTxOut
                                                realisticPP
                                                refDraft
                                    in  la >= mf + refMin

        it "locked is tight"
            $ property
            $ forAll genTokenId
            $ \tid ->
                forAll genKeyHash $ \kh ->
                    forAll genTrieKey $ \key ->
                        forAll genTrieValue $ \val ->
                            forAll genMaxFee
                                $ \(Coin mf) ->
                                    let addr = testAddr kh
                                        sAddr =
                                            cageAddr
                                                Testnet
                                        datum =
                                            mkRequestDatum
                                                tid
                                                addr
                                                key
                                                ( OpInsert
                                                    val
                                                )
                                                mf
                                                0
                                        draft =
                                            mkBasicTxOut
                                                sAddr
                                                ( inject
                                                    (Coin 0)
                                                )
                                                & datumTxOutL
                                                    .~ mkInlineDatum
                                                        datum
                                        refDraft =
                                            mkBasicTxOut
                                                addr
                                                ( inject
                                                    (Coin 0)
                                                )
                                        la =
                                            requestLockedAda
                                                realisticPP
                                                draft
                                                refDraft
                                                mf
                                        Coin refMin =
                                            getMinCoinTxOut
                                                realisticPP
                                                refDraft
                                        feeBuffer =
                                            feeBufferUpperBound
                                                realisticPP
                                        locked =
                                            mf
                                                + feeBuffer
                                                + refMin
                                        adjusted =
                                            getMinCoinTxOut
                                                realisticPP
                                                ( draft
                                                    & valueTxOutL
                                                        .~ inject
                                                            ( Coin
                                                                locked
                                                            )
                                                )
                                    in  la
                                            == max
                                                adjusted
                                                ( Coin
                                                    locked
                                                )

        it "zeroPP derives a zero fee buffer"
            $ property
            $ forAll genTokenId
            $ \tid ->
                forAll genKeyHash $ \kh ->
                    forAll genTrieKey $ \key ->
                        forAll genTrieValue $ \val ->
                            forAll genMaxFee
                                $ \(Coin mf) ->
                                    let addr = testAddr kh
                                        sAddr =
                                            cageAddr
                                                Testnet
                                        datum =
                                            mkRequestDatum
                                                tid
                                                addr
                                                key
                                                ( OpInsert
                                                    val
                                                )
                                                mf
                                                0
                                        draft =
                                            mkBasicTxOut
                                                sAddr
                                                ( inject
                                                    (Coin 0)
                                                )
                                                & datumTxOutL
                                                    .~ mkInlineDatum
                                                        datum
                                        refDraft =
                                            mkBasicTxOut
                                                addr
                                                ( inject
                                                    (Coin 0)
                                                )
                                        la =
                                            requestLockedAda
                                                zeroPP
                                                draft
                                                refDraft
                                                mf
                                        feeBuffer =
                                            feeBufferUpperBound
                                                zeroPP
                                    in  la
                                            == Coin
                                                (mf + feeBuffer)

feeBufferUpperBound :: PParams ConwayEra -> Integer
feeBufferUpperBound pp =
    let Coin minFeeA = pp ^. ppMinFeeAL
        Coin minFeeB = pp ^. ppMinFeeBL
        Coin scriptFee =
            txscriptfee (pp ^. ppPricesL) (pp ^. ppMaxTxExUnitsL)
    in  minFeeB + minFeeA * maxUpdateTxBytes + scriptFee

maxUpdateTxBytes :: Integer
maxUpdateTxBytes = 8192

-- ---------------------------------------------------------
-- Group A2: computeRefund (pure)
-- ---------------------------------------------------------

refundComputationProps :: Spec
refundComputationProps =
    describe "computeRefund" $ do
        it
            "refund + tip + feeShare <= reqVal"
            $ do
                let tip = 100_000
                    perReqFee = 500_000
                    refundOut =
                        computeRefund
                            realisticPP
                            Testnet
                            tip
                            perReqFee
                            mkRequestTxOut
                    Coin refund =
                        refundOut ^. coinTxOutL
                    Coin reqVal =
                        mkRequestTxOut ^. coinTxOutL
                (refund + tip + perReqFee)
                    `shouldSatisfy` (<= reqVal)

        it
            "refund >= minUTxO for viable request"
            $ do
                let tip = 100_000
                    perReqFee = 200_000
                    refundOut =
                        computeRefund
                            realisticPP
                            Testnet
                            tip
                            perReqFee
                            mkRequestTxOut
                    refund =
                        refundOut ^. coinTxOutL
                    minUtxo =
                        getMinCoinTxOut
                            realisticPP
                            refundOut
                refund
                    `shouldSatisfy` (>= minUtxo)

        it
            "exact conservation equation"
            $ do
                let tip = 100_000
                    perReqFee = 500_000
                    Coin reqVal =
                        mkRequestTxOut ^. coinTxOutL
                    refundOut =
                        computeRefund
                            zeroPP
                            Testnet
                            tip
                            perReqFee
                            mkRequestTxOut
                    Coin refund =
                        refundOut ^. coinTxOutL
                -- zeroPP: minUTxO=0, so refund is
                -- exact (no clamping).
                refund
                    `shouldBe` ( reqVal
                                    - tip
                                    - perReqFee
                               )

-- ---------------------------------------------------------
-- Group B: spendingIndex (pure)
-- ---------------------------------------------------------

spendingIndexProps :: Spec
spendingIndexProps =
    describe "spendingIndex" $ do
        it "matches sorted position"
            $ property
            $ forAll genTxIn
            $ \a ->
                forAll genTxIn $ \b ->
                    forAll genTxIn $ \c ->
                        let ins =
                                Set.fromList [a, b, c]
                            sorted =
                                Set.toAscList ins
                        in  all
                                ( \(i, txIn) ->
                                    spendingIndex
                                        txIn
                                        ins
                                        == fromIntegral i
                                )
                                (zip [0 :: Int ..] sorted)

        it "covers all indices"
            $ property
            $ forAll genTxIn
            $ \a ->
                forAll genTxIn $ \b ->
                    forAll genTxIn $ \c ->
                        let ins =
                                Set.fromList [a, b, c]
                            n = Set.size ins
                            indices =
                                sort
                                    $ map
                                        (`spendingIndex` ins)
                                    $ Set.toList ins
                        in  indices
                                == map
                                    fromIntegral
                                    [0 .. n - 1]

-- ---------------------------------------------------------
-- Group C: Request tx properties (IO, realistic PParams)
-- ---------------------------------------------------------

requestTxProps :: Spec
requestTxProps =
    describe "request tx (realistic PParams)" $ do
        it "cage output satisfies minUTxO" $ do
            tx <- runRealisticRequestInsert
            case toOutList tx of
                (cageOut : _) -> do
                    let c = cageOut ^. coinTxOutL
                        minC =
                            getMinCoinTxOut
                                realisticPP
                                cageOut
                    c `shouldSatisfy` (>= minC)
                [] -> expectationFailure "no outputs"

        it "all outputs satisfy minUTxO" $ do
            tx <- runRealisticRequestInsert
            let outs = toOutList tx
            for_ outs $ \o -> do
                let c = o ^. coinTxOutL
                    minC =
                        getMinCoinTxOut
                            realisticPP
                            o
                c `shouldSatisfy` (>= minC)

        it "value preservation" $ do
            (tx, _) <-
                runRealisticRequestInsertWith
            let body = tx ^. bodyTxL
                Coin fee = body ^. feeTxBodyL
                outSum =
                    sum
                        $ map
                            (\o -> let Coin c = o ^. coinTxOutL in c)
                        $ toOutList tx
                inSum =
                    50_000_000 :: Integer
            inSum `shouldSatisfy` (>= outSum + fee)

        it "cage datum has correct token" $ do
            tx <- runRealisticRequestInsert
            case toOutList tx of
                (cageOut : _) ->
                    case extractCageDatum cageOut of
                        Just
                            ( RequestDatum
                                    OnChainRequest
                                        { requestToken =
                                            OnChainTokenId
                                                ( BuiltinByteString
                                                        bs
                                                    )
                                        }
                                ) ->
                                let AssetName sbs =
                                        unTokenId testTid
                                in  bs
                                        `shouldBe` SBS.fromShort
                                            sbs
                        _ ->
                            expectationFailure
                                "not a RequestDatum"
                [] -> expectationFailure "no outputs"

        it "cage datum has correct owner" $ do
            tx <- runRealisticRequestInsert
            case toOutList tx of
                (cageOut : _) ->
                    case extractCageDatum cageOut of
                        Just
                            ( RequestDatum
                                    OnChainRequest
                                        { requestOwner =
                                            BuiltinByteString
                                                bs
                                        }
                                ) ->
                                let KeyHash h = testKh
                                in  bs
                                        `shouldBe` hashToBytes
                                            h
                        _ ->
                            expectationFailure
                                "not a RequestDatum"
                [] -> expectationFailure "no outputs"

        it "cage datum has correct key" $ do
            tx <- runRealisticRequestInsert
            case toOutList tx of
                (cageOut : _) ->
                    case extractCageDatum cageOut of
                        Just
                            ( RequestDatum
                                    OnChainRequest
                                        { requestKey = k
                                        }
                                ) ->
                                k `shouldBe` "mykey"
                        _ ->
                            expectationFailure
                                "not a RequestDatum"
                [] -> expectationFailure "no outputs"

        it "no minting" $ do
            tx <- runRealisticRequestInsert
            let mint = tx ^. bodyTxL . mintTxBodyL
            mint `shouldBe` mempty

        it "no script witnesses" $ do
            tx <- runRealisticRequestInsert
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 0

-- ---------------------------------------------------------
-- Group D: Update tx properties (IO, realistic PParams)
-- ---------------------------------------------------------

updateTxProps :: Spec
updateTxProps =
    describe "update tx (realistic PParams)" $ do
        it "value preservation" $ do
            (tx, _, _) <-
                runRealisticUpdateWith
            let body = tx ^. bodyTxL
                Coin fee = body ^. feeTxBodyL
                outSum =
                    sum
                        $ map
                            (\o -> let Coin c = o ^. coinTxOutL in c)
                        $ toOutList tx
                Coin reqVal = mkRequestTxOut ^. coinTxOutL
                inSum =
                    2_000_000
                        + reqVal
                        + 50_000_000
                        :: Integer
            inSum `shouldSatisfy` (>= outSum + fee)

        it "all outputs satisfy minUTxO" $ do
            tx <- runRealisticUpdate
            for_ (toOutList tx) $ \o -> do
                let c = o ^. coinTxOutL
                    minC =
                        getMinCoinTxOut
                            realisticPP
                            o
                c `shouldSatisfy` (>= minC)

        it "redeemer count = 1 + N requests" $ do
            (tx, _, _) <- runRealisticUpdateWith
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            -- 1 Modify + 1 Contribute (1 request)
            Map.size rdmrs `shouldBe` 2

        it "state output has updated root" $ do
            (tx, _, _) <- runRealisticUpdateWith
            case toOutList tx of
                (stateOut : _) ->
                    case extractCageDatum stateOut of
                        Just (StateDatum s) ->
                            let OnChainRoot r =
                                    stateRoot s
                            in  r
                                    `shouldSatisfy` ( /=
                                                        BS.replicate
                                                            32
                                                            0
                                                    )
                        _ ->
                            expectationFailure
                                "not a StateDatum"
                [] -> expectationFailure "no outputs"

        it "refund amounts correct" $ do
            (tx, _, _) <- runRealisticUpdateWith
            let outs = toOutList tx
            -- state output first, then refund(s)
            case drop 1 outs of
                (refund : _) -> do
                    let Coin c =
                            refund ^. coinTxOutL
                        Coin reqVal =
                            mkRequestTxOut ^. coinTxOutL
                        Coin fee =
                            tx ^. bodyTxL . feeTxBodyL
                    c `shouldBe` reqVal - 1_000_000 - fee
                _ ->
                    expectationFailure "no refund output"

        it "refund meets minUTxO with tight locked ADA" $ do
            tx <- runTightUpdate
            let outs = toOutList tx
            case drop 1 outs of
                (refund : _) -> do
                    let c = refund ^. coinTxOutL
                        minC =
                            getMinCoinTxOut
                                realisticPP
                                refund
                    c `shouldSatisfy` (>= minC)
                _ ->
                    expectationFailure "no refund output"

        it "state + request script witnesses" $ do
            tx <- runRealisticUpdate
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            -- PR #50: state validator + per-cage
            -- request validator are both attached.
            Map.size scripts `shouldBe` 2

        it "required signer = owner" $ do
            (tx, _, _) <- runRealisticUpdateWith
            let signers =
                    tx
                        ^. bodyTxL
                            . reqSignerHashesTxBodyL
            Set.size signers `shouldBe` 1

        it "validity upper bound set" $ do
            (tx, _, _) <- runRealisticUpdateWith
            let ValidityInterval _ hi =
                    tx ^. bodyTxL . vldtTxBodyL
            hi `shouldSatisfy` isSJust

        it "collateral = fee input" $ do
            (tx, _, _) <- runRealisticUpdateWith
            let collat =
                    tx
                        ^. bodyTxL
                            . collateralInputsTxBodyL
            Set.size collat `shouldBe` 1

-- ---------------------------------------------------------
-- Group E: Retract tx properties (IO, realistic PParams)
-- ---------------------------------------------------------

retractTxProps :: Spec
retractTxProps =
    describe "retract tx (realistic PParams)" $ do
        it "value preservation" $ do
            (tx, _, _) <-
                runRealisticRetractWith
            let body = tx ^. bodyTxL
                Coin fee = body ^. feeTxBodyL
                outSum =
                    sum
                        $ map
                            (\o -> let Coin c = o ^. coinTxOutL in c)
                        $ toOutList tx
                Coin reqVal = mkRequestTxOut ^. coinTxOutL
                inSum =
                    reqVal + 50_000_000 :: Integer
            inSum `shouldSatisfy` (>= outSum + fee)

        it "all outputs satisfy minUTxO" $ do
            (tx, _, _) <- runRealisticRetractWith
            for_ (toOutList tx) $ \o -> do
                let c = o ^. coinTxOutL
                    minC =
                        getMinCoinTxOut
                            realisticPP
                            o
                c `shouldSatisfy` (>= minC)

        it "state is reference input" $ do
            (tx, _, stateIn) <-
                runRealisticRetractWith
            let refs =
                    tx
                        ^. bodyTxL
                            . referenceInputsTxBodyL
            Set.member stateIn refs `shouldBe` True

        it "state not consumed" $ do
            (tx, _, stateIn) <-
                runRealisticRetractWith
            let ins =
                    tx ^. bodyTxL . inputsTxBodyL
            Set.member stateIn ins `shouldBe` False

        it "request consumed" $ do
            (tx, reqIn, _) <-
                runRealisticRetractWith
            let ins =
                    tx ^. bodyTxL . inputsTxBodyL
            Set.member reqIn ins `shouldBe` True

        it "exactly 1 redeemer" $ do
            (tx, _, _) <- runRealisticRetractWith
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            Map.size rdmrs `shouldBe` 1

        it "required signer = requester" $ do
            (tx, _, _) <- runRealisticRetractWith
            let signers =
                    tx
                        ^. bodyTxL
                            . reqSignerHashesTxBodyL
            Set.size signers `shouldBe` 1

        it "validity interval = phase 2" $ do
            (tx, _, _) <- runRealisticRetractWith
            let ValidityInterval lo hi =
                    tx ^. bodyTxL . vldtTxBodyL
            lo `shouldSatisfy` isSJust
            hi `shouldSatisfy` isSJust

-- ---------------------------------------------------------
-- Group F: End tx properties (IO, realistic PParams)
-- ---------------------------------------------------------

endTxProps :: Spec
endTxProps =
    describe "end tx (realistic PParams)" $ do
        it "value preservation" $ do
            (tx, _) <- runRealisticEndWith
            let body = tx ^. bodyTxL
                Coin fee = body ^. feeTxBodyL
                outSum =
                    sum
                        $ map
                            (\o -> let Coin c = o ^. coinTxOutL in c)
                        $ toOutList tx
                -- state(2M) + fee(50M) - burned value
                inSum =
                    2_000_000 + 50_000_000 :: Integer
            inSum `shouldSatisfy` (>= outSum + fee)

        it "all outputs satisfy minUTxO" $ do
            tx <- runRealisticEnd
            for_ (toOutList tx) $ \o -> do
                let c = o ^. coinTxOutL
                    minC =
                        getMinCoinTxOut
                            realisticPP
                            o
                c `shouldSatisfy` (>= minC)

        it "burns exactly 1 token" $ do
            tx <- runRealisticEnd
            let MultiAsset ma =
                    tx ^. bodyTxL . mintTxBodyL
                mPolicy =
                    Map.lookup cagePolicyId ma
            mPolicy `shouldSatisfy` isJust
            let assets = fromJust mPolicy
            Map.size assets `shouldBe` 1
            case Map.elems assets of
                [qty] -> qty `shouldBe` (-1)
                _ ->
                    expectationFailure "expected 1 asset"

        it "exactly 2 redeemers" $ do
            tx <- runRealisticEnd
            let (Redeemers rdmrs) =
                    tx ^. witsTxL . rdmrsTxWitsL
            -- 1 spending (End) + 1 minting (Burning)
            Map.size rdmrs `shouldBe` 2

        it "required signer = owner" $ do
            (tx, _) <- runRealisticEndWith
            let signers =
                    tx
                        ^. bodyTxL
                            . reqSignerHashesTxBodyL
            Set.size signers `shouldBe` 1

        it "exactly 1 script witness" $ do
            tx <- runRealisticEnd
            let scripts =
                    tx ^. witsTxL . scriptTxWitsL
            Map.size scripts `shouldBe` 1

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- ---------------------------------------------------------
-- Runners
-- ---------------------------------------------------------

-- | Convert tx outputs to a list.
toOutList :: Tx ConwayEra -> [TxOut ConwayEra]
toOutList tx =
    foldr (:) []
        $ tx ^. bodyTxL . outputsTxBodyL

-- | Set up state + provider, run requestInsert.
runRequestInsert :: IO (Tx ConwayEra)
runRequestInsert = fst <$> runRequestInsertWith

-- | Same but also return the fee TxIn.
runRequestInsertWith
    :: IO (Tx ConwayEra, TxIn)
runRequestInsertWith = do
    (_st, _prov, builder, txIn) <- mkTestFixture
    let feeAddr = testAddr testKh
    bundle <-
        requestInsert
            builder
            testSnap
            testTid
            "mykey"
            "myvalue"
            feeAddr
    pure (envTx bundle, txIn)

-- | Set up state + provider, run requestDelete.
runRequestDelete :: IO (Tx ConwayEra)
runRequestDelete = do
    (_st, _prov, builder, _txIn) <- mkTestFixture
    let feeAddr = testAddr testKh
    envTx
        <$> requestDelete
            builder
            testSnap
            testTid
            "mykey"
            "myval"
            feeAddr

-- | Run retractRequest.
runRetractRequest :: IO (Tx ConwayEra)
runRetractRequest = do
    (tx, _, _) <- runRetractRequestWith
    pure tx

-- | Run retractRequest and return details.
runRetractRequestWith
    :: IO (Tx ConwayEra, TxIn, TxIn)
runRetractRequestWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    -- Generate TxIns
    reqIn <- generate genTxIn
    stateIn <- generate genTxIn
    feeIn <- generate genTxIn
    -- Store request in state
    let req =
            Request
                { requestToken = testTid
                , requestOwner = testKh
                , requestKey = "mykey"
                , requestValue = Insert "myvalue"
                , requestFee = Coin 1_000_000
                , requestSubmittedAt = 0
                }
    putRequest (requests st) (LocatedRequest reqIn req)
    -- Build split cage UTxOs (PR #50 topology):
    -- state at the global state address, request at
    -- the per-cage request address.
    let stateAddr = cageAddr Testnet
        reqAddr = testReqAddr Testnet
        feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos = [(reqIn, mkRequestTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRoutingProvider
                [ (stateAddr, stateUtxos)
                , (reqAddr, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    bundle <-
        retractRequest builder testSnap reqIn feeAddr
    pure (envTx bundle, reqIn, stateIn)

-- | Run updateToken.
runUpdateToken :: IO (Tx ConwayEra)
runUpdateToken = do
    (tx, _, _) <- runUpdateTokenWith
    pure tx

-- | Run updateToken and return details.
runUpdateTokenWith
    :: IO (Tx ConwayEra, TxIn, TxIn)
runUpdateTokenWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    -- Generate TxIns
    stateIn <- generate genTxIn
    reqIn <- generate genTxIn
    feeIn <- generate genTxIn
    -- Build split cage UTxOs (PR #50 topology).
    let stateAddr = cageAddr Testnet
        reqAddr = testReqAddr Testnet
        feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos = [(reqIn, mkRequestTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRoutingProvider
                [ (stateAddr, stateUtxos)
                , (reqAddr, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
    -- Build TrieManager with data
    trieManager <- mkPureTrieManager
    createTrie trieManager testTid
    -- Insert the key so proof exists
    withTrie trieManager testTid $ \trie -> do
        _ <- insert trie "mykey" "existing"
        pure ()
    let builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                trieManager
                dummyProofFn
    bundle <-
        updateToken builder testSnap testTid feeAddr
    pure (envTx bundle, stateIn, reqIn)

-- | Run endToken.
runEndToken :: IO (Tx ConwayEra)
runEndToken = fst <$> runEndTokenWith

-- | Run endToken and return details.
runEndTokenWith :: IO (Tx ConwayEra, TxIn)
runEndTokenWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    stateIn <- generate genTxIn
    feeIn <- generate genTxIn
    let scriptAddr = cageAddr Testnet
        feeAddr = testAddr testKh
        cageUtxos =
            [(stateIn, mkStateTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRoutingProvider
                [ (scriptAddr, cageUtxos)
                , (feeAddr, walletUtxos)
                ]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    bundle <- endToken builder testSnap testTid feeAddr
    pure (envTx bundle, stateIn)

-- | Token ID used across tests.
testTid :: TokenId
testTid = TokenId (AssetName "test-token")

-- | Per-cage request address for 'testTid' under
-- 'testCageConfig'. Mock providers must route
-- request UTxOs here (PR #50 split topology).
testReqAddr :: Network -> Addr
testReqAddr =
    requestAddrFromCfg testCageConfig testTid

-- | Common fixture: mock state with a token,
-- provider with a 50-ADA UTxO, and a wired builder.
mkTestFixture
    :: IO (State IO, Provider IO, TxBuilder IO, TxIn)
mkTestFixture = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    txIn <- generate genTxIn
    let feeAddr = testAddr testKh
        utxo =
            mkBasicTxOut
                feeAddr
                (inject (Coin 50_000_000))
        prov = mkTestProvider [(txIn, utxo)]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    pure (st, prov, builder, txIn)

-- ---------------------------------------------------------
-- Realistic runners
-- ---------------------------------------------------------

-- | Check StrictMaybe is SJust.
isSJust :: StrictMaybe a -> Bool
isSJust (SJust _) = True
isSJust _ = False

-- | Realistic fixture with realisticPP.
mkRealisticFixture
    :: IO (State IO, Provider IO, TxBuilder IO, TxIn)
mkRealisticFixture = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    txIn <- generate genTxIn
    let feeAddr = testAddr testKh
        utxo =
            mkBasicTxOut
                feeAddr
                (inject (Coin 50_000_000))
        prov =
            mkRealisticProvider [(txIn, utxo)]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    pure (st, prov, builder, txIn)

-- | Run requestInsert with realistic PParams.
runRealisticRequestInsert
    :: IO (Tx ConwayEra)
runRealisticRequestInsert =
    fst <$> runRealisticRequestInsertWith

-- | Run requestInsert with realistic PParams,
-- returning the fee TxIn.
runRealisticRequestInsertWith
    :: IO (Tx ConwayEra, TxIn)
runRealisticRequestInsertWith = do
    (_st, _prov, builder, txIn) <-
        mkRealisticFixture
    let feeAddr = testAddr testKh
    bundle <-
        requestInsert
            builder
            testSnap
            testTid
            "mykey"
            "myvalue"
            feeAddr
    pure (envTx bundle, txIn)

-- | Run updateToken with realistic PParams.
runRealisticUpdate :: IO (Tx ConwayEra)
runRealisticUpdate = do
    (tx, _, _) <- runRealisticUpdateWith
    pure tx

-- | Run updateToken with realistic PParams
-- and return details.
runRealisticUpdateWith
    :: IO (Tx ConwayEra, TxIn, TxIn)
runRealisticUpdateWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    stateIn <- generate genTxIn
    reqIn <- generate genTxIn
    feeIn <- generate genTxIn
    let stateAddr = cageAddr Testnet
        reqAddr = testReqAddr Testnet
        feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos = [(reqIn, mkRequestTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRealisticRoutingProvider
                [ (stateAddr, stateUtxos)
                , (reqAddr, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
    trieManager <- mkPureTrieManager
    createTrie trieManager testTid
    withTrie trieManager testTid $ \trie -> do
        _ <- insert trie "mykey" "existing"
        pure ()
    let builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                trieManager
                dummyProofFn
    bundle <-
        updateToken builder testSnap testTid feeAddr
    pure (envTx bundle, stateIn, reqIn)

-- | Run updateToken with tight request locked ADA.
-- The request UTxO has the minimum locked ADA
-- computed by 'requestLockedAda', so the refund
-- is at the minUTxO boundary.
runTightUpdate :: IO (Tx ConwayEra)
runTightUpdate = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    stateIn <- generate genTxIn
    reqIn <- generate genTxIn
    feeIn <- generate genTxIn
    let stateAddr = cageAddr Testnet
        reqAddr = testReqAddr Testnet
        feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos =
            [
                ( reqIn
                , mkTightRequestTxOut realisticPP
                )
            ]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRealisticRoutingProvider
                [ (stateAddr, stateUtxos)
                , (reqAddr, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
    trieManager <- mkPureTrieManager
    createTrie trieManager testTid
    withTrie trieManager testTid $ \trie -> do
        _ <- insert trie "mykey" "existing"
        pure ()
    let builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                trieManager
                dummyProofFn
    envTx
        <$> updateToken builder testSnap testTid feeAddr

-- | Run retractRequest with realistic PParams
-- and return details.
runRealisticRetractWith
    :: IO (Tx ConwayEra, TxIn, TxIn)
runRealisticRetractWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    reqIn <- generate genTxIn
    stateIn <- generate genTxIn
    feeIn <- generate genTxIn
    let req =
            Request
                { requestToken = testTid
                , requestOwner = testKh
                , requestKey = "mykey"
                , requestValue = Insert "myvalue"
                , requestFee = Coin 1_000_000
                , requestSubmittedAt = 0
                }
    putRequest (requests st) (LocatedRequest reqIn req)
    let stateAddr = cageAddr Testnet
        reqAddr = testReqAddr Testnet
        feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos = [(reqIn, mkRequestTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRealisticRoutingProvider
                [ (stateAddr, stateUtxos)
                , (reqAddr, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    bundle <-
        retractRequest builder testSnap reqIn feeAddr
    pure (envTx bundle, reqIn, stateIn)

-- | Run endToken with realistic PParams.
runRealisticEnd :: IO (Tx ConwayEra)
runRealisticEnd = fst <$> runRealisticEndWith

-- | Run endToken with realistic PParams
-- and return details.
runRealisticEndWith
    :: IO (Tx ConwayEra, TxIn)
runRealisticEndWith = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    stateIn <- generate genTxIn
    feeIn <- generate genTxIn
    let scriptAddr = cageAddr Testnet
        feeAddr = testAddr testKh
        cageUtxos =
            [(stateIn, mkStateTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRealisticRoutingProvider
                [ (scriptAddr, cageUtxos)
                , (feeAddr, walletUtxos)
                ]
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                dummyTrieManager
                dummyProofFn
    bundle <- endToken builder testSnap testTid feeAddr
    pure (envTx bundle, stateIn)

-- | Run rejectRequests with mock expired request.
-- The request has submittedAt=0, processTime=300s,
-- retractTime=600s — guaranteed expired.
runRejectRequests :: IO (Tx ConwayEra)
runRejectRequests = do
    st <- mkMockState
    let ts =
            TokenState
                { owner = testKh
                , root = Root (BS.replicate 32 0)
                , tip = Coin 1_000_000
                , processTime = 300_000
                , retractTime = 600_000
                }
    stRef <- generate genTxIn
    putToken (tokens st) testTid (LocatedTokenState stRef ts)
    stateIn <- generate genTxIn
    reqIn <- generate genTxIn
    feeIn <- generate genTxIn
    let feeAddr = testAddr testKh
        stateUtxos = [(stateIn, mkStateTxOut)]
        requestUtxos = [(reqIn, mkRequestTxOut)]
        walletUtxos =
            [
                ( feeIn
                , mkBasicTxOut
                    feeAddr
                    (inject (Coin 50_000_000))
                )
            ]
        prov =
            mkRoutingProvider
                [ (cageAddr Testnet, stateUtxos)
                , (testReqAddr Testnet, requestUtxos)
                , (feeAddr, walletUtxos)
                ]
        trieManager = dummyTrieManager
        builder =
            mkRealTxBuilder
                testCageConfig
                prov
                st
                trieManager
                dummyProofFn
    envTx
        <$> rejectRequests builder testSnap testTid feeAddr
