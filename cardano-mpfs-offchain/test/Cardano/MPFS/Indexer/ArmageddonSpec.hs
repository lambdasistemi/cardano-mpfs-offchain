{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- |
-- Module      : Cardano.MPFS.Indexer.ArmageddonSpec
-- Description : Cage Runner E2E with chain-follower
-- License     : Apache-2.0
--
-- End-to-end tests of the cage backend through the
-- chain-follower Runner. Uses a test-local GADT
-- ('TestColumns') wrapping 'AllColumns' plus a
-- chain-follower rollback column — same pattern as
-- 'UnifiedColumns' but without UTxO.
--
-- Tests restoration, following, rollback, and
-- repeated armageddon-reset cycles through the
-- Runner, exercising the full trie + state layer
-- on a real RocksDB.
module Cardano.MPFS.Indexer.ArmageddonSpec (spec) where

import Control.Monad (forM_)
import Control.Tracer (nullTracer)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Dependent.Map qualified as DMap
import Data.Maybe (fromJust)
import Data.Word (Word16)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    )

import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromStringAsHex
    )
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Hashes (unsafeMakeSafeHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.TxIn qualified as Ledger
import ChainFollower.Backend qualified as Backend
import ChainFollower.Rollbacks.Column (RollbackKV)
import ChainFollower.Rollbacks.Store qualified as Store
import ChainFollower.Rollbacks.Types qualified as CF
import ChainFollower.Runner
    ( Phase (..)
    , processBlock
    , rollbackTo
    )
import Codec.CBOR.Decoding
    ( decodeBytes
    , decodeListLen
    , decodeListLenOf
    )
import Codec.CBOR.Encoding
    ( encodeBytes
    , encodeListLen
    )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Control.Lens (Prism', prism', type (:~:) (..))
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( Codecs (..)
    , DMap
    , DSum ((:=>))
    , GCompare (..)
    , GEq (..)
    , GOrdering (..)
    , RunTransaction (..)
    , Transaction
    , fromPairList
    , mapColumns
    , newRunTransaction
    )
import Database.RocksDB
    ( BatchOp
    , ColumnFamily
    , DB (..)
    , withDBCF
    )
import System.IO.Temp (withSystemTempDirectory)

import Cardano.MPFS.Application
    ( cageColumnFamilies
    , dbConfig
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , BlockId (..)
    , Coin (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Indexer.Codecs
    ( allCodecs
    , slotNoPrism
    )
import Cardano.MPFS.Indexer.Columns (AllColumns)
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageBlockEvents
    , applyCageInverses
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkTransactionalState
    )
import Cardano.MPFS.State (State (..), Tokens (..))
import Cardano.MPFS.Trie.Persistent
    ( mkUnifiedTrieManager
    )

-- ---------------------------------------------------------
-- Test-local column GADT: cage + CF rollbacks
-- ---------------------------------------------------------

data TestColumns x where
    InCage :: AllColumns x -> TestColumns x
    TestRollbacks
        :: TestColumns
            (RollbackKV SlotNo [CageInverseOp] BlockId)

instance GEq TestColumns where
    geq (InCage a) (InCage b) = geq a b
    geq TestRollbacks TestRollbacks = Just Refl
    geq _ _ = Nothing

instance GCompare TestColumns where
    gcompare (InCage a) (InCage b) = gcompare a b
    gcompare (InCage _) _ = GLT
    gcompare _ (InCage _) = GGT
    gcompare TestRollbacks TestRollbacks = GEQ

allTestCodecs :: DMap TestColumns Codecs
allTestCodecs =
    DMap.mapKeysMonotonic InCage allCodecs
        `DMap.union` fromPairList
            [ TestRollbacks
                :=> Codecs
                    { keyCodec = slotNoPrism
                    , valueCodec = cfRollbackPrism
                    }
            ]

-- | Simple CBOR codec for CF.RollbackPoint.
cfRollbackPrism
    :: Prism'
        BS.ByteString
        (CF.RollbackPoint [CageInverseOp] BlockId)
cfRollbackPrism = prism' enc dec
  where
    enc CF.RollbackPoint{CF.rpMeta} =
        toStrictByteString
            $ encodeListLen 2
                <> encodeListLen 0
                <> encodeMeta rpMeta
    dec bs =
        case deserialiseFromBytes
            ( do
                decodeListLenOf 2
                n <- decodeListLen
                mapM_ (const decodeBytes) [1 .. n]
                meta <- decodeMeta
                pure
                    CF.RollbackPoint
                        { CF.rpInverses = []
                        , CF.rpMeta = meta
                        }
            )
            (BSL.fromStrict bs) of
            Right (_, rp) -> Just rp
            Left _ -> Nothing
    encodeMeta Nothing = encodeListLen 0
    encodeMeta (Just (BlockId b)) =
        encodeListLen 1 <> encodeBytes b
    decodeMeta = do
        n <- decodeListLen
        case n of
            0 -> pure Nothing
            1 -> Just . BlockId <$> decodeBytes
            _ -> fail "bad meta len"

-- ---------------------------------------------------------
-- Types
-- ---------------------------------------------------------

type Tx =
    Transaction IO ColumnFamily TestColumns BatchOp

-- ---------------------------------------------------------
-- Cage backend
-- ---------------------------------------------------------

mkCageRestoring
    :: Backend.Restoring
        IO
        Tx
        [CageEvent]
        [CageInverseOp]
        BlockId
mkCageRestoring =
    Backend.Restoring
        { Backend.restore = \events -> do
            _ <-
                mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        events
            pure mkCageRestoring
        , Backend.toFollowing =
            pure mkCageFollowing
        }

mkCageFollowing
    :: Backend.Following
        IO
        Tx
        [CageEvent]
        [CageInverseOp]
        BlockId
mkCageFollowing =
    Backend.Following
        { Backend.follow = \events -> do
            invs <-
                mapColumns InCage
                    $ applyCageBlockEvents
                        mkTransactionalState
                        mkUnifiedTrieManager
                        events
            pure
                ( invs
                , Just (BlockId mempty)
                , mkCageFollowing
                )
        , Backend.toRestoring =
            pure mkCageRestoring
        , Backend.applyInverse =
            mapColumns InCage
                . applyCageInverses
                    mkTransactionalState
                    mkUnifiedTrieManager
                . reverse
        }

-- ---------------------------------------------------------
-- DB bracket
-- ---------------------------------------------------------

withCageRunnerDB
    :: ((forall a. Tx a -> IO a) -> IO r)
    -> IO r
withCageRunnerDB action =
    withSystemTempDirectory "cage-runner-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                ( cageColumnFamilies
                    <> [("test-rollbacks", dbConfig)]
                )
                $ \db -> do
                    let cols =
                            mkColumns
                                (columnFamilies db)
                                allTestCodecs
                        database =
                            mkRocksDBDatabase db cols
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    action runTransaction

-- ---------------------------------------------------------
-- Spec
-- ---------------------------------------------------------

spec :: Spec
spec = describe "Cage Runner E2E" $ do
    it "processes blocks in restoration" $ do
        withCageRunnerDB $ \transact -> do
            let phase0 = InRestoration mkCageRestoring
            phase1 <-
                processBlock
                    nullTracer
                    False
                    transact
                    TestRollbacks
                    maxBound
                    1
                    [CageBoot tokA tokStateA]
                    phase0
            case phase1 of
                InRestoration _ -> pure ()
                InFollowing _ _ ->
                    fail "expected Restoration"

    it "restoration stores a checkpoint (no inverses)" $ do
        withCageRunnerDB $ \transact -> do
            let phase0 = InRestoration mkCageRestoring
            _ <-
                processBlock
                    nullTracer
                    False
                    transact
                    TestRollbacks
                    maxBound
                    1
                    [CageBoot tokA tokStateA]
                    phase0
            -- Runner stores one sentinel checkpoint per
            -- restoration block (empty inverses, no meta)
            count <-
                transact
                    $ Store.countPoints TestRollbacks
            count `shouldBe` 1

    it "restoration then transition to following" $ do
        withCageRunnerDB $ \transact -> do
            let phase0 = InRestoration mkCageRestoring
            phase1 <-
                processBlock
                    nullTracer
                    False
                    transact
                    TestRollbacks
                    maxBound
                    1
                    [CageBoot tokA tokStateA]
                    phase0
            case phase1 of
                InRestoration restoring -> do
                    following <-
                        Backend.toFollowing restoring
                    let phase2 = InFollowing 0 following
                    phase3 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            2
                            [CageRequest txInA reqA]
                            phase2
                    case phase3 of
                        InFollowing n _ ->
                            n `shouldBe` 1
                        InRestoration _ ->
                            fail "expected Following"
                InFollowing _ _ ->
                    fail "expected Restoration"

    it "full lifecycle: restore, follow, rollback" $ do
        withCageRunnerDB $ \transact -> do
            let phase0 = InRestoration mkCageRestoring
            phase1 <-
                processBlock
                    nullTracer
                    False
                    transact
                    TestRollbacks
                    maxBound
                    1
                    [ CageBoot tokA tokStateA
                    , CageRequest txInA reqA
                    ]
                    phase0
            case phase1 of
                InRestoration restoring -> do
                    following <-
                        Backend.toFollowing restoring
                    transact
                        $ Store.armageddonSetup
                            TestRollbacks
                            1
                            (Just (BlockId mempty))
                    let phase2 = InFollowing 1 following
                    phase3 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            2
                            [ CageUpdate
                                tokA
                                (Root "new-root")
                                [txInA]
                            ]
                            phase2
                    phase4 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            3
                            [CageRequest txInB reqB]
                            phase3
                    case phase4 of
                        InFollowing n f -> do
                            (result, _) <-
                                transact
                                    $ rollbackTo
                                        TestRollbacks
                                        f
                                        n
                                        2
                            case result of
                                Store.RollbackSucceeded d ->
                                    d `shouldBe` 1
                                Store.RollbackImpossible ->
                                    fail "unexpected"
                        InRestoration _ ->
                            fail "expected Following"
                InFollowing _ _ ->
                    fail "expected Restoration"

    it "multiple restore-follow-rollback cycles" $ do
        withCageRunnerDB $ \transact -> do
            let doCycle n = do
                    let phase0 =
                            InRestoration mkCageRestoring
                        s = n * 10
                    phase1 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            (s + 1)
                            [CageBoot tokA tokStateA]
                            phase0
                    case phase1 of
                        InRestoration restoring -> do
                            following <-
                                Backend.toFollowing
                                    restoring
                            transact
                                $ Store.armageddonSetup
                                    TestRollbacks
                                    (s + 1)
                                    Nothing
                            let phase2 =
                                    InFollowing
                                        1
                                        following
                            phase3 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    TestRollbacks
                                    maxBound
                                    (s + 2)
                                    [ CageRequest
                                        txInA
                                        reqA
                                    ]
                                    phase2
                            phase4 <-
                                processBlock
                                    nullTracer
                                    False
                                    transact
                                    TestRollbacks
                                    maxBound
                                    (s + 3)
                                    [ CageUpdate
                                        tokA
                                        (Root "r")
                                        [txInA]
                                    ]
                                    phase3
                            case phase4 of
                                InFollowing nn f -> do
                                    _ <-
                                        transact
                                            $ rollbackTo
                                                TestRollbacks
                                                f
                                                nn
                                                (s + 2)
                                    pure ()
                                InRestoration _ ->
                                    fail "expected Following"
                        InFollowing _ _ ->
                            fail "expected Restoration"
            forM_ [0 .. 4 :: SlotNo] doCycle

    it "two tokens with interleaved trie ops" $ do
        withCageRunnerDB $ \transact -> do
            let phase0 = InRestoration mkCageRestoring
            phase1 <-
                processBlock
                    nullTracer
                    False
                    transact
                    TestRollbacks
                    maxBound
                    1
                    [ CageBoot tokA tokStateA
                    , CageBoot tokB tokStateB
                    ]
                    phase0
            case phase1 of
                InRestoration restoring -> do
                    following <-
                        Backend.toFollowing restoring
                    transact
                        $ Store.armageddonSetup
                            TestRollbacks
                            1
                            Nothing
                    let phase2 = InFollowing 1 following
                    phase3 <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            2
                            [ CageRequest txInA reqA
                            , CageRequest txInB reqB
                            ]
                            phase2
                    _ <-
                        processBlock
                            nullTracer
                            False
                            transact
                            TestRollbacks
                            maxBound
                            3
                            [ CageUpdate
                                tokA
                                (Root "rA")
                                [txInA]
                            , CageUpdate
                                tokB
                                (Root "rB")
                                [txInB]
                            ]
                            phase3
                    ts <-
                        transact
                            $ mapColumns InCage
                            $ getToken
                                (tokens mkTransactionalState)
                                tokA
                    ts
                        `shouldBe` Just
                            tokStateA{root = Root "rA"}
                InFollowing _ _ ->
                    fail "expected Restoration"

-- ---------------------------------------------------------
-- Fixtures
-- ---------------------------------------------------------

tokA :: TokenId
tokA =
    TokenId
        (AssetName (SBS.toShort ("token-a" :: BS.ByteString)))

tokB :: TokenId
tokB =
    TokenId
        (AssetName (SBS.toShort ("token-b" :: BS.ByteString)))

tokStateA :: TokenState
tokStateA =
    TokenState
        { owner = testKeyHash
        , root = Root BS.empty
        , maxFee = Coin 1_000_000
        , processTime = 100
        , retractTime = 200
        }

tokStateB :: TokenState
tokStateB =
    TokenState
        { owner = testKeyHash
        , root = Root BS.empty
        , maxFee = Coin 2_000_000
        , processTime = 150
        , retractTime = 250
        }

txInA :: TxIn
txInA = genTestTxIn 0

txInB :: TxIn
txInB = genTestTxIn 1

reqA :: Request
reqA =
    Request
        { requestToken = tokA
        , requestOwner = testKeyHash
        , requestKey = "key-1"
        , requestValue = Insert "value-1"
        , requestFee = Coin 500_000
        , requestSubmittedAt = 1000
        }

reqB :: Request
reqB =
    Request
        { requestToken = tokB
        , requestOwner = testKeyHash
        , requestKey = "key-2"
        , requestValue = Insert "value-2"
        , requestFee = Coin 600_000
        , requestSubmittedAt = 2000
        }

testKeyHash :: KeyHash 'Payment
testKeyHash =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "0000000000000000000000000000\
            \000000000000000000000000000a"

genTestTxIn :: Word16 -> TxIn
genTestTxIn ix =
    Ledger.TxIn
        ( Ledger.TxId
            $ unsafeMakeSafeHash
            $ fromJust
            $ hashFromStringAsHex @Blake2b_256
                "00000000000000000000000000000\
                \00000000000000000000000000000000001"
        )
        (TxIx ix)
