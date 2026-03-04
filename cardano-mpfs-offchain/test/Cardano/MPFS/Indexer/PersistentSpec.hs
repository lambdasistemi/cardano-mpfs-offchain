{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Indexer.PersistentSpec
-- Description : Integration tests for RocksDB-backed State
-- License     : Apache-2.0
--
-- Runs property-based tests for the persistent
-- 'State' implementation backed by RocksDB via
-- 'mkPersistentState'. Each property creates a
-- fresh temporary database. Also includes
-- persistence-specific tests verifying data survives
-- DB close/reopen cycles.
module Cardano.MPFS.Indexer.PersistentSpec
    ( -- * Test suite
      spec

      -- * Test utilities
    , withTestState
    , withTestStateAt
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromJust, isNothing)
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldReturn
    , shouldSatisfy
    )
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck
    ( forAll
    , ioProperty
    , (===)
    , (==>)
    )

import Cardano.Crypto.Hash
    ( Blake2b_224
    , Blake2b_256
    , hashFromStringAsHex
    )
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Hashes (unsafeMakeSafeHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.Slot (SlotNo (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))

import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( newRunTransaction
    )
import Database.RocksDB
    ( DB (..)
    , withDBCF
    )
import System.IO.Temp
    ( withSystemTempDirectory
    )

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
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Generators
    ( genBlockId
    , genRequest
    , genSlotNo
    , genTokenId
    , genTokenState
    , genTxIn
    )
import Cardano.MPFS.Indexer.Codecs (allCodecs)
import Cardano.MPFS.Indexer.Persistent
    ( mkPersistentState
    )
import Cardano.MPFS.State
    ( Checkpoints (..)
    , Requests (..)
    , State (..)
    , Tokens (..)
    )

-- ---------------------------------------------------------
-- Test DB bracket
-- ---------------------------------------------------------

-- | Run an action with a temporary RocksDB that has
-- all cage column families and a persistent 'State'.
withTestState :: (State IO -> IO a) -> IO a
withTestState action =
    withSystemTempDirectory "state-test"
        $ \dir -> withTestStateAt dir action

-- | Open a RocksDB at a specific path with all cage
-- column families and a persistent 'State'. Used for
-- reopen tests.
withTestStateAt
    :: FilePath -> (State IO -> IO a) -> IO a
withTestStateAt dir action =
    withDBCF dir dbConfig cageColumnFamilies
        $ \db -> do
            let columns =
                    mkColumns
                        (columnFamilies db)
                        allCodecs
                database =
                    mkRocksDBDatabase db columns
            rt <- newRunTransaction database
            action (mkPersistentState rt)

-- ---------------------------------------------------------
-- Spec
-- ---------------------------------------------------------

-- | Persistent State test suite.
spec :: Spec
spec = describe "Persistent State" $ do
    describe "Tokens" tokensSpec
    describe "Requests" requestsSpec
    describe "Checkpoints" checkpointsSpec
    describe "persistence-specific" $ do
        it
            "tokens survive reopen"
            tokensSurviveReopen
        it
            "requests survive reopen"
            requestsSurviveReopen
        it
            "checkpoint survives reopen"
            checkpointSurvivesReopen
        it
            "remove persists across reopen"
            removePersistsAcrossReopen

-- ---------------------------------------------------------
-- Token properties
-- ---------------------------------------------------------

tokensSpec :: Spec
tokensSpec = do
    prop "get on empty returns Nothing"
        $ forAll genTokenId
        $ \tid -> ioProperty
            $ withTestState
            $ \st -> do
                r <- getToken (tokens st) tid
                pure (r === Nothing)

    prop "put/get round-trip"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                ioProperty
                    $ withTestState
                    $ \st -> do
                        putToken (tokens st) tid ts
                        r <- getToken (tokens st) tid
                        pure (r === Just ts)

    prop "put/remove/get returns Nothing"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                ioProperty
                    $ withTestState
                    $ \st -> do
                        putToken (tokens st) tid ts
                        removeToken (tokens st) tid
                        r <- getToken (tokens st) tid
                        pure
                            (isNothing r === True)

    prop "put appears in listTokens"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts ->
                ioProperty
                    $ withTestState
                    $ \st -> do
                        putToken (tokens st) tid ts
                        ids <-
                            listTokens (tokens st)
                        pure
                            ( (tid `elem` ids)
                                === True
                            )

    prop "put overwrites previous"
        $ forAll genTokenId
        $ \tid ->
            forAll genTokenState $ \ts1 ->
                forAll genTokenState $ \ts2 ->
                    ioProperty
                        $ withTestState
                        $ \st -> do
                            putToken
                                (tokens st)
                                tid
                                ts1
                            putToken
                                (tokens st)
                                tid
                                ts2
                            r <-
                                getToken
                                    (tokens st)
                                    tid
                            pure (r === Just ts2)

    prop "remove on empty doesn't crash"
        $ forAll genTokenId
        $ \tid -> ioProperty
            $ withTestState
            $ \st -> do
                removeToken (tokens st) tid
                pure (True === True)

-- ---------------------------------------------------------
-- Request properties
-- ---------------------------------------------------------

requestsSpec :: Spec
requestsSpec = do
    prop "put/get round-trip"
        $ forAll genTxIn
        $ \txin ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    ioProperty
                        $ withTestState
                        $ \st -> do
                            putRequest
                                (requests st)
                                txin
                                req
                            r <-
                                getRequest
                                    (requests st)
                                    txin
                            pure (r === Just req)

    prop "put/remove/get returns Nothing"
        $ forAll genTxIn
        $ \txin ->
            forAll genTokenId $ \tid ->
                forAll (genRequest tid) $ \req ->
                    ioProperty
                        $ withTestState
                        $ \st -> do
                            putRequest
                                (requests st)
                                txin
                                req
                            removeRequest
                                (requests st)
                                txin
                            r <-
                                getRequest
                                    (requests st)
                                    txin
                            pure
                                ( isNothing r
                                    === True
                                )

    prop "requestsByToken filters correctly"
        $ forAll genTokenId
        $ \tid1 ->
            forAll genTokenId $ \tid2 ->
                tid1 /= tid2 ==>
                    forAll genTxIn
                        $ \txin1 ->
                            forAll genTxIn $ \txin2 ->
                                txin1
                                    /= txin2
                                    ==> forAll
                                        (genRequest tid1)
                                        $ \req1 ->
                                            forAll
                                                (genRequest tid2)
                                                $ \req2 ->
                                                    ioProperty
                                                        $ withTestState
                                                        $ \st -> do
                                                            putRequest
                                                                (requests st)
                                                                txin1
                                                                req1
                                                            putRequest
                                                                (requests st)
                                                                txin2
                                                                req2
                                                            r <-
                                                                requestsByToken
                                                                    (requests st)
                                                                    tid1
                                                            pure
                                                                ( r
                                                                    === [req1]
                                                                )

    prop "requestsByToken on empty returns []"
        $ forAll genTokenId
        $ \tid -> ioProperty
            $ withTestState
            $ \st -> do
                r <-
                    requestsByToken (requests st) tid
                pure (null r === True)

-- ---------------------------------------------------------
-- Checkpoint properties
-- ---------------------------------------------------------

checkpointsSpec :: Spec
checkpointsSpec = do
    it "get on empty returns Nothing"
        $ withTestState
        $ \st ->
            getCheckpoint (checkpoints st)
                `shouldReturn` Nothing

    prop "put/get round-trip"
        $ forAll genSlotNo
        $ \s ->
            forAll genBlockId $ \b ->
                ioProperty
                    $ withTestState
                    $ \st -> do
                        putCheckpoint
                            (checkpoints st)
                            s
                            b
                        r <-
                            getCheckpoint
                                (checkpoints st)
                        pure
                            (r === Just (s, b))

    prop "put overwrites previous"
        $ forAll genSlotNo
        $ \s1 ->
            forAll genBlockId $ \b1 ->
                forAll genSlotNo $ \s2 ->
                    forAll genBlockId $ \b2 ->
                        ioProperty
                            $ withTestState
                            $ \st -> do
                                putCheckpoint
                                    (checkpoints st)
                                    s1
                                    b1
                                putCheckpoint
                                    (checkpoints st)
                                    s2
                                    b2
                                r <-
                                    getCheckpoint
                                        (checkpoints st)
                                pure
                                    ( r
                                        === Just
                                            (s2, b2)
                                    )

-- ---------------------------------------------------------
-- Persistence-specific tests
-- ---------------------------------------------------------

-- | Put a token, close DB, reopen, verify it's there.
tokensSurviveReopen :: IO ()
tokensSurviveReopen =
    withSystemTempDirectory "reopen-tok"
        $ \dir -> do
            -- Phase 1: write
            withTestStateAt dir $ \st ->
                putToken
                    (tokens st)
                    fixTokenId
                    fixTokenState
            -- Phase 2: reopen and verify
            withTestStateAt dir $ \st -> do
                r <-
                    getToken (tokens st) fixTokenId
                r `shouldBe` Just fixTokenState

-- | Put a request, close DB, reopen, verify it's
-- there.
requestsSurviveReopen :: IO ()
requestsSurviveReopen =
    withSystemTempDirectory "reopen-req"
        $ \dir -> do
            -- Phase 1: write
            withTestStateAt dir $ \st ->
                putRequest
                    (requests st)
                    fixTxIn
                    fixRequest
            -- Phase 2: reopen and verify
            withTestStateAt dir $ \st -> do
                r <-
                    getRequest (requests st) fixTxIn
                r `shouldBe` Just fixRequest

-- | Put a checkpoint, close DB, reopen, verify.
checkpointSurvivesReopen :: IO ()
checkpointSurvivesReopen =
    withSystemTempDirectory "reopen-cp"
        $ \dir -> do
            -- Phase 1: write
            withTestStateAt dir $ \st ->
                putCheckpoint
                    (checkpoints st)
                    fixSlot
                    fixBlockId
            -- Phase 2: reopen and verify
            withTestStateAt dir $ \st -> do
                r <-
                    getCheckpoint (checkpoints st)
                r
                    `shouldBe` Just
                        (fixSlot, fixBlockId)

-- | Put then remove a token, close DB, reopen,
-- verify removal persisted.
removePersistsAcrossReopen :: IO ()
removePersistsAcrossReopen =
    withSystemTempDirectory "reopen-rm"
        $ \dir -> do
            -- Phase 1: write then remove
            withTestStateAt dir $ \st -> do
                putToken
                    (tokens st)
                    fixTokenId
                    fixTokenState
                removeToken (tokens st) fixTokenId
            -- Phase 2: reopen and verify gone
            withTestStateAt dir $ \st -> do
                r <-
                    getToken (tokens st) fixTokenId
                r `shouldSatisfy` isNothing

-- ---------------------------------------------------------
-- Test fixtures
-- ---------------------------------------------------------

-- | Deterministic test token ID.
fixTokenId :: TokenId
fixTokenId =
    TokenId (AssetName (SBS.toShort "test-token"))

-- | Deterministic test key hash (28-byte Blake2b-224).
-- 56 hex chars = 28 bytes.
fixKeyHash :: KeyHash 'Payment
fixKeyHash =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            (replicate 56 'a')

-- | Deterministic test token state.
fixTokenState :: TokenState
fixTokenState =
    TokenState
        { owner = fixKeyHash
        , root = Root (BS.replicate 32 0)
        , maxFee = Coin 1_000_000
        , processTime = 60
        , retractTime = 120
        }

-- | Deterministic test TxIn.
fixTxIn :: TxIn
fixTxIn =
    TxIn
        ( TxId
            $ unsafeMakeSafeHash
            $ fromJust
            $ hashFromStringAsHex
                @Blake2b_256
                (replicate 64 'b')
        )
        (TxIx 0)

-- | Deterministic test request.
fixRequest :: Request
fixRequest =
    Request
        { requestToken = fixTokenId
        , requestOwner = fixKeyHash
        , requestKey = "hello"
        , requestValue = Insert "world"
        , requestFee = Coin 1_000_000
        , requestSubmittedAt = 0
        }

-- | Deterministic test slot.
fixSlot :: SlotNo
fixSlot = SlotNo 42

-- | Deterministic test block ID.
fixBlockId :: BlockId
fixBlockId =
    BlockId (BS.replicate 32 0xBB)
