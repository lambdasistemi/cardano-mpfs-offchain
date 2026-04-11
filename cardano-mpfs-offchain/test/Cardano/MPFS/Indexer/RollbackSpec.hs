{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Indexer.RollbackSpec
-- Description : Rollback integration tests
-- License     : Apache-2.0
--
-- Ports the rollback scenarios from the original TS
-- @state.unit.test.ts@: apply events at specific
-- slots, rollback to a target slot, verify state.
-- Uses mock state and pure trie manager.
module Cardano.MPFS.Indexer.RollbackSpec (spec) where

import Control.Monad (forM_)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.IORef
    ( IORef
    , modifyIORef'
    , newIORef
    , readIORef
    )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust, fromMaybe)
import Data.Word (Word16)

import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldReturn
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

import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp (..)
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageEvent
    , applyCageInverses
    , computeInverse
    )
import Cardano.MPFS.Mock.State (mkMockState)
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Trie (TrieManager (..))
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )

-- | Rollback environment: state, trie manager,
-- and inverse store (slot -> [CageInverseOp]).
data RollbackEnv = RollbackEnv
    { envState :: !(State IO)
    , envTM :: !(TrieManager IO)
    , envInvs
        :: !(IORef (Map SlotNo [CageInverseOp]))
    }

-- | Create a fresh rollback environment.
newRollbackEnv :: IO RollbackEnv
newRollbackEnv = do
    st <- mkMockState
    tm <- mkPureTrieManager
    ref <- newIORef Map.empty
    pure
        RollbackEnv
            { envState = st
            , envTM = tm
            , envInvs = ref
            }

-- | Apply a cage event at a given slot, storing
-- the inverse ops for later rollback.
applyAtSlot
    :: RollbackEnv -> SlotNo -> CageEvent -> IO ()
applyAtSlot RollbackEnv{..} slot evt = do
    cageInvs <- computeInverse envState evt
    trieInvs <- applyCageEvent envState envTM evt
    let allInvs = cageInvs ++ trieInvs
    modifyIORef' envInvs
        $ Map.insertWith (++) slot allInvs

-- | Roll back to a target slot by replaying inverse
-- ops for all slots > target in reverse order.
-- Mirrors the logic of 'rollbackToSlot'.
rollback :: RollbackEnv -> SlotNo -> IO ()
rollback RollbackEnv{..} targetSlot = do
    invMap <- readIORef envInvs
    let slotsToRevert =
            reverse
                $ filter (> targetSlot)
                $ Map.keys invMap
    forM_ slotsToRevert $ \slot -> do
        let invs =
                fromMaybe [] (Map.lookup slot invMap)
        applyCageInverses
            envState
            envTM
            (reverse invs)
        modifyIORef' envInvs (Map.delete slot)

spec :: Spec
spec = describe "Rollback" $ do
    tokenTests
    requestTests
    updateTests

-- ---------------------------------------------------------
-- Token rollback tests (from state.unit.test.ts)
-- ---------------------------------------------------------

tokenTests :: Spec
tokenTests = describe "Token rollback" $ do
    it "can rollback a token addition" $ do
        env <- newRollbackEnv
        let st = envState env
            tid = tokA
            ts = tokStateA
        applyAtSlot env 1 (CageBoot tid ts)
        getToken (tokens st) tid
            `shouldReturn` Just ts
        rollback env 0
        getToken (tokens st) tid
            `shouldReturn` Nothing

    it "can rollback a token removal" $ do
        env <- newRollbackEnv
        let st = envState env
            tid = tokA
            ts = tokStateA
        applyAtSlot env 1 (CageBoot tid ts)
        applyAtSlot env 2 (CageBurn tid)
        getToken (tokens st) tid
            `shouldReturn` Nothing
        rollback env 1
        getToken (tokens st) tid
            `shouldReturn` Just ts

    it
        "can rollback a token removal to before \
        \the token addition"
        $ do
            env <- newRollbackEnv
            let st = envState env
                tid = tokA
                ts = tokStateA
            applyAtSlot env 1 (CageBoot tid ts)
            applyAtSlot env 2 (CageBurn tid)
            rollback env 0
            getToken (tokens st) tid
                `shouldReturn` Nothing

-- ---------------------------------------------------------
-- Request rollback tests (from state.unit.test.ts)
-- ---------------------------------------------------------

requestTests :: Spec
requestTests = describe "Request rollback" $ do
    it "can rollback a request addition" $ do
        env <- newRollbackEnv
        let st = envState env
        applyAtSlot env 1 (CageRequest txInA reqA)
        getRequest (requests st) txInA
            `shouldReturn` Just reqA
        rollback env 0
        getRequest (requests st) txInA
            `shouldReturn` Nothing

    it "can rollback a request removal" $ do
        env <- newRollbackEnv
        let st = envState env
        applyAtSlot env 1 (CageRequest txInA reqA)
        applyAtSlot env 2 (CageRetract txInA)
        getRequest (requests st) txInA
            `shouldReturn` Nothing
        rollback env 1
        getRequest (requests st) txInA
            `shouldReturn` Just reqA

    it
        "can rollback a request removal to before \
        \the request addition"
        $ do
            env <- newRollbackEnv
            let st = envState env
            applyAtSlot
                env
                1
                (CageRequest txInA reqA)
            applyAtSlot env 2 (CageRetract txInA)
            rollback env 0
            getRequest (requests st) txInA
                `shouldReturn` Nothing

-- ---------------------------------------------------------
-- Update rollback tests (from state.unit.test.ts)
-- ---------------------------------------------------------

updateTests :: Spec
updateTests = describe "Update rollback" $ do
    it "can rollback a token update" $ do
        env <- newRollbackEnv
        let st = envState env
            tid = tokA
            ts = tokStateA
            newRoot = rootB
        applyAtSlot env 1 (CageBoot tid ts)
        applyAtSlot
            env
            2
            (CageUpdate tid newRoot [])
        mTs <- getToken (tokens st) tid
        fmap root mTs `shouldBe` Just newRoot
        rollback env 1
        mTs' <- getToken (tokens st) tid
        fmap root mTs' `shouldBe` Just (root ts)

    it
        "can rollback a token update with \
        \consumed requests"
        $ do
            env <- newRollbackEnv
            let st = envState env
                tid = tokA
                ts = tokStateA
                newRoot = rootB
            applyAtSlot env 1 (CageBoot tid ts)
            applyAtSlot
                env
                1
                (CageRequest txInA reqA)
            applyAtSlot
                env
                2
                ( CageUpdate
                    tid
                    newRoot
                    [txInA]
                )
            -- Request should be consumed
            getRequest (requests st) txInA
                `shouldReturn` Nothing
            rollback env 1
            -- Token root restored
            mTs <- getToken (tokens st) tid
            fmap root mTs
                `shouldBe` Just (root ts)
            -- Request restored
            getRequest (requests st) txInA
                `shouldReturn` Just reqA

-- ---------------------------------------------------------
-- Fixed test data
-- ---------------------------------------------------------

tokA :: TokenId
tokA = TokenId (AssetName (SBS.toShort ("token-a" :: BS.ByteString)))

tokStateA :: TokenState
tokStateA =
    TokenState
        { owner = testKeyHash
        , root = rootA
        , tip = Coin 1_000_000
        , processTime = 100
        , retractTime = 200
        }

rootA :: Root
rootA = Root (BS.replicate 32 0)

rootB :: Root
rootB = Root "new-root-hash-bytes"

txInA :: TxIn
txInA = genTestTxIn 0

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

-- ---------------------------------------------------------
-- Crypto helpers (minimal, deterministic)
-- ---------------------------------------------------------

testKeyHash :: KeyHash 'Payment
testKeyHash =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "0000000000000000000000000000000000000000000000000000000a"

genTestTxIn :: Word16 -> TxIn
genTestTxIn ix =
    Ledger.TxIn
        ( Ledger.TxId
            $ unsafeMakeSafeHash
            $ fromJust
            $ hashFromStringAsHex @Blake2b_256
                "0000000000000000000000000000000000000000000000000000000000000001"
        )
        (TxIx ix)
