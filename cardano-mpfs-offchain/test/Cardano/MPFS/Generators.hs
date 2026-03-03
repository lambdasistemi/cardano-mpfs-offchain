{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Generators
-- Description : QuickCheck generators for MPFS domain types
-- License     : Apache-2.0
module Cardano.MPFS.Generators
    ( -- * Generators
      genTokenId
    , genRoot
    , genKeyHash
    , genTxIn
    , genTokenState
    , genRequest
    , genOperation
    , genSlotNo
    , genBlockId
    , genCageEvent
    , genCageInverseOp
    , genMaxFee
    , genTrieKey
    , genTrieValue

      -- * Sequence generators
    , genValidEventSequence
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromJust)
import Data.Word (Word16, Word64)
import Numeric (showHex)

import Test.QuickCheck
    ( Gen
    , arbitrary
    , choose
    , elements
    , listOf1
    , oneof
    , shuffle
    , sublistOf
    , vectorOf
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
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , CageInverseOp (..)
    )

-- | Generate a hex string of given byte-length
-- (each byte = 2 hex chars).
genHex :: Int -> Gen String
genHex n =
    concatMap (\b -> padHex (showHex b ""))
        <$> vectorOf n (choose (0 :: Int, 255))
  where
    padHex [c] = ['0', c]
    padHex s = s

-- | Generate a 'TokenId' wrapping a random
-- 'AssetName' (1-32 bytes).
genTokenId :: Gen TokenId
genTokenId = do
    len <- choose (1, 32)
    bs <- SBS.pack <$> vectorOf len arbitrary
    pure $ TokenId (AssetName bs)

-- | Generate a 32-byte 'Root' hash.
genRoot :: Gen Root
genRoot = Root . BS.pack <$> vectorOf 32 arbitrary

-- | Generate a 'KeyHash' ''Payment' from a random
-- 28-byte Blake2b-224 hash.
genKeyHash :: Gen (KeyHash 'Payment)
genKeyHash = do
    hex <- genHex 28
    pure
        $ KeyHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224 hex

-- | Generate a 'TxIn' from a random 'TxId' and
-- small index.
genTxIn :: Gen TxIn
genTxIn = do
    hex <- genHex 32
    let txId =
            TxId
                $ unsafeMakeSafeHash
                $ fromJust
                $ hashFromStringAsHex @Blake2b_256 hex
    ix <- TxIx <$> choose (0 :: Word16, 10)
    pure $ TxIn txId ix

-- | Generate a 'Coin' value.
genCoin :: Gen Coin
genCoin = Coin <$> choose (0, 10_000_000)

-- | Generate a POSIXTime (ms).
genPosixTime :: Gen Integer
genPosixTime =
    fromIntegral
        <$> choose (0 :: Int, 2_000_000_000)

-- | Generate a 'TokenState' with random owner,
-- root, maxFee, processTime, and retractTime.
genTokenState :: Gen TokenState
genTokenState =
    TokenState
        <$> genKeyHash
        <*> genRoot
        <*> genCoin
        <*> genPosixTime
        <*> genPosixTime

-- | Generate a random 'Operation'.
genOperation :: Gen Operation
genOperation = do
    bs1 <- BS.pack <$> vectorOf 8 arbitrary
    bs2 <- BS.pack <$> vectorOf 8 arbitrary
    elements
        [ Insert bs1
        , Delete bs1
        , Update bs1 bs2
        ]

-- | Generate a 'Request' for a given 'TokenId'.
genRequest :: TokenId -> Gen Request
genRequest tid = do
    owner <- genKeyHash
    k <- BS.pack <$> vectorOf 8 arbitrary
    op <- genOperation
    fee <- genCoin
    submittedAt <-
        fromIntegral
            <$> choose (0 :: Int, 2_000_000_000)
    pure
        Request
            { requestToken = tid
            , requestOwner = owner
            , requestKey = k
            , requestValue = op
            , requestFee = fee
            , requestSubmittedAt = submittedAt
            }

-- | Generate a 'SlotNo'.
genSlotNo :: Gen SlotNo
genSlotNo = SlotNo <$> choose (0 :: Word64, 1000000)

-- | Generate a 'BlockId' (32-byte hash).
genBlockId :: Gen BlockId
genBlockId =
    BlockId . BS.pack <$> vectorOf 32 arbitrary

-- | Generate a 'CageEvent'.
genCageEvent :: Gen CageEvent
genCageEvent = do
    tid <- genTokenId
    oneof
        [ CageBoot tid <$> genTokenState
        , CageRequest
            <$> genTxIn
            <*> genRequest tid
        , CageUpdate tid
            <$> genRoot
            <*> listOf1 genTxIn
        , CageRetract <$> genTxIn
        , pure (CageBurn tid)
        ]

-- | Generate a maxFee in the range
-- 500,000 .. 5,000,000 lovelace.
genMaxFee :: Gen Coin
genMaxFee = Coin <$> choose (500_000, 5_000_000)

-- | Generate a trie key (1-64 bytes).
genTrieKey :: Gen BS.ByteString
genTrieKey = do
    len <- choose (1, 64)
    BS.pack <$> vectorOf len arbitrary

-- | Generate a trie value (1-256 bytes).
genTrieValue :: Gen BS.ByteString
genTrieValue = do
    len <- choose (1, 256)
    BS.pack <$> vectorOf len arbitrary

-- | Generate a random 'ByteString' (1-16 bytes).
genBS :: Gen BS.ByteString
genBS = do
    len <- choose (1, 16)
    BS.pack <$> vectorOf len arbitrary

-- | Generate a 'CageInverseOp'.
genCageInverseOp :: Gen CageInverseOp
genCageInverseOp = do
    tid <- genTokenId
    oneof
        [ InvRestoreToken tid <$> genTokenState
        , pure (InvRemoveToken tid)
        , InvRestoreRequest
            <$> genTxIn
            <*> genRequest tid
        , InvRemoveRequest <$> genTxIn
        , InvRestoreRoot tid <$> genRoot
        , InvTrieInsert tid <$> genBS <*> genBS
        , InvTrieDelete tid <$> genBS
        ]

-- | Generate a valid sequence of 'CageEvent's
-- respecting ordering constraints:
--
-- * 'CageBoot' precedes any 'CageUpdate' or
--   'CageBurn' for the same token
-- * 'CageRequest' precedes any 'CageRetract' for
--   the same 'TxIn'
-- * No duplicate boots for the same token
-- * No duplicate requests for the same 'TxIn'
--
-- Generates 1-3 independent tokens, each with a
-- boot followed by optional requests, updates,
-- retracts, and an optional burn.
genValidEventSequence :: Gen [CageEvent]
genValidEventSequence = do
    numTokens <- choose (1 :: Int, 3)
    tids <- vectorOf numTokens genTokenId
    tss <- vectorOf numTokens genTokenState
    concat <$> traverse genTokenEvents (zip tids tss)
  where
    genTokenEvents (tid, ts) = do
        -- Generate 0-3 requests
        numReqs <- choose (0 :: Int, 3)
        reqTxIns <- vectorOf numReqs genTxIn
        reqs <- vectorOf numReqs (genRequest tid)
        let requestEvts =
                zipWith CageRequest reqTxIns reqs
        -- Optionally retract some requests
        retracted <- sublistOf reqTxIns
        let retractEvts = map CageRetract retracted
        -- Optionally update (consuming non-retracted
        -- requests)
        let available =
                filter
                    (`notElem` retracted)
                    reqTxIns
        doUpdate <- elements [True, False]
        updateEvts <-
            if doUpdate
                then do
                    newRoot <- genRoot
                    pure
                        [ CageUpdate
                            tid
                            newRoot
                            available
                        ]
                else pure []
        -- Optionally burn
        doBurn <- elements [True, False]
        let burnEvts =
                [CageBurn tid | doBurn]
        -- Assemble in valid order:
        -- boot, then requests, then retracts
        -- + updates interleaved, then burn
        middle <-
            shuffle (retractEvts ++ updateEvts)
        pure
            $ [CageBoot tid ts]
                ++ requestEvts
                ++ middle
                ++ burnEvts
