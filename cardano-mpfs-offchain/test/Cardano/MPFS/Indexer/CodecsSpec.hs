-- |
-- Module      : Cardano.MPFS.Indexer.CodecsSpec
-- Description : Round-trip property tests for cage codecs
-- License     : Apache-2.0
--
-- Mirrors Lean theorems from @Phase4.Codecs@:
-- round-trip, determinism, and injectivity.
module Cardano.MPFS.Indexer.CodecsSpec (spec) where

import Control.Lens (preview, review)
import Data.ByteString qualified as BS

import Test.Hspec (Spec, describe)
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck
    ( Gen
    , choose
    , forAll
    , vectorOf
    , (=/=)
    , (===)
    , (==>)
    )

import MTS.Rollbacks.Types (RollbackPoint (..))

import Cardano.MPFS.Core.Types (BlockId)
import Cardano.MPFS.Generators
    ( genBlockId
    , genCageInverseOp
    , genRequest
    , genSlotNo
    , genTokenId
    , genTokenState
    , genTxIn
    )
import Cardano.MPFS.Indexer.Codecs
    ( checkpointPrism
    , requestPrism
    , rollbackPointPrism
    , slotNoPrism
    , tokenIdPrism
    , tokenStatePrism
    , txInPrism
    , unitPrism
    )
import Cardano.MPFS.Indexer.Columns
    ( CageCheckpoint (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageInverseOp
    )

spec :: Spec
spec = describe "Codecs" $ do
    -- -------------------------------------------
    -- Round-trip (Lean: prop_codecRoundTrip)
    -- -------------------------------------------
    describe "round-trip" $ do
        prop "tokenIdPrism"
            $ forAll genTokenId
            $ \tid ->
                preview
                    tokenIdPrism
                    (review tokenIdPrism tid)
                    === Just tid

        prop "tokenStatePrism"
            $ forAll genTokenState
            $ \ts ->
                preview
                    tokenStatePrism
                    (review tokenStatePrism ts)
                    === Just ts

        prop "txInPrism"
            $ forAll genTxIn
            $ \txin ->
                preview
                    txInPrism
                    (review txInPrism txin)
                    === Just txin

        prop "requestPrism"
            $ forAll genTokenId
            $ \tid ->
                forAll (genRequest tid) $ \req ->
                    preview
                        requestPrism
                        (review requestPrism req)
                        === Just req

        prop "unitPrism"
            $ preview unitPrism (review unitPrism ())
                === Just ()

        prop "checkpointPrism"
            $ forAll genSlotNo
            $ \s ->
                forAll genBlockId $ \b ->
                    let cp = CageCheckpoint s b
                    in  preview
                            checkpointPrism
                            ( review
                                checkpointPrism
                                cp
                            )
                            === Just cp

        prop "slotNoPrism"
            $ forAll genSlotNo
            $ \s ->
                preview
                    slotNoPrism
                    (review slotNoPrism s)
                    === Just s

        prop "rollbackPointPrism"
            $ forAll genRollbackPoint
            $ \rp ->
                preview
                    rollbackPointPrism
                    ( review
                        rollbackPointPrism
                        rp
                    )
                    === Just rp

    -- -------------------------------------------
    -- Injectivity (Lean: roundTrip_implies_injective)
    -- -------------------------------------------
    describe "injectivity" $ do
        prop "tokenIdPrism — distinct values, distinct bytes"
            $ forAll genTokenId
            $ \t1 ->
                forAll genTokenId $ \t2 ->
                    t1 /= t2 ==>
                        review tokenIdPrism t1
                            =/= review tokenIdPrism t2

        prop "txInPrism — distinct values, distinct bytes"
            $ forAll genTxIn
            $ \t1 ->
                forAll genTxIn $ \t2 ->
                    t1 /= t2 ==>
                        review txInPrism t1
                            =/= review txInPrism t2

    -- -------------------------------------------
    -- Non-empty (Lean: prop_codecNonEmpty)
    -- -------------------------------------------
    describe "non-empty encoding" $ do
        prop "tokenStatePrism produces non-empty bytes"
            $ forAll genTokenState
            $ \ts ->
                not
                    (BS.null (review tokenStatePrism ts))

        prop "requestPrism produces non-empty bytes"
            $ forAll genTokenId
            $ \tid ->
                forAll (genRequest tid) $ \req ->
                    not
                        ( BS.null
                            (review requestPrism req)
                        )

        prop "txInPrism produces non-empty bytes"
            $ forAll genTxIn
            $ \txin ->
                not
                    (BS.null (review txInPrism txin))

        prop "checkpointPrism produces non-empty bytes"
            $ forAll genSlotNo
            $ \s ->
                forAll genBlockId $ \b ->
                    not
                        ( BS.null
                            ( review
                                checkpointPrism
                                (CageCheckpoint s b)
                            )
                        )
  where
    genRollbackPoint
        :: Gen
            (RollbackPoint CageInverseOp BlockId)
    genRollbackPoint = do
        n <- choose (0 :: Int, 5)
        invs <- vectorOf n genCageInverseOp
        pure
            RollbackPoint
                { rpInverses = invs
                , rpMeta = Nothing
                }
