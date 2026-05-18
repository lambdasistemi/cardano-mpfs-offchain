{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Indexer.ReadsSpec
-- Description : Tests for indexer read primitives
-- License     : Apache-2.0
module Cardano.MPFS.Indexer.ReadsSpec (spec) where

import Control.Lens (view)
import Data.ByteString.Lazy qualified as BSL

import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldContain
    )
import Test.QuickCheck (generate)

import CSMT.Core.Types (Indirect (..))
import CSMT.Hashes (mkHash)
import CSMT.Interface (FromKV (..))
import Cardano.Ledger.Binary
    ( decodeFull
    , natVersion
    , serialize
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( context
    , hashAddressKey
    )

import Cardano.MPFS.Core.Types (TxIn)
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.Indexer.Reads (addressScopedLeafKey)

spec :: Spec
spec = describe "address-prefixed UTxO leaves" $ do
    it "strips the queried address prefix before decoding TxIn" $ do
        txIn <- generate genTxIn
        let CSMTContext{fromKV = fkv} = context
            txInBytes = encodeTxIn txIn
            addressKey = hashAddressKey "queried-address"
            leaf =
                Indirect
                    { jump =
                        addressKey
                            <> view (isoK fkv) txInBytes
                    , value = mkHash "unused"
                    }
        case addressScopedLeafKey fkv addressKey leaf of
            Right keyBytes ->
                decodeTxIn keyBytes `shouldBe` Right txIn
            Left err ->
                expectationFailure err

    it "reports a mismatch when a leaf is outside the queried prefix" $ do
        txIn <- generate genTxIn
        let CSMTContext{fromKV = fkv} = context
            txInBytes = encodeTxIn txIn
            queriedKey = hashAddressKey "queried-address"
            otherKey = hashAddressKey "other-address"
            leaf =
                Indirect
                    { jump =
                        otherKey
                            <> view (isoK fkv) txInBytes
                    , value = mkHash "unused"
                    }
        case addressScopedLeafKey fkv queriedKey leaf of
            Left err ->
                err
                    `shouldContain` "did not start with queried \
                                    \address prefix"
            Right _ ->
                expectationFailure
                    "Expected address prefix mismatch"

encodeTxIn :: TxIn -> BSL.ByteString
encodeTxIn =
    serialize (natVersion @11)

decodeTxIn :: BSL.ByteString -> Either String TxIn
decodeTxIn bs =
    case decodeFull (natVersion @11) bs of
        Left err -> Left (show err)
        Right txIn -> Right txIn
