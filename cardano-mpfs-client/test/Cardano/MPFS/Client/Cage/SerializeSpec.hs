{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Client.Cage.SerializeSpec
-- Description : Unit tests for submission-ready cage tx serialization.
module Cardano.MPFS.Client.Cage.SerializeSpec
    ( spec
    ) where

import Data.ByteString qualified as BS
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )

import Cardano.Ledger.Api.Tx (mkBasicTx)
import Cardano.Ledger.Api.Tx.Body (mkBasicTxBody)
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.MPFS.Client.Cage.Serialize (serializeCageTx)
import Cardano.Tx.Ledger (ConwayTx)

-- | A minimal, deterministic Conway transaction. Needs no proof
-- fixtures: an empty body wrapped in a basic tx is enough to
-- exercise serialization.
basicTx :: ConwayTx
basicTx = mkBasicTx mkBasicTxBody

spec :: Spec
spec = describe "serializeCageTx" $ do
    it "matches the ledger CBOR encoding at protocol version 11"
        $ serializeCageTx basicTx
        `shouldBe` serialize' (natVersion @11) basicTx
    it "produces non-empty CBOR"
        $ serializeCageTx basicTx
        `shouldSatisfy` ((> 0) . BS.length)
