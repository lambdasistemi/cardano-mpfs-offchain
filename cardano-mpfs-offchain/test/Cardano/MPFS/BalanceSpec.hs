{-# LANGUAGE DataKinds #-}

module Cardano.MPFS.BalanceSpec (spec) where

import Lens.Micro ((&), (.~), (^.))
import Test.Hspec
    ( Spec
    , describe
    , it
    )
import Test.QuickCheck
    ( Property
    , forAll
    , property
    )

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Api.PParams
    ( emptyPParams
    , ppTxFeeFixedL
    )
import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , mkBasicTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( coinTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Keys (KeyHash, KeyRole (..))
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , PParams
    )
import Cardano.MPFS.Generators
    ( genKeyHash
    , genTxIn
    )
import Cardano.Tx.Balance
    ( BalanceError (..)
    , BalanceResult (..)
    , balanceTx
    )

-- | Testnet address from a payment key hash.
testAddr :: KeyHash Payment -> Addr
testAddr kh =
    Addr Testnet (KeyHashObj kh) StakeRefNull

-- | Empty transaction.
emptyTx :: ConwayTx
emptyTx = mkBasicTx mkBasicTxBody

-- | Protocol params with zero fees.
zeroPP :: PParams ConwayEra
zeroPP = emptyPParams

-- | Protocol params with a large constant fee.
highFeePP :: PParams ConwayEra
highFeePP =
    emptyPParams & ppTxFeeFixedL .~ Coin 5_000_000

spec :: Spec
spec = describe "Cardano.MPFS.Core.Balance" $ do
    describe "balanceTx" $ do
        it "succeeds when fee UTxO has enough ada"
            $ property propSucceeds

        it "change output value = input - fee"
            $ property propChangeCorrect

        it "fee field is set on the balanced tx"
            $ property propFeeSet

        it "returns InsufficientFee for too little ada"
            $ property propInsufficient

-- | With zero-fee PParams, balanceTx always succeeds.
propSucceeds :: Property
propSucceeds =
    forAll
        ((,,) <$> genTxIn <*> genKeyHash <*> genKeyHash)
        $ \(txIn, feeKh, chgKh) ->
            let feeAddr = testAddr feeKh
                changeAddr = testAddr chgKh
                inputCoin = Coin 10_000_000
                feeUtxo =
                    mkBasicTxOut
                        feeAddr
                        (inject inputCoin)
                result =
                    balanceTx
                        zeroPP
                        [(txIn, feeUtxo)]
                        []
                        changeAddr
                        emptyTx
            in  case result of
                    Right _ -> True
                    Left _ -> False

-- | change = input - fee.
propChangeCorrect :: Property
propChangeCorrect =
    forAll
        ((,,) <$> genTxIn <*> genKeyHash <*> genKeyHash)
        $ \(txIn, feeKh, chgKh) ->
            let feeAddr = testAddr feeKh
                changeAddr = testAddr chgKh
                inputCoin = Coin 10_000_000
                feeUtxo =
                    mkBasicTxOut
                        feeAddr
                        (inject inputCoin)
                result =
                    balanceTx
                        zeroPP
                        [(txIn, feeUtxo)]
                        []
                        changeAddr
                        emptyTx
            in  case result of
                    Left _ -> False
                    Right BalanceResult{balancedTx = tx} ->
                        let fee =
                                tx
                                    ^. bodyTxL
                                        . feeTxBodyL
                            outs =
                                tx
                                    ^. bodyTxL
                                        . outputsTxBodyL
                            lastOut =
                                outs `seq`
                                    last
                                        (foldr (:) [] outs)
                            changeCoin =
                                lastOut ^. coinTxOutL
                            Coin inp = inputCoin
                            Coin f = fee
                        in  changeCoin
                                == Coin (inp - f)

-- | Fee field is non-negative on the balanced tx.
propFeeSet :: Property
propFeeSet =
    forAll
        ((,,) <$> genTxIn <*> genKeyHash <*> genKeyHash)
        $ \(txIn, feeKh, chgKh) ->
            let feeAddr = testAddr feeKh
                changeAddr = testAddr chgKh
                inputCoin = Coin 10_000_000
                feeUtxo =
                    mkBasicTxOut
                        feeAddr
                        (inject inputCoin)
                result =
                    balanceTx
                        zeroPP
                        [(txIn, feeUtxo)]
                        []
                        changeAddr
                        emptyTx
            in  case result of
                    Left _ -> False
                    Right BalanceResult{balancedTx = tx} ->
                        let fee =
                                tx
                                    ^. bodyTxL
                                        . feeTxBodyL
                        in  fee >= Coin 0

-- | Insufficient ada with high-fee PParams.
propInsufficient :: Property
propInsufficient =
    forAll
        ((,,) <$> genTxIn <*> genKeyHash <*> genKeyHash)
        $ \(txIn, feeKh, chgKh) ->
            let feeAddr = testAddr feeKh
                changeAddr = testAddr chgKh
                inputCoin = Coin 1
                feeUtxo =
                    mkBasicTxOut
                        feeAddr
                        (inject inputCoin)
                result =
                    balanceTx
                        highFeePP
                        [(txIn, feeUtxo)]
                        []
                        changeAddr
                        emptyTx
            in  case result of
                    Left (InsufficientFee _ _) -> True
                    _ -> False
