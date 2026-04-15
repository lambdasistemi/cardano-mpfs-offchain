{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.SubmitterSpec
-- Description : E2E tests for the N2C Submitter
-- License     : Apache-2.0
module Cardano.MPFS.E2E.SubmitterSpec (spec) where

import Lens.Micro ((&), (.~))
import Test.Hspec
    ( Spec
    , around
    , describe
    , it
    , shouldSatisfy
    )

import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Api.Tx (mkBasicTx)
import Cardano.Ledger.Api.Tx.Body
    ( mkBasicTxBody
    , vldtTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( SlotNo (..)
    , StrictMaybe (SJust, SNothing)
    )

import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Provider.NodeClient
    ( mkNodeClientProvider
    )
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.Submitter.N2C (mkN2CSubmitter)
import Cardano.Node.Client.Balance
    ( BalanceResult (..)
    , balanceTx
    )
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisSignKey
    , withDevnet
    )

spec :: Spec
spec =
    around withDevnet'
        $ describe "Submitter.N2C"
        $ do
            it "submits a simple ADA transfer"
                $ \(lsq, ltxs) -> do
                    let provider =
                            mkNodeClientProvider lsq
                        submitter =
                            mkN2CSubmitter ltxs
                    utxos <-
                        queryUTxOs
                            provider
                            genesisAddr
                    pp <-
                        queryProtocolParams provider
                    case utxos of
                        [] ->
                            error
                                "no genesis UTxOs"
                        (feeUtxo : _) -> do
                            let vldt =
                                    ValidityInterval
                                        SNothing
                                        ( SJust
                                            ( SlotNo
                                                100_000
                                            )
                                        )
                                body =
                                    mkBasicTxBody
                                        & vldtTxBodyL
                                            .~ vldt
                                tx = mkBasicTx body
                            case balanceTx
                                pp
                                [feeUtxo]
                                genesisAddr
                                tx of
                                Left err ->
                                    error
                                        $ "balanceTx failed: "
                                            <> show err
                                Right BalanceResult{balancedTx = balanced} -> do
                                    let signed =
                                            addKeyWitness
                                                genesisSignKey
                                                balanced
                                    result <-
                                        submitTx
                                            submitter
                                            signed
                                    result
                                        `shouldSatisfy` isSubmitted
  where
    withDevnet' action =
        withDevnet $ curry action

-- | Check if a 'SubmitResult' is 'Submitted'.
isSubmitted :: SubmitResult -> Bool
isSubmitted (Submitted _) = True
isSubmitted _ = False
