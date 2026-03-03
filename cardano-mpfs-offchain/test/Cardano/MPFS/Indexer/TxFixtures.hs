{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Indexer.TxFixtures
-- Description : Test transaction builders for cage event detection
-- License     : Apache-2.0
--
-- Builds minimal @Tx ConwayEra@ values encoding cage
-- events (boot, burn, request, update, retract) for
-- property tests of 'detectCageEvents' and
-- 'detectFromTx'.
module Cardano.MPFS.Indexer.TxFixtures
    ( -- * Transaction builders
      mkBootTx
    , mkBurnTx
    , mkRequestTx
    , mkUpdateTx
    , mkRetractTx

      -- * Test script hash
    , testScriptHash
    , testPolicyId
    , testCageAddr
    ) where

import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust)
import Data.Sequence.Strict qualified as StrictSeq
import Data.Set qualified as Set
import Data.Word (Word32)
import Lens.Micro ((&), (.~))

import Cardano.Crypto.Hash
    ( Blake2b_224
    , hashFromStringAsHex
    , hashToBytes
    )
import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Alonzo.Scripts (AsIx (..))
import Cardano.Ledger.Api.Tx
    ( Tx
    , mkBasicTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( inputsTxBodyL
    , mintTxBodyL
    , mkBasicTxBody
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    )
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Keys (KeyHash (..))
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    , PolicyID (..)
    )
import Cardano.Ledger.TxIn (TxIn)
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    )

import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , ConwayEra
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( mkInlineDatum
    , placeholderExUnits
    , toLedgerData
    , toPlcData
    )

-- | A fixed test script hash (28 bytes of 0xAA).
testScriptHash :: ScriptHash
testScriptHash =
    ScriptHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_224
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            \aaaaaaaaaaaaaaaaaaaaaaaaaaaa"

-- | Test policy ID from 'testScriptHash'.
testPolicyId :: PolicyID
testPolicyId = PolicyID testScriptHash

-- | Test cage address (Testnet).
testCageAddr :: Addr
testCageAddr =
    Addr
        Testnet
        (ScriptHashObj testScriptHash)
        StakeRefNull

-- | Build a boot transaction: mints +1 token and
-- creates a StateDatum output at the cage address.
mkBootTx
    :: TokenId
    -> TokenState
    -> TxIn
    -- ^ Seed input
    -> Tx ConwayEra
mkBootTx tid ts@TokenState{..} seedInput =
    let assetName = unTokenId tid
        mintMA =
            MultiAsset
                $ Map.singleton testPolicyId
                $ Map.singleton assetName 1
        stateDatum = mkStateDatum ts root
        outValue =
            MaryValue (Coin 2_000_000) mintMA
        txOut =
            mkBasicTxOut testCageAddr outValue
                & datumTxOutL
                    .~ mkInlineDatum
                        (toPlcData stateDatum)
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton seedInput
                & outputsTxBodyL
                    .~ StrictSeq.singleton txOut
                & mintTxBodyL .~ mintMA
    in  mkBasicTx body

-- | Build a burn transaction: mints -1 token.
mkBurnTx
    :: TokenId
    -> TxIn
    -- ^ Dummy input
    -> Tx ConwayEra
mkBurnTx tid dummyInput =
    let assetName = unTokenId tid
        burnMA =
            MultiAsset
                $ Map.singleton testPolicyId
                $ Map.singleton assetName (-1)
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton dummyInput
                & mintTxBodyL .~ burnMA
    in  mkBasicTx body

-- | Build a request transaction: output at cage
-- address with RequestDatum.
mkRequestTx
    :: Request
    -> TxIn
    -- ^ Dummy input
    -> Tx ConwayEra
mkRequestTx Request{..} dummyInput =
    let onChainTid = mkOnChainTid requestToken
        KeyHash ownerH = requestOwner
        onChainOp = toOnChainOp requestValue
        Coin fee = requestFee
        reqDatum =
            RequestDatum
                OnChainRequest
                    { requestToken = onChainTid
                    , requestOwner =
                        BuiltinByteString
                            (hashToBytes ownerH)
                    , requestKey = requestKey
                    , requestValue = onChainOp
                    , requestFee = fee
                    , requestSubmittedAt =
                        requestSubmittedAt
                    }
        txOut =
            mkBasicTxOut
                testCageAddr
                (inject (Coin 2_000_000))
                & datumTxOutL
                    .~ mkInlineDatum
                        (toPlcData reqDatum)
        body =
            mkBasicTxBody
                & inputsTxBodyL
                    .~ Set.singleton dummyInput
                & outputsTxBodyL
                    .~ StrictSeq.singleton txOut
    in  mkBasicTx body

-- | Build an update transaction: spends the state
-- input with a Modify redeemer, outputs new state.
mkUpdateTx
    :: TokenId
    -> TokenState
    -- ^ Token state (for output datum)
    -> Root
    -- ^ New root
    -> [TxIn]
    -- ^ Consumed request inputs
    -> TxIn
    -- ^ State input
    -> Tx ConwayEra
mkUpdateTx tid ts _newRoot reqInputs stateInput =
    let assetName = unTokenId tid
        newStateDatum =
            mkStateDatum ts _newRoot
        tokenMA =
            MultiAsset
                $ Map.singleton testPolicyId
                $ Map.singleton assetName 1
        outValue =
            MaryValue (Coin 2_000_000) tokenMA
        txOut =
            mkBasicTxOut testCageAddr outValue
                & datumTxOutL
                    .~ mkInlineDatum
                        (toPlcData newStateDatum)
        allInputs =
            Set.fromList
                (stateInput : reqInputs)
        stateIx =
            spendIdx stateInput allInputs
        redeemer = Modify []
        redeemers =
            Redeemers
                $ Map.singleton
                    ( ConwaySpending
                        (AsIx stateIx)
                    )
                    ( toLedgerData redeemer
                    , placeholderExUnits
                    )
        body =
            mkBasicTxBody
                & inputsTxBodyL .~ allInputs
                & outputsTxBodyL
                    .~ StrictSeq.singleton txOut
    in  mkBasicTx body
            & witsTxL . rdmrsTxWitsL .~ redeemers

-- | Build a retract transaction: spends a request
-- input with a Retract redeemer.
mkRetractTx
    :: TxIn
    -- ^ Request input to retract
    -> TxIn
    -- ^ Additional input (needed for spending index)
    -> Tx ConwayEra
mkRetractTx reqInput extraInput =
    let allInputs =
            Set.fromList [reqInput, extraInput]
        reqIx = spendIdx reqInput allInputs
        dummyRef =
            OnChainTxOutRef
                (BuiltinByteString "")
                0
        redeemer = Retract dummyRef
        redeemers =
            Redeemers
                $ Map.singleton
                    ( ConwaySpending
                        (AsIx reqIx)
                    )
                    ( toLedgerData redeemer
                    , placeholderExUnits
                    )
        body =
            mkBasicTxBody
                & inputsTxBodyL .~ allInputs
    in  mkBasicTx body
            & witsTxL . rdmrsTxWitsL .~ redeemers

-- ---------------------------------------------------------
-- Internal helpers
-- ---------------------------------------------------------

-- | Convert a domain 'TokenId' to on-chain encoding.
mkOnChainTid :: TokenId -> OnChainTokenId
mkOnChainTid (TokenId (AssetName sbs)) =
    OnChainTokenId
        $ BuiltinByteString
        $ SBS.fromShort sbs

-- | Convert a domain 'Operation' to on-chain.
toOnChainOp :: Operation -> OnChainOperation
toOnChainOp (Insert v) = OpInsert v
toOnChainOp (Delete v) = OpDelete v
toOnChainOp (Update o n) = OpUpdate o n

-- | Build a 'StateDatum' from 'TokenState' and root.
mkStateDatum :: TokenState -> Root -> CageDatum
mkStateDatum TokenState{..} r =
    let KeyHash ownerH = owner
        Coin mf = maxFee
    in  StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString
                        (hashToBytes ownerH)
                , stateRoot =
                    OnChainRoot (unRoot r)
                , stateMaxFee = mf
                , stateProcessTime = processTime
                , stateRetractTime = retractTime
                }

-- | Compute spending index of a TxIn in a set.
spendIdx :: TxIn -> Set.Set TxIn -> Word32
spendIdx needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] = error "spendIdx: not found"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs
