{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.TxBuilder.Real.Boot
-- Description : Boot token: pure description of POST /tx/boot
-- License     : Apache-2.0
--
-- Pure description of @POST \/tx\/boot@. The module
-- exports 'bootTokenCore', which takes the cage
-- configuration, the bundle snapshot, the indexer-
-- resolved wallet inputs, and the owner address, and
-- returns either a 'BootCore' (if the inputs decode
-- and are non-empty) or a 'BootCoreError'.
--
-- A 'BootCore' carries everything needed to drive
-- the 'Cardano.Node.Client.TxBuild' DSL's 'build'
-- loop:
--
-- * the 'TxBuild' program describing the mint;
-- * the @[(TxIn, TxOut)]@ list 'build' uses for fee
--   and change-output computation;
-- * the change/owner address;
-- * the @WitnessedInput@s the proof-bearing response
--   carries verbatim;
-- * the snapshot that anchors the proofs.
--
-- The actual @IO@ step that runs 'build' lives in
-- "Cardano.MPFS.TxBuilder.Real" — that's where the
-- 'Provider' (and therefore the protocol parameters
-- and the script evaluator) is held.
--
-- __Invariant__: this module MUST NOT call
-- 'Cardano.MPFS.Provider.queryUTxOs'. In fact this
-- module is pure: it imports neither 'Provider' nor
-- 'IO'.
module Cardano.MPFS.TxBuilder.Real.Boot
    ( BootCore (..)
    , BootCoreError (..)
    , bootTokenCore
    ) where

import Data.Functor.Const (Const)
import Data.Map.Strict qualified as Map
import Data.Void (Void)

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Binary (DecoderError)
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    )

import Cardano.MPFS.Core.OnChain
    ( MintRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( ConwayEra
    , TxIn
    )
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , ResolvedWalletInput
    , WitnessedInput
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Boot.Inputs
    ( InputRow (..)
    , decodeAll
    , ledgerPair
    , rowToWitness
    )
import Cardano.MPFS.TxBuilder.Real.Boot.Transaction
    ( bootAssetName
    , bootStateDatum
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , cagePolicyIdFromCfg
    , mkCageScript
    , txInToRef
    )

import Cardano.Node.Client.TxBuild
    ( TxBuild
    , attachScript
    , collateral
    , mint
    , payTo'
    , spend
    )

-- | Reasons why building a 'BootCore' might fail
-- before any IO runs.
data BootCoreError
    = -- | The indexer-resolved bytes for some input
      -- failed to decode as a Conway 'TxOut'. Signals
      -- indexer corruption upstream.
      BootCoreDecodeFailed DecoderError
    | -- | The HTTP layer should have rejected this
      -- before reaching us; surface a loud error if
      -- it slips through.
      BootCoreEmptyInputs
    deriving (Show)

-- | All the data the IO layer needs to drive the
-- DSL's 'build' loop and emit a proof-bearing
-- 'ProofEnvelope BootProof'.
data BootCore = BootCore
    { bcProgram :: TxBuild (Const ()) Void ()
    -- ^ The 'TxBuild' program. Pure — describes what
    -- the transaction does without running scripts
    -- or balancing fees.
    , bcInputs :: [(TxIn, TxOut ConwayEra)]
    -- ^ Ledger-typed view of the picked wallet
    -- inputs. Fed to 'build' for change-output and
    -- fee computation.
    , bcAddr :: Addr
    -- ^ Owner / change address.
    , bcFunding :: [WitnessedInput]
    -- ^ The witnessed-input rows that travel
    -- verbatim into 'BootProof.bootFunding'.
    , bcSnapshot :: BundleSnapshot
    -- ^ Snapshot anchoring the proofs.
    }

-- | Build a pure 'BootCore' from the boot-handler's
-- inputs. Returns 'Left' if the inputs are malformed
-- (the HTTP layer should already have rejected those
-- cases, but we surface them rather than 'error' on
-- structural problems).
bootTokenCore
    :: CageConfig
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> Addr
    -> Either BootCoreError BootCore
bootTokenCore cfg snap inputs addr =
    case decodeAll inputs of
        Left err ->
            Left (BootCoreDecodeFailed err)
        Right [] ->
            Left BootCoreEmptyInputs
        Right (seedRow : restRows) ->
            Right (mkCore seedRow restRows)
  where
    mkCore seedRow restRows =
        let pickedRows = case restRows of
                [] -> [seedRow]
                (u : _) -> [seedRow, u]
            pickedLedgerPairs =
                map ledgerPair pickedRows
            seedRef = rowRef seedRow
            collatRef =
                fst (last pickedLedgerPairs)
            an = bootAssetName seedRef
            policyId = cagePolicyIdFromCfg cfg
            script = mkCageScript cfg
            scriptAddr =
                cageAddrFromCfg cfg (network cfg)
            mintAssets = Map.singleton an 1
            mintMA =
                MultiAsset
                    $ Map.singleton
                        policyId
                        mintAssets
            stateValue =
                MaryValue
                    (Coin 2_000_000)
                    mintMA
            redeemer = Minting (txInToRef seedRef)
            program :: TxBuild (Const ()) Void ()
            program = do
                _ <- spend seedRef
                attachScript script
                mint policyId mintAssets redeemer
                _ <-
                    payTo'
                        scriptAddr
                        stateValue
                        (bootStateDatum cfg addr)
                collateral collatRef
        in  BootCore
                { bcProgram = program
                , bcInputs = pickedLedgerPairs
                , bcAddr = addr
                , bcFunding =
                    map rowToWitness pickedRows
                , bcSnapshot = snap
                }
