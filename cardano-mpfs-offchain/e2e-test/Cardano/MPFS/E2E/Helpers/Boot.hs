{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.E2E.Helpers.Boot
-- Description : Test helper — wallet-side boot inputs
-- License     : Apache-2.0
--
-- Some legacy boot-builder tests act as their own
-- wallet on a private devnet UTxO set, so
-- 'queryUTxOs' remains acceptable for wallet-side
-- sentinel inputs. Proof-bearing facts helpers below
-- wait for CSMT readiness and read through the local
-- indexer path.
--
-- 'walletBootInputs' materialises the legacy
-- @[ResolvedWalletInput]@ argument still present in the
-- internal test builder interface, by:
--
-- 1. Calling 'queryUTxOs' against the wallet
--    address.
-- 2. Re-encoding each 'TxOut' to the canonical
--    ledger CBOR.
-- 3. Pairing each input with empty proof bytes
--    (these tests use sentinel snapshots whose
--    CSMT root is empty, so the proofs are not
--    verified).
--
-- Production servers MUST NOT use this path for
-- proof-bearing facts. They read the local CSMT-backed
-- indexer in a single transaction.
module Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    , withBootFactsTxBuilder
    , awaitProofReadsReady
    ) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.Text qualified as T

import Control.Applicative ((<|>))
import Control.Monad (when)

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Plutus.ExUnits (Prices (..))

import Cardano.MPFS.API.Types
    ( BootFacts (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTxWithEval)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.Eval (decodeEvalContext)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Facts (verifyBootFacts)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.HTTP.Server (mkBootFacts)
import Cardano.MPFS.Indexer.Reads
    ( IndexerReadError (..)
    , readMerkleRoot
    , readSnapshot
    , readWalletInputsAt
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder
    ( BootProof (..)
    , BundleSnapshot (..)
    , ProofEnvelope (..)
    , ResolvedWalletInput
    , TxBuilder (..)
    , WitnessedInput (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )

-- | Build a list of @[ResolvedWalletInput]@ from
-- the wallet's own LSQ view. Empty proof bytes are
-- emitted; tests use empty-root sentinel snapshots
-- where proofs are not verified.
walletBootInputs
    :: Provider IO -> Addr -> IO [ResolvedWalletInput]
walletBootInputs prov addr = do
    utxos <- queryUTxOs prov addr
    pure
        [ ( tin
          , serialize' (natVersion @11) tout
          , BS.empty
          )
        | (tin, tout) <- utxos
        ]

-- | Replace the production boot stub in an E2E context
-- with the post-pivot wallet-side flow. Production code
-- must not call 'bootToken'; E2E tests keep the old
-- record method only as a compatibility seam while they
-- exercise the same facts/verify/build path a wallet uses.
withBootFactsTxBuilder
    :: CageConfig
    -> Context IO
    -> Context IO
withBootFactsTxBuilder cfg ctx =
    ctx
        { txBuilder =
            (txBuilder ctx)
                { bootToken =
                    \_ _ addr ->
                        buildBootEnvelopeFromFacts cfg ctx addr
                }
        }

-- | Wait until the application may serve CSMT proof reads.
awaitProofReadsReady :: Context IO -> IO ()
awaitProofReadsReady ctx = go (120 :: Int)
  where
    go 0 =
        fail
            "E2E proof reads did not become ready \
            \after CSMT restoration"
    go n = do
        ready <- indexerProofsReady ctx
        if ready
            then pure ()
            else do
                threadDelay 500_000
                go (n - 1)

buildBootEnvelopeFromFacts
    :: CageConfig
    -> Context IO
    -> Addr
    -> IO (ProofEnvelope BootProof)
buildBootEnvelopeFromFacts cfg ctx addr = do
    awaitProofReadsReady ctx
    (mSnap, mRoot, eInputs) <-
        runIndexerTx ctx $ do
            snap <- readSnapshot
            root <- readMerkleRoot
            walletInputs <- readWalletInputsAt addr
            pure (snap, root, walletInputs)
    snap <-
        case mSnap <|> fmap fallbackSnapshot mRoot of
            Nothing ->
                fail "E2E boot facts: UTxO root unavailable"
            Just value -> pure value
    inputs <-
        requireIndexerRead
            "E2E boot facts: readWalletInputsAt failed"
            eInputs
    when (null inputs)
        $ fail "E2E boot facts: no wallet UTxOs at address"
    pp <- queryProtocolParams (provider ctx)
    let facts = mkBootFacts snap inputs pp
        trustedRoot = factsTrustedRoot facts
    verified <-
        case verifyBootFacts trustedRoot facts of
            Left err ->
                fail
                    $ "E2E boot facts: verifyBootFacts failed: "
                        <> show err
            Right value -> pure value
    evalCtxWire <- evalContext ctx
    evalCtx <-
        case decodeEvalContext evalCtxWire of
            Left err ->
                fail
                    $ "E2E boot facts: decodeEvalContext failed: "
                        <> show err
            Right value -> pure value
    tx <-
        case bootCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                fail
                    $ "E2E boot facts: bootCageTx failed: "
                        <> show err
            Right value -> pure value
    pure
        ProofEnvelope
            { envTx = tx
            , envSnapshot = snap
            , envProof =
                BootProof
                    (map witnessedInputFromResolved inputs)
            }

requireIndexerRead
    :: String
    -> Either IndexerReadError a
    -> IO a
requireIndexerRead label result =
    case result of
        Left (IndexerReadError msg) ->
            fail $ label <> ": " <> T.unpack msg
        Right value ->
            pure value

factsTrustedRoot :: BootFacts -> TrustedRoot
factsTrustedRoot BootFacts{bfSnapshot = VerificationSnapshot{..}} =
    TrustedRoot vsUtxoRoot

witnessedInputFromResolved
    :: ResolvedWalletInput -> WitnessedInput
witnessedInputFromResolved (tin, out, proof) =
    WitnessedInput
        { witnessedRef = tin
        , witnessedTxOut = out
        , witnessedCsmtProof = proof
        }

fallbackSnapshot :: BS.ByteString -> BundleSnapshot
fallbackSnapshot root =
    BundleSnapshot
        { snapshotUtxoRoot = root
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

permissiveWalletPolicy :: WalletPolicy
permissiveWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

toClientCageConfig :: CageConfig -> Client.CageConfig
toClientCageConfig cfg =
    Client.CageConfig
        { Client.cageScriptBytes = cageScriptBytes cfg
        , Client.requestScriptBytes = requestScriptBytes cfg
        , Client.cfgScriptHash = cfgScriptHash cfg
        , Client.defaultProcessTime = defaultProcessTime cfg
        , Client.defaultRetractTime = defaultRetractTime cfg
        , Client.defaultTip = defaultTip cfg
        , Client.network = network cfg
        }
