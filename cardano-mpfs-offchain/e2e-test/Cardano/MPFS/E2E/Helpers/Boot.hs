{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

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
    , genesisCageConfig
    , genesisCageConfigWith
    , ensureBootFunding
    , withBootFactsTxBuilder
    , withWalletBootTxBuilder
    , awaitProofReadsReady
    , registerStakeCredIfNeeded
    ) where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Data.ByteString qualified as BS
import Data.Map.Strict qualified as Map
import Data.Text qualified as T

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (Testnet)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )
import Cardano.Ledger.Coin qualified as Ledger
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisSignKey
    )
import Cardano.Tx.Build qualified as Tx

import Cardano.MPFS.API.Types
    ( BootFacts (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTxWithEval)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.Eval (decodeEvalContext)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Facts
    ( unsafeVerifiedBootFacts
    , verifyBootFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint (CageScripts)
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
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
import Cardano.MPFS.Submitter (SubmitResult (..), Submitter (..))
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

-- | Build the standard genesis e2e cage config from blueprint scripts.
genesisCageConfig :: CageScripts -> CageConfig
genesisCageConfig =
    genesisCageConfigWith 5_000 5_000 (Coin 1_000_000)

-- | Build a genesis e2e cage config with caller-specific timing and tip.
genesisCageConfigWith
    :: Integer
    -> Integer
    -> Coin
    -> CageScripts
    -> CageConfig
genesisCageConfigWith processTime retractTime tip (stateBytes, requestBytes, mStakingBytes) =
    let appliedStateBytes =
            Client.applyPreviousPolicies [] stateBytes
    in  CageConfig
            { cageScriptBytes = appliedStateBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = Client.computeScriptHash appliedStateBytes
            , defaultProcessTime = processTime
            , defaultRetractTime = retractTime
            , defaultTip = tip
            , network = Testnet
            , cfgStakeScript =
                fmap
                    (\bs -> (bs, Client.computeScriptHash bs))
                    mStakingBytes
            }

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

-- | Replace the production boot stub with a wallet-side boot
-- that uses explicitly-provided inputs (no indexer needed).
-- Use in test contexts where 'followerEnabled' is 'False'.
withWalletBootTxBuilder
    :: CageConfig
    -> Context IO
    -> Context IO
withWalletBootTxBuilder cfg ctx =
    ctx
        { txBuilder =
            (txBuilder ctx)
                { bootToken = buildBootEnvelopeFromWalletInputs cfg ctx
                }
        }

buildBootEnvelopeFromWalletInputs
    :: CageConfig
    -> Context IO
    -> BundleSnapshot
    -> [ResolvedWalletInput]
    -> Addr
    -> IO (ProofEnvelope BootProof)
buildBootEnvelopeFromWalletInputs cfg ctx snap inputs _addr = do
    pp <- queryProtocolParams (provider ctx)
    let facts = mkBootFacts snap inputs pp
        -- Skip proof verification: CageSpec runs without the CSMT
        -- indexer (followerEnabled = False) so inclusion proofs are
        -- empty. The cage TX builder only uses ueRef + ueTxOutCbor.
        verified = unsafeVerifiedBootFacts facts
    evalCtxWire <- evalContext ctx
    evalCtx <-
        case decodeEvalContext evalCtxWire of
            Left err ->
                fail
                    $ "E2E boot: decodeEvalContext failed: "
                        <> show err
            Right v -> pure v
    tx <-
        case bootCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                fail
                    $ "E2E boot: bootCageTx failed: "
                        <> show err
            Right v -> pure v
    pure
        ProofEnvelope
            { envTx = tx
            , envSnapshot = snap
            , envProof =
                BootProof
                    (map witnessedInputFromResolved inputs)
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
        { wpMaxFee = Ledger.Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Ledger.Coin 10_000
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

-- | Ensure the genesis wallet has distinct spend and collateral UTxOs.
--
-- The first call splits the single genesis UTxO into a 50 ADA output plus
-- change. Later calls are no-ops; a boot leaves its untouched collateral
-- and change output available for the next boot.
ensureBootFunding :: Context IO -> IO ()
ensureBootFunding ctx = do
    utxos <- queryUTxOs (provider ctx) genesisAddr
    case utxos of
        [] ->
            fail "ensureBootFunding: no genesis UTxOs available"
        _funding : _collateral : _rest ->
            pure ()
        [funding] -> do
            pp <- queryProtocolParams (provider ctx)
            let prog :: Tx.TxBuild NoCtx String ()
                prog =
                    void
                        $ Tx.payTo
                            genesisAddr
                            (inject (Ledger.Coin 50_000_000))
            result <-
                Tx.build
                    (Tx.mkPParamsBound pp)
                    (Tx.InterpretIO noCtx)
                    (\_ -> pure Map.empty)
                    [funding]
                    []
                    genesisAddr
                    prog
            case result of
                Left err ->
                    fail
                        $ "ensureBootFunding: build failed: "
                            <> show err
                Right unsigned -> do
                    let signed = addKeyWitness genesisSignKey unsigned
                    res <- submitTx (submitter ctx) signed
                    case res of
                        Submitted _ ->
                            awaitNodeConfirmed (fst funding)
                        Rejected msg
                            | "All inputs are spent" `BS.isInfixOf` msg ->
                                awaitNodeConfirmed (fst funding)
                            | otherwise ->
                                fail
                                    $ "ensureBootFunding: submit rejected: "
                                        <> show msg
  where
    awaitNodeConfirmed spentRef = go (240 :: Int)
      where
        go 0 =
            fail
                "ensureBootFunding: split tx not confirmed on-chain \
                \after 120s"
        go n = do
            utxos <- queryUTxOs (provider ctx) genesisAddr
            when (any ((== spentRef) . fst) utxos)
                $ threadDelay 500_000
                    >> go (n - 1)

-- | Register the cage staking credential on the devnet if
-- 'cfgStakeScript' is set. Must be called once before any
-- Update or End transaction; those do a zero-ADA withdrawal
-- from the staking script, which requires the credential to
-- be registered in the ledger rewards map first.
-- No-op when 'cfgStakeScript' is 'Nothing'.
registerStakeCredIfNeeded :: CageConfig -> Context IO -> IO ()
registerStakeCredIfNeeded cfg ctx =
    case cfgStakeScript cfg of
        Nothing -> pure ()
        Just (_, stakeHash) -> do
            pp <- queryProtocolParams (provider ctx)
            utxos <- queryUTxOs (provider ctx) genesisAddr
            feeInput <-
                case utxos of
                    [] ->
                        fail
                            "registerStakeCredIfNeeded: \
                            \no genesis UTxOs available"
                    (u : _) -> pure u
            let prog :: Tx.TxBuild NoCtx String ()
                prog =
                    void
                        $ Tx.certify
                            ( Tx.ConwayTxCertDeleg
                                ( Tx.ConwayRegCert
                                    (Tx.ScriptHashObj stakeHash)
                                    Tx.SNothing
                                )
                            )
                            Tx.PubKeyCert
            result <-
                Tx.build
                    (Tx.mkPParamsBound pp)
                    (Tx.InterpretIO noCtx)
                    (\_ -> pure Map.empty)
                    [feeInput]
                    []
                    genesisAddr
                    prog
            case result of
                Left err ->
                    fail
                        $ "registerStakeCredIfNeeded: \
                          \build failed: "
                            <> show err
                Right unsigned -> do
                    let signed = addKeyWitness genesisSignKey unsigned
                    res <- submitTx (submitter ctx) signed
                    case res of
                        Submitted _ ->
                            awaitNodeConfirmed (fst feeInput)
                        Rejected msg
                            | "All inputs are spent" `BS.isInfixOf` msg ->
                                awaitNodeConfirmed (fst feeInput)
                            | otherwise ->
                                fail
                                    $ "registerStakeCredIfNeeded: \
                                      \submit rejected: "
                                        <> show msg
  where
    awaitNodeConfirmed spentRef = go (240 :: Int)
      where
        go 0 =
            fail
                "registerStakeCredIfNeeded: \
                \cert tx not confirmed on-chain after 120s"
        go n = do
            utxos <- queryUTxOs (provider ctx) genesisAddr
            when (any ((== spentRef) . fst) utxos)
                $ threadDelay 500_000
                    >> go (n - 1)

data NoCtx a

noCtx :: NoCtx a -> IO a
noCtx q = case q of {}
