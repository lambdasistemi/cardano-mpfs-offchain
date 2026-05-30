{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.CageSpec
-- Description : E2E tests for the full cage protocol
-- License     : Apache-2.0
module Cardano.MPFS.E2E.CageSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Lens.Micro ((^.))
import System.Directory
    ( createDirectoryIfMissing
    , getTemporaryDirectory
    , removePathForcibly
    )
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.FilePath ((</>))
import System.Process (readProcessWithExitCode)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldSatisfy
    )

import Cardano.Ledger.Api.Tx
    ( bodyTxL
    , txIdTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( inputsTxBodyL
    , mintTxBodyL
    , referenceInputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , TxIx (..)
    )
import Cardano.Ledger.Binary (serialize)
import Cardano.Ledger.Core (eraProtVerLow)
import Cardano.Ledger.Mary.Value
    ( MultiAsset (..)
    )
import Cardano.Ledger.TxIn (TxIn (..))
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , Coin (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    , withBootFactsTxBuilder
    )
import Cardano.MPFS.Provider
    ( Provider (..)
    , SlotNo (..)
    )
import Cardano.MPFS.State
    ( Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.Trie (TrieManager (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , TxBuilder (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , cagePolicyIdFromCfg
    , computeScriptHash
    , requestAddrFromCfg
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    , keyHashFromSignKey
    )

-- | Full cage protocol E2E test spec.
-- Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Cage E2E" $ do
    mPath <-
        runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it
                "skipped (MPFS_BLUEPRINT \
                \not set)"
                (pure () :: IO ())
        Just path -> do
            eScripts <-
                runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it
                        ( "blueprint error: "
                            <> err
                        )
                        (expectationFailure err)
                Right scripts ->
                    cageFlowSpec path scripts

-- ---------------------------------------------------------
-- Test implementation
-- ---------------------------------------------------------

-- | Full cage flow: boot, request, update,
-- and retract.
cageFlowSpec
    :: FilePath -> CageScripts -> Spec
cageFlowSpec bpPath scripts =
    it "boot, request, update, retract"
        $ withE2E scripts
        $ \_sock startMs cfg ctx -> do
            let scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Step 1: Boot token
            bootInputs <-
                walletBootInputs
                    (provider ctx)
                    genesisAddr
            bundleBoot <-
                bootToken
                    (txBuilder ctx)
                    emptySnap
                    bootInputs
                    genesisAddr
            let unsignedBoot = envTx bundleBoot
                signedBoot =
                    addKeyWitness
                        genesisSignKey
                        unsignedBoot

            bootResult <-
                submitTx
                    (submitter ctx)
                    signedBoot
            assertSubmitted bootResult
            awaitTx

            -- Extract TokenId from mint field
            let tokenId =
                    extractTokenId cfg signedBoot
                requestAddr =
                    requestAddrFromCfg
                        cfg
                        tokenId
                        (network cfg)

            -- Register in mock state + trie
            createTrie
                (trieManager ctx)
                tokenId
            let ts =
                    TokenState
                        { owner =
                            keyHashFromSignKey
                                genesisSignKey
                        , root =
                            Root
                                ( BS.replicate
                                    32
                                    0
                                )
                        , tip =
                            Coin 1_000_000
                        , processTime =
                            30_000
                        , retractTime =
                            30_000
                        }
            let bootStateTxIn =
                    TxIn
                        (txIdTx signedBoot)
                        (TxIx 1)
            putToken
                (tokens (state ctx))
                tokenId
                (LocatedTokenState bootStateTxIn ts)

            -- Assert: cage address has UTxO
            cageUtxos <-
                queryUTxOs
                    (provider ctx)
                    scriptAddr
            cageUtxos
                `shouldSatisfy` (not . null)

            -- Step 2: Request insert
            bundleReq <-
                requestInsert
                    (txBuilder ctx)
                    emptySnap
                    tokenId
                    "hello"
                    "world"
                    genesisAddr
            let unsignedReq = envTx bundleReq
                signedReq =
                    addKeyWitness
                        genesisSignKey
                        unsignedReq
            reqResult <-
                submitTx
                    (submitter ctx)
                    signedReq
            assertSubmitted reqResult
            awaitTx

            -- Assert: request address has the
            -- pending request UTxO.
            requestUtxos <-
                queryUTxOs
                    (provider ctx)
                    requestAddr
            requestUtxos
                `shouldSatisfy` (not . null)

            -- Step 3: Update token
            bundleUpdate <-
                updateToken
                    (txBuilder ctx)
                    emptySnap
                    tokenId
                    genesisAddr
            let unsignedUpdate = envTx bundleUpdate
                signedUpdate =
                    addKeyWitness
                        genesisSignKey
                        unsignedUpdate
            maybeDumpTxForAiken
                (provider ctx)
                cfg
                [requestAddr]
                startMs
                bpPath
                "update"
                signedUpdate
            updateResult <-
                submitTx
                    (submitter ctx)
                    signedUpdate
            assertSubmitted updateResult
            awaitTx

            -- Assert: still has cage UTxOs but
            -- request was consumed
            cageUtxos3 <-
                queryUTxOs
                    (provider ctx)
                    scriptAddr
            cageUtxos3
                `shouldSatisfy` (not . null)
            requestUtxosAfterUpdate <-
                queryUTxOs
                    (provider ctx)
                    requestAddr
            length requestUtxosAfterUpdate
                `shouldSatisfy` (< length requestUtxos)

            -- Step 4: Request + retract
            -- Submit a second request
            bundleReq2 <-
                requestInsert
                    (txBuilder ctx)
                    emptySnap
                    tokenId
                    "bye"
                    "moon"
                    genesisAddr
            let unsignedReq2 = envTx bundleReq2
                signedReq2 =
                    addKeyWitness
                        genesisSignKey
                        unsignedReq2
            req2Result <-
                submitTx
                    (submitter ctx)
                    signedReq2
            assertSubmitted req2Result
            awaitTx

            -- Extract request TxIn (cage output
            -- is at index 0 in balanced tx)
            let req2TxIn =
                    TxIn
                        (txIdTx signedReq2)
                        (TxIx 0)
            -- Register in mock state
            let req2 =
                    Request
                        { requestToken = tokenId
                        , requestOwner =
                            keyHashFromSignKey
                                genesisSignKey
                        , requestKey = "bye"
                        , requestValue =
                            Insert "moon"
                        , requestFee =
                            Coin 1_000_000
                        , requestSubmittedAt = 0
                        }
            putRequest
                (requests (state ctx))
                (LocatedRequest req2TxIn req2)

            requestUtxos2 <-
                queryUTxOs
                    (provider ctx)
                    requestAddr
            -- Has request + state UTxOs
            length requestUtxos2
                `shouldSatisfy` (> length requestUtxosAfterUpdate)

            -- Wait for Phase 2 (process_time =
            -- 30s after request submitted_at)
            threadDelay 32_000_000

            -- Retract the second request
            bundleRetract <-
                retractRequest
                    (txBuilder ctx)
                    emptySnap
                    req2TxIn
                    genesisAddr
            let unsignedRetract = envTx bundleRetract
                signedRetract =
                    addKeyWitness
                        genesisSignKey
                        unsignedRetract
            retractResult <-
                submitTx
                    (submitter ctx)
                    signedRetract
            assertSubmitted retractResult
            awaitTx

            -- Assert: request UTxO gone
            requestUtxos3 <-
                queryUTxOs
                    (provider ctx)
                    requestAddr
            length requestUtxos3
                `shouldSatisfy` (< length requestUtxos2)

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

-- | Start a devnet node, wire a full 'Context IO',
-- wait for N2C to connect, then run the action.
-- Uses the exact system start time from the
-- genesis to avoid slot/POSIX conversion drift.
withE2E
    :: CageScripts
    -> ( FilePath
         -> Integer
         -> CageConfig
         -> Context IO
         -> IO a
       )
    -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    sysTmp <- getTemporaryDirectory
    let rocksDir =
            sysTmp </> "cardano-mpfs-e2e-rocks"
    removePathForcibly rocksDir
    createDirectoryIfMissing True rocksDir
    withCardanoNode gDir $ \sock startMs -> do
        let dbDir =
                rocksDir </> "db"
            genesisJson =
                gDir </> "shelley-genesis.json"
        let cfg = cageCfg scripts
            appCfg =
                AppConfig
                    { epochSlots =
                        EpochSlots 4320
                    , shelleyGenesisPath =
                        genesisJson
                    , socketPath = sock
                    , dbPath = dbDir
                    , channelCapacity = 16
                    , cageConfig = cfg
                    , byronGenesisPath =
                        Nothing
                    , followerEnabled =
                        False
                    , appTracer =
                        nullTracer
                    }
        withApplication appCfg $ \ctx -> do
            let ctx' = withBootFactsTxBuilder cfg ctx
            _ <-
                queryProtocolParams
                    (provider ctx')
            action sock startMs cfg ctx'

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | Placeholder snapshot used by e2e tests. The
-- builder embeds it verbatim but does not yet use
-- it to drive tx construction.
emptySnap :: BundleSnapshot
emptySnap =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

-- | Assert that a submit result is 'Submitted'.
assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

-- | Extract the 'TokenId' from a boot
-- transaction's mint field.
extractTokenId
    :: CageConfig -> ConwayTx -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        assets =
            Map.toList
                ( ma
                    Map.! cagePolicyIdFromCfg cfg
                )
    in  case assets of
            [(an, _)] -> TokenId an
            _ -> error "extractTokenId: unexpected assets"

-- | Wait for a transaction to be confirmed
-- (~50 devnet blocks at 0.1s slots).
awaitTx :: IO ()
awaitTx = threadDelay 5_000_000

-- | Optionally dump and simulate a signed transaction with Aiken.
--
-- The helper is intentionally quiet by default. Set
-- @MPFS_E2E_AIKEN_SIMULATE=1@ when debugging validator failures; the
-- signed transaction, resolved inputs, resolved outputs, and simulator
-- streams are written under @/tmp/aiken-<label>-*@.
maybeDumpTxForAiken
    :: Provider IO
    -> CageConfig
    -> [Addr]
    -> Integer
    -> FilePath
    -> String
    -> ConwayTx
    -> IO ()
maybeDumpTxForAiken prov cfg extraScriptAddrs startMs bpPath label tx = do
    enabled <-
        maybe False truthy
            <$> lookupEnv "MPFS_E2E_AIKEN_SIMULATE"
    when enabled dumpAndSimulate
  where
    dumpAndSimulate :: IO ()
    dumpAndSimulate = do
        let ver = eraProtVerLow @ConwayEra
            spentIns =
                Set.toAscList
                    ( tx
                        ^. bodyTxL
                            . inputsTxBodyL
                    )
            refIns =
                Set.toAscList
                    ( tx
                        ^. bodyTxL
                            . referenceInputsTxBodyL
                    )
            allIns = spentIns <> refIns
            scriptAddr =
                cageAddrFromCfg cfg (network cfg)
        walletUtxos <-
            queryUTxOs prov genesisAddr
        scriptUtxos <-
            concat
                <$> traverse
                    (queryUTxOs prov)
                    (scriptAddr : extraScriptAddrs)
        let utxoMap =
                Map.fromList
                    (walletUtxos <> scriptUtxos)
            resolve tin =
                case Map.lookup tin utxoMap of
                    Just out -> (tin, out)
                    Nothing ->
                        error
                            $ "maybeDumpTxForAiken: unresolved "
                                <> show tin
            resolved = map resolve allIns
            txIns :: [TxIn]
            txIns = map fst resolved
            txOuts :: [TxOut ConwayEra]
            txOuts = map snd resolved
            prefix =
                "/tmp/aiken-" <> label
            stdoutPath =
                prefix <> "-stdout.txt"
            stderrPath =
                prefix <> "-stderr.txt"
            encodeCborHex value =
                B16.encode
                    $ BSL.toStrict
                    $ serialize ver value
        BS.writeFile
            (prefix <> "-tx.hex")
            (encodeCborHex tx)
        BS.writeFile
            (prefix <> "-inputs.hex")
            (encodeCborHex txIns)
        BS.writeFile
            (prefix <> "-outputs.hex")
            (encodeCborHex txOuts)
        result <-
            try
                $ readProcessWithExitCode
                    "aiken"
                    [ "tx"
                    , "simulate"
                    , prefix <> "-tx.hex"
                    , prefix <> "-inputs.hex"
                    , prefix <> "-outputs.hex"
                    , "--slot-length"
                    , "100"
                    , "--zero-time"
                    , show startMs
                    , "--zero-slot"
                    , "0"
                    , "--blueprint"
                    , bpPath
                    ]
                    ""
        case result of
            Left (e :: SomeException) ->
                expectationFailure
                    $ "aiken tx simulate failed to run: "
                        <> show e
            Right (exitCode, stdout', stderr') -> do
                writeFile stdoutPath stdout'
                writeFile stderrPath stderr'
                case exitCode of
                    ExitSuccess -> pure ()
                    ExitFailure{} ->
                        expectationFailure
                            $ "aiken tx simulate failed: "
                                <> show exitCode
                                <> "; see "
                                <> stdoutPath
                                <> " and "
                                <> stderrPath

truthy :: String -> Bool
truthy value =
    value `notElem` ["", "0", "false", "False", "no", "No"]

-- ---------------------------------------------------------
-- Config
-- ---------------------------------------------------------

-- | Build a 'CageConfig' from state and request script bytes.
cageCfg
    :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 30_000
        , defaultRetractTime = 30_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
