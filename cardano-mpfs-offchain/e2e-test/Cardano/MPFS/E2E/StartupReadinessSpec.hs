{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.StartupReadinessSpec
-- Description : E2E proof for the startup readiness contract (#275)
-- License     : Apache-2.0
--
-- Drives a fresh-DB devnet startup through the full
-- restoration → following lifecycle and asserts that
-- @\/ready@ returns 503 until the cage follower has
-- crossed the stability window, then 200 — without
-- ever taking the armageddon reset path.
module Cardano.MPFS.E2E.StartupReadinessSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
    ( async
    , cancel
    , withAsync
    )
import Control.Concurrent.STM
    ( TBQueue
    , TVar
    , atomically
    , flushTBQueue
    , newTBQueueIO
    , newTVarIO
    , readTVar
    , readTVarIO
    , registerDelay
    , retry
    , writeTBQueue
    , writeTVar
    )
import Control.Exception (bracket)
import Control.Monad (forever, replicateM_, when)
import Control.Tracer (Tracer (..))
import Data.Aeson
    ( eitherDecode
    , encode
    )
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.IORef
    ( IORef
    , atomicModifyIORef'
    , newIORef
    , readIORef
    )
import Data.Map.Strict qualified as Map
import Data.Word (Word64)
import Lens.Micro ((&), (.~), (^.))
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    , statusCode
    )
import Network.Wai (Application, Request (..))
import Network.Wai.Test
    ( SRequest (..)
    , SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    , simpleBody
    , simpleStatus
    , srequest
    )
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    )

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Ledger.Address (serialiseAddr)
import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , mkBasicTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( mintTxBodyL
    , mkBasicTxBody
    , vldtTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , StrictMaybe (SJust, SNothing)
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.Slot (SlotNo (..))
import Cardano.Node.Client.Balance
    ( BalanceResult (..)
    , balanceTx
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , devnetMagic
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )
import Cardano.Node.Client.N2C.Connection
    ( newLSQChannel
    , newLTxSChannel
    , runNodeClient
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootRequest (..)
    , EndRequest (..)
    , StatusResponse (..)
    )
import Cardano.MPFS.API.Types.Common (TokenIdJSON (..))
import Cardano.MPFS.API.Types.Facts
    ( BootFacts
    , EndFacts
    )
import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.End (endCageTx)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Facts
    ( verifyBootFacts
    , verifyEndFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types
    ( ConwayEra
    , TokenId (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Provider.NodeClient
    ( mkNodeClientProvider
    )
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.Submitter.N2C (mkN2CSubmitter)
import Cardano.MPFS.Trace
    ( AppTrace (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , computeScriptHash
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( ReplayEvent (..)
    )

-- * Entry point

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Startup readiness" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (no MPFS_BLUEPRINT)"
                $ pure @IO ()
        Just path -> do
            eScripts <- runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right scripts ->
                    startupReadinessSpec scripts

-- * Spec body

startupReadinessSpec :: CageScripts -> Spec
startupReadinessSpec scripts =
    it
        "/ready 503 → 200 across restoration→following, then boot+end via live HTTP"
        $ runReadinessE2E scripts

runReadinessE2E :: CageScripts -> IO ()
runReadinessE2E scripts = do
    gDir <- genesisDir
    withCardanoNode gDir $ \socketPath _startMs ->
        withSystemTempDirectory "mpfs-startup-readiness"
            $ \tmpDir -> do
                let cfg = cageCfg scripts
                    dbPath = tmpDir </> "db"

                -- Pre-inject Conway-era Tx into the
                -- devnet chain BEFORE we boot the
                -- MPFS app. This guarantees that the
                -- restoration-phase blocks the
                -- follower processes contain real
                -- transactions, so the KV journal
                -- accumulates a non-trivial entry
                -- count and 'ReplayStart' fires with
                -- @remaining > 0@ (spec FR-007).
                preInjectTxs socketPath 5

                eventsQ <-
                    newTBQueueIO 4096
                seenReady <- newTVarIO False
                pollLog <- newIORef ([] :: [(Bool, Int)])

                let appCfg =
                        AppConfig
                            { epochSlots = EpochSlots 4320
                            , shelleyGenesisPath =
                                gDir </> "shelley-genesis.json"
                            , socketPath = socketPath
                            , dbPath = dbPath
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , byronGenesisPath = Nothing
                            , followerEnabled = True
                            , appTracer =
                                captureTracer eventsQ seenReady
                            }

                withApplication appCfg $ \ctx -> do
                    let app = mkApp ctx
                    withAsync (txInjector ctx 30) $ \_ ->
                        withAsync (readyPoller app seenReady pollLog) $ \_ -> do
                            -- Wait for the readiness gate to flip,
                            -- bounded by a generous timeout.
                            awaited <-
                                awaitReady seenReady 90_000_000
                            when (not awaited)
                                $ expectationFailure
                                    "TraceReady was not observed within 90s"
                            -- Let the poller record post-Ready
                            -- samples AND give the
                            -- restoration→following transition's
                            -- @toFull@ journal replay time to
                            -- complete and emit
                            -- @TraceReplay ReplayStop@ into the
                            -- queue. 5s is well under the
                            -- @CrashRecoverySpec@ budget.
                            threadDelay 5_000_000

                    -- Drain captured events and poll log.
                    events <-
                        atomically $ flushTBQueue eventsQ
                    polls <-
                        reverse <$> readIORef pollLog

                    debugDumpEvents events

                    -- Contract assertions.
                    assertReadyOrdering polls
                    assertNoArmageddon events
                    assertReplayHasWork events
                    assertClassificationOnce events
                    assertReadyEmittedOnce events

                    -- Wait for the indexer's checkpoint to
                    -- approach the chain tip before driving
                    -- the boot. Otherwise the wallet UTxOs we
                    -- get back from @/facts/boot@ may be
                    -- stale (still pointing at outputs that
                    -- the pre-injection chain has since
                    -- spent), and @/tx/submit@ rejects the
                    -- resulting boot Tx with
                    -- @BadInputsUTxO@.
                    awaitCheckpointNearTip app 60

                    -- Drive the live HTTP API: boot, observe,
                    -- end, observe.
                    drivenBootEnd cfg app ctx

-- * Tracer plumbing

-- | A 'Tracer' that mirrors every 'AppTrace' into the
-- spec's TBQueue. When the runtime emits 'TraceReady',
-- the @seenReady@ flag is atomically set so the poller
-- thread can record (was-ready, http-status) pairs in
-- a well-ordered way.
captureTracer
    :: TBQueue AppTrace
    -> TVar Bool
    -> Tracer IO AppTrace
captureTracer eventsQ seenReady = Tracer $ \event -> do
    atomically $ do
        writeTBQueue eventsQ event
        case event of
            TraceReady -> writeTVar seenReady True
            _ -> pure ()

-- | Block until @seenReady@ becomes 'True' or the
-- supplied microsecond budget expires.
awaitReady :: TVar Bool -> Int -> IO Bool
awaitReady seenReady micros = do
    timer <- registerDelay micros
    atomically $ do
        ready <- readTVar seenReady
        if ready
            then pure True
            else do
                expired <- readTVar timer
                if expired then pure False else retry

-- * /ready polling

-- | Background thread: every 100ms record
-- @(seenReady, /ready status)@ until the test tears
-- it down via @withAsync@.
readyPoller
    :: Application
    -> TVar Bool
    -> IORef [(Bool, Int)]
    -> IO ()
readyPoller app seenReady pollLog = forever $ do
    wasReady <- readTVarIO seenReady
    resp <- httpGet app "/ready"
    let code = statusCode (simpleStatus resp)
    atomicModifyIORef' pollLog $ \xs ->
        ((wasReady, code) : xs, ())
    threadDelay 100_000

-- | Assert the /ready ordering contract:
--
--   * every poll observed BEFORE 'TraceReady' returned 503
--   * every poll observed AFTER 'TraceReady' returned 200
--
-- A poll where we observed @TraceReady@ already but the
-- response was still 503 is also a violation (the TVar
-- flip and the trace emission happen atomically in the
-- wrapping tracer, so by the time @seenReady@ is True
-- the HTTP handler MUST see Ready).
assertReadyOrdering :: [(Bool, Int)] -> IO ()
assertReadyOrdering polls = do
    when (null polls)
        $ expectationFailure
            "readyPoller produced no samples; \
            \the test exited too quickly"
    let bad =
            [ (wasReady, code)
            | (wasReady, code) <- polls
            , (wasReady, code) /= (False, 503)
            , (wasReady, code) /= (True, 200)
            ]
    bad `shouldBe` []
    -- And both sides of the gate must have been
    -- exercised — the test is degenerate otherwise.
    any (\(_, c) -> c == 503) polls
        `shouldBe` True
    any (\(_, c) -> c == 200) polls
        `shouldBe` True

-- * Event-level assertions

-- | Assert no 'TraceArmageddon' fires during phases
-- 1–3 (spec FR-012). The legitimate "initial Origin
-- entry for an empty rollbacks store" call site at
-- @Application.hs:386@ also emits 'TraceArmageddon',
-- but that runs BEFORE 'TraceStartupClassification'
-- — so the contract violation we are guarding
-- against is an armageddon emission AFTER the
-- classification fires.
assertNoArmageddon :: [AppTrace] -> IO ()
assertNoArmageddon events =
    case dropUntilClassification events of
        Nothing ->
            -- The classification assertion below
            -- will catch this case with a more
            -- precise message; nothing for us to
            -- check yet.
            pure ()
        Just rest ->
            case [() | TraceArmageddon{} <- rest] of
                [] -> pure ()
                _ ->
                    expectationFailure
                        "TraceArmageddon fired \
                        \after startup \
                        \classification — the fix \
                        \must not use the \
                        \armageddon reset path \
                        \during phases 1–3"
  where
    dropUntilClassification (TraceStartupClassification _ _ : xs) =
        Just xs
    dropUntilClassification (_ : xs) =
        dropUntilClassification xs
    dropUntilClassification [] = Nothing

-- | At least one 'TraceReplay (ReplayStart …)' with
-- @rsChunkSize > 0@ must be present, otherwise the
-- devnet driver did not inject enough Conway-era Tx to
-- populate the KV journal and the test silently degrades
-- into a no-op-recovery exercise (spec FR-007 / FR-008).
--
-- Note: the upstream @rsEntriesRemaining@ field counts
-- entries left AFTER the current chunk's replay, so for
-- a single-chunk replay it will be zero even when work
-- happened. The chunk-size field is the one that
-- distinguishes "no work" from "any work" (#275).
assertReplayHasWork :: [AppTrace] -> IO ()
assertReplayHasWork events = do
    let chunkSizes =
            [ chunkSize
            | TraceReplay (ReplayStart chunkSize _ _ _ _) <-
                events
            ]
    when (null chunkSizes || all (== 0) chunkSizes)
        $ expectationFailure
            "phase 1 produced no journal entries — \
            \devnet driver did not inject enough txs"

-- | Exactly one 'TraceStartupClassification' with
-- @fresh_db = true@ — this run uses 'withSystemTempDirectory'
-- so by construction the DB starts empty.
assertClassificationOnce :: [AppTrace] -> IO ()
assertClassificationOnce events =
    case [ ev
         | ev@(TraceStartupClassification _ _) <- events
         ] of
        [TraceStartupClassification True _] -> pure ()
        [TraceStartupClassification False n] ->
            expectationFailure
                $ "expected fresh-DB classification, \
                  \got persistent-DB with "
                    <> show n
                    <> " rollbacks"
        [] ->
            expectationFailure
                "TraceStartupClassification was not \
                \emitted at all"
        many ->
            expectationFailure
                $ "TraceStartupClassification was \
                  \emitted "
                    <> show (length many)
                    <> " times (expected exactly 1)"

assertReadyEmittedOnce :: [AppTrace] -> IO ()
assertReadyEmittedOnce events =
    case [() | TraceReady <- events] of
        [_] -> pure ()
        [] ->
            expectationFailure
                "TraceReady was not emitted"
        many ->
            expectationFailure
                $ "TraceReady was emitted "
                    <> show (length many)
                    <> " times (expected exactly 1)"

-- * Debug helpers

-- | Dump the captured event stream when
-- @MPFS_E2E_DEBUG@ is truthy. Used to diagnose
-- pre-injection / phase-1 sizing on the devnet.
debugDumpEvents :: [AppTrace] -> IO ()
debugDumpEvents events = do
    enabled <-
        maybe False truthy
            <$> lookupEnv "MPFS_E2E_DEBUG"
    when enabled $ do
        putStrLn
            $ "[StartupReadinessSpec] captured "
                <> show (length events)
                <> " AppTrace events:"
        mapM_ (putStrLn . ("  " <>) . show) events
  where
    truthy "" = False
    truthy "0" = False
    truthy "false" = False
    truthy "False" = False
    truthy _ = True

-- * Pre-injection driver

-- | Open an independent N2C connection against the
-- live devnet socket, submit @n@ self-payments from
-- the genesis address back to itself, then tear the
-- connection down. Runs BEFORE the MPFS app starts,
-- so the chain follower's phase-1 blocks contain
-- real Conway-era Tx.
preInjectTxs :: FilePath -> Int -> IO ()
preInjectTxs sock n = do
    lsqCh <- newLSQChannel 16
    ltxsCh <- newLTxSChannel 16
    bracket
        ( async
            $ runNodeClient
                devnetMagic
                sock
                lsqCh
                ltxsCh
        )
        cancel
        $ \_ -> do
            -- Give the N2C handshake a moment to
            -- settle and the chain a moment to
            -- produce a few blocks past genesis.
            threadDelay 2_000_000
            let prov = mkNodeClientProvider lsqCh
                sub = mkN2CSubmitter ltxsCh
            replicateInjections n prov sub
            -- Let the last submitted Tx land in a
            -- block before we hand off to the MPFS
            -- app.
            threadDelay 2_000_000

replicateInjections
    :: Int -> Provider IO -> Submitter IO -> IO ()
replicateInjections 0 _ _ = pure ()
replicateInjections n prov sub = do
    _ <- tryInjectionOnce prov sub
    threadDelay 800_000
    replicateInjections (n - 1) prov sub

tryInjectionOnce
    :: Provider IO
    -> Submitter IO
    -> IO (Either String ())
tryInjectionOnce prov sub = do
    utxos <- queryUTxOs prov genesisAddr
    case utxos of
        [] -> pure (Left "no genesis UTxOs")
        (feeUtxo : _) -> do
            pp <- queryProtocolParams prov
            let vldt =
                    ValidityInterval
                        SNothing
                        (SJust (SlotNo 1_000_000))
                body =
                    mkBasicTxBody
                        & vldtTxBodyL .~ vldt
                tx = mkBasicTx body
            case balanceTx pp [feeUtxo] genesisAddr tx of
                Left err ->
                    pure
                        $ Left
                        $ "balanceTx: " <> show err
                Right BalanceResult{balancedTx = balanced} -> do
                    let signed =
                            addKeyWitness
                                genesisSignKey
                                balanced
                    submitOutcome <-
                        submitTx sub signed
                    case submitOutcome of
                        Submitted _ -> pure (Right ())
                        Rejected reason ->
                            pure
                                $ Left
                                $ "submit: " <> show reason

-- * tx-injection driver (in-flight)

-- | Background thread: submit up to @n@ small
-- self-payments from the devnet genesis address back
-- to itself so that phase-1 blocks contain real
-- Conway-era Tx and the KV journal accumulates.
--
-- Failures are swallowed — each iteration re-queries
-- the live UTxO set, so a transient race is harmless.
txInjector :: Context IO -> Int -> IO ()
txInjector ctx n =
    replicateM_ n $ do
        result <- tryOneInjection ctx
        case result of
            Right () -> threadDelay 1_500_000
            Left _ -> threadDelay 1_500_000

tryOneInjection
    :: Context IO -> IO (Either String ())
tryOneInjection ctx = do
    utxos <-
        queryUTxOs (provider ctx) genesisAddr
    case utxos of
        [] -> pure (Left "no genesis UTxOs")
        (feeUtxo : _) -> do
            pp <-
                queryProtocolParams (provider ctx)
            let vldt =
                    ValidityInterval
                        SNothing
                        (SJust (SlotNo 1_000_000))
                body =
                    mkBasicTxBody
                        & vldtTxBodyL .~ vldt
                tx = mkBasicTx body
            case balanceTx pp [feeUtxo] genesisAddr tx of
                Left err ->
                    pure
                        $ Left
                        $ "balanceTx: " <> show err
                Right BalanceResult{balancedTx = balanced} -> do
                    let signed =
                            addKeyWitness
                                genesisSignKey
                                balanced
                    submitOutcome <-
                        submitTx (submitter ctx) signed
                    case submitOutcome of
                        Submitted _ -> pure (Right ())
                        Rejected reason ->
                            pure
                                $ Left
                                $ "submit: " <> show reason

-- * Boot + end through the live HTTP API

drivenBootEnd
    :: CageConfig
    -> Application
    -> Context IO
    -> IO ()
drivenBootEnd cfg app ctx = do
    -- Boot a token through /facts/boot + bootCageTx.
    trusted <- requireTrustedRoot app
    facts <- postBootFacts app
    verifiedBoot <-
        case verifyBootFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "verifyBootFacts: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    unsignedBoot <-
        case bootCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verifiedBoot of
            Left err ->
                expectationFailure
                    ("bootCageTx: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    let signedBoot =
            addKeyWitness genesisSignKey unsignedBoot
        tokenId = extractTokenId cfg signedBoot
    bootResult <- submitTx (submitter ctx) signedBoot
    assertSubmitted bootResult
    -- Let the cage follower index the boot.
    awaitTokenVisible app tokenId True

    -- End the token through /facts/end + endCageTx.
    endFacts <- postEndFacts app tokenId
    trustedEnd <- requireTrustedRoot app
    verifiedEnd <-
        case verifyEndFacts
            (toClientCageConfig cfg)
            trustedEnd
            endFacts of
            Left err ->
                expectationFailure
                    ("verifyEndFacts: " <> show err)
                    *> error "unreachable"
            Right v -> pure v
    unsignedEnd <-
        case endCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verifiedEnd of
            Left err ->
                expectationFailure
                    ("endCageTx: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    let signedEnd =
            addKeyWitness genesisSignKey unsignedEnd
    endResult <- submitTx (submitter ctx) signedEnd
    assertSubmitted endResult
    -- Let the cage follower index the end.
    awaitTokenVisible app tokenId False

-- | Poll @/status@ until the indexer's
-- @checkpoint_slot@ is within @epsilon@ of
-- @tip_slot@, bounded by @timeoutSec@ seconds.
awaitCheckpointNearTip
    :: Application -> Int -> IO ()
awaitCheckpointNearTip app timeoutSec =
    go (timeoutSec * 4)
  where
    epsilon :: Word64
    epsilon = 2
    go 0 =
        expectationFailure
            "indexer checkpoint did not approach \
            \chain tip within timeout"
    go n = do
        resp <- httpGet app "/status"
        simpleStatus resp `shouldBe` status200
        case eitherDecode (simpleBody resp) of
            Left _ -> retry'
            Right s ->
                case checkpointSlot s of
                    Nothing -> retry'
                    Just cp
                        | tipSlot s == 0 -> retry'
                        | tipSlot s
                            >= cp + epsilon ->
                            retry'
                        | otherwise -> pure ()
      where
        retry' = do
            threadDelay 250_000
            go (n - 1)

-- | Pull a 'TrustedRoot' from @/status@. The /ready
-- gate has already flipped, so @utxo_root@ must be
-- available — fail closed if it is not.
requireTrustedRoot :: Application -> IO TrustedRoot
requireTrustedRoot app = do
    resp <- httpGet app "/status"
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("decode /status: " <> err)
                *> error "unreachable"
        Right StatusResponse{currentUtxoRoot} ->
            case currentUtxoRoot of
                Nothing ->
                    expectationFailure
                        "utxo_root absent after Ready"
                        *> error "unreachable"
                Just root -> pure (TrustedRoot root)

postBootFacts :: Application -> IO BootFacts
postBootFacts app = do
    resp <-
        httpPostJsonBoot
            app
            BootRequest
                { brAddr =
                    Hex (serialiseAddr genesisAddr)
                }
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("decode BootFacts: " <> err)
                *> error "unreachable"
        Right facts -> pure facts

postEndFacts
    :: Application -> TokenId -> IO EndFacts
postEndFacts app tokenId = do
    let TokenId (AssetName sbs) = tokenId
    resp <-
        httpPostJsonEnd
            app
            EndRequest
                { erToken =
                    TokenIdJSON (SBS.fromShort sbs)
                , erAddr =
                    Hex (serialiseAddr genesisAddr)
                }
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("decode EndFacts: " <> err)
                *> error "unreachable"
        Right facts -> pure facts

-- | Poll @GET /tokens/<hex>@ until visibility matches
-- @expected@. Mirrors the @CrashRecoverySpec@ budget.
awaitTokenVisible
    :: Application -> TokenId -> Bool -> IO ()
awaitTokenVisible app tokenId expected = go (60 :: Int)
  where
    go 0 =
        expectationFailure
            $ "token visibility did not reach "
                <> show expected
                <> " within timeout"
    go n = do
        visible <- tokenVisible app tokenId
        if visible == expected
            then pure ()
            else do
                threadDelay 1_000_000
                go (n - 1)

tokenVisible
    :: Application -> TokenId -> IO Bool
tokenVisible app tokenId = do
    resp <-
        httpGet app ("/tokens/" <> tokenIdHex tokenId)
    pure (simpleStatus resp == status200)

-- * HTTP helpers

httpGet :: Application -> ByteString -> IO SResponse
httpGet app path =
    runSession
        (request (setPath defaultRequest path))
        app

httpPostJsonBoot
    :: Application -> BootRequest -> IO SResponse
httpPostJsonBoot app body =
    runSession
        ( srequest
            SRequest
                { simpleRequest =
                    (setPath defaultRequest "/facts/boot")
                        { requestMethod = methodPost
                        , requestHeaders =
                            [
                                ( hContentType
                                , "application/json"
                                )
                            ]
                        }
                , simpleRequestBody = encode body
                }
        )
        app

httpPostJsonEnd
    :: Application -> EndRequest -> IO SResponse
httpPostJsonEnd app body =
    runSession
        ( srequest
            SRequest
                { simpleRequest =
                    (setPath defaultRequest "/facts/end")
                        { requestMethod = methodPost
                        , requestHeaders =
                            [
                                ( hContentType
                                , "application/json"
                                )
                            ]
                        }
                , simpleRequestBody = encode body
                }
        )
        app

-- * Token-id plumbing

extractTokenId :: CageConfig -> Tx ConwayEra -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "extractTokenId: unexpected mint shape"

tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

-- * Misc helpers

assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

permissiveWalletPolicy :: WalletPolicy
permissiveWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices =
            Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

toClientCageConfig :: CageConfig -> Client.CageConfig
toClientCageConfig cfg =
    Client.CageConfig
        { Client.cageScriptBytes = cageScriptBytes cfg
        , Client.requestScriptBytes = requestScriptBytes cfg
        , Client.cfgScriptHash = cfgScriptHash cfg
        , Client.defaultProcessTime =
            defaultProcessTime cfg
        , Client.defaultRetractTime =
            defaultRetractTime cfg
        , Client.defaultTip = defaultTip cfg
        , Client.network = network cfg
        }

cageCfg :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash = computeScriptHash stateBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
