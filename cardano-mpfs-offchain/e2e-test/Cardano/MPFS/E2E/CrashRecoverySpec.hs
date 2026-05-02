{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.CrashRecoverySpec
-- Description : Crash recovery E2E tests
-- License     : Apache-2.0
--
-- Submit cage transactions to a devnet, run the MPFS
-- follower, kill during various phases, restart, and
-- verify both UTxO merkle root and cage state survive.
module Cardano.MPFS.E2E.CrashRecoverySpec
    ( spec
    ) where

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Coin (Coin (..))
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
    ( BlockId (..)
    , SlotNo (..)
    , TokenId
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    )
import Cardano.MPFS.State
    ( State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.Trace (AppTrace (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , TxBuilder (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( computeScriptHash
    )
import Cardano.Node.Client.E2E.CrashRecovery
    ( KillResult (..)
    , killDuring
    )
import Cardano.Node.Client.E2E.Devnet
    ( withCardanoNode
    )
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( ReplayEvent (..)
    )
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
    ( atomically
    , newEmptyTMVarIO
    , readTMVar
    , tryPutTMVar
    )
import Control.Monad (void, when)
import Control.Tracer
    ( Tracer (..)
    , traceWith
    )
import Data.ByteString qualified as BS
import Data.Foldable (for_)
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp
    ( withSystemTempDirectory
    )
import System.Timeout (timeout)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    , shouldSatisfy
    )

-- * Phases

-- | Phases derived from 'AppTrace'.
-- MPFS skips restoration (always starts in
-- InFollowing), so only Replaying and Following
-- are observable.
data Phase
    = Replaying
    | Following
    deriving (Show, Eq)

-- | Map 'AppTrace' to a phase.
traceToPhase :: AppTrace -> Maybe Phase
traceToPhase = \case
    TraceReplay ReplayStart{} -> Just Replaying
    TraceBlockReceived{} -> Just Following
    _ -> Nothing

-- * Spec

spec :: Spec
spec = describe "Crash recovery" $ do
    mPath <-
        runIO $ lookupEnv "MPFS_BLUEPRINT"
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
                    crashRecoverySpecs scripts

crashRecoverySpecs
    :: CageScripts -> Spec
crashRecoverySpecs scripts = do
    it "kill during following — cage state survives"
        $ killAndVerify scripts Following

-- * Test logic

-- | Boot a token via the follower, kill the app
-- during a phase, restart, verify the token state
-- and merkle root are consistent.
killAndVerify
    :: CageScripts -> Phase -> IO ()
killAndVerify scripts targetPhase = do
    gDir <- genesisDir
    withCardanoNode gDir $ \socketPath _startMs -> do
        let cfg = cageCfg scripts

        -- Step 1: run the app, boot a token, let
        -- the follower index it
        (tokenId, expectedRoot) <-
            withSystemTempDirectory "mpfs-setup"
                $ \dbPath -> do
                    withMpfsSynced
                        socketPath
                        cfg
                        dbPath
                        $ \ctx -> do
                            -- Boot a token
                            tid <-
                                bootAndAwait ctx
                            -- Record expected state
                            r <- utxoRoot ctx
                            pure (tid, r)
        debugLog
            $ "Booted token: "
                ++ show tokenId
        debugLog
            $ "Expected root: "
                ++ show expectedRoot

        -- Step 2: fresh DB, populate + kill
        withSystemTempDirectory "mpfs-kill"
            $ \dbPath -> do
                -- First run: populate then kill
                debugLog
                    $ "Kill during "
                        ++ show targetPhase
                        ++ "..."
                KillResult{phasesSeen} <-
                    killMpfsDuring
                        targetPhase
                        socketPath
                        cfg
                        dbPath
                debugLog
                    $ "Phases seen: "
                        ++ show phasesSeen
                last phasesSeen
                    `shouldBe` targetPhase

                -- Step 3: restart, verify
                debugLog "Restarting..."
                withMpfsSynced
                    socketPath
                    cfg
                    dbPath
                    $ \ctx -> do
                        -- Verify merkle root
                        root <- utxoRoot ctx
                        debugLog
                            $ "Root after restart: "
                                ++ show root
                        root
                            `shouldBe` expectedRoot

                        -- Verify cage state
                        mToken <-
                            getToken
                                ( tokens
                                    (state ctx)
                                )
                                tokenId
                        debugLog
                            $ "Token after restart: "
                                ++ show
                                    ( fmap
                                        ( const
                                            ("present" :: String)
                                        )
                                        mToken
                                    )
                        mToken
                            `shouldSatisfy` (/= Nothing)

debugLog :: String -> IO ()
debugLog msg = do
    enabled <-
        maybe False truthy
            <$> lookupEnv "MPFS_E2E_DEBUG"
    when enabled (putStrLn msg)

truthy :: String -> Bool
truthy value =
    value `notElem` ["", "0", "false", "False", "no", "No"]

-- * App helpers

-- | Run the MPFS app until blocks are processed.
withMpfsSynced
    :: FilePath
    -> CageConfig
    -> FilePath
    -> (Context IO -> IO a)
    -> IO a
withMpfsSynced socketPath cfg dbPath callback = do
    syncedVar <- newEmptyTMVarIO
    let tracer = Tracer $ \case
            TraceBlockReceived{} ->
                atomically
                    $ void
                    $ tryPutTMVar syncedVar ()
            _ -> pure ()
    appCfg <-
        mkAppConfig
            socketPath
            cfg
            tracer
            dbPath
    withApplication appCfg $ \ctx -> do
        _ <-
            timeout 60_000_000
                $ atomically
                $ readTMVar syncedVar
        -- Let a few more blocks process
        threadDelay 5_000_000
        callback ctx

-- | Kill the MPFS app during a target phase.
killMpfsDuring
    :: Phase
    -> FilePath
    -> CageConfig
    -> FilePath
    -> IO (KillResult Phase)
killMpfsDuring
    targetPhase
    socketPath
    cfg
    dbPath =
        killDuring targetPhase
            $ \phaseTracer callback -> do
                let tracer = Tracer $ \event ->
                        for_
                            (traceToPhase event)
                            (traceWith phaseTracer)
                appCfg <-
                    mkAppConfig
                        socketPath
                        cfg
                        tracer
                        dbPath
                withApplication appCfg $ \_ ->
                    callback (pure ())

-- | Build an 'AppConfig'.
mkAppConfig
    :: FilePath
    -> CageConfig
    -> Tracer IO AppTrace
    -> FilePath
    -> IO AppConfig
mkAppConfig socketPath cfg tracer dbPath = do
    gDir <- genesisDir
    pure
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
            , atomicCageReaderOverride = Nothing
            , appTracer = tracer
            }

-- | Boot a token and wait for the follower to
-- index it.
bootAndAwait :: Context IO -> IO TokenId
bootAndAwait ctx = do
    inputs <-
        walletBootInputs (provider ctx) genesisAddr
    bundle <-
        bootToken
            (txBuilder ctx)
            emptySnap
            inputs
            genesisAddr
    let unsignedBoot = envTx bundle
        signed =
            addKeyWitness genesisSignKey unsignedBoot
    result <- submitTx (submitter ctx) signed
    case result of
        Submitted _txId -> do
            -- Wait for follower to process
            threadDelay 10_000_000
            tids <-
                listTokens (tokens (state ctx))
            case tids of
                [tid] -> pure tid
                _ ->
                    error
                        $ "expected 1 token, got: "
                            ++ show (length tids)
        Rejected reason ->
            error
                $ "boot rejected: "
                    ++ show reason

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

-- * Config

cageCfg
    :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 15_000
        , defaultRetractTime = 15_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
