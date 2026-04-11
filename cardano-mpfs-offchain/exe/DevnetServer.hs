{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Main
-- Description : Devnet server for E2E testing
-- License     : Apache-2.0
--
-- Starts a single-node devnet and serves the MPFS
-- HTTP API against it. Intended for E2E testing of
-- external clients (e.g. cardano-mpfs-browser).
--
-- Usage:
--
-- @
-- mpfs-devnet-server --port 8080
-- @
--
-- Requires @cardano-node@ in PATH and
-- @MPFS_BLUEPRINT@ set to the cage blueprint path.
-- Set @E2E_GENESIS_DIR@ to the devnet genesis
-- directory (defaults to @e2e-test\/genesis@).
module Main (main) where

import Control.Concurrent (threadDelay)
import Data.ByteString.Short qualified as SBS
import Network.Wai.Handler.Warp qualified as Warp
import System.Environment
    ( getArgs
    , lookupEnv
    )
import System.FilePath ((</>))
import System.IO (hFlush, stdout)
import System.IO.Temp
    ( withSystemTempDirectory
    )

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Ledger.BaseTypes (Network (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( applyVersion
    , extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Core.Types (Coin (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( computeScriptHash
    )
import Cardano.Node.Client.E2E.Devnet
    ( withCardanoNode
    )
import Cardano.Node.Client.E2E.Setup
    ( genesisDir
    )

main :: IO ()
main = do
    port <- parsePort
    scriptBytes <- loadScript
    gDir <- genesisDir
    putStrLn
        $ "Starting devnet server on port "
            <> show port
    hFlush stdout
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-devnet-server"
            $ \tmpDir -> do
                let cfg = mkCageCfg scriptBytes
                    appCfg =
                        AppConfig
                            { epochSlots =
                                EpochSlots 4320
                            , shelleyGenesisPath =
                                gDir
                                    </> "shelley-genesis.json"
                            , socketPath = sock
                            , dbPath =
                                tmpDir </> "db"
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , byronGenesisPath =
                                Nothing
                            , followerEnabled = True
                            , appTracer = nullTracer
                            }
                withApplication appCfg $ \ctx -> do
                    _ <-
                        queryProtocolParams
                            (provider ctx)
                    threadDelay 5_000_000
                    let url =
                            "http://localhost:"
                                <> show port
                    putStrLn $ "Serving on " <> url
                    putStrLn
                        $ "Swagger UI: "
                            <> url
                            <> "/swagger-ui"
                    hFlush stdout
                    Warp.run port (mkApp ctx)

-- -------------------------------------------------
-- CLI
-- -------------------------------------------------

-- | Parse @--port N@ from args, default 8080.
parsePort :: IO Int
parsePort = go <$> getArgs
  where
    go ("--port" : n : _) = read n
    go (_ : rest) = go rest
    go [] = 8080

-- | Load cage script from @MPFS_BLUEPRINT@ env.
loadScript :: IO SBS.ShortByteString
loadScript = do
    mPath <- lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            error
                "MPFS_BLUEPRINT not set. \
                \Point it to the cage \
                \blueprint JSON."
        Just path -> do
            ebp <- loadBlueprint path
            case ebp of
                Left err ->
                    error
                        $ "Blueprint error: " <> err
                Right bp ->
                    case extractCompiledCode
                        "cage."
                        bp of
                        Nothing ->
                            error
                                "cage script not \
                                \found in blueprint"
                        Just sb ->
                            pure
                                $ applyVersion 1 sb

-- -------------------------------------------------
-- Config
-- -------------------------------------------------

mkCageCfg
    :: SBS.ShortByteString
    -> CageConfig
mkCageCfg scriptBytes =
    CageConfig
        { cageScriptBytes = scriptBytes
        , cfgScriptHash =
            computeScriptHash scriptBytes
        , defaultProcessTime = 300_000
        , defaultRetractTime = 300_000
        , defaultTip = Coin 2_000_000
        , network = Testnet
        }
