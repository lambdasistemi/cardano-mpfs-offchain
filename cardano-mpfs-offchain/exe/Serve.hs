{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Main
-- Description : MPFS HTTP service
-- License     : Apache-2.0
--
-- Production service: connect to a running
-- cardano-node via N2C, index cage events, and
-- serve the HTTP API on a configurable port.
--
-- @
-- mpfs-serve
--   --socket \/path\/to\/node.socket
--   --db \/path\/to\/db
--   --port 3000
--   --shelley-genesis \/path\/to\/shelley-genesis.json
--   --blueprint \/path\/to\/cage.json
-- @
--
-- Optional: @--byron-genesis@, @--epoch-slots@
module Main (main) where

import Network.Wai.Handler.Warp qualified as Warp
import System.Directory
    ( createDirectoryIfMissing
    )
import System.Environment (getArgs)
import System.IO (hFlush, stdout)

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Ledger.BaseTypes (Network (..))

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types (Coin (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Trace (jsonLinesTracer)
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( computeScriptHash
    )

-- -------------------------------------------------
-- CLI
-- -------------------------------------------------

-- | Parsed command-line arguments.
data Args = Args
    { argSocket :: FilePath
    , argDb :: FilePath
    , argPort :: Int
    , argShelleyGenesis :: FilePath
    , argByronGenesis :: Maybe FilePath
    , argBlueprint :: FilePath
    , argEpochSlots :: Word
    , argNetwork :: Network
    }

-- | Default arguments.
defaultArgs :: Args
defaultArgs =
    Args
        { argSocket = ""
        , argDb = ""
        , argPort = 3000
        , argShelleyGenesis = ""
        , argByronGenesis = Nothing
        , argBlueprint = ""
        , argEpochSlots = 21_600
        , argNetwork = Testnet
        }

-- | Parse command-line arguments.
parseArgs :: [String] -> Args
parseArgs = go defaultArgs
  where
    go a [] = a
    go a ("--socket" : v : rest) =
        go a{argSocket = v} rest
    go a ("--db" : v : rest) =
        go a{argDb = v} rest
    go a ("--port" : v : rest) =
        go a{argPort = read v} rest
    go a ("--shelley-genesis" : v : rest) =
        go a{argShelleyGenesis = v} rest
    go a ("--byron-genesis" : v : rest) =
        go
            a{argByronGenesis = Just v}
            rest
    go a ("--blueprint" : v : rest) =
        go a{argBlueprint = v} rest
    go a ("--epoch-slots" : v : rest) =
        go a{argEpochSlots = read v} rest
    go a ("--mainnet" : rest) =
        go a{argNetwork = Mainnet} rest
    go _ (unknown : _) =
        error
            $ "Unknown argument: " <> unknown

-- -------------------------------------------------
-- Main
-- -------------------------------------------------

main :: IO ()
main = do
    args <- parseArgs <$> getArgs
    validate args
    cageScripts <- loadScripts (argBlueprint args)
    createDirectoryIfMissing True (argDb args)
    let cfg = mkCageCfg args cageScripts
        appCfg = mkAppConfig args cfg
    putStrLn
        $ "mpfs-serve starting on port "
            <> show (argPort args)
    putStrLn
        $ "  socket: " <> argSocket args
    putStrLn
        $ "  db: " <> argDb args
    hFlush stdout
    withApplication appCfg $ \ctx -> do
        _ <-
            queryProtocolParams
                (provider ctx)
        let url =
                "http://localhost:"
                    <> show (argPort args)
        putStrLn $ "Serving on " <> url
        putStrLn
            $ "Swagger UI: "
                <> url
                <> "/swagger-ui"
        hFlush stdout
        Warp.run (argPort args) (mkApp ctx)

-- -------------------------------------------------
-- Validation
-- -------------------------------------------------

-- | Check required arguments are present.
validate :: Args -> IO ()
validate args = do
    when (null (argSocket args))
        $ error "--socket is required"
    when (null (argDb args))
        $ error "--db is required"
    when (null (argShelleyGenesis args))
        $ error "--shelley-genesis is required"
    when (null (argBlueprint args))
        $ error "--blueprint is required"

-- -------------------------------------------------
-- Config
-- -------------------------------------------------

-- | Load state and request scripts from blueprint.
loadScripts :: FilePath -> IO CageScripts
loadScripts path = do
    eScripts <- loadCageScripts path
    case eScripts of
        Left err -> error $ "Blueprint error: " <> err
        Right scripts -> pure scripts

mkCageCfg
    :: Args
    -> CageScripts
    -> CageConfig
mkCageCfg args (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 300_000
        , defaultRetractTime = 300_000
        , defaultTip = Coin 2_000_000
        , network = argNetwork args
        }

mkAppConfig :: Args -> CageConfig -> AppConfig
mkAppConfig args cfg =
    AppConfig
        { epochSlots =
            EpochSlots
                (fromIntegral (argEpochSlots args))
        , shelleyGenesisPath =
            argShelleyGenesis args
        , socketPath = argSocket args
        , dbPath = argDb args
        , channelCapacity = 16
        , cageConfig = cfg
        , byronGenesisPath =
            argByronGenesis args
        , followerEnabled = True
        , appTracer = jsonLinesTracer
        }

-- | Re-export for validate.
when :: Bool -> IO () -> IO ()
when True a = a
when False _ = pure ()
