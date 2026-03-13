{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Minimal runner for smoke-testing the MPFS
-- follower against a preprod cardano-node.
module Main (main) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromMaybe)
import System.Directory
    ( createDirectoryIfMissing
    )

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Crypto.Hash.Class (hashFromBytes)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Hashes (ScriptHash (..))

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Core.Types (Coin (..))
import Cardano.MPFS.Trace (jsonLinesTracer)
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )

-- | Preprod configuration.
preprodConfig
    :: FilePath -> FilePath -> AppConfig
preprodConfig sock dbDir =
    AppConfig
        { epochSlots = EpochSlots 21_600
        , shelleyGenesisPath =
            "/tmp/cardano-preprod/config\
            \/shelley-genesis.json"
        , socketPath = sock
        , dbPath = dbDir
        , channelCapacity = 16
        , cageConfig = dummyCageConfig
        , byronGenesisPath =
            Just
                "/tmp/cardano-preprod/config\
                \/byron-genesis.json"
        , followerEnabled = True
        , appTracer = jsonLinesTracer
        }

-- | Dummy cage config — no real script, so cage
-- event detection will find nothing. UTxO indexing
-- and split-mode phase transitions still exercise.
dummyCageConfig :: CageConfig
dummyCageConfig =
    CageConfig
        { cageScriptBytes = SBS.empty
        , cfgScriptHash =
            ScriptHash
                $ fromMaybe
                    (error "bad dummy hash")
                    (hashFromBytes (BS.replicate 28 0))
        , defaultProcessTime = 300_000
        , defaultRetractTime = 300_000
        , defaultMaxFee = Coin 2_000_000
        , network = Testnet
        , systemStartPosixMs =
            1_654_041_600_000
        , slotLengthMs = 1_000
        }

main :: IO ()
main = do
    let sock =
            "/tmp/cardano-preprod/ipc/node.socket"
    let dbDir = "/tmp/mpfs-preprod-db"
    createDirectoryIfMissing True dbDir
    do
        putStrLn
            "Starting MPFS follower \
            \against preprod"
        putStrLn $ "  socket: " <> sock
        putStrLn $ "  db: " <> dbDir
        withApplication (preprodConfig sock dbDir)
            $ \_ctx -> do
                putStrLn
                    "Application started, \
                    \syncing..."
                -- Block forever (Ctrl-C to stop)
                threadDelay maxBound
