{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Application
-- Description : Application wiring and lifecycle
-- License     : Apache-2.0
--
-- Top-level wiring module that assembles all
-- service interfaces into a fully operational
-- 'Context IO'. The bracket 'withApplication' opens
-- a shared RocksDB database with 11 column families
-- (4 UTxO + 7 cage\/trie), connects to a local
-- Cardano node via two N2C connections, and builds
-- the production 'Provider', 'Submitter', persistent
-- 'State', persistent 'TrieManager', real
-- 'TxBuilder', and a 'CageFollower' that processes
-- blocks from ChainSync. On exit it cancels both
-- connection threads and closes the database.
--
-- __Invariant: one block = one DB transaction.__
-- All mutations for a single block — UTxO CSMT
-- changes, cage state\/trie mutations, rollback
-- storage, and checkpoint — execute inside one
-- atomic RocksDB write batch via 'UnifiedColumns'.
-- The 'CageFollower' lifts sub-transactions with
-- @'mapColumns' 'InUtxo'@ and @'mapColumns' 'InCage'@
-- to combine them into a single commit.
--
-- Connection 1: ChainSync via cardano-utxo-csmt —
-- blocks processed by 'CageFollower'.
-- Connection 2: LocalStateQuery + LocalTxSubmission
-- for UTxO queries, protocol params, and tx
-- submission.
--
-- Optionally seeds a fresh database from a CBOR
-- bootstrap file (see "Cardano.MPFS.Core.Bootstrap")
-- so chain sync can resume from the bootstrap point
-- rather than genesis.
module Cardano.MPFS.Application
    ( -- * Configuration
      AppConfig (..)

      -- * Lifecycle
    , withApplication

      -- * RocksDB setup
    , dbConfig
    , allColumnFamilies
    , cageColumnFamilies
    ) where

import Control.Concurrent.Async
    ( async
    , cancel
    , link
    )
import Control.Exception (throwIO)
import Control.Monad (when)
import Control.Tracer (nullTracer)
import Data.IORef (newIORef)
import Data.Maybe (isNothing)

import Cardano.Chain.Slotting (EpochSlots)
import Data.ByteString.Lazy qualified as BSL

import Database.KV.Cursor (firstEntry)
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( iterating
    , mapColumns
    , newRunTransaction
    )
import Database.KV.Transaction qualified as L
    ( RunTransaction (..)
    , Transaction
    )
import Database.RocksDB
    ( Config (..)
    , DB (..)
    , withDBCF
    )
import Ouroboros.Network.Magic (NetworkMagic)
import Ouroboros.Network.Point (WithOrigin (..))

import Cardano.UTxOCSMT.Application.ChainSyncN2C
    ( mkN2CChainSyncApplication
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Armageddon
    ( setup
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    , CSMTOps (..)
    , mkCSMTOps
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction qualified as CSMT
    ( RunTransaction (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Update
    ( countRollbackPoints
    , sampleRollbackPoints
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( armageddonParams
    , context
    , prisms
    , slotHash
    )
import Cardano.UTxOCSMT.Ouroboros.ConnectionN2C
    ( runLocalNodeApplication
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Point
    )

import Ouroboros.Network.Block qualified as Network

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Bootstrap
    ( BootstrapHeader (..)
    , foldBootstrapEntries
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.Indexer.CageFollower
    ( mkCageIntersector
    )
import Cardano.MPFS.Indexer.Codecs (allUnifiedCodecs)
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Indexer.Persistent
    ( mkPersistentState
    )
import Cardano.MPFS.Mock.Skeleton
    ( mkSkeletonIndexer
    )
import Cardano.MPFS.Provider.NodeClient
    ( mkNodeClientProvider
    )
import Cardano.MPFS.State qualified as CageSt
import Cardano.MPFS.Submitter.N2C (mkN2CSubmitter)
import Cardano.MPFS.Trie.Persistent
    ( mkPersistentTrieManager
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real
    ( mkRealTxBuilder
    )
import Cardano.Node.Client.N2C.Connection
    ( newLSQChannel
    , newLTxSChannel
    , runNodeClient
    )

-- | Application configuration.
data AppConfig = AppConfig
    { epochSlots :: !EpochSlots
    -- ^ Byron epoch slots (21600 mainnet/preprod, 4320 preview)
    , networkMagic :: !NetworkMagic
    -- ^ Network magic (e.g. mainnet, preview)
    , socketPath :: !FilePath
    -- ^ Path to the cardano-node Unix socket
    , dbPath :: !FilePath
    -- ^ Path to the RocksDB database directory
    , channelCapacity :: !Int
    -- ^ TBQueue capacity for N2C channels
    , cageConfig :: !CageConfig
    -- ^ Cage script and protocol parameters
    , bootstrapFile :: !(Maybe FilePath)
    -- ^ CBOR bootstrap file for fresh DB seeding
    , followerEnabled :: !Bool
    -- ^ Start CageFollower ChainSync processing
    }

-- | Default RocksDB configuration.
dbConfig :: Config
dbConfig =
    Config
        { createIfMissing = True
        , errorIfExists = False
        , paranoidChecks = False
        , maxFiles = Nothing
        , prefixLength = Nothing
        , bloomFilter = False
        }

-- | All column families: 4 UTxO (cardano-utxo-csmt)
-- followed by 7 cage\/trie. Order matters —
-- cardano-utxo-csmt consumes the first 4 via its
-- internal 'Columns' GADT, and our 'AllColumns'
-- GADT consumes the remaining 7.
allColumnFamilies :: [(String, Config)]
allColumnFamilies =
    utxoColumnFamilies <> cageColumnFamilies
  where
    utxoColumnFamilies =
        [ ("kv", dbConfig)
        , ("csmt", dbConfig)
        , ("rollbacks", dbConfig)
        , ("config", dbConfig)
        ]

-- | Cage-only column families (7). Used by tests
-- that don't need the UTxO index.
cageColumnFamilies :: [(String, Config)]
cageColumnFamilies =
    [ ("tokens", dbConfig)
    , ("requests", dbConfig)
    , ("cage-cfg", dbConfig)
    , ("cage-rollbacks", dbConfig)
    , ("trie-nodes", dbConfig)
    , ("trie-kv", dbConfig)
    , ("trie-meta", dbConfig)
    ]

-- | Run an action with a fully wired 'Context IO'.
--
-- Opens RocksDB with 11 column families, creates
-- the UTxO state machine and cage state, starts
-- two N2C connections (ChainSync + LSQ\/LTxS),
-- and tears down on exit.
withApplication
    :: AppConfig
    -- ^ Application configuration
    -> (Context IO -> IO a)
    -- ^ Action receiving the fully wired context
    -> IO a
withApplication cfg action =
    withDBCF
        (dbPath cfg)
        dbConfig
        allColumnFamilies
        $ \db -> do
            -- Unified database over all 11 CFs
            let unifiedCols =
                    mkColumns
                        (columnFamilies db)
                        (allUnifiedCodecs prisms)
                unifiedDb =
                    mkRocksDBDatabase db unifiedCols
            L.RunTransaction run <-
                newRunTransaction unifiedDb

            -- Project into cage columns (5–10)
            let cageRt =
                    L.RunTransaction
                        (run . mapColumns InCage)
                st = mkPersistentState cageRt

            -- Project into UTxO columns (1–4)
            let utxoRt =
                    CSMT.RunTransaction
                        (run . mapColumns InUtxo)

            -- Trie: columns 9–11 (skip 8)
            case drop 8 (columnFamilies db) of
                (nodesCF : kvCF : metaCF : _) -> do
                    tm <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF

                    -- CSMT operations (for both
                    -- bootstrap and block processing)
                    let ops =
                            mkCSMTOps
                                (fromKV context)
                                (hashing context)

                    -- Bootstrap seeding on fresh DB
                    seedBootstrap
                        (bootstrapFile cfg)
                        st
                        utxoRt
                        ops

                    -- Ensure UTxO rollback points
                    -- are initialized (Origin entry).
                    -- Required because we bypass
                    -- the CSMT's self-initializing
                    -- newState/createUpdateState.
                    empty <-
                        CSMT.transact utxoRt
                            $ iterating
                                RollbackPoints
                            $ isNothing
                                <$> firstEntry
                    when empty
                        $ setup
                            nullTracer
                            utxoRt
                            armageddonParams

                    -- Sample rollback points for
                    -- intersection and count for
                    -- the follower's IORef
                    availPts <-
                        run
                            $ mapColumns InUtxo
                            $ iterating
                                RollbackPoints
                                sampleRollbackPoints
                    initialCount <-
                        run
                            $ mapColumns InUtxo
                            $ iterating
                                RollbackPoints
                                countRollbackPoints
                    countRef <-
                        newIORef initialCount

                    let startPts :: [Point]
                        startPts =
                            if null availPts
                                then
                                    [ Network.Point
                                        Origin
                                    ]
                                else availPts

                    -- Connection 1: ChainSync
                    -- (optional, controlled by
                    -- followerEnabled)
                    mChainThread <-
                        if followerEnabled cfg
                            then do
                                let cageIntersector =
                                        mkCageIntersector
                                            ( cfgScriptHash
                                                $ cageConfig
                                                    cfg
                                            )
                                            nullTracer
                                            ops
                                            slotHash
                                            countRef
                                            run
                                    chainSyncApp =
                                        mkN2CChainSyncApplication
                                            nullTracer
                                            nullTracer
                                            nullTracer
                                            (\_ -> pure ())
                                            (pure ())
                                            Nothing
                                            cageIntersector
                                            startPts
                                t <-
                                    async $ do
                                        er <-
                                            runLocalNodeApplication
                                                (epochSlots cfg)
                                                (networkMagic cfg)
                                                (socketPath cfg)
                                                chainSyncApp
                                        case er of
                                            Left e ->
                                                throwIO e
                                            Right () ->
                                                pure ()
                                link t
                                pure (Just t)
                            else pure Nothing

                    -- Connection 2: LSQ + LTxS
                    idx <- mkSkeletonIndexer
                    lsqCh <-
                        newLSQChannel
                            (channelCapacity cfg)
                    ltxsCh <-
                        newLTxSChannel
                            (channelCapacity cfg)
                    nodeThread <-
                        async
                            $ runNodeClient
                                (networkMagic cfg)
                                (socketPath cfg)
                                lsqCh
                                ltxsCh
                    let prov =
                            mkNodeClientProvider
                                lsqCh
                        ctx =
                            Context
                                { provider = prov
                                , submitter =
                                    mkN2CSubmitter
                                        ltxsCh
                                , state = st
                                , trieManager = tm
                                , txBuilder =
                                    mkRealTxBuilder
                                        ( cageConfig
                                            cfg
                                        )
                                        prov
                                        st
                                        tm
                                , indexer = idx
                                }
                    result <- action ctx
                    mapM_ cancel mChainThread
                    cancel nodeThread
                    pure result
                _ ->
                    error
                        "Expected at least 11 \
                        \column families"

-- | Seed a fresh database from a bootstrap CBOR
-- file. Sets the initial checkpoint and inserts
-- genesis UTxOs into the CSMT so chain sync can
-- resume from the bootstrap point. No-op if the
-- database already has a checkpoint or no
-- bootstrap file is configured.
seedBootstrap
    :: Maybe FilePath
    -> CageSt.State IO
    -> CSMT.RunTransaction cf op slot hash BSL.ByteString BSL.ByteString IO
    -> CSMTOps
        ( L.Transaction
            IO
            cf
            (Columns slot hash BSL.ByteString BSL.ByteString)
            op
        )
        BSL.ByteString
        BSL.ByteString
        hash
    -> IO ()
seedBootstrap Nothing _ _ _ = pure ()
seedBootstrap (Just fp) st runner ops =
    do
        existing <-
            CageSt.getCheckpoint
                (CageSt.checkpoints st)
        when (isNothing existing) $ do
            foldBootstrapEntries
                fp
                onHeader
                onEntry
  where
    onHeader BootstrapHeader{..} =
        case bootstrapBlockHash of
            Nothing ->
                -- Genesis bootstrap: no block hash,
                -- no checkpoint. ChainSync starts
                -- from Origin.
                pure ()
            Just h ->
                CageSt.putCheckpoint
                    (CageSt.checkpoints st)
                    (SlotNo bootstrapSlot)
                    (BlockId h)
                    []
    onEntry k v =
        CSMT.transact runner
            $ csmtInsert
                ops
                (BSL.fromStrict k)
                (BSL.fromStrict v)
