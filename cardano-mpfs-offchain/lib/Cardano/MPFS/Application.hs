{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Application
-- Description : Application wiring and lifecycle
-- License     : Apache-2.0
--
-- Top-level wiring module that assembles all
-- service interfaces into a fully operational
-- 'Context IO'. The bracket 'withApplication' opens
-- a shared RocksDB database with 12 column families
-- (5 UTxO + 7 cage\/trie), connects to a local
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
-- Optionally seeds a fresh database from Shelley
-- (and Byron) genesis files so chain sync can resume
-- with genesis UTxOs already in the tree.
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

import Cardano.Chain.Slotting (EpochSlots)
import Control.Concurrent.Async
    ( async
    , cancel
    , link
    )
import Control.Exception (throwIO)
import Control.Monad (when)
import Control.Tracer (Tracer, contramap)
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short (toShort)
import Data.IORef (newIORef)
import Data.Maybe (isNothing)
import Ouroboros.Consensus.HardFork.Combinator
    ( OneEraHash (..)
    )

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
import Ouroboros.Network.Magic
    ( NetworkMagic (..)
    )
import Ouroboros.Network.Point
    ( Block (..)
    , WithOrigin (..)
    )

import Cardano.Ledger.Shelley.Genesis
    ( ShelleyGenesis
    , sgNetworkMagic
    )
import Cardano.UTxOCSMT.Application.BlockFetch
    ( HeaderSkipProgress (..)
    )
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
    , journalEmpty
    , kvOnlyCSMTOps
    , mkCSMTOps
    , replayJournal
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction qualified as CSMT
    ( RunTransaction (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Update
    ( Phase (..)
    , mkSplitMode
    , sampleRollbackPoints
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( armageddonParams
    , context
    , prisms
    , slotHash
    )
import Cardano.UTxOCSMT.Bootstrap.Genesis
    ( genesisStabilityWindow
    , genesisUtxoPairs
    , readByronGenesisUtxoPairs
    , readShelleyGenesis
    )
import Cardano.UTxOCSMT.Ouroboros.ConnectionN2C
    ( runLocalNodeApplication
    )
import Cardano.UTxOCSMT.Ouroboros.Types
    ( Point
    )
import MTS.Rollbacks.Store qualified as Store

import Ouroboros.Consensus.Cardano.Node ()
import Ouroboros.Network.Block qualified as Network

import Cardano.MPFS.Context (Context (..))
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
import Cardano.MPFS.Trace
    ( AppTrace (..)
    , adaptUpdate
    )
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
    -- ^ Byron epoch slots (21600 mainnet\/preprod,
    -- 4320 preview). Not in shelley genesis.
    , shelleyGenesisPath :: !FilePath
    -- ^ Path to shelley-genesis.json. Used to derive
    -- 'NetworkMagic', stability window, and other
    -- network parameters at startup.
    , socketPath :: !FilePath
    -- ^ Path to the cardano-node Unix socket
    , dbPath :: !FilePath
    -- ^ Path to the RocksDB database directory
    , channelCapacity :: !Int
    -- ^ TBQueue capacity for N2C channels
    , cageConfig :: !CageConfig
    -- ^ Cage script and protocol parameters
    , byronGenesisPath :: !(Maybe FilePath)
    -- ^ Optional path to @byron-genesis.json@.
    -- When set, Byron non-AVVM balances are seeded
    -- alongside Shelley initial funds on fresh DB.
    , followerEnabled :: !Bool
    -- ^ Start CageFollower ChainSync processing
    , appTracer :: Tracer IO AppTrace
    -- ^ Application event tracer
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

-- | All column families: 5 UTxO (cardano-utxo-csmt,
-- including journal) followed by 7 cage\/trie.
-- Order matters — cardano-utxo-csmt consumes the
-- first 5 via its internal 'Columns' GADT, and our
-- 'AllColumns' GADT consumes the remaining 7.
allColumnFamilies :: [(String, Config)]
allColumnFamilies =
    utxoColumnFamilies <> cageColumnFamilies
  where
    utxoColumnFamilies =
        [ ("kv", dbConfig)
        , ("csmt", dbConfig)
        , ("rollbacks", dbConfig)
        , ("config", dbConfig)
        , ("journal", dbConfig)
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
-- Opens RocksDB with 12 column families, creates
-- the UTxO state machine and cage state, starts
-- two N2C connections (ChainSync + LSQ\/LTxS),
-- and tears down on exit.
withApplication
    :: AppConfig
    -- ^ Application configuration
    -> (Context IO -> IO a)
    -- ^ Action receiving the fully wired context
    -> IO a
withApplication cfg action = do
    -- Read shelley genesis for network parameters
    genesis <-
        readShelleyGenesis
            (shelleyGenesisPath cfg)
    let networkMagic =
            NetworkMagic (sgNetworkMagic genesis)
        stabilityWindow =
            genesisStabilityWindow genesis

    withDBCF
        (dbPath cfg)
        dbConfig
        allColumnFamilies
        $ \db -> do
            -- Unified database over all 12 CFs
            let unifiedCols =
                    mkColumns
                        (columnFamilies db)
                        (allUnifiedCodecs prisms)
                unifiedDb =
                    mkRocksDBDatabase db unifiedCols
            L.RunTransaction run <-
                newRunTransaction unifiedDb

            -- Project into cage columns (6–12)
            let cageRt =
                    L.RunTransaction
                        (run . mapColumns InCage)
                st = mkPersistentState cageRt

            -- Project into UTxO columns (1–5)
            let utxoRt =
                    CSMT.RunTransaction
                        (run . mapColumns InUtxo)

            -- Trie: columns 10–12 (skip 9)
            case drop 9 (columnFamilies db) of
                (nodesCF : kvCF : metaCF : _) -> do
                    tm <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF

                    -- CSMT operations (for both
                    -- bootstrap and block processing)
                    let fullOps =
                            mkCSMTOps
                                (fromKV context)
                                (hashing context)
                        kvOps =
                            kvOnlyCSMTOps
                                BSL.toStrict

                    -- Seed genesis UTxOs on fresh DB
                    seedGenesis
                        genesis
                        (byronGenesisPath cfg)
                        st
                        utxoRt
                        fullOps

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
                            ( contramap
                                TraceArmageddon
                                (appTracer cfg)
                            )
                            utxoRt
                            armageddonParams

                    -- Detect starting phase for
                    -- split mode
                    jEmpty <-
                        CSMT.transact
                            utxoRt
                            journalEmpty
                    let startPhase =
                            if not empty && jEmpty
                                then Full
                                else KVOnly
                        isAtTip curSlot tipSlot =
                            tipSlot - curSlot
                                < SlotNo
                                    stabilityWindow
                        replay =
                            replayJournal
                                1000
                                BSL.fromStrict
                                (fromKV context)
                                (hashing context)
                                utxoRt
                    splitMode <-
                        mkSplitMode
                            kvOps
                            fullOps
                            isAtTip
                            replay
                            startPhase

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
                            $ Store.countPoints
                                RollbackPoints
                    countRef <-
                        newIORef initialCount

                    -- In KVOnly, the CSMT bypass
                    -- doesn't store rollback points,
                    -- so availPts may be stale. Use
                    -- the cage checkpoint instead —
                    -- it's updated every block.
                    cageCp <-
                        CageSt.getCheckpoint
                            (CageSt.checkpoints st)
                    let startPts :: [Point]
                        startPts = case startPhase of
                            Full
                                | not (null availPts) ->
                                    availPts
                            _
                                | Just (s, bid) <-
                                    cageCp ->
                                    [ cageCheckpointToPoint
                                        s
                                        bid
                                    ]
                            _ ->
                                [ Network.Point
                                    Origin
                                ]

                    -- Connection 1: ChainSync
                    -- (optional, controlled by
                    -- followerEnabled)
                    mChainThread <-
                        if followerEnabled cfg
                            then do
                                let csmtArmageddon =
                                        setup
                                            ( contramap
                                                TraceArmageddon
                                                ( appTracer
                                                    cfg
                                                )
                                            )
                                            utxoRt
                                            armageddonParams
                                    cageIntersector =
                                        mkCageIntersector
                                            ( cfgScriptHash
                                                $ cageConfig
                                                    cfg
                                            )
                                            ( contramap
                                                adaptUpdate
                                                (appTracer cfg)
                                            )
                                            splitMode
                                            slotHash
                                            countRef
                                            run
                                            csmtArmageddon
                                    chainSyncApp =
                                        mkN2CChainSyncApplication
                                            ( contramap
                                                ( TraceBlockReceived
                                                    . Network.blockSlot
                                                )
                                                (appTracer cfg)
                                            )
                                            ( contramap
                                                TraceChainTip
                                                (appTracer cfg)
                                            )
                                            ( contramap
                                                ( \p ->
                                                    TraceSkipProgress
                                                        (skipCurrentSlot p)
                                                        (skipTargetSlot p)
                                                )
                                                (appTracer cfg)
                                            )
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
                                                networkMagic
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
                                networkMagic
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
                        "Expected at least 12 \
                        \column families"

-- | Seed a fresh database with genesis UTxOs from
-- Shelley (and optionally Byron) genesis files.
-- Inserts entries into the CSMT so chain sync can
-- start with genesis UTxOs already in the tree.
-- No-op if the database already has a checkpoint.
seedGenesis
    :: ShelleyGenesis
    -> Maybe FilePath
    -- ^ Optional Byron genesis path
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
seedGenesis genesis mByronPath st runner ops = do
    existing <-
        CageSt.getCheckpoint
            (CageSt.checkpoints st)
    when (isNothing existing) $ do
        -- Shelley initial funds
        mapM_ insertPair (genesisUtxoPairs genesis)
        -- Byron non-AVVM balances (if configured)
        case mByronPath of
            Nothing -> pure ()
            Just fp -> do
                byronPairs <-
                    readByronGenesisUtxoPairs fp
                mapM_ insertPair byronPairs
  where
    insertPair (k, v) =
        CSMT.transact runner
            $ csmtInsert ops k v

-- | Convert a cage checkpoint to a chain
-- intersection 'Point'.
cageCheckpointToPoint
    :: SlotNo -> BlockId -> Point
cageCheckpointToPoint (SlotNo s) (BlockId h) =
    Network.Point
        $ At
        $ Block
            (SlotNo s)
            (OneEraHash $ toShort h)
