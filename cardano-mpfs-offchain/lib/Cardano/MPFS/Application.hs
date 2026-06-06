{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Application
-- Description : Application wiring and lifecycle
-- License     : Apache-2.0
--
-- Top-level wiring module that assembles all
-- service interfaces into a fully operational
-- 'Context IO'. The bracket 'withApplication' opens
-- a shared RocksDB database with 14 column families
-- (6 UTxO + 7 cage\/trie + 1 composed rollback), connects to a local
-- Cardano node via two N2C connections, and builds
-- the production 'Provider', 'Submitter', persistent
-- 'State', persistent 'TrieManager', real
-- 'TxBuilder', and a 'CageFollower' that processes
-- blocks from ChainSync. On exit it cancels both
-- connection threads and closes the database.
--
-- On open, 'withApplication' runs the INV-3
-- pre-flight via 'checkSchemaMigration' and
-- refuses to start on a pre-#247 database whose
-- 'TrieKV' carries rows but whose
-- 'TrieRawValues' is empty — silent fallback to
-- @Nothing@ on legacy keys is a correctness
-- bug, not a degraded mode.
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
    , unifiedCodecs

      -- * Schema migration check
    , checkSchemaMigration
    , SchemaMigrationRequired (..)
    , schemaMigrationMessage
    ) where

import Cardano.Chain.Slotting (EpochSlots)
import Cardano.UTxOCSMT.Application.Run.Query (queryAwaitValue)
import Control.Concurrent.Async
    ( async
    , cancel
    , link
    )
import Control.Concurrent.STM
    ( atomically
    , modifyTVar'
    , newTVarIO
    )
import Control.Exception (finally, throwIO)
import Control.Monad (when)
import Control.Tracer (Tracer (..), contramap, traceWith)
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short (toShort)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Ouroboros.Consensus.HardFork.Combinator
    ( OneEraHash (..)
    )

import Database.KV.Cursor (firstEntry)
import Database.KV.Database (Codecs, mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( iterating
    , mapColumns
    , newRunTransaction
    , query
    , runTransactionUnguarded
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

import CSMT.Hashes
    ( generateInclusionProof
    , renderHash
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
    , DbState (..)
    , ReadyState (..)
    , mkCSMTOps
    , openCSMTOps
    , queryMerkleRoot
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction qualified as CSMT
    ( RunTransaction (..)
    )
import Cardano.UTxOCSMT.Application.Metrics
    ( MetricsEvent (..)
    , metricsFold
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( armageddonParams
    , context
    , prisms
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
import ChainFollower.Backend (Init (..), Restoring (..))
import ChainFollower.Rollbacks.Store qualified as CFStore
import ChainFollower.Rollbacks.Types (RollbackPoint (..))
import ChainFollower.Runner (Phase (..))
import Control.Lens (iso)
import Data.Tracer.Fold (foldTracer)
import Data.Tracer.Timestamp (utcTimestampTracer)

import Ouroboros.Consensus.Cardano.Node ()
import Ouroboros.Network.Block qualified as Network

import Cardano.Ledger.Binary
    ( EncCBOR
    , natVersion
    , serialize
    )

import CSMT.Hashes.Types (Hash)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.Indexer.Backend
    ( composedInit
    )
import Cardano.MPFS.Indexer.CageFollower
    ( mkCageIntersector
    )
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx (..)
    )
import Data.Dependent.Map (DMap)

import Cardano.MPFS.Indexer.Codecs (allUnifiedCodecs)
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , SchemaMigrationRequired (..)
    , UnifiedColumns (..)
    , schemaMigrationMessage
    )
import Cardano.MPFS.Indexer.Persistent
    ( mkPersistentState
    )
import Cardano.MPFS.Provider.NodeClient
    ( mkNodeClientProvider
    , queryEvalContext
    )
import Cardano.MPFS.State qualified as CageSt
import Cardano.MPFS.Submitter.N2C (mkN2CSubmitter)
import Cardano.MPFS.Trace
    ( AppTrace (..)
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

-- | All column families: 6 UTxO (cardano-utxo-csmt,
-- including journal and Runner rollbacks) followed
-- by 7 cage\/trie plus 1 composed rollback
-- (chain-follower). Order matters — cardano-utxo-csmt
-- consumes the first 6 via its internal 'Columns'
-- GADT, our 'AllColumns' GADT consumes the next 7,
-- then 'InRollbacks' gets the 14th.
allColumnFamilies :: [(String, Config)]
allColumnFamilies =
    utxoColumnFamilies
        <> cageColumnFamilies
        <> [("composed-rollbacks", dbConfig)]
  where
    utxoColumnFamilies =
        [ ("kv", dbConfig)
        , ("csmt", dbConfig)
        , ("config", dbConfig)
        , ("journal", dbConfig)
        , ("metrics", dbConfig)
        , ("rollbacks", dbConfig)
        ]

-- | Cage-only column families (7). Used by tests
-- that don't need the UTxO index. The
-- @trie-raw-values@ column (added for #247
-- Slice 1) stores raw value bytes keyed by the
-- hashed trie key, mirroring 'TrieKV' so
-- 'Trie.lookup' can return the original value
-- once the lookup contract change lands in a
-- later slice.
cageColumnFamilies :: [(String, Config)]
cageColumnFamilies =
    [ ("tokens", dbConfig)
    , ("requests", dbConfig)
    , ("cage-cfg", dbConfig)
    , ("trie-nodes", dbConfig)
    , ("trie-kv", dbConfig)
    , ("trie-meta", dbConfig)
    , ("trie-raw-values", dbConfig)
    ]

-- | Run an action with a fully wired 'Context IO'.
--
-- Opens RocksDB with 14 column families (6 UTxO
-- + 7 cage\/trie + 1 composed rollback), runs
-- the INV-3 schema-migration pre-flight (refuses
-- to start on pre-#247 databases), creates the
-- UTxO state machine and cage state, starts two
-- N2C connections (ChainSync + LSQ\/LTxS), and
-- tears down on exit.
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
            -- Unified database over all 14 CFs
            let unifiedCols =
                    mkColumns
                        (columnFamilies db)
                        (allUnifiedCodecs prisms)
                unifiedDb =
                    mkRocksDBDatabase db unifiedCols
            L.RunTransaction run <-
                newRunTransaction unifiedDb

            -- Project into cage columns (7–13)
            let cageRt =
                    L.RunTransaction
                        (run . mapColumns InCage)
                st =
                    (mkPersistentState cageRt)
                        { CageSt.checkpoints =
                            CageSt.Checkpoints
                                { CageSt.getCheckpoint =
                                    latestRollbackPoint
                                        run
                                , CageSt.putCheckpoint =
                                    \_ _ -> pure ()
                                }
                        }

            -- Project into UTxO columns (1–6)
            let utxoRt =
                    CSMT.RunTransaction
                        (run . mapColumns InUtxo)

            -- INV-3 (#247): refuse to start if
            -- legacy trie data is present without
            -- the new TrieRawValues column.
            checkSchemaMigration
                (run . mapColumns InCage)

            -- Trie: CFs at indices 9–11 (6 UTxO + 3 cage
            -- before trie-nodes, trie-kv, trie-meta).
            -- The 7th cage CF, trie-raw-values, sits
            -- at index 12 and is touched only via the
            -- 'TrieRawValues' AllColumns constructor.
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
                                Rollbacks
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

                    -- Open CSMT ops with crash
                    -- recovery
                    let utxoRunUnguarded =
                            runTransactionUnguarded
                                unifiedDb
                                . mapColumns InUtxo
                    dbState <-
                        openCSMTOps
                            4
                            1000
                            (iso BSL.toStrict BSL.fromStrict)
                            (fromKV context)
                            (hashing context)
                            (CSMT.transact utxoRt)
                            utxoRunUnguarded
                            ( traceWith
                                $ contramap TraceReplay
                                $ appTracer cfg
                            )
                    let resolveDb (NeedsRecovery recover) =
                            recover >>= resolveDb
                        resolveDb (Ready (ChooseKVOnly ops)) =
                            pure ops
                        resolveDb (Ready (ChooseFull _)) =
                            error "openCSMTOps: unexpected ChooseFull"
                    kvOnlyOps <- resolveDb dbState

                    -- Initialize chain-follower Backend
                    let backendInit =
                            composedInit
                                ( cfgScriptHash
                                    $ cageConfig cfg
                                )
                                kvOnlyOps

                    -- Count rollback points and sample
                    -- intersection candidates from
                    -- composed rollback column
                    initialCount <-
                        run
                            $ CFStore.countPoints
                                InRollbacks
                    history <-
                        run
                            $ CFStore.queryHistory
                                InRollbacks
                    let startPts :: [Point]
                        startPts
                            | null history =
                                [Network.Point Origin]
                            | otherwise =
                                [ cageCheckpointToPoint
                                    s
                                    (fromMaybe (BlockId mempty) (rpMeta rp))
                                | (s, rp) <- history
                                ]

                    -- Initialize Phase:
                    -- Existing DB: toFollowing replays journal
                    -- Fresh DB: InRestoration (fast KVOnly)
                    restoring <-
                        start backendInit
                    initialPhase <-
                        if initialCount > 0
                            then do
                                following <-
                                    toFollowing restoring
                                pure
                                    $ InFollowing
                                        initialCount
                                        following
                            else
                                pure
                                    $ InRestoration restoring
                    -- Metrics pipeline: fold events
                    -- into a Metrics snapshot stored
                    -- in an IORef
                    metricsRef <- newIORef Nothing
                    let shareMetrics downstream =
                            Tracer $ \a -> do
                                writeIORef metricsRef (Just a)
                                traceWith downstream a
                    metricsFolded <-
                        foldTracer
                            metricsFold
                            (shareMetrics (Tracer $ const $ pure ()))
                    let metricsTracer =
                            utcTimestampTracer metricsFolded

                    -- Commit notification TVar for awaitUtxo
                    commitNotify' <-
                        newTVarIO (0 :: Int)
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
                                let onCommit =
                                        atomically
                                            $ modifyTVar'
                                                commitNotify'
                                                (+ 1)
                                    cageIntersector =
                                        mkCageIntersector
                                            (fromIntegral stabilityWindow)
                                            run
                                            backendInit
                                            csmtArmageddon
                                            onCommit
                                            initialPhase
                                    chainSyncApp =
                                        mkN2CChainSyncApplication
                                            ( contramap
                                                ( TraceBlockReceived
                                                    . Network.blockSlot
                                                )
                                                (appTracer cfg)
                                            )
                                            ( Tracer $ \slot -> do
                                                traceWith
                                                    metricsTracer
                                                    (ChainTipEvent slot)
                                                traceWith
                                                    ( contramap
                                                        TraceChainTip
                                                        (appTracer cfg)
                                                    )
                                                    slot
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
                        exists txIn =
                            CSMT.transact utxoRt
                                $ fmap isJust
                                $ query KVCol
                                $ cborEncode txIn
                        resolve txIn =
                            CSMT.transact utxoRt
                                $ fmap
                                    (fmap BSL.toStrict)
                                $ query KVCol
                                $ cborEncode txIn
                        root =
                            CSMT.transact utxoRt
                                $ fmap renderHash
                                    <$> queryMerkleRoot
                                        ( hashing
                                            context
                                        )
                        proof txIn =
                            CSMT.transact utxoRt $ do
                                let fkv =
                                        fromKV context
                                result <-
                                    generateInclusionProof
                                        fkv
                                        KVCol
                                        CSMTCol
                                        ( cborEncode
                                            txIn
                                        )
                                pure
                                    $ fmap snd result
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
                                        proof
                                , cfgCage =
                                    cageConfig cfg
                                , utxoExists = exists
                                , resolveUtxo = resolve
                                , awaitUtxo =
                                    queryAwaitValue
                                        commitNotify'
                                        resolve
                                , utxoRoot = root
                                , utxoProof = proof
                                , evalContext =
                                    queryEvalContext genesis lsqCh
                                , runIndexerTx =
                                    \(IndexerTx body) ->
                                        run body
                                , readMetrics =
                                    readIORef metricsRef
                                }
                    action ctx
                        `finally` do
                            mapM_ cancel mChainThread
                            cancel nodeThread
                _ ->
                    error
                        "Expected at least 14 \
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

-- | Read the latest rollback point from the
-- composed rollback column as a checkpoint.
latestRollbackPoint
    :: ( forall a
          . L.Transaction
                IO
                cf
                (UnifiedColumns Point hash BSL.ByteString BSL.ByteString)
                op
                a
         -> IO a
       )
    -> IO (Maybe (SlotNo, BlockId))
latestRollbackPoint run = do
    history <-
        run $ CFStore.queryHistory InRollbacks
    pure $ case history of
        [] -> Nothing
        pts ->
            let (s, rp) = last pts
            in  Just
                    ( s
                    , fromMaybe
                        (BlockId mempty)
                        (rpMeta rp)
                    )

-- | Pre-applied unified codecs for all 14 column
-- families. Useful for tools that open the database
-- directly (e.g. inspectors) without needing to
-- import @cardano-utxo-csmt@ internals.
unifiedCodecs
    :: DMap
        ( UnifiedColumns
            Point
            Hash
            BSL.ByteString
            BSL.ByteString
        )
        Codecs
unifiedCodecs = allUnifiedCodecs prisms

-- | CBOR-encode a ledger type using protocol
-- version 11.
cborEncode :: EncCBOR a => a -> BSL.ByteString
cborEncode = serialize (natVersion @11)

-- | INV-3 (#247) startup pre-flight. Inspects
-- the legacy 'TrieKV' and the new
-- 'TrieRawValues' columns. If 'TrieKV' has rows
-- but 'TrieRawValues' is empty, the database was
-- written by a pre-#247 indexer that did not
-- persist raw value bytes — 'Trie.lookup' would
-- silently return @Nothing@ for every legacy key
-- once the lookup contract change lands. Refuses
-- to start with 'SchemaMigrationRequired' so the
-- operator drops the RocksDB directory and
-- resyncs the indexer from genesis.
checkSchemaMigration
    :: ( forall a
          . L.Transaction IO cf AllColumns op a
         -> IO a
       )
    -- ^ Transaction runner projected into
    -- 'AllColumns' (in 'withApplication': @run .
    -- mapColumns InCage@; in tests: the raw
    -- 'AllColumns' runner).
    -> IO ()
checkSchemaMigration run = do
    stale <- run $ do
        trieKVEmpty <-
            iterating TrieKV
                $ isNothing <$> firstEntry
        trieRawValuesEmpty <-
            iterating TrieRawValues
                $ isNothing <$> firstEntry
        pure (not trieKVEmpty && trieRawValuesEmpty)
    when stale $ throwIO SchemaMigrationRequired
