{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Cardano.MPFS.Trie.Persistent
-- Description : RocksDB-backed TrieManager with token-prefixed keys
-- License     : Apache-2.0
--
-- Production implementation of the 'TrieManager'
-- interface backed by shared RocksDB column
-- families (@trie-nodes@ and @trie-kv@). Multiple
-- tokens share the same column families — isolation
-- is achieved by prefixing every key with a
-- length-prefixed serialization of the 'TokenId'.
--
-- The prefix is @(1-byte length ++ raw asset name)@,
-- so iterators can scan only a single token's
-- entries. A 'mkPrefixedTrieDB' wraps the raw
-- RocksDB handle into a @rocksdb-kv-transactions@
-- 'Database' that prepends and strips the prefix
-- transparently.
--
-- Speculative sessions use 'runSpeculation' which
-- reads from the real database but buffers writes
-- in memory, discarding them after the callback
-- returns. This is used by 'updateTokenImpl' to
-- generate proofs without mutating state.
module Cardano.MPFS.Trie.Persistent
    ( -- * Construction
      mkPersistentTrieManager
    , withPersistentTrieManager
    ) where

import Control.Lens (Prism')

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.IORef
    ( IORef
    , modifyIORef'
    , newIORef
    , readIORef
    )
import Data.Set (Set)
import Data.Set qualified as Set

import Cardano.MPFS.Indexer.Columns (TrieStatus (..))

import Database.KV.Database
    ( Codecs (..)
    , Column (..)
    , DSum ((:=>))
    , Database (..)
    , fromPairList
    , prefixDatabase
    )
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( Transaction
    , query
    , runSpeculation
    , runTransactionUnguarded
    )
import Database.RocksDB
    ( BatchOp (..)
    , ColumnFamily
    , Config (..)
    , DB (..)
    , createIterator
    , destroyIterator
    , iterEntry
    , iterNext
    , iterSeek
    , iterValid
    , withDBCF
    , write
    )

import MPF.Backend.Standalone
    ( MPFStandalone (..)
    , MPFStandaloneCodecs (..)
    )
import MPF.Deletion (deleting)
import MPF.Hashes
    ( MPFHash
    , MPFHashing (..)
    , mkMPFHash
    , mpfHashing
    , renderMPFHash
    )
import MPF.Insertion (inserting)
import MPF.Interface
    ( HexIndirect (..)
    , HexKey
    , byteStringToHexKey
    , hexKeyPrism
    , mpfCodecs
    )
import MPF.Proof.Insertion
    ( mkMPFInclusionProof
    )
import MPF.Test.Lib
    ( fromHexKVIdentity
    , mpfHashCodecs
    )

import Cardano.MPFS.Core.OnChain (ProofStep)
import Cardano.MPFS.Core.Proof
    ( serializeProof
    , toProofSteps
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Root (..)
    , TokenId (..)
    )
import Cardano.MPFS.Trie
    ( Proof (..)
    , Trie (..)
    , TrieManager (..)
    )

-- | Create a persistent 'TrieManager IO' backed by
-- shared RocksDB column families. Each token's trie
-- data is isolated via key prefixing. On startup,
-- scans the @metaCF@ to rebuild the known\/hidden
-- sets so tries survive restarts.
mkPersistentTrieManager
    :: DB
    -- ^ Shared RocksDB handle
    -> ColumnFamily
    -- ^ Column family for trie nodes
    -> ColumnFamily
    -- ^ Column family for trie key-value pairs
    -> ColumnFamily
    -- ^ Column family for trie registry metadata
    -> IO (TrieManager IO)
mkPersistentTrieManager db nodesCF kvCF metaCF = do
    (known, hidden) <- scanTrieMeta db metaCF
    knownRef <- newIORef known
    hiddenRef <- newIORef hidden
    pure
        TrieManager
            { withTrie =
                persistentWithTrie
                    db
                    nodesCF
                    kvCF
                    knownRef
                    hiddenRef
            , withSpeculativeTrie =
                persistentWithSpeculativeTrie
                    db
                    nodesCF
                    kvCF
                    knownRef
                    hiddenRef
            , createTrie =
                persistentCreateTrie
                    db
                    nodesCF
                    kvCF
                    metaCF
                    knownRef
                    hiddenRef
            , deleteTrie =
                persistentDeleteTrie
                    db
                    nodesCF
                    kvCF
                    metaCF
                    knownRef
                    hiddenRef
            , hideTrie =
                persistentHideTrie
                    db
                    metaCF
                    hiddenRef
            , unhideTrie =
                persistentUnhideTrie
                    db
                    metaCF
                    hiddenRef
            }

-- | Bracket that opens a RocksDB database, creates
-- the @nodes@, @kv@, and @meta@ column families,
-- builds a persistent 'TrieManager IO', runs the
-- action, and closes the database.
withPersistentTrieManager
    :: FilePath
    -- ^ Path to the RocksDB database directory
    -> (TrieManager IO -> IO a)
    -- ^ Action receiving the trie manager
    -> IO a
withPersistentTrieManager path action =
    withDBCF
        path
        defaultConfig
        [ ("nodes", defaultConfig)
        , ("kv", defaultConfig)
        , ("meta", defaultConfig)
        ]
        $ \db@DB{columnFamilies} ->
            case columnFamilies of
                [nodesCF, kvCF, metaCF] -> do
                    mgr <-
                        mkPersistentTrieManager
                            db
                            nodesCF
                            kvCF
                            metaCF
                    action mgr
                _ ->
                    error
                        "withPersistentTrieManager: \
                        \expected 3 column families"

-- | Default RocksDB configuration.
defaultConfig :: Config
defaultConfig =
    Config
        { createIfMissing = True
        , errorIfExists = False
        , paranoidChecks = False
        , maxFiles = Nothing
        , prefixLength = Nothing
        , bloomFilter = False
        }

-- | Serialize a 'TokenId' to a prefix byte string.
tokenPrefix :: TokenId -> ByteString
tokenPrefix (TokenId (AssetName sbs)) =
    let raw = SBS.fromShort sbs
        len = BS.length raw
    in  BS.singleton (fromIntegral len) <> raw

-- | Run an action with a token's trie. Creates a
-- prefixed 'Database' for the token and wraps MPF
-- operations.
persistentWithTrie
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> IORef (Set TokenId)
    -> TokenId
    -> (Trie IO -> IO a)
    -> IO a
persistentWithTrie
    db
    nodesCF
    kvCF
    knownRef
    hiddenRef
    tid
    action = do
        hidden <- readIORef hiddenRef
        if Set.member tid hidden
            then
                error
                    $ "Trie is hidden: " ++ show tid
            else do
                known <- readIORef knownRef
                if Set.member tid known
                    then do
                        let pfx = tokenPrefix tid
                            database =
                                mkPrefixedTrieDB
                                    db
                                    nodesCF
                                    kvCF
                                    pfx
                        action
                            (mkPersistentTrie database)
                    else
                        error
                            $ "Trie not found: "
                                ++ show tid

-- | Run a speculative (dry-run) session against a
-- token's trie. Reads from a snapshot and buffers
-- writes for read-your-writes, but discards all
-- mutations at the end.
persistentWithSpeculativeTrie
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> IORef (Set TokenId)
    -> TokenId
    -> ( forall n
          . Monad n
         => Trie n
         -> n a
       )
    -> IO a
persistentWithSpeculativeTrie
    db
    nodesCF
    kvCF
    knownRef
    hiddenRef
    tid
    action = do
        hidden <- readIORef hiddenRef
        if Set.member tid hidden
            then
                error
                    $ "Trie is hidden: " ++ show tid
            else do
                known <- readIORef knownRef
                if Set.member tid known
                    then do
                        let pfx = tokenPrefix tid
                            database =
                                mkPrefixedTrieDB
                                    db
                                    nodesCF
                                    kvCF
                                    pfx
                        runSpeculation
                            database
                            ( action
                                mkTransactionalTrie
                            )
                    else
                        error
                            $ "Trie not found: "
                                ++ show tid

-- | Create a new empty trie for a token. Previous
-- data is deleted if the token already exists.
-- The registry entry is written atomically with
-- the prefix deletes.
persistentCreateTrie
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> IORef (Set TokenId)
    -> TokenId
    -> IO ()
persistentCreateTrie
    db
    nodesCF
    kvCF
    metaCF
    knownRef
    hiddenRef
    tid = do
        -- Always delete: the DB may contain stale data
        -- from a previous TrieManager (different
        -- knownRef) or from a previous run.
        ops1 <- collectDeleteOps db nodesCF pfx
        ops2 <- collectDeleteOps db kvCF pfx
        let metaOp =
                PutCF
                    metaCF
                    (tokenIdToKey tid)
                    (encodeTrieStatus Visible)
        write db (metaOp : ops1 ++ ops2)
        modifyIORef' knownRef (Set.insert tid)
        modifyIORef' hiddenRef (Set.delete tid)
      where
        pfx = tokenPrefix tid

-- | Delete a token's trie and all its data.
-- The registry entry is removed atomically with
-- the prefix deletes.
persistentDeleteTrie
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> IORef (Set TokenId)
    -> TokenId
    -> IO ()
persistentDeleteTrie
    db
    nodesCF
    kvCF
    metaCF
    knownRef
    hiddenRef
    tid = do
        ops1 <- collectDeleteOps db nodesCF pfx
        ops2 <- collectDeleteOps db kvCF pfx
        let metaOp =
                DelCF metaCF (tokenIdToKey tid)
        write db (metaOp : ops1 ++ ops2)
        modifyIORef' knownRef (Set.delete tid)
        modifyIORef' hiddenRef (Set.delete tid)
      where
        pfx = tokenPrefix tid

-- | Mark a token's trie as hidden. Data is preserved
-- in RocksDB; 'withTrie' will fail until
-- 'unhideTrie'. The status change is persisted so
-- it survives restarts.
persistentHideTrie
    :: DB
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> TokenId
    -> IO ()
persistentHideTrie db metaCF hiddenRef tid = do
    write
        db
        [ PutCF
            metaCF
            (tokenIdToKey tid)
            (encodeTrieStatus Hidden)
        ]
    modifyIORef' hiddenRef (Set.insert tid)

-- | Restore a hidden token's trie to visible. The
-- status change is persisted so it survives
-- restarts.
persistentUnhideTrie
    :: DB
    -> ColumnFamily
    -> IORef (Set TokenId)
    -> TokenId
    -> IO ()
persistentUnhideTrie db metaCF hiddenRef tid = do
    write
        db
        [ PutCF
            metaCF
            (tokenIdToKey tid)
            (encodeTrieStatus Visible)
        ]
    modifyIORef' hiddenRef (Set.delete tid)

-- --------------------------------------------------------
-- Prefixed Database construction
-- --------------------------------------------------------

-- | Create a 'Database' that prefixes all keys with
-- the given prefix and routes operations to the
-- shared column families. Uses 'mkRocksDBDatabase'
-- for proper snapshot support and 'prefixDatabase'
-- for transparent key isolation.
mkPrefixedTrieDB
    :: DB
    -> ColumnFamily
    -> ColumnFamily
    -> ByteString
    -> Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
mkPrefixedTrieDB db nodesCF kvCF pfx =
    prefixDatabase pfx
        $ mkRocksDBDatabase db trieColumns
  where
    isoMPFHash :: Prism' ByteString MPFHash
    isoMPFHash = mpfValueCodec mpfHashCodecs
    trieColumns =
        fromPairList
            [ MPFStandaloneKVCol
                :=> Column
                    { family = kvCF
                    , codecs =
                        Codecs
                            { keyCodec =
                                hexKeyPrism
                            , valueCodec =
                                isoMPFHash
                            }
                    }
            , MPFStandaloneMPFCol
                :=> Column
                    { family = nodesCF
                    , codecs =
                        mpfCodecs isoMPFHash
                    }
            ]

-- --------------------------------------------------------
-- Trie construction from prefixed Database
-- --------------------------------------------------------

-- | Create a 'Trie IO' from a prefixed 'Database'.
mkPersistentTrie
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> Trie IO
mkPersistentTrie database =
    Trie
        { insert = persistentInsert database
        , delete = persistentDelete database
        , lookup = persistentLookup database
        , getRoot = persistentGetRoot database
        , getProof = persistentGetProof database
        , getProofSteps =
            persistentGetProofSteps database
        }

-- | A 'Trie' whose operations run directly in
-- the 'Transaction' monad without committing.
-- Used by 'runSpeculation' for dry-run sessions.
mkTransactionalTrie
    :: Trie
        ( Transaction
            IO
            ColumnFamily
            (MPFStandalone HexKey MPFHash MPFHash)
            BatchOp
        )
mkTransactionalTrie =
    Trie
        { insert = transactionalInsert
        , delete = transactionalDelete
        , lookup = transactionalLookup
        , getRoot = persistentGetRootT
        , getProof = transactionalGetProof
        , getProofSteps =
            transactionalGetProofSteps
        }

transactionalInsert
    :: ByteString
    -> ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
transactionalInsert k v = do
    inserting
        fromHexKVIdentity
        mpfHashing
        MPFStandaloneKVCol
        MPFStandaloneMPFCol
        (byteStringToHexKey (hashBS k))
        (mkMPFHash v)
    persistentGetRootT

transactionalDelete
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
transactionalDelete k = do
    deleting
        fromHexKVIdentity
        mpfHashing
        MPFStandaloneKVCol
        MPFStandaloneMPFCol
        (byteStringToHexKey (hashBS k))
    persistentGetRootT

transactionalLookup
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe ByteString)
transactionalLookup k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneMPFCol
            hexKey
    pure $ case mProof of
        Nothing -> Nothing
        Just _ -> Just (hashBS k)

transactionalGetProof
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe Proof)
transactionalGetProof k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneMPFCol
            hexKey
    pure $ fmap (Proof . serializeProof) mProof

transactionalGetProofSteps
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe [ProofStep])
transactionalGetProofSteps k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneMPFCol
            hexKey
    pure $ fmap toProofSteps mProof

persistentInsert
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> ByteString
    -> IO Root
persistentInsert database k v =
    runTransactionUnguarded database $ do
        inserting
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneKVCol
            MPFStandaloneMPFCol
            (byteStringToHexKey (hashBS k))
            (mkMPFHash v)
        persistentGetRootT

persistentDelete
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO Root
persistentDelete database k =
    runTransactionUnguarded database $ do
        deleting
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneKVCol
            MPFStandaloneMPFCol
            (byteStringToHexKey (hashBS k))
        persistentGetRootT

persistentLookup
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe ByteString)
persistentLookup database k =
    runTransactionUnguarded database $ do
        let hexKey =
                byteStringToHexKey (hashBS k)
        mProof <-
            mkMPFInclusionProof
                fromHexKVIdentity
                mpfHashing
                MPFStandaloneMPFCol
                hexKey
        pure $ case mProof of
            Nothing -> Nothing
            Just _ -> Just (hashBS k)

persistentGetRoot
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> IO Root
persistentGetRoot database =
    runTransactionUnguarded database persistentGetRootT

-- | Get root hash within a transaction.
persistentGetRootT
    :: Database.KV.Transaction.Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
persistentGetRootT = do
    mi <- query MPFStandaloneMPFCol []
    pure $ case mi of
        Nothing -> Root BS.empty
        Just HexIndirect{hexIsLeaf, hexJump, hexValue} ->
            Root
                $ renderMPFHash
                $ if hexIsLeaf
                    then
                        leafHash
                            mpfHashing
                            hexJump
                            hexValue
                    else hexValue

persistentGetProof
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe Proof)
persistentGetProof database k =
    runTransactionUnguarded database $ do
        let hexKey =
                byteStringToHexKey (hashBS k)
        mProof <-
            mkMPFInclusionProof
                fromHexKVIdentity
                mpfHashing
                MPFStandaloneMPFCol
                hexKey
        pure $ fmap (Proof . serializeProof) mProof

persistentGetProofSteps
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe [ProofStep])
persistentGetProofSteps database k =
    runTransactionUnguarded database $ do
        let hexKey =
                byteStringToHexKey (hashBS k)
        mProof <-
            mkMPFInclusionProof
                fromHexKVIdentity
                mpfHashing
                MPFStandaloneMPFCol
                hexKey
        pure $ fmap toProofSteps mProof

-- --------------------------------------------------------
-- Helpers
-- --------------------------------------------------------

-- | Hash bytes using MPF convention.
hashBS :: ByteString -> ByteString
hashBS = renderMPFHash . mkMPFHash

-- | Serialize a 'TokenId' to its raw asset name
-- bytes (same encoding as 'tokenIdPrism').
tokenIdToKey :: TokenId -> ByteString
tokenIdToKey (TokenId (AssetName sbs)) =
    SBS.fromShort sbs

-- | Decode raw asset name bytes back to 'TokenId'.
tokenIdFromKey :: ByteString -> TokenId
tokenIdFromKey =
    TokenId . AssetName . SBS.toShort

-- | Encode 'TrieStatus' as a single byte.
encodeTrieStatus :: TrieStatus -> ByteString
encodeTrieStatus Visible = BS.singleton 0x01
encodeTrieStatus Hidden = BS.singleton 0x02

-- | Decode a single byte to 'TrieStatus'.
decodeTrieStatus :: ByteString -> Maybe TrieStatus
decodeTrieStatus bs = case BS.uncons bs of
    Just (0x01, rest)
        | BS.null rest -> Just Visible
    Just (0x02, rest)
        | BS.null rest -> Just Hidden
    _ -> Nothing

-- | Scan the trie-meta column family and return
-- the sets of known (visible) and hidden tokens.
scanTrieMeta
    :: DB
    -> ColumnFamily
    -> IO (Set TokenId, Set TokenId)
scanTrieMeta db metaCF = do
    i <- createIterator db (Just metaCF)
    iterSeek i BS.empty
    result <- go i Set.empty Set.empty
    destroyIterator i
    pure result
  where
    go i known hidden = do
        v <- iterValid i
        if v
            then do
                me <- iterEntry i
                case me of
                    Just (k, val) ->
                        case decodeTrieStatus val of
                            Just Visible -> do
                                iterNext i
                                go
                                    i
                                    ( Set.insert
                                        (tokenIdFromKey k)
                                        known
                                    )
                                    hidden
                            Just Hidden -> do
                                iterNext i
                                go
                                    i
                                    known
                                    ( Set.insert
                                        (tokenIdFromKey k)
                                        hidden
                                    )
                            Nothing -> do
                                iterNext i
                                go i known hidden
                    Nothing ->
                        pure (known, hidden)
            else pure (known, hidden)

-- | Collect delete operations for all entries with
-- a given prefix in a column family.
collectDeleteOps
    :: DB
    -> ColumnFamily
    -> ByteString
    -> IO [BatchOp]
collectDeleteOps db cf pfx = do
    i <- createIterator db (Just cf)
    iterSeek i pfx
    ops <- go i []
    destroyIterator i
    pure ops
  where
    go i acc = do
        v <- iterValid i
        if v
            then do
                me <- iterEntry i
                case me of
                    Just (k, _)
                        | pfx `BS.isPrefixOf` k -> do
                            iterNext i
                            go i (DelCF cf k : acc)
                    _ -> pure (reverse acc)
            else pure (reverse acc)
