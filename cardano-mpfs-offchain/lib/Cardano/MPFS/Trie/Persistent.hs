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
-- interface backed by RocksDB. Multiple tokens share
-- the same column families ('TrieNodes', 'TrieKV',
-- 'TrieMeta') — isolation is achieved by scoping
-- MPF operations to a per-token 'HexKey' prefix.
--
-- Two layers are provided:
--
--   * __Transactional__ ('mkUnifiedTrie',
--     'mkUnifiedTrieManager'): operates in
--     @'Transaction' m cf 'AllColumns' ops@,
--     composable into a single DB commit.
--   * __IO__ ('mkPersistentTrieManager'): wraps
--     the transactional layer with 'IORef' caches
--     for outside-block usage and speculative
--     sessions.
module Cardano.MPFS.Trie.Persistent
    ( -- * Transactional (composable) layer
      mkUnifiedTrie
    , mkUnifiedTrieManager
    , tokenHexPrefix

      -- * IO layer (with caches)
    , mkPersistentTrieManager
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

import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , TrieStatus (..)
    )

import Database.KV.Database
    ( Codecs (..)
    , Column (..)
    , DSum ((:=>))
    , Database (..)
    , Pos (..)
    , QueryIterator (..)
    , fromPairList
    )
import Database.KV.Transaction
    ( Transaction
    , runSpeculation
    , runTransactionUnguarded
    )
import Database.KV.Transaction qualified as KV
    ( delete
    , insert
    , query
    )
import Database.RocksDB
    ( BatchOp (..)
    , ColumnFamily
    , Config (..)
    , DB (..)
    , createIterator
    , destroyIterator
    , getCF
    , iterEntry
    , iterLast
    , iterNext
    , iterPrev
    , iterSeek
    , iterValid
    , withDBCF
    , write
    )

import MPF.Backend.Standalone
    ( MPFStandalone (..)
    , MPFStandaloneCodecs (..)
    )
import MPF.Deletion (deleteSubtree, deleting)
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

-- --------------------------------------------------------
-- Token prefix
-- --------------------------------------------------------

-- | Convert a 'TokenId' to an MPF 'HexKey' prefix
-- for namespace isolation.
tokenHexPrefix :: TokenId -> HexKey
tokenHexPrefix =
    byteStringToHexKey . tokenPrefix

-- | Serialize a 'TokenId' to a prefix byte string.
-- Format: @(1-byte length ++ raw asset name)@.
tokenPrefix :: TokenId -> ByteString
tokenPrefix (TokenId (AssetName sbs)) =
    let raw = SBS.fromShort sbs
        len = BS.length raw
    in  BS.singleton (fromIntegral len) <> raw

-- --------------------------------------------------------
-- Transactional trie (AllColumns)
-- --------------------------------------------------------

-- | A 'Trie' operating on 'AllColumns' selectors
-- within a transaction. All operations are scoped
-- to the given token prefix — no auto-commit.
mkUnifiedTrie
    :: (Monad m)
    => HexKey
    -- ^ Token prefix from 'tokenHexPrefix'
    -> Trie
        (Transaction m cf AllColumns ops)
mkUnifiedTrie pfx =
    Trie
        { insert = unifiedInsert pfx
        , delete = unifiedDelete pfx
        , lookup = unifiedLookup pfx
        , getRoot = unifiedGetRoot pfx
        , getProof = unifiedGetProof pfx
        , getProofSteps =
            unifiedGetProofSteps pfx
        }

unifiedInsert
    :: (Monad m)
    => HexKey
    -> ByteString
    -> ByteString
    -> Transaction m cf AllColumns ops Root
unifiedInsert pfx k v = do
    inserting
        pfx
        fromHexKVIdentity
        mpfHashing
        TrieKV
        TrieNodes
        (byteStringToHexKey (hashBS k))
        (mkMPFHash v)
    unifiedGetRoot pfx

unifiedDelete
    :: (Monad m)
    => HexKey
    -> ByteString
    -> Transaction m cf AllColumns ops Root
unifiedDelete pfx k = do
    deleting
        pfx
        fromHexKVIdentity
        mpfHashing
        TrieKV
        TrieNodes
        (byteStringToHexKey (hashBS k))
    unifiedGetRoot pfx

unifiedLookup
    :: (Monad m)
    => HexKey
    -> ByteString
    -> Transaction
        m
        cf
        AllColumns
        ops
        (Maybe ByteString)
unifiedLookup pfx k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            pfx
            fromHexKVIdentity
            mpfHashing
            TrieNodes
            hexKey
    pure $ case mProof of
        Nothing -> Nothing
        Just _ -> Just (hashBS k)

unifiedGetRoot
    :: (Monad m)
    => HexKey
    -> Transaction m cf AllColumns ops Root
unifiedGetRoot pfx = do
    mi <- KV.query TrieNodes pfx
    pure $ case mi of
        Nothing -> Root BS.empty
        Just
            HexIndirect
                { hexIsLeaf
                , hexJump
                , hexValue
                } ->
                Root
                    $ renderMPFHash
                    $ if hexIsLeaf
                        then
                            leafHash
                                mpfHashing
                                hexJump
                                hexValue
                        else hexValue

unifiedGetProof
    :: (Monad m)
    => HexKey
    -> ByteString
    -> Transaction
        m
        cf
        AllColumns
        ops
        (Maybe Proof)
unifiedGetProof pfx k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            pfx
            fromHexKVIdentity
            mpfHashing
            TrieNodes
            hexKey
    pure $ fmap (Proof . serializeProof) mProof

unifiedGetProofSteps
    :: (Monad m)
    => HexKey
    -> ByteString
    -> Transaction
        m
        cf
        AllColumns
        ops
        (Maybe [ProofStep])
unifiedGetProofSteps pfx k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            pfx
            fromHexKVIdentity
            mpfHashing
            TrieNodes
            hexKey
    pure $ fmap toProofSteps mProof

-- --------------------------------------------------------
-- Transactional TrieManager (AllColumns)
-- --------------------------------------------------------

-- | A 'TrieManager' operating on 'AllColumns'
-- selectors within a transaction. Visibility is
-- checked via 'TrieMeta' queries (no 'IORef's).
-- Compose with other transactional operations into
-- a single atomic block commit.
mkUnifiedTrieManager
    :: (Monad m)
    => TrieManager
        (Transaction m cf AllColumns ops)
mkUnifiedTrieManager =
    TrieManager
        { withTrie = \tid action -> do
            mStatus <- KV.query TrieMeta tid
            case mStatus of
                Just Visible ->
                    action
                        (mkUnifiedTrie (tokenHexPrefix tid))
                Just Hidden ->
                    error
                        $ "Trie is hidden: "
                            ++ show tid
                Nothing ->
                    error
                        $ "Trie not found: "
                            ++ show tid
        , withSpeculativeTrie = \_ _ ->
            error
                "withSpeculativeTrie: not \
                \supported in Transaction"
        , createTrie = \tid -> do
            -- Delete any existing data
            deleteSubtree
                TrieNodes
                (tokenHexPrefix tid)
            -- Write registry entry
            KV.insert TrieMeta tid Visible
        , deleteTrie = \tid -> do
            deleteSubtree
                TrieNodes
                (tokenHexPrefix tid)
            KV.delete TrieMeta tid
        , hideTrie = \tid ->
            KV.insert TrieMeta tid Hidden
        , unhideTrie = \tid ->
            KV.insert TrieMeta tid Visible
        }

-- --------------------------------------------------------
-- IO layer (with IORef caches)
-- --------------------------------------------------------

-- | Create a persistent 'TrieManager IO' backed by
-- shared RocksDB column families. Each token's trie
-- data is isolated via key prefixing. On startup,
-- scans the @metaCF@ to rebuild the known\/hidden
-- sets so tries survive restarts.
--
-- This IO-based manager uses 'IORef' caches for
-- fast visibility checks. Use for outside-block
-- operations ('TxBuilder' speculation) and for
-- serving queries while ChainSync is idle.
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

-- --------------------------------------------------------
-- IO-layer: withTrie / withSpeculativeTrie
-- --------------------------------------------------------

-- | Run an action with a token's trie (IO layer).
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
-- token's trie. Buffers writes, discards on return.
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
                                mkSpeculativeTrie
                            )
                    else
                        error
                            $ "Trie not found: "
                                ++ show tid

-- --------------------------------------------------------
-- IO-layer: create / delete / hide / unhide
-- --------------------------------------------------------

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
-- Prefixed Database (for IO-layer and speculation)
-- --------------------------------------------------------

-- | Create a 'Database' that prefixes all keys
-- with the given prefix. Used by the IO layer and
-- speculative sessions.
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
    let trieDB =
            Database
                { valueAt = \cf key ->
                    getCF db cf (pfx <> key)
                , applyOps = write db
                , mkOperation = \cf key mv ->
                    case mv of
                        Just v ->
                            PutCF cf (pfx <> key) v
                        Nothing ->
                            DelCF cf (pfx <> key)
                , columns =
                    fromPairList
                        [ MPFStandaloneKVCol
                            :=> Column
                                { family = kvCF
                                , codecs =
                                    Codecs
                                        { keyCodec =
                                            hexKeyPrism
                                        , valueCodec =
                                            isoMPFHash'
                                        }
                                }
                        , MPFStandaloneMPFCol
                            :=> Column
                                { family = nodesCF
                                , codecs =
                                    mpfCodecs
                                        isoMPFHash'
                                }
                        ]
                , newIterator = \cf ->
                    mkPrefixedIterator db cf pfx
                , withSnapshot = \f -> f trieDB
                }
    in  trieDB
  where
    isoMPFHash' :: Prism' ByteString MPFHash
    isoMPFHash' = mpfValueCodec mpfHashCodecs

-- --------------------------------------------------------
-- Prefixed iterator (for IO layer)
-- --------------------------------------------------------

mkPrefixedIterator
    :: DB
    -> ColumnFamily
    -> ByteString
    -> IO (QueryIterator IO)
mkPrefixedIterator db cf pfx = do
    i <- createIterator db (Just cf)
    pure
        QueryIterator
            { step = \case
                PosFirst -> iterSeek i pfx
                PosLast -> do
                    iterSeek i (incrementPrefix pfx)
                    v <- iterValid i
                    if v
                        then iterPrev i
                        else iterLast i
                PosNext -> iterNext i
                PosPrev -> iterPrev i
                PosAny k -> iterSeek i (pfx <> k)
                PosDestroy -> destroyIterator i
            , isValid = do
                v <- iterValid i
                if v
                    then do
                        me <- iterEntry i
                        case me of
                            Just (k, _) ->
                                pure
                                    (pfx
                                        `BS.isPrefixOf` k
                                    )
                            Nothing -> pure False
                    else pure False
            , entry = do
                me <- iterEntry i
                case me of
                    Just (k, v)
                        | pfx `BS.isPrefixOf` k ->
                            pure
                                $ Just
                                    ( BS.drop
                                        (BS.length pfx)
                                        k
                                    , v
                                    )
                    _ -> pure Nothing
            }

incrementPrefix :: ByteString -> ByteString
incrementPrefix bs
    | BS.null bs = BS.singleton 0
    | otherwise =
        let lastByte = BS.last bs
        in  if lastByte == 0xFF
                then
                    incrementPrefix (BS.init bs)
                        <> BS.singleton 0
                else
                    BS.init bs
                        <> BS.singleton
                            (lastByte + 1)

-- --------------------------------------------------------
-- IO-layer Trie (from prefixed Database)
-- --------------------------------------------------------

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
        , getProof =
            persistentGetProof database
        , getProofSteps =
            persistentGetProofSteps database
        }

-- | A 'Trie' for speculative (dry-run) sessions.
mkSpeculativeTrie
    :: Trie
        ( Transaction
            IO
            ColumnFamily
            ( MPFStandalone
                HexKey
                MPFHash
                MPFHash
            )
            BatchOp
        )
mkSpeculativeTrie =
    Trie
        { insert = speculativeInsert
        , delete = speculativeDelete
        , lookup = speculativeLookup
        , getRoot = speculativeGetRoot
        , getProof = speculativeGetProof
        , getProofSteps =
            speculativeGetProofSteps
        }

-- --------------------------------------------------------
-- IO-layer trie operations
-- --------------------------------------------------------

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
            []
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneKVCol
            MPFStandaloneMPFCol
            (byteStringToHexKey (hashBS k))
            (mkMPFHash v)
        speculativeGetRoot

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
            []
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneKVCol
            MPFStandaloneMPFCol
            (byteStringToHexKey (hashBS k))
        speculativeGetRoot

persistentLookup
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe ByteString)
persistentLookup database k =
    runTransactionUnguarded database
        $ speculativeLookup k

persistentGetRoot
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> IO Root
persistentGetRoot database =
    runTransactionUnguarded
        database
        speculativeGetRoot

persistentGetProof
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe Proof)
persistentGetProof database k =
    runTransactionUnguarded database
        $ speculativeGetProof k

persistentGetProofSteps
    :: Database
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
    -> ByteString
    -> IO (Maybe [ProofStep])
persistentGetProofSteps database k =
    runTransactionUnguarded database
        $ speculativeGetProofSteps k

-- --------------------------------------------------------
-- Speculative trie operations (MPFStandalone cols)
-- --------------------------------------------------------

speculativeInsert
    :: ByteString
    -> ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
speculativeInsert k v = do
    inserting
        []
        fromHexKVIdentity
        mpfHashing
        MPFStandaloneKVCol
        MPFStandaloneMPFCol
        (byteStringToHexKey (hashBS k))
        (mkMPFHash v)
    speculativeGetRoot

speculativeDelete
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
speculativeDelete k = do
    deleting
        []
        fromHexKVIdentity
        mpfHashing
        MPFStandaloneKVCol
        MPFStandaloneMPFCol
        (byteStringToHexKey (hashBS k))
    speculativeGetRoot

speculativeLookup
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe ByteString)
speculativeLookup k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            []
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneMPFCol
            hexKey
    pure $ case mProof of
        Nothing -> Nothing
        Just _ -> Just (hashBS k)

speculativeGetRoot
    :: Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        Root
speculativeGetRoot = do
    mi <- KV.query MPFStandaloneMPFCol []
    pure $ case mi of
        Nothing -> Root BS.empty
        Just
            HexIndirect
                { hexIsLeaf
                , hexJump
                , hexValue
                } ->
                Root
                    $ renderMPFHash
                    $ if hexIsLeaf
                        then
                            leafHash
                                mpfHashing
                                hexJump
                                hexValue
                        else hexValue

speculativeGetProof
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe Proof)
speculativeGetProof k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            []
            fromHexKVIdentity
            mpfHashing
            MPFStandaloneMPFCol
            hexKey
    pure $ fmap (Proof . serializeProof) mProof

speculativeGetProofSteps
    :: ByteString
    -> Transaction
        IO
        ColumnFamily
        (MPFStandalone HexKey MPFHash MPFHash)
        BatchOp
        (Maybe [ProofStep])
speculativeGetProofSteps k = do
    let hexKey =
            byteStringToHexKey (hashBS k)
    mProof <-
        mkMPFInclusionProof
            []
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

-- | Decode raw asset name bytes back to
-- 'TokenId'.
tokenIdFromKey :: ByteString -> TokenId
tokenIdFromKey =
    TokenId . AssetName . SBS.toShort

encodeTrieStatus :: TrieStatus -> ByteString
encodeTrieStatus Visible = BS.singleton 0x01
encodeTrieStatus Hidden = BS.singleton 0x02

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
                        | pfx `BS.isPrefixOf` k ->
                            do
                                iterNext i
                                go
                                    i
                                    (DelCF cf k : acc)
                    _ -> pure (reverse acc)
            else pure (reverse acc)
