-- |
-- Module      : Cardano.MPFS.Trie.Pure
-- Description : Pure in-memory Trie backed by mts:mpf
-- License     : Apache-2.0
--
-- In-memory implementation of the 'Trie' interface
-- backed by an 'IORef' holding an 'MPFInMemoryDB'
-- from the @mts:mpf@ library.
--
-- All keys and values are hashed through MPF
-- conventions ('mkMPFHash') so proof paths match
-- what the Aiken on-chain validator expects.
--
-- Use 'mkPureTrie' for standalone testing, or
-- 'mkPureTrieFromRef' when sharing the underlying
-- database with a 'PureManager' (see
-- "Cardano.MPFS.Trie.PureManager").
-- For production use "Cardano.MPFS.Trie.Persistent".
module Cardano.MPFS.Trie.Pure
    ( -- * Construction
      mkPureTrie
    , mkPureTrieFromRef

      -- * Internals (for TrieManager)
    , getRootFromDb
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef
    ( IORef
    , modifyIORef'
    , newIORef
    , readIORef
    )

import MPF.Backend.Pure
    ( MPFInMemoryDB
    , emptyMPFInMemoryDB
    , runMPFPure
    )
import MPF.Hashes
    ( mkMPFHash
    , renderMPFHash
    )
import MPF.Interface (byteStringToHexKey)
import MPF.Test.Lib
    ( deleteMPFM
    , getRootHashM
    , insertByteStringM
    , proofMPFM
    )

import Cardano.MPFS.Core.OnChain (ProofStep)
import Cardano.MPFS.Core.Proof (serializeProof, toProofSteps)
import Cardano.MPFS.Core.Types (Root (..))
import Cardano.MPFS.Trie (Proof (..), Trie (..))

-- | Create a new empty 'Trie IO' backed by a fresh
-- 'IORef' holding an empty in-memory MPF database.
mkPureTrie :: IO (Trie IO)
mkPureTrie = do
    ref <- newIORef emptyMPFInMemoryDB
    pure (mkPureTrieFromRef ref)

-- | Build a 'Trie IO' from an existing 'IORef'.
-- Allows sharing the database with a 'TrieManager'.
mkPureTrieFromRef :: IORef MPFInMemoryDB -> Trie IO
mkPureTrieFromRef ref =
    Trie
        { insert = pureInsert ref
        , delete = pureDelete ref
        , lookup = pureLookup ref
        , getRoot = pureGetRoot ref
        , getProof = pureGetProof ref
        , getProofSteps = pureGetProofSteps ref
        }

-- | Insert a key-value pair. Hashes both key and
-- value to match Aiken-compatible MPF convention.
pureInsert
    :: IORef MPFInMemoryDB
    -> ByteString
    -> ByteString
    -> IO Root
pureInsert ref k v = do
    db <- readIORef ref
    let ((), db') =
            runMPFPure db (insertByteStringM k v)
    modifyIORef' ref (const db')
    getRootFromDb db'

-- | Delete a key from the trie.
pureDelete
    :: IORef MPFInMemoryDB
    -> ByteString
    -> IO Root
pureDelete ref k = do
    db <- readIORef ref
    let hexKey =
            byteStringToHexKey
                $ renderMPFHash
                $ mkMPFHash k
        ((), db') =
            runMPFPure db (deleteMPFM hexKey)
    modifyIORef' ref (const db')
    getRootFromDb db'

-- | Look up a value by key. Returns the raw hash
-- bytes if the key exists in the trie.
pureLookup
    :: IORef MPFInMemoryDB
    -> ByteString
    -> IO (Maybe ByteString)
pureLookup ref k = do
    db <- readIORef ref
    let hexKey =
            byteStringToHexKey
                $ renderMPFHash
                $ mkMPFHash k
        (mProof, _) =
            runMPFPure db (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just _ -> Just (renderMPFHash (mkMPFHash k))

-- | Get current root hash.
pureGetRoot :: IORef MPFInMemoryDB -> IO Root
pureGetRoot ref = readIORef ref >>= getRootFromDb

-- | Get root hash from a database snapshot.
getRootFromDb :: MPFInMemoryDB -> IO Root
getRootFromDb db =
    let (mHash, _) = runMPFPure db getRootHashM
    in  pure $ case mHash of
            Nothing -> Root B.empty
            Just h -> Root (renderMPFHash h)

-- | Generate a Merkle proof for a key.
pureGetProof
    :: IORef MPFInMemoryDB
    -> ByteString
    -> IO (Maybe Proof)
pureGetProof ref k = do
    db <- readIORef ref
    let hexKey =
            byteStringToHexKey
                $ renderMPFHash
                $ mkMPFHash k
        (mProof, _) =
            runMPFPure db (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just proof ->
            Just (Proof (serializeProof proof))

-- | Generate on-chain proof steps for a key.
pureGetProofSteps
    :: IORef MPFInMemoryDB
    -> ByteString
    -> IO (Maybe [ProofStep])
pureGetProofSteps ref k = do
    db <- readIORef ref
    let hexKey =
            byteStringToHexKey
                $ renderMPFHash
                $ mkMPFHash k
        (mProof, _) =
            runMPFPure db (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just proof -> Just (toProofSteps proof)
