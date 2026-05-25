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
    , PureTrieState (..)
    , getRootFromDb
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef
    ( IORef
    , atomicModifyIORef'
    , newIORef
    , readIORef
    )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map

import MPF.Backend.Pure
    ( MPFInMemoryDB
    , emptyMPFInMemoryDB
    , runMPFPure
    )
import MPF.Hashes
    ( mkMPFHash
    , renderMPFHash
    )
import MPF.Interface (HexKey, byteStringToHexKey)
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

data PureTrieState = PureTrieState
    { ptsMpfDb :: MPFInMemoryDB
    , ptsRawValues :: Map HexKey ByteString
    }

-- | Create a new empty 'Trie IO' backed by a fresh
-- 'IORef' holding an empty in-memory MPF database.
mkPureTrie :: IO (Trie IO)
mkPureTrie = do
    ref <-
        newIORef
            $ PureTrieState
                { ptsMpfDb = emptyMPFInMemoryDB
                , ptsRawValues = Map.empty
                }
    pure (mkPureTrieFromRef ref)

-- | Build a 'Trie IO' from an existing 'IORef'.
-- Allows sharing the database with a 'TrieManager'.
mkPureTrieFromRef :: IORef PureTrieState -> Trie IO
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
    :: IORef PureTrieState
    -> ByteString
    -> ByteString
    -> IO Root
pureInsert ref k v =
    atomicModifyIORef' ref
        $ \state ->
            let hexKey = rawValueKey k
                ((), db') =
                    runMPFPure
                        (ptsMpfDb state)
                        (insertByteStringM k v)
                state' =
                    state
                        { ptsMpfDb = db'
                        , ptsRawValues =
                            Map.insert
                                hexKey
                                v
                                (ptsRawValues state)
                        }
            in  (state', rootFromDb db')

-- | Delete a key from the trie.
pureDelete
    :: IORef PureTrieState
    -> ByteString
    -> IO Root
pureDelete ref k =
    atomicModifyIORef' ref
        $ \state ->
            let hexKey = rawValueKey k
                ((), db') =
                    runMPFPure
                        (ptsMpfDb state)
                        (deleteMPFM hexKey)
                state' =
                    state
                        { ptsMpfDb = db'
                        , ptsRawValues =
                            Map.delete
                                hexKey
                                (ptsRawValues state)
                        }
            in  (state', rootFromDb db')

-- | Look up a value by key. Returns the raw bytes
-- if the key exists in the trie.
pureLookup
    :: IORef PureTrieState
    -> ByteString
    -> IO (Maybe ByteString)
pureLookup ref k = do
    state <- readIORef ref
    let hexKey = rawValueKey k
        (mProof, _) =
            runMPFPure
                (ptsMpfDb state)
                (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just _ -> Map.lookup hexKey (ptsRawValues state)

-- | Get current root hash.
pureGetRoot :: IORef PureTrieState -> IO Root
pureGetRoot ref =
    getRootFromDb . ptsMpfDb =<< readIORef ref

-- | Get root hash from a database snapshot.
getRootFromDb :: MPFInMemoryDB -> IO Root
getRootFromDb = pure . rootFromDb

rootFromDb :: MPFInMemoryDB -> Root
rootFromDb db =
    let (mHash, _) = runMPFPure db getRootHashM
    in  case mHash of
            Nothing -> Root (B.replicate 32 0)
            Just h -> Root (renderMPFHash h)

-- | Generate a Merkle proof for a key.
pureGetProof
    :: IORef PureTrieState
    -> ByteString
    -> IO (Maybe Proof)
pureGetProof ref k = do
    state <- readIORef ref
    let hexKey = rawValueKey k
        (mProof, _) =
            runMPFPure
                (ptsMpfDb state)
                (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just proof ->
            Just (Proof (serializeProof proof))

-- | Generate on-chain proof steps for a key.
pureGetProofSteps
    :: IORef PureTrieState
    -> ByteString
    -> IO (Maybe [ProofStep])
pureGetProofSteps ref k = do
    state <- readIORef ref
    let hexKey = rawValueKey k
        (mProof, _) =
            runMPFPure
                (ptsMpfDb state)
                (proofMPFM hexKey)
    pure $ case mProof of
        Nothing -> Nothing
        Just proof -> Just (toProofSteps proof)

rawValueKey :: ByteString -> HexKey
rawValueKey =
    byteStringToHexKey
        . renderMPFHash
        . mkMPFHash
