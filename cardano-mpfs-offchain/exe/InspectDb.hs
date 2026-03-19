{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Main
-- Description : Database inspector for MPFS RocksDB
-- License     : Apache-2.0
--
-- Reads an MPFS RocksDB database and reports
-- per-column statistics: entry counts, first\/last
-- keys, and rollback-specific diagnostics (empty vs
-- non-empty inverse lists).
--
-- Usage: @mpfs-inspect-db \/path\/to\/db@
module Main (main) where

import Data.Function (fix)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

import ChainFollower.Rollbacks.Types
    ( RollbackPoint (..)
    )
import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , lastEntry
    , nextEntry
    )
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( KV
    , RunTransaction (..)
    , Transaction
    , iterating
    , mapColumns
    , newRunTransaction
    )
import Database.RocksDB
    ( Config (..)
    , DB (..)
    , withDBCF
    )

import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    , unifiedCodecs
    )
import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.ComposedInv
    ( ComposedInv (..)
    )

-- | Scan a cursor counting entries.
scanCount
    :: (Monad m)
    => m (Maybe (Entry c))
    -- ^ Next entry action
    -> Maybe (Entry c)
    -- ^ Current entry
    -> m Int
scanCount next = go 0
  where
    go n = \case
        Nothing -> pure n
        Just _ -> do
            nxt <- next
            go (n + 1) nxt

main :: IO ()
main = do
    args <- getArgs
    dbPath <- case args of
        [p] -> pure p
        _ -> do
            hPutStrLn
                stderr
                "Usage: mpfs-inspect-db <db-path>"
            exitFailure
    putStrLn $ "Database: " <> dbPath
    putStrLn ""
    withDBCF
        dbPath
        dbConfig{createIfMissing = False}
        allColumnFamilies
        $ \db -> do
            let cols =
                    mkColumns
                        (columnFamilies db)
                        unifiedCodecs
                udb = mkRocksDBDatabase db cols
            RunTransaction run <-
                newRunTransaction udb

            inspectRollbacks run
            inspectColumn run "tokens" CageTokens
            inspectColumn
                run
                "requests"
                CageRequests
            inspectColumn run "cage-cfg" CageCfg
            inspectColumn
                run
                "trie-nodes"
                TrieNodes
            inspectColumn run "trie-kv" TrieKV
            inspectColumn run "trie-meta" TrieMeta

-- | Inspect the unified rollbacks column with
-- breakdown of empty vs non-empty cage inverses.
inspectRollbacks
    :: ( forall a
          . Transaction
                IO
                cf
                ( UnifiedColumns
                    slot
                    hash
                    key
                    value
                )
                op
                a
         -> IO a
       )
    -> IO ()
inspectRollbacks run = do
    (total, cageEmpty) <-
        run
            $ iterating InRollbacks
            $ do
                me <- firstEntry
                ($ (me, 0 :: Int, 0 :: Int))
                    $ fix
                    $ \go -> \case
                        (Nothing, t, e) ->
                            pure (t, e)
                        ( Just Entry{entryValue}
                            , t
                            , e
                            ) -> do
                                let invs =
                                        rpInverses
                                            entryValue
                                    e' =
                                        if all
                                            ( null
                                                . cageInverses
                                            )
                                            invs
                                            then e + 1
                                            else e
                                nxt <- nextEntry
                                go (nxt, t + 1, e')
    mFirst <-
        run
            $ iterating InRollbacks firstEntry
    mLast <-
        run
            $ iterating InRollbacks lastEntry
    putStrLn "rollbacks:"
    putStrLn $ "  total:          " <> show total
    putStrLn
        $ "  cage-empty:     "
            <> show cageEmpty
    putStrLn
        $ "  cage-non-empty: "
            <> show (total - cageEmpty)
    case mFirst of
        Nothing -> pure ()
        Just (Entry k v) -> do
            let invs = rpInverses v
            putStrLn
                $ "  first: slot="
                    <> show k
                    <> " #utxo="
                    <> show
                        ( sum
                            $ map
                                ( length
                                    . utxoInverses
                                )
                                invs
                        )
                    <> " #cage="
                    <> show
                        ( sum
                            $ map
                                ( length
                                    . cageInverses
                                )
                                invs
                        )
    case mLast of
        Nothing -> pure ()
        Just (Entry k v) -> do
            let invs = rpInverses v
            putStrLn
                $ "  last:  slot="
                    <> show k
                    <> " #utxo="
                    <> show
                        ( sum
                            $ map
                                ( length
                                    . utxoInverses
                                )
                                invs
                        )
                    <> " #cage="
                    <> show
                        ( sum
                            $ map
                                ( length
                                    . cageInverses
                                )
                                invs
                        )
    putStrLn ""

-- | Inspect a cage column: count entries and
-- show first\/last keys.
inspectColumn
    :: (Show k)
    => ( forall a
          . Transaction
                IO
                cf
                ( UnifiedColumns
                    slot
                    hash
                    key
                    value
                )
                op
                a
         -> IO a
       )
    -> String
    -- ^ Column label
    -> AllColumns (KV k v)
    -- ^ Column selector
    -> IO ()
inspectColumn run label col = do
    count <-
        run
            $ mapColumns InCage
            $ iterating col
            $ do
                me <- firstEntry
                scanCount nextEntry me
    mFirst <-
        run
            $ mapColumns InCage
            $ iterating col firstEntry
    mLast <-
        run
            $ mapColumns InCage
            $ iterating col lastEntry
    putStrLn $ label <> ":"
    putStrLn $ "  count: " <> show count
    case mFirst of
        Nothing -> pure ()
        Just (Entry k _) ->
            putStrLn $ "  first: " <> show k
    case mLast of
        Nothing -> pure ()
        Just (Entry k _) ->
            putStrLn $ "  last:  " <> show k
    putStrLn ""
