-- |
-- Module      : Main
-- Description : mpfs-verify CLI entry point.
--
-- Reads a proof-bearing JSON response from a file or stdin, extracts
-- the 'VerificationSnapshot' it carries, and runs the offline snapshot
-- verifier. Prints a pass/fail result with the baked-in @utxo_root@
-- and @chainpoint@. Later slices extend this to cover witnessed UTxO
-- and trie proofs as their response shapes ship.
module Main (main) where

import Cardano.MPFS.Client
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    , VerifyError
    , statusSnapshot
    , verifyVerificationSnapshot
    )
import Data.Aeson (Value, eitherDecodeStrict)
import Data.Aeson.Types qualified as Aeson
import Data.ByteString qualified as BS
import Data.Text qualified as T
import Data.Text.IO qualified as TIO
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (hPutStrLn, stderr, stdin)

main :: IO ()
main = do
    args <- getArgs
    bs <- case args of
        [] -> BS.hGetContents stdin
        [path] -> BS.readFile path
        _ -> usage >> exitFailure
    case eitherDecodeStrict bs of
        Left e -> fail' ("invalid JSON: " <> T.pack e)
        Right value -> dispatch value

usage :: IO ()
usage =
    hPutStrLn
        stderr
        "Usage: mpfs-verify [FILE]\n\n\
        \Reads a proof-bearing JSON response (default: stdin) and\n\
        \verifies the snapshot it carries."

dispatch :: Value -> IO ()
dispatch value =
    case Aeson.parse statusSnapshot value of
        Aeson.Error msg ->
            fail' ("could not read snapshot: " <> T.pack msg)
        Aeson.Success Nothing ->
            fail' "no utxo_root present; the indexed snapshot is not ready"
        Aeson.Success (Just snap) -> report snap

report :: VerificationSnapshot -> IO ()
report snap@VerificationSnapshot{..} =
    case verifyVerificationSnapshot snap of
        Left err -> failVerify err snap
        Right () -> do
            TIO.putStrLn "pass"
            TIO.putStrLn $ "  utxo_root:  " <> unHex utxoRoot
            TIO.putStrLn
                $ "  chainpoint: slot "
                    <> T.pack (show (slot chainpoint))
                    <> " block "
                    <> unHex (blockId chainpoint)
            exitSuccess

failVerify :: VerifyError -> VerificationSnapshot -> IO ()
failVerify err VerificationSnapshot{..} = do
    TIO.hPutStrLn stderr "fail"
    TIO.hPutStrLn stderr $ "  utxo_root:  " <> unHex utxoRoot
    TIO.hPutStrLn stderr
        $ "  chainpoint: slot "
            <> T.pack (show (slot chainpoint))
            <> " block "
            <> unHex (blockId chainpoint)
    TIO.hPutStrLn stderr $ "  reason:     " <> T.pack (show err)
    exitFailure

fail' :: T.Text -> IO a
fail' msg = TIO.hPutStrLn stderr msg >> exitFailure
