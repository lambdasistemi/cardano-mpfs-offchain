-- |
-- Module      : Cardano.MPFS.CLI.Output
-- Description : stdout/stderr separation for a scriptable CLI.
--
-- The CLI contract is: a single JSON object per invocation on stdout,
-- all human-readable diagnostics on stderr. This module is the only
-- place that writes to either stream so the contract stays in one spot.
module Cardano.MPFS.CLI.Output
    ( emitJson
    , emitResult
    , logErr
    , die
    ) where

import Data.Aeson (ToJSON, Value, encode, toJSON)
import Data.ByteString.Lazy.Char8 qualified as BL8
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

-- | Write a single JSON value to stdout, newline-terminated.
emitJson :: Value -> IO ()
emitJson = BL8.putStrLn . encode

-- | Write any JSON-encodable result to stdout.
emitResult :: ToJSON a => a -> IO ()
emitResult = emitJson . toJSON

-- | Write a diagnostic line to stderr, prefixed with the tool name.
logErr :: String -> IO ()
logErr = hPutStrLn stderr . ("mpfs-cli: " <>)

-- | Log a fatal diagnostic to stderr and exit non-zero. stdout stays
-- empty so callers never parse a half-result.
die :: String -> IO a
die msg = logErr msg >> exitFailure
