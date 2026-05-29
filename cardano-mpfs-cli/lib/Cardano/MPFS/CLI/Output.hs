-- |
-- Module      : Cardano.MPFS.CLI.Output
-- Description : stdout/stderr separation for a scriptable CLI.
--
-- The CLI contract is: a single JSON object per invocation on stdout,
-- all human-readable diagnostics on stderr. This module is the only
-- place that writes to either stream so the contract stays in one spot.
module Cardano.MPFS.CLI.Output
    ( emitJson
    , logErr
    ) where

import Data.Aeson (Value, encode)
import Data.ByteString.Lazy.Char8 qualified as BL8
import System.IO (hPutStrLn, stderr)

-- | Write a single JSON value to stdout, newline-terminated.
emitJson :: Value -> IO ()
emitJson = BL8.putStrLn . encode

-- | Write a diagnostic line to stderr, prefixed with the tool name.
logErr :: String -> IO ()
logErr = hPutStrLn stderr . ("mpfs-cli: " <>)
