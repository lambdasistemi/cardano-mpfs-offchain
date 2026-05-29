-- |
-- Module      : Main
-- Description : mpfs-cli entry point.
module Main (main) where

import Cardano.MPFS.CLI.Options (commandInfo)
import Cardano.MPFS.CLI.Run (run)
import Options.Applicative (execParser)

main :: IO ()
main = execParser commandInfo >>= run
