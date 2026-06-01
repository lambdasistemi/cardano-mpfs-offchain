-- |
-- Module      : Main
-- Description : Cross-target cage reactor entry point.
module Main (main) where

import Cardano.MPFS.Client.Cage.Reactor (runCageEnvelope)
import Data.ByteString qualified as BS

main :: IO ()
main = BS.getContents >>= BS.putStr . runCageEnvelope
