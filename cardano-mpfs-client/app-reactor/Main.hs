-- |
-- Module      : Main
-- Description : Cross-target verifier reactor entry point.
--
-- Reads a JSON request envelope on stdin and writes a deterministic
-- one-line verdict on stdout, via the pure
-- 'Cardano.MPFS.Client.Verify.Reactor.runEnvelope'. The same binary is
-- compiled to native, WASM (wasm32-wasi reactor), and GHC-JS; the
-- cross-target suite asserts the three produce byte-identical output for
-- the same input (constitution IX).
module Main (main) where

import Cardano.MPFS.Client.Verify.Reactor (runEnvelope)
import Data.ByteString qualified as BS

main :: IO ()
main = BS.getContents >>= BS.putStr . runEnvelope
