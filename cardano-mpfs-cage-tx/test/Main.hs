module Main (main) where

import Cardano.MPFS.Client.Cage.ReactorSpec qualified as ReactorSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec ReactorSpec.spec
