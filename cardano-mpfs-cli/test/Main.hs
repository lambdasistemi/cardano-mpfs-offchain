-- |
-- Module      : Main
-- Description : mpfs-cli unit test entry point.
module Main (main) where

import Cardano.MPFS.CLI.OptionsSpec qualified as OptionsSpec
import Cardano.MPFS.CLI.SignSpec qualified as SignSpec
import Test.Hspec (hspec)

main :: IO ()
main = hspec $ do
    OptionsSpec.spec
    SignSpec.spec
