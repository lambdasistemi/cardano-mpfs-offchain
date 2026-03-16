module Main (main) where

import Cardano.MPFS.HTTP.Swagger (renderSwaggerJSON)
import Data.ByteString.Lazy.Char8 qualified as BL

main :: IO ()
main = BL.putStrLn renderSwaggerJSON
