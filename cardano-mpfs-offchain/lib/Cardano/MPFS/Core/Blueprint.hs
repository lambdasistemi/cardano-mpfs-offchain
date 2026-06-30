{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.Core.Blueprint
-- Description : CIP-57 blueprint (re-exports from cage)
-- License     : Apache-2.0
--
-- Re-exports CIP-57 blueprint parsing from
-- @cardano-mpfs-cage@ and provides offchain-specific
-- helpers for loading the split state/request scripts.
module Cardano.MPFS.Core.Blueprint
    ( -- * Schema types
      Blueprint (..)
    , Validator (..)
    , Schema (..)
    , Constructor (..)

      -- * Loading
    , loadBlueprint
    , CageScripts
    , loadCageScripts

      -- * Validation
    , validateData

      -- * Script hash extraction
    , extractScriptHash

      -- * Compiled code extraction
    , extractCompiledCode

      -- * Parameter application
    , applyDataParam
    , applyBytesParam
    , applyOutputRef
    , applyRequestParams
    ) where

import Data.ByteString.Short qualified as SBS
import Data.Text qualified as T

import Cardano.MPFS.Cage.Blueprint
    ( Blueprint (..)
    , Constructor (..)
    , Schema (..)
    , Validator (..)
    , applyBytesParam
    , applyDataParam
    , applyOutputRef
    , applyRequestParams
    , extractCompiledCode
    , extractScriptHash
    , loadBlueprint
    , validateData
    )

-- | Unparameterized state, request, and optional staking
-- validator UPLC bytes loaded from the upstream
-- split-validator blueprint.
type CageScripts =
    ( SBS.ShortByteString
    , SBS.ShortByteString
    , Maybe SBS.ShortByteString
    )

-- | Load the state, request, and optional staking
-- validator compiled code from a split-validator cage
-- blueprint.
loadCageScripts :: FilePath -> IO (Either String CageScripts)
loadCageScripts path = do
    ebp <- loadBlueprint path
    pure $ do
        bp <- ebp
        stateBytes <- requireCompiledCode "state.state" bp
        requestBytes <-
            requireCompiledCode "request.request" bp
        let stakingBytes =
                extractCompiledCode "staking.staking" bp
        pure (stateBytes, requestBytes, stakingBytes)
  where
    requireCompiledCode prefix bp =
        maybe
            ( Left
                $ "compiled code not found in blueprint: "
                    <> T.unpack prefix
            )
            Right
            (extractCompiledCode prefix bp)
