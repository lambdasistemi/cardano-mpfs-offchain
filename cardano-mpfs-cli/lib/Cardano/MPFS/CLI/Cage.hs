{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module      : Cardano.MPFS.CLI.Cage
-- Description : Resolve the client CageConfig from a blueprint.
--
-- Per A-01: the cage validator scripts come from a blueprint resolved
-- as @--cage-config FILE@ or, failing that, the @MPFS_BLUEPRINT@
-- environment variable; there is no silent built-in blueprint. The
-- blueprint is protocol material the client owns, so the CLI parses it
-- locally (via @cardano-mpfs-cage@) rather than trusting the server for
-- validator-script provenance.
module Cardano.MPFS.CLI.Cage
    ( resolveCageConfig
    , ownerAddressBytes
    ) where

import Cardano.Crypto.DSIGN
    ( Ed25519DSIGN
    , SignKeyDSIGN
    , deriveVerKeyDSIGN
    )
import Cardano.Ledger.Address (Addr (..), serialiseAddr)
import Cardano.Ledger.BaseTypes (Network (Testnet))
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Credential
    ( Credential (KeyHashObj)
    , StakeReference (StakeRefNull)
    )
import Cardano.Ledger.Keys (VKey (..), hashKey)
import Data.ByteString (ByteString)
import Data.ByteString.Short (ShortByteString)
import System.Environment (lookupEnv)

import Cardano.MPFS.Cage.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , applyPreviousPolicies
    , computeScriptHash
    )

-- | Resolve a client 'CageConfig' from the cage blueprint. The path is
-- the @--cage-config@ argument if present, else @MPFS_BLUEPRINT@; if
-- neither is set the CLI fails with a clear message rather than guess.
resolveCageConfig :: Maybe FilePath -> IO (Either String CageConfig)
resolveCageConfig mPath = do
    resolved <- case mPath of
        Just p -> pure (Just p)
        Nothing -> lookupEnv "MPFS_BLUEPRINT"
    case resolved of
        Nothing ->
            pure
                $ Left
                    "no --cage-config FILE supplied and MPFS_BLUEPRINT is \
                    \unset; pass --cage-config or set MPFS_BLUEPRINT to your \
                    \cage blueprint JSON"
        Just path -> do
            blueprint <- loadBlueprint path
            pure $ do
                bp <- blueprint
                stateBytes <- requireCode "state.state" bp
                requestBytes <- requireCode "request.request" bp
                Right (buildCageConfig stateBytes requestBytes)
  where
    requireCode prefix bp =
        maybe
            (Left ("blueprint: compiled code not found: " <> show prefix))
            Right
            (extractCompiledCode prefix bp)

-- | Assemble a 'CageConfig' from the split validator bytes. Timing, tip,
-- and network use sensible defaults (the blueprint carries none); a
-- mainnet or custom-timing flag is a future addition.
buildCageConfig :: ShortByteString -> ShortByteString -> CageConfig
buildCageConfig stateBytes requestBytes =
    let appliedStateBytes = applyPreviousPolicies [] stateBytes
    in  CageConfig
            { cageScriptBytes = appliedStateBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = computeScriptHash appliedStateBytes
            , defaultProcessTime = 300_000
            , defaultRetractTime = 600_000
            , defaultTip = Coin 1_000_000
            , network = Testnet
            }

-- | The requester's enterprise address bytes, as the @POST /facts/*@
-- endpoints expect (the inverse of the server's @decodeAddrEither@):
-- network header + payment key hash, no stake part.
ownerAddressBytes
    :: Network -> SignKeyDSIGN Ed25519DSIGN -> ByteString
ownerAddressBytes net sk =
    serialiseAddr (Addr net (KeyHashObj keyHash) StakeRefNull)
  where
    keyHash = hashKey (VKey (deriveVerKeyDSIGN sk))
