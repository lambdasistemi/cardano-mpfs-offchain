{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.ChainSyncSpec
-- Description : E2E tests for CageFollower ChainSync processing
-- License     : Apache-2.0
--
-- Exercises the CageFollower's automatic block processing.
-- Unlike 'IndexerSpec' which manually calls 'detectFromTx'
-- and 'applyCageEvent', these tests submit transactions and
-- poll persistent RocksDB state to verify auto-indexing.
module Cardano.MPFS.E2E.ChainSyncSpec (spec) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Lens.Micro ((^.))
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    , shouldSatisfy
    )

import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    )
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Mary.Value (MultiAsset (..))

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( applyVersion
    , extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State
    ( Checkpoints (..)
    , Requests (..)
    , State (..)
    , Tokens (..)
    )
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.TxBuilder (TxBuilder (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , computeScriptHash
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    , keyHashFromSignKey
    )

-- | ChainSync E2E test spec.
-- Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "ChainSync E2E" $ do
    mPath <-
        runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it
                "skipped (MPFS_BLUEPRINT \
                \not set)"
                (pure () :: IO ())
        Just path -> do
            ebp <-
                runIO $ loadBlueprint path
            case ebp of
                Left err ->
                    it
                        ( "blueprint error: "
                            <> err
                        )
                        (expectationFailure err)
                Right bp ->
                    case extractCompiledCode
                        "cage."
                        bp of
                        Nothing ->
                            it "no compiled code"
                                $ expectationFailure
                                    "cage script not \
                                    \found in blueprint"
                        Just scriptBytes ->
                            let applied =
                                    applyVersion
                                        1
                                        scriptBytes
                            in  chainsyncSpecs applied

-- ---------------------------------------------------------
-- Test cases
-- ---------------------------------------------------------

-- | All ChainSync E2E test cases.
chainsyncSpecs :: SBS.ShortByteString -> Spec
chainsyncSpecs scriptBytes = do
    -- Test 1: boot auto-indexes token
    it "boot auto-indexes token" $ do
        withE2E scriptBytes $ \cfg ctx -> do
            -- Submit boot tx
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let tokenId =
                    extractTokenId cfg signedBoot

            -- Poll until CageFollower indexes
            mTs <-
                pollUntilJust 30
                    $ getToken
                        (tokens (state ctx))
                        tokenId
            case mTs of
                Nothing ->
                    expectationFailure
                        "token not auto-indexed \
                        \within timeout"
                Just ts -> do
                    owner ts
                        `shouldBe` keyHashFromSignKey
                            genesisSignKey
                    root ts
                        `shouldBe` Root
                            (BS.replicate 32 0)

    -- Test 2: request auto-indexes
    it "request auto-indexes" $ do
        withE2E scriptBytes $ \cfg ctx -> do
            -- Submit boot tx
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let tokenId =
                    extractTokenId cfg signedBoot

            -- Poll until boot is auto-indexed
            mBoot <-
                pollUntilJust 30
                    $ getToken
                        (tokens (state ctx))
                        tokenId
            case mBoot of
                Nothing ->
                    expectationFailure
                        "boot not auto-indexed \
                        \within timeout"
                Just _ -> do
                    -- Submit request tx
                    _ <-
                        buildAndSubmit ctx
                            $ requestInsert
                                (txBuilder ctx)
                                tokenId
                                "hello"
                                "world"
                                genesisAddr

                    -- Poll until request is
                    -- auto-indexed
                    mReqs <-
                        pollUntilJust 30 $ do
                            rs <-
                                requestsByToken
                                    ( requests
                                        (state ctx)
                                    )
                                    tokenId
                            if null rs
                                then pure Nothing
                                else pure (Just rs)
                    case mReqs of
                        Nothing ->
                            expectationFailure
                                "request not \
                                \auto-indexed \
                                \within timeout"
                        Just rs ->
                            rs
                                `shouldSatisfy` any
                                    ( \r ->
                                        requestKey r
                                            == "hello"
                                            && requestValue
                                                r
                                                == Insert
                                                    "world"
                                    )

    -- Test 3: checkpoint tracks processed blocks
    it "checkpoint tracks processed blocks" $ do
        withE2E scriptBytes $ \cfg ctx -> do
            -- Submit boot tx to ensure blocks
            -- are being processed
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let tokenId =
                    extractTokenId cfg signedBoot

            -- Poll until boot is auto-indexed
            mBoot <-
                pollUntilJust 30
                    $ getToken
                        (tokens (state ctx))
                        tokenId
            case mBoot of
                Nothing ->
                    expectationFailure
                        "boot not auto-indexed \
                        \within timeout"
                Just _ -> do
                    -- Checkpoint should be set
                    mCp <-
                        getCheckpoint
                            (checkpoints (state ctx))
                    case mCp of
                        Nothing ->
                            expectationFailure
                                "no checkpoint \
                                \after processing"
                        Just (SlotNo s, _) ->
                            s
                                `shouldSatisfy` (> 0)

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

-- | Start a devnet node, wire a full 'Context IO'
-- with CageFollower, wait for N2C to connect,
-- then run the action.
withE2E
    :: SBS.ShortByteString
    -> ( CageConfig
         -> Context IO
         -> IO a
       )
    -> IO a
withE2E scriptBytes action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock startMs ->
        withSystemTempDirectory "mpfs-chainsync"
            $ \tmpDir -> do
                let dbDir =
                        tmpDir </> "db"
                    genesisJson =
                        gDir
                            </> "shelley-genesis.json"
                let cfg =
                        cageCfg scriptBytes startMs
                    appCfg =
                        AppConfig
                            { epochSlots =
                                EpochSlots 4320
                            , shelleyGenesisPath =
                                genesisJson
                            , socketPath = sock
                            , dbPath = dbDir
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , byronGenesisPath =
                                Nothing
                            , followerEnabled =
                                True
                            , appTracer =
                                nullTracer
                            }
                withApplication appCfg $ \ctx -> do
                    _ <-
                        queryProtocolParams
                            (provider ctx)
                    -- Let ChainSync catch up to
                    -- the tip before submitting txs
                    threadDelay 10_000_000
                    action cfg ctx

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | Build, sign, submit, and wait for a tx.
buildAndSubmit
    :: Context IO
    -> IO (Tx ConwayEra)
    -> IO (Tx ConwayEra)
buildAndSubmit ctx buildTx = do
    unsigned <- buildTx
    let signed =
            addKeyWitness
                genesisSignKey
                unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx
    pure signed

-- | Assert that a submit result is 'Submitted'.
assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

-- | Wait for a transaction to be confirmed.
awaitTx :: IO ()
awaitTx = threadDelay 5_000_000

-- | Extract the 'TokenId' from a boot
-- transaction's mint field.
extractTokenId
    :: CageConfig -> Tx ConwayEra -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        assets =
            Map.toList
                ( ma
                    Map.! cagePolicyIdFromCfg cfg
                )
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "extractTokenId: \
                    \unexpected assets"

-- | Poll an action until it returns 'Just',
-- with a timeout in seconds. Returns 'Nothing'
-- if the timeout expires.
pollUntilJust
    :: Int -> IO (Maybe a) -> IO (Maybe a)
pollUntilJust timeoutSec action = go attempts
  where
    attempts = timeoutSec * 2
    go 0 = action
    go n = do
        result <- action
        case result of
            Just _ -> pure result
            Nothing ->
                threadDelay 500_000
                    >> go (n - 1)

-- ---------------------------------------------------------
-- Config
-- ---------------------------------------------------------

-- | Build a 'CageConfig' from applied script bytes
-- and the system start time.
cageCfg
    :: SBS.ShortByteString
    -> Integer
    -> CageConfig
cageCfg scriptBytes startMs =
    CageConfig
        { cageScriptBytes = scriptBytes
        , cfgScriptHash =
            computeScriptHash scriptBytes
        , defaultProcessTime = 15_000
        , defaultRetractTime = 15_000
        , defaultMaxFee = Coin 1_000_000
        , network = Testnet
        , systemStartPosixMs = startMs
        , slotLengthMs = 100
        }
