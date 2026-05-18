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
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , LocatedRequest (..)
    , LocatedTokenState (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    , withBootFactsTxBuilder
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
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , TxBuilder (..)
    )
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
            eScripts <-
                runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it
                        ( "blueprint error: "
                            <> err
                        )
                        (expectationFailure err)
                Right scripts ->
                    chainsyncSpecs scripts

-- ---------------------------------------------------------
-- Test cases
-- ---------------------------------------------------------

-- | All ChainSync E2E test cases.
chainsyncSpecs :: CageScripts -> Spec
chainsyncSpecs scripts = do
    -- Test 1: boot auto-indexes token
    it "boot auto-indexes token" $ do
        withE2E scripts $ \cfg ctx -> do
            -- Submit boot tx
            bootInputs <-
                walletBootInputs
                    (provider ctx)
                    genesisAddr
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        emptySnap
                        bootInputs
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
                Just (LocatedTokenState _ ts) -> do
                    owner ts
                        `shouldBe` keyHashFromSignKey
                            genesisSignKey
                    root ts
                        `shouldBe` Root
                            (BS.replicate 32 0)

    -- Test 2: request auto-indexes
    it "request auto-indexes" $ do
        withE2E scripts $ \cfg ctx -> do
            -- Submit boot tx
            bootInputs <-
                walletBootInputs
                    (provider ctx)
                    genesisAddr
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        emptySnap
                        bootInputs
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
                                emptySnap
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
                                    ( \(LocatedRequest _ r) ->
                                        requestKey r
                                            == "hello"
                                            && requestValue
                                                r
                                                == Insert
                                                    "world"
                                    )

    -- Test 3: checkpoint tracks processed blocks
    it "checkpoint tracks processed blocks" $ do
        withE2E scripts $ \cfg ctx -> do
            -- Submit boot tx to ensure blocks
            -- are being processed
            bootInputs <-
                walletBootInputs
                    (provider ctx)
                    genesisAddr
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        emptySnap
                        bootInputs
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
    :: CageScripts
    -> ( CageConfig
         -> Context IO
         -> IO a
       )
    -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory "mpfs-chainsync"
            $ \tmpDir -> do
                let dbDir =
                        tmpDir </> "db"
                    genesisJson =
                        gDir
                            </> "shelley-genesis.json"
                let cfg =
                        cageCfg scripts
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
                    let ctx' =
                            withBootFactsTxBuilder cfg ctx
                    _ <-
                        queryProtocolParams
                            (provider ctx')
                    -- Let ChainSync catch up to
                    -- the tip before submitting txs
                    threadDelay 10_000_000
                    action cfg ctx'

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

-- | Build, sign, submit, and wait for a tx.
buildAndSubmit
    :: Context IO
    -> IO (ProofEnvelope p)
    -> IO (Tx ConwayEra)
buildAndSubmit ctx buildBundle = do
    bundle <- buildBundle
    let unsigned = envTx bundle
        signed =
            addKeyWitness
                genesisSignKey
                unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx
    pure signed

-- | Placeholder snapshot used by e2e tests. The
-- builder embeds it verbatim but does not yet use
-- it to drive tx construction.
emptySnap :: BundleSnapshot
emptySnap =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

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

-- | Build a 'CageConfig' from state and request script bytes.
cageCfg
    :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 15_000
        , defaultRetractTime = 15_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
