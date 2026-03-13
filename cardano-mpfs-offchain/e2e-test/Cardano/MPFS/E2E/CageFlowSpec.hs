{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.CageFlowSpec
-- Description : Full cage flow E2E with CageFollower
-- License     : Apache-2.0
--
-- Exercises the full cage protocol (boot, request,
-- update, retract) with @followerEnabled = True@,
-- verifying that the CageFollower auto-indexes all
-- events and that txBuilder operations work against
-- follower-populated state.
module Cardano.MPFS.E2E.CageFlowSpec (spec) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Lens.Micro ((^.))
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO.Temp
    ( withSystemTempDirectory
    )
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    )

import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , txIdTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( mintTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , TxIx (..)
    )
import Cardano.Ledger.Mary.Value
    ( MultiAsset (..)
    )
import Cardano.Ledger.TxIn (TxIn (..))

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
import Cardano.MPFS.Core.Bootstrap.Genesis
    ( generateBootstrapFile
    )
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State
    ( Requests (..)
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
import Cardano.Node.Client.E2E.Devnet
    ( withCardanoNode
    )
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness

    , genesisAddr
    , genesisDir
    , genesisSignKey
    , keyHashFromSignKey
    )

-- | CageFlow E2E test spec.
-- Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "CageFlow E2E" $ do
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
                            in  cageFlowSpec applied

-- ---------------------------------------------------------
-- Test implementation
-- ---------------------------------------------------------

-- | Full cage flow via CageFollower: boot, request,
-- update, and retract — all auto-indexed.
cageFlowSpec :: SBS.ShortByteString -> Spec
cageFlowSpec scriptBytes =
    it "full cage flow via CageFollower" $ do
        withE2E scriptBytes $ \cfg ctx -> do
            -- Step 1: Boot token
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let tokenId =
                    extractTokenId cfg signedBoot

            -- Step 2: Poll until boot auto-indexed
            ts <-
                pollOrFail 30 "boot"
                    $ getToken
                        (tokens (state ctx))
                        tokenId
            owner ts
                `shouldBe` keyHashFromSignKey
                    genesisSignKey
            root ts
                `shouldBe` Root
                    (BS.replicate 32 0)

            -- Step 3: Request insert
            _ <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tokenId
                        "hello"
                        "world"
                        genesisAddr

            -- Step 4: Poll until request
            -- auto-indexed
            _ <-
                pollOrFail 30 "request" $ do
                    rs <-
                        requestsByToken
                            ( requests
                                (state ctx)
                            )
                            tokenId
                    if null rs
                        then pure Nothing
                        else pure (Just rs)

            -- Step 5: Update token (against
            -- follower-populated state)
            _ <-
                buildAndSubmit ctx
                    $ updateToken
                        (txBuilder ctx)
                        tokenId
                        genesisAddr

            -- Step 6: Poll until update reflected
            -- (request consumed)
            _ <-
                pollOrFail 30 "update" $ do
                    rs <-
                        requestsByToken
                            ( requests
                                (state ctx)
                            )
                            tokenId
                    if null rs
                        then pure (Just ())
                        else pure Nothing

            -- Step 7: Second request + wait for
            -- Phase 2
            signedReq2 <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tokenId
                        "bye"
                        "moon"
                        genesisAddr

            _ <-
                pollOrFail 30 "request-2" $ do
                    rs <-
                        requestsByToken
                            ( requests
                                (state ctx)
                            )
                            tokenId
                    if null rs
                        then pure Nothing
                        else pure (Just rs)

            -- Wait for Phase 2
            -- (processTime = 15s)
            threadDelay 17_000_000

            -- Step 8: Retract
            let req2TxIn =
                    TxIn
                        (txIdTx signedReq2)
                        (TxIx 0)
            _ <-
                buildAndSubmit ctx
                    $ retractRequest
                        (txBuilder ctx)
                        req2TxIn
                        genesisAddr

            -- Step 9: Poll until retract reflected
            pollOrFail 30 "retract" $ do
                rs <-
                    requestsByToken
                        (requests (state ctx))
                        tokenId
                if null rs
                    then pure (Just ())
                    else pure Nothing

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

-- | Start a devnet node, wire a full 'Context IO'
-- with CageFollower enabled, wait for N2C to
-- connect, then run the action.
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
        withSystemTempDirectory "mpfs-cageflow"
            $ \tmpDir -> do
                let bsFile =
                        tmpDir </> "bootstrap.cbor"
                    dbDir =
                        tmpDir </> "db"
                    genesisJson =
                        gDir
                            </> "shelley-genesis.json"
                generateBootstrapFile
                    genesisJson
                    bsFile
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
                            , bootstrapFile =
                                Just bsFile
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

-- | Poll an action until 'Just', failing the
-- test on timeout.
pollOrFail
    :: Int -> String -> IO (Maybe a) -> IO a
pollOrFail timeout label action = do
    result <- pollUntilJust timeout action
    case result of
        Nothing -> do
            expectationFailure
                ( label
                    <> " not auto-indexed"
                    <> " within timeout"
                )
            error "unreachable"
        Just x -> pure x

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
