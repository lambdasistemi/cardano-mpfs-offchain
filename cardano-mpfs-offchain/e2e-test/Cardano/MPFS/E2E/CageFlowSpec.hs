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
    ( bodyTxL
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
import Cardano.Tx.Ledger (ConwayTx)

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
    , LocatedTokenState (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( awaitProofReadsReady
    , walletBootInputs
    , withBootFactsTxBuilder
    )
import Cardano.MPFS.Provider
    ( Provider (..)
    , SlotNo (..)
    )
import Cardano.MPFS.State
    ( Requests (..)
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
            eScripts <-
                runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it
                        ( "blueprint error: "
                            <> err
                        )
                        (expectationFailure err)
                Right scripts -> do
                    cageFlowSpec scripts
                    deleteFlowSpec scripts
                    rejectFlowSpec scripts

-- ---------------------------------------------------------
-- Test implementation
-- ---------------------------------------------------------

-- | Full cage flow via CageFollower: boot, request,
-- update, and retract — all auto-indexed.
cageFlowSpec :: CageScripts -> Spec
cageFlowSpec scripts =
    it "full cage flow via CageFollower" $ do
        withE2E scripts $ \cfg ctx -> do
            -- Step 1: Boot token
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

            -- Step 2: Poll until boot auto-indexed
            LocatedTokenState _ ts <-
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
                        emptySnap
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
                        emptySnap
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
                        emptySnap
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
                        emptySnap
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

-- | Delete and mixed batch via CageFollower:
-- insert → update → delete → update → mixed batch.
deleteFlowSpec :: CageScripts -> Spec
deleteFlowSpec scripts =
    it "delete and mixed batch via CageFollower"
        $ do
            withE2E scripts $ \cfg ctx -> do
                -- Boot
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
                _ <-
                    pollOrFail 30 "boot"
                        $ getToken
                            (tokens (state ctx))
                            tokenId

                -- Insert "aaa"
                _ <-
                    buildAndSubmit ctx
                        $ requestInsert
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "aaa"
                            "val1"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-aaa" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if null rs
                            then pure Nothing
                            else pure (Just rs)

                -- Update (insert "aaa")
                _ <-
                    buildAndSubmit ctx
                        $ updateToken
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            genesisAddr
                -- Wait for trie root to update
                -- (follower must commit)
                _ <-
                    pollOrFail 60 "update-insert"
                        $ do
                            mTs <-
                                getToken
                                    (tokens (state ctx))
                                    tokenId
                            pure $ case mTs of
                                Just (LocatedTokenState _ t)
                                    | root t
                                        /= Root
                                            ( BS.replicate
                                                32
                                                0
                                            ) ->
                                        Just ()
                                _ -> Nothing

                -- Delete "aaa" (value "val1")
                _ <-
                    buildAndSubmit ctx
                        $ requestDelete
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "aaa"
                            "val1"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-del" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if null rs
                            then pure Nothing
                            else pure (Just rs)

                -- Save pre-delete root
                Just (LocatedTokenState _ preDelTs) <-
                    getToken
                        (tokens (state ctx))
                        tokenId
                let preDelRoot = root preDelTs
                -- Update (delete "aaa")
                _ <-
                    buildAndSubmit ctx
                        $ updateToken
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            genesisAddr
                _ <-
                    pollOrFail 60 "update-delete"
                        $ do
                            mTs <-
                                getToken
                                    (tokens (state ctx))
                                    tokenId
                            pure $ case mTs of
                                Just (LocatedTokenState _ t)
                                    | root t
                                        /= preDelRoot ->
                                        Just ()
                                _ -> Nothing

                -- Mixed batch: insert "bbb" +
                -- insert "ccc"
                _ <-
                    buildAndSubmit ctx
                        $ requestInsert
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "bbb"
                            "val2"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-bbb" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if null rs
                            then pure Nothing
                            else pure (Just ())
                _ <-
                    buildAndSubmit ctx
                        $ requestInsert
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "ccc"
                            "val3"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-ccc" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if length rs >= 2
                            then pure (Just ())
                            else pure Nothing

                -- Save pre-batch root
                Just (LocatedTokenState _ preBatchTs) <-
                    getToken
                        (tokens (state ctx))
                        tokenId
                let preBatchRoot = root preBatchTs
                -- Update (batch insert bbb+ccc)
                _ <-
                    buildAndSubmit ctx
                        $ updateToken
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            genesisAddr
                _ <-
                    pollOrFail 60 "update-batch"
                        $ do
                            mTs <-
                                getToken
                                    (tokens (state ctx))
                                    tokenId
                            pure $ case mTs of
                                Just (LocatedTokenState _ t)
                                    | root t
                                        /= preBatchRoot ->
                                        Just ()
                                _ -> Nothing

                -- Mixed: delete "bbb" + insert "ddd"
                _ <-
                    buildAndSubmit ctx
                        $ requestDelete
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "bbb"
                            "val2"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-del2" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if null rs
                            then pure Nothing
                            else pure (Just ())
                _ <-
                    buildAndSubmit ctx
                        $ requestInsert
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "ddd"
                            "val4"
                            genesisAddr
                _ <-
                    pollOrFail 30 "request-ddd" $ do
                        rs <-
                            requestsByToken
                                ( requests
                                    (state ctx)
                                )
                                tokenId
                        if length rs >= 2
                            then pure (Just ())
                            else pure Nothing

                -- Save pre-mixed root
                Just (LocatedTokenState _ preMixedTs) <-
                    getToken
                        (tokens (state ctx))
                        tokenId
                let preMixedRoot = root preMixedTs
                -- Update (mixed: delete bbb +
                -- insert ddd)
                _ <-
                    buildAndSubmit ctx
                        $ updateToken
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            genesisAddr
                pollOrFail 60 "update-mixed" $ do
                    mTs <-
                        getToken
                            (tokens (state ctx))
                            tokenId
                    pure $ case mTs of
                        Just (LocatedTokenState _ t)
                            | root t
                                /= preMixedRoot ->
                                Just ()
                        _ -> Nothing

-- | Reject via CageFollower:
-- boot → insert request → wait Phase 3 → reject
-- → verify requests consumed, root unchanged.
rejectFlowSpec :: CageScripts -> Spec
rejectFlowSpec scripts =
    it "reject expired requests via CageFollower"
        $ do
            withE2E scripts $ \cfg ctx -> do
                -- Boot
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
                _ <-
                    pollOrFail 30 "boot"
                        $ getToken
                            (tokens (state ctx))
                            tokenId

                -- Insert request
                _ <-
                    buildAndSubmit ctx
                        $ requestInsert
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            "reject-me"
                            "val"
                            genesisAddr
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

                -- Wait for Phase 3
                -- process_time=15s, retract_time=15s
                -- Phase 3 starts at submitted_at+30s
                threadDelay 32_000_000

                -- Record root before reject
                Just (LocatedTokenState _ preRejectTs) <-
                    getToken
                        (tokens (state ctx))
                        tokenId
                let preRejectRoot = root preRejectTs

                -- Reject
                _ <-
                    buildAndSubmit ctx
                        $ rejectRequests
                            (txBuilder ctx)
                            emptySnap
                            tokenId
                            genesisAddr

                -- Verify requests consumed
                pollOrFail 60 "reject" $ do
                    rs <-
                        requestsByToken
                            (requests (state ctx))
                            tokenId
                    if null rs
                        then do
                            -- Verify root unchanged
                            mTs <-
                                getToken
                                    ( tokens
                                        (state ctx)
                                    )
                                    tokenId
                            pure $ case mTs of
                                Just (LocatedTokenState _ t)
                                    | root t
                                        == preRejectRoot ->
                                        Just ()
                                _ -> Nothing
                        else pure Nothing

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

-- | Start a devnet node, wire a full 'Context IO'
-- with CageFollower enabled, wait for N2C to
-- connect, then run the action.
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
        withSystemTempDirectory "mpfs-cageflow"
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
                    awaitProofReadsReady ctx'
                    action cfg ctx'

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
    -> IO (ProofEnvelope p)
    -> IO ConwayTx
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
    :: CageConfig -> ConwayTx -> TokenId
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

-- | Build a 'CageConfig' from state and request script bytes.
cageCfg
    :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes, mStakingBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 15_000
        , defaultRetractTime = 15_000
        , defaultTip = Coin 100_000
        , network = Testnet
        , cfgStakeScript =
            fmap
                (\bs -> (bs, computeScriptHash bs))
                mStakingBytes
        }
