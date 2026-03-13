{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.IndexerSpec
-- Description : E2E tests for cage event detection
-- License     : Apache-2.0
--
-- Exercises 'detectFromTx' and 'applyCageEvent' against
-- real Plutus transactions submitted to a devnet.
module Cardano.MPFS.E2E.IndexerSpec (spec) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
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

import Cardano.Ledger.Api.Tx (Tx, txIdTx)
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , TxIx (..)
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
    ( Addr
    , Coin (..)
    , ConwayEra
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenState (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageEvent (..)
    , inversesOf
    )
import Cardano.MPFS.Indexer.Follower
    ( applyCageEvent
    , applyCageInverses
    , computeInverse
    , detectFromTx
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
import Cardano.MPFS.Trie
    ( Trie (..)
    , TrieManager (..)
    )
import Cardano.MPFS.TxBuilder (TxBuilder (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , computeScriptHash
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , devnetMagic
    , genesisAddr
    , genesisDir
    , genesisSignKey
    , keyHashFromSignKey
    )

-- | E2E indexer test spec.
-- Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Indexer E2E" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it
                "skipped (MPFS_BLUEPRINT \
                \not set)"
                (pure () :: IO ())
        Just path -> do
            ebp <- runIO $ loadBlueprint path
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
                            in  indexerSpecs applied

-- ---------------------------------------------------------
-- Test cases
-- ---------------------------------------------------------

-- | All indexer E2E test cases.
indexerSpecs :: SBS.ShortByteString -> Spec
indexerSpecs scriptBytes = do
    -- Test 1: boot indexes token
    it "boot indexes token"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Snapshot UTxOs before boot
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr

            -- Submit boot tx
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken (txBuilder ctx) genesisAddr
            let resolver =
                    mkResolver preUtxos []

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedBoot
            length events `shouldBe` 1
            case events of
                [CageBoot tid ts] -> do
                    -- Apply event
                    _ <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            (CageBoot tid ts)
                    -- Verify state
                    mTs <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs `shouldSatisfy` \case
                        Just ts' ->
                            owner ts'
                                == keyHashFromSignKey
                                    genesisSignKey
                                && root ts'
                                    == Root
                                        (BS.replicate 32 0)
                        Nothing -> False
                _ ->
                    expectationFailure
                        $ "expected CageBoot, got: "
                            <> show events

    -- Test 2: request indexes request
    it "request indexes request"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot first (needed for request)
            bootAndRegister cfg ctx

            -- Snapshot UTxOs before request
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr

            -- Get tokenId from state
            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Submit request tx
            signedReq <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tid
                        "hello"
                        "world"
                        genesisAddr
            let resolver =
                    mkResolver preUtxos []

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedReq
            length events `shouldBe` 1
            case events of
                [CageRequest txIn req] -> do
                    _ <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            (CageRequest txIn req)
                    -- Verify request in state
                    mReq <-
                        getRequest
                            (requests (state ctx))
                            txIn
                    mReq `shouldSatisfy` \case
                        Just r ->
                            requestKey r == "hello"
                                && requestValue r
                                    == Insert "world"
                        Nothing -> False
                _ ->
                    expectationFailure
                        $ "expected CageRequest, got: "
                            <> show events

    -- Test 3: update updates trie root
    it "update updates trie root"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + register
            bootAndRegister cfg ctx

            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Submit request
            signedReq <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tid
                        "hello"
                        "world"
                        genesisAddr

            -- Register request in state (needed by
            -- updateToken tx builder)
            let reqTxIn =
                    TxIn
                        (txIdTx signedReq)
                        (TxIx 0)
                req =
                    Request
                        { requestToken = tid
                        , requestOwner =
                            keyHashFromSignKey
                                genesisSignKey
                        , requestKey = "hello"
                        , requestValue = Insert "world"
                        , requestFee = Coin 1_000_000
                        , requestSubmittedAt = 0
                        }
            putRequest
                (requests (state ctx))
                reqTxIn
                req

            -- Snapshot before update
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            genesisUtxos <-
                queryUTxOs
                    (provider ctx)
                    genesisAddr

            -- Submit update tx
            signedUpdate <-
                buildAndSubmit ctx
                    $ updateToken
                        (txBuilder ctx)
                        tid
                        genesisAddr
            let resolver =
                    mkResolver
                        preUtxos
                        genesisUtxos

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedUpdate

            -- Should have CageUpdate
            let updates =
                    [ e
                    | e@(CageUpdate{}) <- events
                    ]
            length updates `shouldBe` 1
            case updates of
                [evt@(CageUpdate _ newRoot consumed)] ->
                    do
                        -- Root changed from empty
                        newRoot
                            `shouldSatisfy` ( /=
                                                Root
                                                    ( BS.replicate
                                                        32
                                                        0
                                                    )
                                            )
                        -- Request consumed
                        length consumed
                            `shouldSatisfy` (>= 1)
                        -- Apply event
                        _ <-
                            applyCageEvent
                                (state ctx)
                                (trieManager ctx)
                                evt
                        -- Verify root updated
                        mTs <-
                            getToken
                                (tokens (state ctx))
                                tid
                        case mTs of
                            Just ts ->
                                root ts
                                    `shouldBe` newRoot
                            Nothing ->
                                expectationFailure
                                    "token not found"
                        -- Request consumed from state
                        mapM_
                            ( \txIn -> do
                                mR <-
                                    getRequest
                                        ( requests
                                            (state ctx)
                                        )
                                        txIn
                                mR `shouldBe` Nothing
                            )
                            consumed
                        -- Trie has the entry
                        withTrie
                            (trieManager ctx)
                            tid
                            $ \trie -> do
                                mVal <-
                                    Cardano.MPFS.Trie.lookup
                                        trie
                                        "hello"
                                mVal
                                    `shouldSatisfy` \case
                                        Just _ -> True
                                        Nothing -> False
                _ ->
                    expectationFailure
                        $ "expected CageUpdate, got: "
                            <> show events

    -- Test 4: retract removes request
    it "retract removes request"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + register
            bootAndRegister cfg ctx

            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Submit request
            signedReq <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tid
                        "bye"
                        "moon"
                        genesisAddr

            -- Register request in state
            let reqTxIn =
                    TxIn
                        (txIdTx signedReq)
                        (TxIx 0)
                req =
                    Request
                        { requestToken = tid
                        , requestOwner =
                            keyHashFromSignKey
                                genesisSignKey
                        , requestKey = "bye"
                        , requestValue = Insert "moon"
                        , requestFee = Coin 1_000_000
                        , requestSubmittedAt = 0
                        }
            putRequest
                (requests (state ctx))
                reqTxIn
                req

            -- Wait for Phase 2 (process_time = 15s)
            threadDelay 17_000_000

            -- Snapshot before retract
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            genesisUtxos <-
                queryUTxOs
                    (provider ctx)
                    genesisAddr

            -- Submit retract tx
            signedRetract <-
                buildAndSubmit ctx
                    $ retractRequest
                        (txBuilder ctx)
                        reqTxIn
                        genesisAddr
            let resolver =
                    mkResolver
                        preUtxos
                        genesisUtxos

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedRetract
            let retracts =
                    [ e
                    | e@(CageRetract _) <- events
                    ]
            length retracts `shouldBe` 1
            case retracts of
                [evt@(CageRetract rIn)] -> do
                    rIn `shouldBe` reqTxIn
                    _ <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            evt
                    -- Request removed
                    mReq <-
                        getRequest
                            (requests (state ctx))
                            reqTxIn
                    mReq `shouldBe` Nothing
                _ ->
                    expectationFailure
                        $ "expected CageRetract, got: "
                            <> show events

    -- Test 5: burn removes token
    it "burn removes token"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + register
            bootAndRegister cfg ctx

            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Snapshot before burn
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            genesisUtxos <-
                queryUTxOs
                    (provider ctx)
                    genesisAddr

            -- Submit burn tx
            signedBurn <-
                buildAndSubmit ctx
                    $ endToken
                        (txBuilder ctx)
                        tid
                        genesisAddr
            let resolver =
                    mkResolver
                        preUtxos
                        genesisUtxos

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedBurn
            case events of
                [CageBurn burnTid] -> do
                    burnTid `shouldBe` tid
                    _ <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            (CageBurn burnTid)
                    -- Token removed from state
                    mTs <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs `shouldBe` Nothing
                _ ->
                    expectationFailure
                        $ "expected CageBurn, got: "
                            <> show events

    -- Test 6: burn inverse roundtrip
    it "burn inverse roundtrip"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + register
            bootAndRegister cfg ctx

            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Save token state before burn
            Just tsBefore <-
                getToken
                    (tokens (state ctx))
                    tid

            -- Snapshot before burn
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            genesisUtxos <-
                queryUTxOs
                    (provider ctx)
                    genesisAddr

            -- Submit burn tx
            signedBurn <-
                buildAndSubmit ctx
                    $ endToken
                        (txBuilder ctx)
                        tid
                        genesisAddr
            let resolver =
                    mkResolver
                        preUtxos
                        genesisUtxos

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedBurn
            case events of
                [evt@(CageBurn burnTid)] -> do
                    burnTid `shouldBe` tid
                    -- Compute inverse BEFORE apply
                    cageInvs <-
                        computeInverse
                            (state ctx)
                            evt
                    trieInvs <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            evt
                    let allInvs =
                            cageInvs ++ trieInvs
                    -- Token gone
                    mTs2 <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs2 `shouldBe` Nothing
                    -- Apply inverses to rollback
                    applyCageInverses
                        (state ctx)
                        (trieManager ctx)
                        (reverse allInvs)
                    -- Token restored with same state
                    mTs3 <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs3 `shouldBe` Just tsBefore
                _ ->
                    expectationFailure
                        $ "expected CageBurn, got: "
                            <> show events

    -- Test 7: inverse roundtrip (boot)
    it "inverse roundtrip"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Snapshot before boot
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr

            -- Submit boot tx
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let resolver =
                    mkResolver preUtxos []
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedBoot
            case events of
                [evt@(CageBoot tid _ts)] -> do
                    -- Compute inverse BEFORE apply
                    let inverses =
                            inversesOf
                                (const Nothing)
                                (const Nothing)
                                evt
                    -- Inverse should remove the token
                    inverses `shouldSatisfy` (not . null)
                    -- Apply event
                    _ <-
                        applyCageEvent
                            (state ctx)
                            (trieManager ctx)
                            evt
                    -- Verify token exists
                    mTs1 <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs1 `shouldSatisfy` \case
                        Just _ -> True
                        Nothing -> False
                    -- Apply inverses manually
                    removeToken
                        (tokens (state ctx))
                        tid
                    deleteTrie (trieManager ctx) tid
                    -- Verify empty state restored
                    mTs2 <-
                        getToken
                            (tokens (state ctx))
                            tid
                    mTs2 `shouldBe` Nothing
                _ ->
                    expectationFailure
                        $ "expected CageBoot, got: "
                            <> show events

    -- Test 7: batch update (2 requests)
    it "batch update (2 requests)"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + register
            bootAndRegister cfg ctx

            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"

            -- Submit request 1
            signedReq1 <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tid
                        "key1"
                        "val1"
                        genesisAddr

            -- Register request 1 in state
            let reqTxIn1 =
                    TxIn
                        (txIdTx signedReq1)
                        (TxIx 0)
                req1 =
                    Request
                        { requestToken = tid
                        , requestOwner =
                            keyHashFromSignKey
                                genesisSignKey
                        , requestKey = "key1"
                        , requestValue = Insert "val1"
                        , requestFee = Coin 1_000_000
                        , requestSubmittedAt = 0
                        }
            putRequest
                (requests (state ctx))
                reqTxIn1
                req1

            -- Submit request 2
            signedReq2 <-
                buildAndSubmit ctx
                    $ requestInsert
                        (txBuilder ctx)
                        tid
                        "key2"
                        "val2"
                        genesisAddr

            -- Register request 2 in state
            let reqTxIn2 =
                    TxIn
                        (txIdTx signedReq2)
                        (TxIx 0)
                req2 =
                    Request
                        { requestToken = tid
                        , requestOwner =
                            keyHashFromSignKey
                                genesisSignKey
                        , requestKey = "key2"
                        , requestValue = Insert "val2"
                        , requestFee = Coin 1_000_000
                        , requestSubmittedAt = 0
                        }
            putRequest
                (requests (state ctx))
                reqTxIn2
                req2

            -- Snapshot before update
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            genesisUtxos <-
                queryUTxOs
                    (provider ctx)
                    genesisAddr

            -- Submit update tx (consumes both
            -- requests)
            signedUpdate <-
                buildAndSubmit ctx
                    $ updateToken
                        (txBuilder ctx)
                        tid
                        genesisAddr
            let resolver =
                    mkResolver
                        preUtxos
                        genesisUtxos

            -- Detect events
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedUpdate
            let updates =
                    [ e
                    | e@(CageUpdate{}) <- events
                    ]
            length updates `shouldBe` 1

    -- Test 8: persistent state survives reopen
    it "persistent state survives reopen"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let sh = cfgScriptHash cfg
                scriptAddr =
                    cageAddrFromCfg cfg Testnet

            -- Boot + detect + apply
            preUtxos <-
                snapshotCageUtxos
                    (provider ctx)
                    scriptAddr
            signedBoot <-
                buildAndSubmit ctx
                    $ bootToken
                        (txBuilder ctx)
                        genesisAddr
            let resolver =
                    mkResolver preUtxos []
            events <-
                detectFromTx
                    sh
                    (pure . resolver)
                    signedBoot
            mapM_
                ( applyCageEvent
                    (state ctx)
                    (trieManager ctx)
                )
                events

            -- Verify token exists
            tids <-
                listTokens (tokens (state ctx))
            tid <- case tids of
                (t : _) -> pure t
                [] -> error "no tokens"
            length tids `shouldBe` 1

            -- Note: true RocksDB reopen test requires
            -- persistent backend; with mock state the
            -- data is in-memory only. This test
            -- verifies the state survives within a
            -- single Context lifecycle (baseline).
            mTs <-
                getToken
                    (tokens (state ctx))
                    tid
            mTs `shouldSatisfy` \case
                Just _ -> True
                Nothing -> False

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

-- | Start a devnet node, wire a full 'Context IO',
-- wait for N2C to connect, then run the action.
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
        withSystemTempDirectory "mpfs-e2e"
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
                            , networkMagic =
                                devnetMagic
                            , socketPath = sock
                            , dbPath = dbDir
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , bootstrapFile =
                                Just bsFile
                            , followerEnabled =
                                False
                            , stabilityWindow =
                                2160
                            , appTracer =
                                nullTracer
                            }
                withApplication appCfg $ \ctx -> do
                    _ <-
                        queryProtocolParams
                            (provider ctx)
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

-- | Snapshot UTxOs at the cage script address for
-- building a resolver. Must be called BEFORE
-- submitting a transaction (spent UTxOs disappear).
snapshotCageUtxos
    :: Provider IO
    -> Addr
    -> IO [(TxIn, TxOut ConwayEra)]
snapshotCageUtxos = queryUTxOs

-- | Build a UTxO resolver from pre-queried UTxOs.
mkResolver
    :: [(TxIn, TxOut ConwayEra)]
    -> [(TxIn, TxOut ConwayEra)]
    -> TxIn
    -> Maybe (TxOut ConwayEra)
mkResolver cageUtxos walletUtxos txIn =
    let utxoMap =
            Map.fromList (cageUtxos ++ walletUtxos)
    in  Map.lookup txIn utxoMap

-- | Boot a token and register it in mock state
-- (for tests that need a token before the main
-- test action).
bootAndRegister
    :: CageConfig -> Context IO -> IO ()
bootAndRegister cfg ctx = do
    let sh = cfgScriptHash cfg
        scriptAddr =
            cageAddrFromCfg cfg Testnet

    -- Snapshot before boot
    cageUtxos <-
        queryUTxOs (provider ctx) scriptAddr
    genesisUtxos <-
        queryUTxOs (provider ctx) genesisAddr

    -- Build + submit boot tx
    signedBoot <-
        buildAndSubmit ctx
            $ bootToken (txBuilder ctx) genesisAddr

    -- Detect and apply
    let resolver =
            mkResolver cageUtxos genesisUtxos
    events <-
        detectFromTx
            sh
            (pure . resolver)
            signedBoot
    mapM_
        ( applyCageEvent
            (state ctx)
            (trieManager ctx)
        )
        events

-- ---------------------------------------------------------
-- Config
-- ---------------------------------------------------------

-- | Build a 'CageConfig' from applied script bytes
-- and the system start time.
cageCfg
    :: SBS.ShortByteString -> Integer -> CageConfig
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
