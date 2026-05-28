{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.FactsMatrixSpec
-- Description : Live coverage matrix for migrated facts endpoints
-- License     : Apache-2.0
--
-- One row per migrated facts endpoint. Each row proves the
-- same live boundary:
--
--   1. POST the facts request to the running app via WAI.
--   2. Decode the wire response.
--   3. Run the offline @cardano-mpfs-client@ verifier.
--   4. Run the local cage builder to produce an unsigned tx.
--   5. Sign, submit, and wait for the tx to be indexed.
--   6. Observe the expected on-chain side-effect via the
--      indexer-backed read endpoints.
--   7. Confirm the replaced @\/tx\/*@ legacy route is gone
--      at the live HTTP boundary.
--
-- This slice is the skeleton plus the boot row. Later
-- slices add request-insert, request-delete, end, and the
-- live legacy-route absence assertions.
module Cardano.MPFS.E2E.FactsMatrixSpec
    ( spec
    , matrixMatch
    ) where

import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Data.Aeson
    ( FromJSON
    , ToJSON
    , Value
    , eitherDecode
    , encode
    , object
    , withObject
    , (.:)
    )
import Data.Aeson.Types (parseEither)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Foldable (toList)
import Data.Functor (($>))
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Lens.Micro ((^.))
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    )
import Network.Wai
    ( Application
    , Request (..)
    )
import Network.Wai.Test
    ( SRequest (..)
    , SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    , simpleBody
    , simpleStatus
    , srequest
    )
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
    )

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address
    ( Addr
    , serialiseAddr
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , txIdTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( mintTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.BaseTypes (Network (..), TxIx (..))
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootFacts
    , BootRequest (..)
    , DeleteRequest (..)
    , EndFacts
    , EndRequest (..)
    , FactResponse (..)
    , InsertRequest (..)
    , ProofResponse (..)
    , RejectRequest (..)
    , RequestDeleteFacts
    , RequestInsertFacts
    , RequestUpdateFacts
    , RetractFacts
    , RetractRequest (..)
    , StatusResponse (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    )
import Cardano.MPFS.API.Types.Common
    ( TokenIdJSON (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RejectFacts
    , UpdateFacts
    )
import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.End (endCageTx)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Cage.Reject (rejectCageTx)
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Retract (retractCageTx)
import Cardano.MPFS.Client.Cage.Update (updateCageTx)
import Cardano.MPFS.Client.Facts
    ( FactAbsentFacts (..)
    , FactPresentFacts (..)
    , verifyBootFacts
    , verifyEndFacts
    , verifyFactAbsentFacts
    , verifyFactPresentFacts
    , verifyRejectFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    , verifyRetractFacts
    , verifyUpdateFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( ConwayEra
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Submitter
    ( SubmitResult (..)
    , Submitter (..)
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , computeScriptHash
    , extractCageDatum
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )

-- | Hspec match string for the matrix scenario. Exposed so
-- the @just e2e-facts-matrix@ recipe and the quickstart can
-- reference the same literal.
matrixMatch :: String
matrixMatch =
    "facts API coverage matrix \
    \proves every migrated facts endpoint"

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Facts API coverage matrix" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (MPFS_BLUEPRINT not set)"
                $ pure @IO ()
        Just path -> do
            eScripts <- runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right scripts ->
                    matrixSpec scripts

matrixSpec :: CageScripts -> Spec
matrixSpec scripts =
    it matrixMatch
        $ withE2E scripts
        $ \cfg ctx -> do
            let app = mkApp ctx
            tokenId <- runBootRow cfg ctx app
            runRequestInsertRow
                cfg
                ctx
                app
                tokenId
                matrixInsertKey
                matrixInsertValue
            runUpdateRow cfg ctx app tokenId
            factIndexed app tokenId matrixInsertKey
            runRequestUpdateRow
                cfg
                ctx
                app
                tokenId
                matrixInsertKey
                matrixInsertValue
                matrixUpdatedValue
            runUpdateRow cfg ctx app tokenId
            factIndexed app tokenId matrixInsertKey
            runRequestDeleteRow
                cfg
                ctx
                app
                tokenId
                matrixInsertKey
                matrixUpdatedValue
            runUpdateRow cfg ctx app tokenId
            factAbsent app tokenId matrixInsertKey
            -- Retract row: re-insert a fresh request to
            -- have something to retract, then exercise the
            -- /facts/retract flow.
            runRetractRow
                cfg
                ctx
                app
                tokenId
                matrixRetractKey
                matrixRetractValue
            -- Reject row: re-insert a fresh request, wait
            -- past the Phase 3 deadline, then exercise the
            -- /facts/reject flow.
            runRejectRow
                cfg
                ctx
                app
                tokenId
                matrixRejectKey
                matrixRejectValue
            -- End requires an empty request set, but
            -- driving boot+insert+process+delete+process
            -- on a single token exhausts the wallet's
            -- spendable lovelace and trips Conway phase-1
            -- collateral checks. Boot a fresh token to
            -- exercise the /facts/end row in isolation.
            -- The existing HTTPLifecycle E2E follows the
            -- same pattern.
            endTokenId <- runBootRow cfg ctx app
            runEndRow cfg ctx app endTokenId
            assertLegacyRoutesGone app

-- | Key/value pair the matrix uses for its first insert.
matrixInsertKey :: ByteString
matrixInsertKey = "matrix-key"

matrixInsertValue :: ByteString
matrixInsertValue = "matrix-value"

-- | Updated value produced by the request-update row.
matrixUpdatedValue :: ByteString
matrixUpdatedValue = "matrix-updated-value"

-- | Key/value pair the matrix uses for the retract row's
-- throwaway insert.
matrixRetractKey :: ByteString
matrixRetractKey = "matrix-retract-key"

matrixRetractValue :: ByteString
matrixRetractValue = "matrix-retract-value"

-- | Key/value pair the matrix uses for the reject row's
-- throwaway insert.
matrixRejectKey :: ByteString
matrixRejectKey = "matrix-reject-key"

matrixRejectValue :: ByteString
matrixRejectValue = "matrix-reject-value"

-- ---------------------------------------------------------
-- Row helpers
-- ---------------------------------------------------------

-- | Boot row: @POST \/facts\/boot \-> verifyBootFacts \->
-- bootCageTx \-> submit \-> token indexed@.
runBootRow
    :: CageConfig -> Context IO -> Application -> IO TokenId
runBootRow cfg ctx app = do
    trusted <- waitForTrustedRoot app
    facts <- postBootFacts app genesisAddr
    verified <-
        case verifyBootFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "boot row: verifyBootFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value -> pure value
    unsigned <-
        case bootCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("boot row: bootCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
        tokenId = extractTokenId cfg signed
    result <- submitTx (submitter ctx) signed
    assertSubmitted "boot row" result
    awaitTx app (txIdTx signed)
    visible <- tokenVisible app tokenId
    visible `shouldBe` True
    pure tokenId

-- | Request-insert row:
-- @POST \/facts\/request\/insert \-> verifyRequestInsertFacts
-- \-> requestInsertCageTx \-> submit \-> request indexed@.
runRequestInsertRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> IO ()
runRequestInsertRow cfg ctx app tokenId key value = do
    trusted <- waitForTrustedRoot app
    facts <- postRequestInsertFacts app tokenId key value genesisAddr
    verified <-
        case verifyRequestInsertFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "insert row: verifyRequestInsertFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value' -> pure value'
    unsigned <-
        case requestInsertCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( "insert row: requestInsertCageTx \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "insert row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId

-- | Request-update row:
-- @POST \/facts\/request\/update \-> verifyRequestUpdateFacts
-- \-> requestUpdateCageTx \-> submit \-> update request indexed@.
-- The 'matrixSpec' caller is responsible for the subsequent
-- process/update step that materialises the new value in the trie.
runRequestUpdateRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> IO ()
runRequestUpdateRow cfg ctx app tokenId key oldValue newValue = do
    trusted <- waitForTrustedRoot app
    facts <-
        postRequestUpdateFacts
            app
            tokenId
            key
            oldValue
            newValue
            genesisAddr
    verified <-
        case verifyRequestUpdateFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "update request row: \
                      \verifyRequestUpdateFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value' -> pure value'
    unsigned <-
        case requestUpdateCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( "update request row: \
                      \requestUpdateCageTx failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "update request row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId

-- | Request-delete row:
-- @POST \/facts\/request\/delete \-> verifyRequestDeleteFacts
-- \-> requestDeleteCageTx \-> submit \-> delete request
-- indexed@. The 'matrixSpec' caller is responsible for the
-- subsequent process/update step that materialises the
-- removal in the trie.
runRequestDeleteRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> IO ()
runRequestDeleteRow cfg ctx app tokenId key value = do
    trusted <- waitForTrustedRoot app
    facts <- postRequestDeleteFacts app tokenId key value genesisAddr
    verified <-
        case verifyRequestDeleteFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "delete row: verifyRequestDeleteFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value' -> pure value'
    unsigned <-
        case requestDeleteCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( "delete row: requestDeleteCageTx \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "delete row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId

-- | Update row: @POST \/facts\/update \->
-- verifyUpdateFacts \-> updateCageTx \-> submit \->
-- expected trie root indexed@.
runUpdateRow
    :: CageConfig -> Context IO -> Application -> TokenId -> IO ()
runUpdateRow cfg ctx app tokenId = do
    trusted <- waitForTrustedRoot app
    facts <- postUpdateFacts app tokenId genesisAddr
    verified <-
        case verifyUpdateFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "update row: verifyUpdateFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value -> pure value
    unsigned <-
        case updateCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("update row: updateCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    expectedRoot <- expectedUpdateRoot unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "update row" result
    awaitTx app (txIdTx signed)
    pendingRequestsEmpty app tokenId
    tokenRootIndexed app tokenId expectedRoot

expectedUpdateRoot :: Tx ConwayEra -> IO ByteString
expectedUpdateRoot tx =
    case [ r
         | out <- toList (tx ^. bodyTxL . outputsTxBodyL)
         , Just (StateDatum state) <- [extractCageDatum out]
         , let OnChainRoot r = stateRoot state
         ] of
        [r] -> pure r
        roots ->
            expectationFailure
                ( "update row: expected one state output root, got "
                    <> show (length roots)
                )
                *> error "unreachable"

-- | Retract row: insert a fresh pending request, wait for
-- Phase 2 validity, then run
-- @POST \/facts\/retract \-> verifyRetractFacts \->
-- retractCageTx \-> submit \-> pending request consumed@.
-- This row exercises the full retract facts flow on the
-- local cluster end to end.
runRetractRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> IO ()
runRetractRow cfg ctx app tokenId key value = do
    -- 1. Insert a fresh pending request to retract.
    trusted <- waitForTrustedRoot app
    insertFacts <-
        postRequestInsertFacts
            app
            tokenId
            key
            value
            genesisAddr
    insertVerified <-
        case verifyRequestInsertFacts trusted insertFacts of
            Left err ->
                expectationFailure
                    ( "retract row: verifyRequestInsertFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    insertTx <-
        case requestInsertCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            insertVerified of
            Left err ->
                expectationFailure
                    ( "retract row: requestInsertCageTx \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signedInsert =
            addKeyWitness genesisSignKey insertTx
        insertTxId = txIdTx signedInsert
        reqTxIn = TxIn insertTxId (TxIx 0)
    insertResult <-
        submitTx (submitter ctx) signedInsert
    assertSubmitted "retract row insert" insertResult
    awaitTx app insertTxId
    pendingRequestsNonEmpty app tokenId
    -- 2. Wait for Phase 2 to open. process_time and
    -- retract_time are both 5s in the matrix config, so
    -- a real-time 7s wait lands inside the window.
    threadDelay 7_000_000
    -- 3. Retract via the facts flow.
    retractTrusted <- waitForTrustedRoot app
    retractFacts <-
        postRetractFacts
            app
            reqTxIn
            genesisAddr
    retractVerified <-
        case verifyRetractFacts retractTrusted retractFacts of
            Left err ->
                expectationFailure
                    ( "retract row: verifyRetractFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    retractTx <-
        case retractCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            retractVerified of
            Left err ->
                expectationFailure
                    ( "retract row: retractCageTx failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signedRetract =
            addKeyWitness genesisSignKey retractTx
    retractResult <-
        submitTx (submitter ctx) signedRetract
    assertSubmitted "retract row submit" retractResult
    awaitTx app (txIdTx signedRetract)
    pendingRequestsEmpty app tokenId

-- | Reject row: insert a fresh request, wait past the
-- Phase 3 deadline (@submitted_at + process_time +
-- retract_time@), then run
-- @POST \/facts\/reject \-> verifyRejectFacts \->
-- rejectCageTx \-> submit \-> pending request consumed@.
-- The matrix cage's @process_time@ and @retract_time@ are
-- both 5 s, so an 11 s real-time wait lands clearly past
-- the deadline.
runRejectRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> IO ()
runRejectRow cfg ctx app tokenId key value = do
    trusted <- waitForTrustedRoot app
    insertFacts <-
        postRequestInsertFacts
            app
            tokenId
            key
            value
            genesisAddr
    insertVerified <-
        case verifyRequestInsertFacts trusted insertFacts of
            Left err ->
                expectationFailure
                    ( "reject row: verifyRequestInsertFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    insertTx <-
        case requestInsertCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            insertVerified of
            Left err ->
                expectationFailure
                    ( "reject row: requestInsertCageTx \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signedInsert = addKeyWitness genesisSignKey insertTx
        insertTxId = txIdTx signedInsert
    insertResult <-
        submitTx (submitter ctx) signedInsert
    assertSubmitted "reject row insert" insertResult
    awaitTx app insertTxId
    pendingRequestsNonEmpty app tokenId
    -- 2. Wait past the Phase 3 deadline. process_time and
    -- retract_time are both 5 s; an 11 s wall-clock wait
    -- crosses the deadline with a small safety margin.
    threadDelay 11_000_000
    -- 3. Reject via the facts flow.
    rejectTrusted <- waitForTrustedRoot app
    rejectFactsResp <-
        postRejectFacts app tokenId genesisAddr
    rejectVerified <-
        case verifyRejectFacts rejectTrusted rejectFactsResp of
            Left err ->
                expectationFailure
                    ( "reject row: verifyRejectFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    rejectTx <-
        case rejectCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            rejectVerified of
            Left err ->
                expectationFailure
                    ( "reject row: rejectCageTx failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signedReject = addKeyWitness genesisSignKey rejectTx
    rejectResult <-
        submitTx (submitter ctx) signedReject
    assertSubmitted "reject row submit" rejectResult
    awaitTx app (txIdTx signedReject)
    pendingRequestsEmpty app tokenId

-- | End row: @POST \/facts\/end \-> verifyEndFacts \->
-- endCageTx \-> submit \-> token removed@.
runEndRow
    :: CageConfig -> Context IO -> Application -> TokenId -> IO ()
runEndRow cfg ctx app tokenId = do
    trusted <- waitForTrustedRoot app
    facts <- postEndFacts app tokenId genesisAddr
    verified <-
        case verifyEndFacts
            (toClientCageConfig cfg)
            trusted
            facts of
            Left err ->
                expectationFailure
                    ( "end row: verifyEndFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value' -> pure value'
    unsigned <-
        case endCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("end row: endCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "end row" result
    awaitTx app (txIdTx signed)
    tokenRemoved app tokenId

-- | Live-boundary check: every migrated facts endpoint has
-- a replaced legacy @\/tx\/*@ route. POSTing to any of them
-- against the running app must not return 200; the matrix
-- fails if the legacy route is still reachable. This catches
-- a regression that source-level grep gates would miss.
assertLegacyRoutesGone :: Application -> IO ()
assertLegacyRoutesGone app =
    mapM_
        (assertLegacyRouteGone app)
        [ "/tx/boot"
        , "/tx/request/insert"
        , "/tx/request/delete"
        , "/tx/request/update"
        , "/tx/update"
        , "/tx/retract"
        , "/tx/reject"
        , "/tx/end"
        ]

assertLegacyRouteGone :: Application -> ByteString -> IO ()
assertLegacyRouteGone app path = do
    resp <- postJson app path (object [])
    when (simpleStatus resp == status200)
        $ expectationFailure
        $ "legacy route reachable at "
            <> show path
            <> ": "
            <> show (simpleBody resp)

-- ---------------------------------------------------------
-- HTTP plumbing (copied locally to avoid touching other
-- E2E specs in this slice)
-- ---------------------------------------------------------

awaitTimeout :: Int
awaitTimeout = 60

waitForTrustedRoot :: Application -> IO TrustedRoot
waitForTrustedRoot app = do
    mRoot <-
        pollUntilJust 60 $ do
            resp <- get app "/status"
            simpleStatus resp `shouldBe` status200
            case eitherDecode (simpleBody resp) of
                Left err ->
                    expectationFailure
                        ("matrix: could not decode status: " <> err)
                        $> Nothing
                Right StatusResponse{currentUtxoRoot} ->
                    pure currentUtxoRoot
    case mRoot of
        Nothing ->
            expectationFailure
                "matrix: /status did not expose utxo_root"
                *> error "unreachable"
        Just root ->
            pure (TrustedRoot root)

postBootFacts :: Application -> Addr -> IO BootFacts
postBootFacts app addr =
    postFactsRequest
        app
        "/facts/boot"
        BootRequest{brAddr = Hex (serialiseAddr addr)}
        "boot row"
        "BootFacts"

postRequestInsertFacts
    :: Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> Addr
    -> IO RequestInsertFacts
postRequestInsertFacts app tokenId key value addr =
    postFactsRequest
        app
        "/facts/request/insert"
        InsertRequest
            { irToken = tokenIdJSON tokenId
            , irKey = Hex key
            , irValue = Hex value
            , irAddr = Hex (serialiseAddr addr)
            }
        "insert row"
        "RequestInsertFacts"

postRequestDeleteFacts
    :: Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> Addr
    -> IO RequestDeleteFacts
postRequestDeleteFacts app tokenId key value addr =
    postFactsRequest
        app
        "/facts/request/delete"
        DeleteRequest
            { drToken = tokenIdJSON tokenId
            , drKey = Hex key
            , drValue = Hex value
            , drAddr = Hex (serialiseAddr addr)
            }
        "delete row"
        "RequestDeleteFacts"

postRequestUpdateFacts
    :: Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> Addr
    -> IO RequestUpdateFacts
postRequestUpdateFacts app tokenId key oldValue newValue addr =
    postFactsRequest
        app
        "/facts/request/update"
        UpdateValueRequest
            { uvrToken = tokenIdJSON tokenId
            , uvrKey = Hex key
            , uvrOldValue = Hex oldValue
            , uvrNewValue = Hex newValue
            , uvrAddr = Hex (serialiseAddr addr)
            }
        "update request row"
        "RequestUpdateFacts"

postUpdateFacts
    :: Application -> TokenId -> Addr -> IO UpdateFacts
postUpdateFacts app tokenId addr =
    postFactsRequest
        app
        "/facts/update"
        UpdateRequest
            { urToken = tokenIdJSON tokenId
            , urAddr = Hex (serialiseAddr addr)
            }
        "update row"
        "UpdateFacts"

postEndFacts
    :: Application -> TokenId -> Addr -> IO EndFacts
postEndFacts app tokenId addr =
    postFactsRequest
        app
        "/facts/end"
        EndRequest
            { erToken = tokenIdJSON tokenId
            , erAddr = Hex (serialiseAddr addr)
            }
        "end row"
        "EndFacts"

postRetractFacts
    :: Application -> TxIn -> Addr -> IO RetractFacts
postRetractFacts app reqTxIn addr =
    postFactsRequest
        app
        "/facts/retract"
        RetractRequest
            { rrUtxo = txInToHashIx reqTxIn
            , rrAddr = Hex (serialiseAddr addr)
            }
        "retract row"
        "RetractFacts"

postRejectFacts
    :: Application -> TokenId -> Addr -> IO RejectFacts
postRejectFacts app tokenId addr =
    postFactsRequest
        app
        "/facts/reject"
        RejectRequest
            { rejToken = tokenIdJSON tokenId
            , rejAddr = Hex (serialiseAddr addr)
            }
        "reject row"
        "RejectFacts"

-- | Format a 'TxIn' as the @txhash#ix@ string expected by
-- the retract request body.
txInToHashIx :: TxIn -> Text
txInToHashIx (TxIn txId (TxIx ix)) =
    TE.decodeUtf8 (txIdHex txId)
        <> "#"
        <> T.pack (show ix)

postFactsRequest
    :: (ToJSON req, FromJSON res)
    => Application
    -> ByteString
    -> req
    -> String
    -> String
    -> IO res
postFactsRequest app path body label resName = do
    resp <- postJson app path body
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ( label
                    <> ": decode "
                    <> resName
                    <> ": "
                    <> err
                )
                *> error "unreachable"
        Right facts -> pure facts

postJson
    :: ToJSON body
    => Application
    -> ByteString
    -> body
    -> IO SResponse
postJson app path body =
    runSession
        ( srequest
            SRequest
                { simpleRequest =
                    (setPath defaultRequest path)
                        { requestMethod = methodPost
                        , requestHeaders =
                            [(hContentType, "application/json")]
                        }
                , simpleRequestBody = encode body
                }
        )
        app

-- | Convert a ledger 'TokenId' to its wire 'TokenIdJSON'.
tokenIdJSON :: TokenId -> TokenIdJSON
tokenIdJSON (TokenId (AssetName sbs)) =
    TokenIdJSON (SBS.fromShort sbs)

-- | Poll @\/tokens\/:id\/requests@ until the request list
-- is non-empty, or fail.
pendingRequestsNonEmpty :: Application -> TokenId -> IO ()
pendingRequestsNonEmpty app tokenId = do
    mRes <-
        pollUntilJust 60 $ do
            resp <-
                get app
                    $ "/tokens/"
                        <> tokenIdHex tokenId
                        <> "/requests"
            if simpleStatus resp == status200
                then case eitherDecode (simpleBody resp) of
                    Left _ -> pure Nothing
                    Right val ->
                        pure (requestsCount val)
                else pure Nothing
    case mRes of
        Just n
            | n > 0 -> pure ()
            | otherwise ->
                expectationFailure
                    "insert row: requests list \
                    \stayed empty after submit"
        Nothing ->
            expectationFailure
                "insert row: requests endpoint never \
                \returned a decodable response"
  where
    requestsCount :: Value -> Maybe Int
    requestsCount v =
        case parseEither
            ( withObject "requestsResponse" $ \o ->
                o .: "requests"
            )
            v of
            Right (rs :: [Value]) ->
                if null rs then Nothing else Just (length rs)
            Left _ -> Nothing

-- | Poll @\/tokens\/:id\/requests@ until the requests list
-- reports empty, or fail.
pendingRequestsEmpty :: Application -> TokenId -> IO ()
pendingRequestsEmpty app tokenId = do
    mDone <-
        pollUntilJust 60 $ do
            resp <-
                get app
                    $ "/tokens/"
                        <> tokenIdHex tokenId
                        <> "/requests"
            if simpleStatus resp /= status200
                then pure Nothing
                else case eitherDecode (simpleBody resp) of
                    Left _ -> pure Nothing
                    Right val ->
                        pure (requestsEmpty val)
    case mDone of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "process row: requests list did not drain \
                \after submit"
  where
    requestsEmpty :: Value -> Maybe ()
    requestsEmpty v =
        case parseEither
            ( withObject "requestsResponse" $ \o ->
                o .: "requests"
            )
            v of
            Right (rs :: [Value])
                | null rs -> Just ()
                | otherwise -> Nothing
            Left _ -> Nothing

-- | Poll @\/tokens\/:id\/root@ until it returns the
-- expected root.
tokenRootIndexed :: Application -> TokenId -> ByteString -> IO ()
tokenRootIndexed app tokenId expectedRoot = do
    mDone <-
        pollUntilJust 60 $ do
            resp <-
                get app
                    $ "/tokens/"
                        <> tokenIdHex tokenId
                        <> "/root"
            if simpleStatus resp /= status200
                then pure Nothing
                else case eitherDecode (simpleBody resp) of
                    Left _ -> pure Nothing
                    Right (Hex actualRoot)
                        | actualRoot == expectedRoot -> pure (Just ())
                        | otherwise -> pure Nothing
    case mDone of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "update row: /tokens/:id/root never returned \
                \the locally built update root"

-- | Poll @\/tokens\/:id\/facts\/:key@ until it returns 200.
factIndexed
    :: Application -> TokenId -> ByteString -> IO ()
factIndexed app tokenId key = do
    mOk <-
        pollUntilJust 60 $ do
            resp <- get app (factPath tokenId key)
            if simpleStatus resp /= status200
                then pure Nothing
                else case eitherDecode (simpleBody resp) of
                    Left _ -> pure Nothing
                    Right factResp@FactResponse{frSnapshot} ->
                        case verifyFactPresentFacts
                            (TrustedRoot (vsUtxoRoot frSnapshot))
                            FactPresentFacts
                                { fpfKey = Hex key
                                , fpfResponse = factResp
                                } of
                            Right _ -> pure (Just ())
                            Left _ -> pure Nothing
    case mOk of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "process row: fact never verified at \
                \/tokens/:id/facts/:key after process"

-- | Poll @\/tokens\/:id\/facts\/:key@ until it returns
-- something other than 200 (the fact has been removed).
factAbsent
    :: Application -> TokenId -> ByteString -> IO ()
factAbsent app tokenId key = do
    mGone <-
        pollUntilJust 60 $ do
            resp <- get app (factPath tokenId key)
            if simpleStatus resp == status200
                then pure Nothing
                else pure (Just ())
    case mGone of
        Just () -> factAbsenceVerified app tokenId key
        Nothing ->
            expectationFailure
                "delete row: fact still present at \
                \/tokens/:id/facts/:key after delete+process"

factPath :: TokenId -> ByteString -> ByteString
factPath tokenId key =
    "/tokens/"
        <> tokenIdHex tokenId
        <> "/facts/"
        <> B16.encode key

proofPath :: TokenId -> ByteString -> ByteString
proofPath tokenId key =
    "/tokens/"
        <> tokenIdHex tokenId
        <> "/proofs/"
        <> B16.encode key

factAbsenceVerified :: Application -> TokenId -> ByteString -> IO ()
factAbsenceVerified app tokenId key = do
    mOk <-
        pollUntilJust 60 $ do
            resp <- get app (proofPath tokenId key)
            if simpleStatus resp /= status200
                then pure Nothing
                else case eitherDecode (simpleBody resp) of
                    Left _ -> pure Nothing
                    Right proofResp@ProofResponse{prSnapshot} ->
                        case verifyFactAbsentFacts
                            (TrustedRoot (vsUtxoRoot prSnapshot))
                            FactAbsentFacts
                                { fafKey = Hex key
                                , fafResponse = proofResp
                                } of
                            Right _ -> pure (Just ())
                            Left _ -> pure Nothing
    case mOk of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "delete row: absence proof never verified at \
                \/tokens/:id/proofs/:key after delete+process"

-- | Poll @\/tokens\/:id@ until it returns something other
-- than 200 (the token has been ended).
tokenRemoved :: Application -> TokenId -> IO ()
tokenRemoved app tokenId = do
    mGone <-
        pollUntilJust 60 $ do
            resp <-
                get app
                    $ "/tokens/" <> tokenIdHex tokenId
            if simpleStatus resp == status200
                then pure Nothing
                else pure (Just ())
    case mGone of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "end row: token still visible at \
                \/tokens/:id after end+process"

awaitTx :: Application -> TxId -> IO ()
awaitTx app tid = do
    resp <- get app path
    simpleStatus resp `shouldBe` status200
  where
    path =
        "/tx/"
            <> txIdHex tid
            <> "?timeout="
            <> bshow awaitTimeout
    bshow =
        BS.pack . map (fromIntegral . fromEnum) . show

tokenVisible :: Application -> TokenId -> IO Bool
tokenVisible app tokenId = do
    resp <- get app ("/tokens/" <> tokenIdHex tokenId)
    pure (simpleStatus resp == status200)

get :: Application -> ByteString -> IO SResponse
get app path =
    runSession
        (request (setPath defaultRequest path))
        app

assertSubmitted :: String -> SubmitResult -> IO ()
assertSubmitted _ (Submitted _) = pure ()
assertSubmitted label (Rejected reason) =
    expectationFailure
        $ label <> ": tx rejected: " <> show reason

pollUntilJust :: Int -> IO (Maybe a) -> IO (Maybe a)
pollUntilJust timeoutSec action = go (timeoutSec * 2)
  where
    go 0 = action
    go n = do
        result <- action
        case result of
            Just _ -> pure result
            Nothing ->
                threadDelay 500_000 >> go (n - 1)

extractTokenId :: CageConfig -> Tx ConwayEra -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma = tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "matrix: extractTokenId: unexpected mint"

tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

txIdHex :: TxId -> ByteString
txIdHex (TxId sh) =
    B16.encode $ Crypto.hashToBytes $ extractHash sh

permissiveWalletPolicy :: WalletPolicy
permissiveWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }

toClientCageConfig :: CageConfig -> Client.CageConfig
toClientCageConfig cfg =
    Client.CageConfig
        { Client.cageScriptBytes = cageScriptBytes cfg
        , Client.requestScriptBytes = requestScriptBytes cfg
        , Client.cfgScriptHash = cfgScriptHash cfg
        , Client.defaultProcessTime = defaultProcessTime cfg
        , Client.defaultRetractTime = defaultRetractTime cfg
        , Client.defaultTip = defaultTip cfg
        , Client.network = network cfg
        }

withE2E
    :: CageScripts
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory "mpfs-facts-matrix" $ \tmpDir -> do
            let cfg = cageCfg scripts
                appCfg =
                    AppConfig
                        { epochSlots = EpochSlots 4320
                        , shelleyGenesisPath =
                            gDir </> "shelley-genesis.json"
                        , socketPath = sock
                        , dbPath = tmpDir </> "db"
                        , channelCapacity = 16
                        , cageConfig = cfg
                        , byronGenesisPath = Nothing
                        , followerEnabled = True
                        , appTracer = nullTracer
                        }
            withApplication appCfg $ \ctx -> do
                _ <- queryProtocolParams (provider ctx)
                threadDelay 10_000_000
                action cfg ctx

cageCfg :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash = computeScriptHash stateBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
