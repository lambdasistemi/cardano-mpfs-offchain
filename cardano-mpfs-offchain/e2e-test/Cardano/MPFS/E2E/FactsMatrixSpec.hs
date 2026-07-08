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
import Data.Foldable (toList, traverse_)
import Data.Functor (($>))
import Data.List (maximumBy, sort)
import Data.Map.Strict qualified as Map
import Data.Ord (comparing)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Lens.Micro ((&), (.~), (^.))
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
    ( bodyTxL
    , txIdTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( feeTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( coinTxOutL
    , datumTxOutL
    , getMinCoinTxOut
    , mkBasicTxOut
    , valueTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    , TxIx (..)
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.Tx.Ledger (ConwayTx)

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
    , RequestInsertFacts (..)
    , RequestUpdateFacts
    , RequestsResponse (..)
    , RetractFacts
    , RetractRequest (..)
    , StatusResponse (..)
    , TxInJSON (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    , WitnessedRequest (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.API.Types.Common
    ( TokenIdJSON (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( RejectFacts (..)
    , UpdateFacts (..)
    )
import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTxWithEval)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.End (endCageTxWithEval)
import Cardano.MPFS.Client.Cage.Eval
    ( DecodedEvalContext (..)
    , decodeEvalContext
    )
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Cage.Reject (rejectCageTxWithEval)
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Retract (retractCageTxWithEval)
import Cardano.MPFS.Client.Cage.Update (updateCageTxWithEval)
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
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( awaitProofReadsReady
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
    , mkInlineDatum
    , mkRequestDatum
    , requestAddrFromCfg
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )
import Cardano.Tx.Balance
    ( BalanceResult (..)
    , balanceTx
    )
import Cardano.Tx.Build qualified as TxBuild

data NoCtx a

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
            insertBuffer <-
                runRequestInsertRow
                    cfg
                    ctx
                    app
                    tokenId
                    matrixInsertKey
                    matrixInsertValue
            runUpdateRow cfg ctx app tokenId (Just insertBuffer)
            factIndexed app tokenId matrixInsertKey
            updateBuffer <-
                runRequestUpdateRow
                    cfg
                    ctx
                    app
                    tokenId
                    matrixInsertKey
                    matrixInsertValue
                    matrixUpdatedValue
            runUpdateRow cfg ctx app tokenId (Just updateBuffer)
            factIndexed app tokenId matrixInsertKey
            deleteBuffer <-
                runRequestDeleteRow
                    cfg
                    ctx
                    app
                    tokenId
                    matrixInsertKey
                    matrixUpdatedValue
            runUpdateRow cfg ctx app tokenId (Just deleteBuffer)
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
            rejectFee <-
                runRejectRow
                    cfg
                    ctx
                    app
                    tokenId
                    matrixRejectKey
                    matrixRejectValue
            runSmallRefundRejectRow
                cfg
                ctx
                app
                tokenId
                rejectFee
            subsetUpdateTokenId <- runBootRow cfg ctx app
            runUpdateSubsetRow cfg ctx app subsetUpdateTokenId
            subsetRejectTokenId <- runBootRow cfg ctx app
            runRejectSubsetRow cfg ctx app subsetRejectTokenId
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

matrixSmallRefundRejectKey :: ByteString
matrixSmallRefundRejectKey = "matrix-small-refund-reject-key"

matrixSmallRefundRejectValue :: ByteString
matrixSmallRefundRejectValue = "matrix-small-refund-reject-value"

matrixSubsetUpdateRequests :: [(ByteString, ByteString)]
matrixSubsetUpdateRequests =
    [ ("subset-update-a", "value-a")
    , ("subset-update-b", "value-b")
    , ("subset-update-c", "value-c")
    ]

matrixSubsetRejectRequests :: [(ByteString, ByteString)]
matrixSubsetRejectRequests =
    [ ("subset-reject-a", "value-a")
    , ("subset-reject-b", "value-b")
    , ("subset-reject-c", "value-c")
    ]

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
    evalCtx <- decodedEvalContext ctx
    unsigned <-
        case bootCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("boot row: bootCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    assertRealisticFee "boot row" unsigned
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
    -> IO Coin
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
    feeCapacity <-
        requestFeeCapacity
            ctx
            "insert row request buffer"
            unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "insert row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId
    pure feeCapacity

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
    -> IO Coin
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
    feeCapacity <-
        requestFeeCapacity
            ctx
            "update request row request buffer"
            unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "update request row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId
    pure feeCapacity

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
    -> IO Coin
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
    feeCapacity <-
        requestFeeCapacity
            ctx
            "delete row request buffer"
            unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "delete row" result
    awaitTx app (txIdTx signed)
    pendingRequestsNonEmpty app tokenId
    pure feeCapacity

-- | Update row: @POST \/facts\/update \->
-- verifyUpdateFacts \-> updateCageTx \-> submit \->
-- expected trie root indexed@.
runUpdateRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> Maybe Coin
    -> IO ()
runUpdateRow cfg ctx app tokenId expectedFeeCapacity = do
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
    evalCtx <- decodedEvalContext ctx
    unsigned <-
        case updateCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("update row: updateCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    assertRealisticFee "update row" unsigned
    traverse_
        ( \feeCapacity ->
            assertRequestBufferCovers
                "update row request buffer"
                feeCapacity
                unsigned
        )
        expectedFeeCapacity
    expectedRoot <- expectedUpdateRoot unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "update row" result
    awaitTx app (txIdTx signed)
    pendingRequestsEmpty app tokenId
    tokenRootIndexed app tokenId expectedRoot

expectedUpdateRoot :: ConwayTx -> IO ByteString
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
    evalCtx <- decodedEvalContext ctx
    retractTx <-
        case retractCageTxWithEval
            evalCtx
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
    assertRealisticFee "retract row" retractTx
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
    -> IO Coin
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
    rejectFeeCapacity <-
        requestFeeCapacity
            ctx
            "reject row request buffer"
            insertTx
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
    evalCtx <- decodedEvalContext ctx
    rejectTx <-
        case rejectCageTxWithEval
            evalCtx
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
    assertRealisticFee "reject row" rejectTx
    assertRequestBufferCovers
        "reject row request buffer"
        rejectFeeCapacity
        rejectTx
    let signedReject = addKeyWitness genesisSignKey rejectTx
    rejectResult <-
        submitTx (submitter ctx) signedReject
    assertSubmitted "reject row submit" rejectResult
    awaitTx app (txIdTx signedReject)
    pendingRequestsEmpty app tokenId
    pure (rejectTx ^. bodyTxL . feeTxBodyL)

-- | Small-refund reject regression row: create a pending
-- request whose raw reject refund is below the refund
-- output's min-UTxO, then prove the #62 validator accepts
-- the bounded owner-funded top-up to min-UTxO.
runSmallRefundRejectRow
    :: CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> Coin
    -> IO ()
runSmallRefundRejectRow cfg ctx app tokenId measuredRejectFee = do
    trusted <- waitForTrustedRoot app
    insertFacts <-
        postRequestInsertFacts
            app
            tokenId
            matrixSmallRefundRejectKey
            matrixSmallRefundRejectValue
            genesisAddr
    case verifyRequestInsertFacts trusted insertFacts of
        Left err ->
            expectationFailure
                ( "small-refund reject: verifyRequestInsertFacts \
                  \failed: "
                    <> show err
                )
                *> error "unreachable"
        Right _ -> pure ()
    (insertTx, lockedRequestCoin) <-
        buildSmallRefundRequestInsertTx
            cfg
            ctx
            tokenId
            matrixSmallRefundRejectKey
            matrixSmallRefundRejectValue
            (rifSubmittedAt insertFacts)
            measuredRejectFee
    let signedInsert = addKeyWitness genesisSignKey insertTx
    insertResult <- submitTx (submitter ctx) signedInsert
    assertSubmitted "small-refund reject insert" insertResult
    awaitTx app (txIdTx signedInsert)
    pendingRequestsNonEmpty app tokenId
    threadDelay 11_000_000
    rejectTrusted <- waitForTrustedRoot app
    rejectFactsResp <- postRejectFacts app tokenId genesisAddr
    rejectVerified <-
        case verifyRejectFacts rejectTrusted rejectFactsResp of
            Left err ->
                expectationFailure
                    ( "small-refund reject: verifyRejectFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    evalCtx <- decodedEvalContext ctx
    rejectTx <-
        case rejectCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            rejectVerified of
            Left err ->
                expectationFailure
                    ( "small-refund reject: rejectCageTxWithEval \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    assertRealisticFee "small-refund reject" rejectTx
    assertSmallRefundTopUp
        ctx
        cfg
        lockedRequestCoin
        rejectTx
    let signedReject = addKeyWitness genesisSignKey rejectTx
    rejectResult <- submitTx (submitter ctx) signedReject
    assertSubmitted "small-refund reject submit" rejectResult
    awaitTx app (txIdTx signedReject)
    pendingRequestsEmpty app tokenId

buildSmallRefundRequestInsertTx
    :: CageConfig
    -> Context IO
    -> TokenId
    -> ByteString
    -> ByteString
    -> Integer
    -> Coin
    -> IO (ConwayTx, Coin)
buildSmallRefundRequestInsertTx
    cfg
    ctx
    tokenId
    key
    value
    submittedAt
    (Coin measuredRejectFee) = do
        pp <- queryProtocolParams (provider ctx)
        walletUtxos <- queryUTxOs (provider ctx) genesisAddr
        case walletUtxos of
            [] ->
                expectationFailure
                    "small-refund reject: no wallet UTxOs"
                    *> error "unreachable"
            _ -> do
                let funding =
                        maximumBy
                            (comparing (\(_, out) -> out ^. coinTxOutL))
                            walletUtxos
                    Coin tip = defaultTip cfg
                    Coin refundMin =
                        getMinCoinTxOut
                            pp
                            (mkBasicTxOut genesisAddr (inject (Coin 0)))
                    targetRawRefund = max 1 (refundMin `div` 2)
                    targetLocked =
                        Coin (tip + measuredRejectFee + targetRawRefund)
                    scriptAddr =
                        requestAddrFromCfg cfg tokenId (network cfg)
                    datum =
                        mkInlineDatum
                            $ mkRequestDatum
                                tokenId
                                genesisAddr
                                key
                                (OpInsert value)
                                tip
                                submittedAt
                    requestDraft =
                        mkBasicTxOut scriptAddr (inject (Coin 0))
                            & datumTxOutL .~ datum
                    requestMin = getMinCoinTxOut pp requestDraft
                    lockedCoin = max requestMin targetLocked
                    requestOut =
                        requestDraft
                            & valueTxOutL .~ inject lockedCoin
                    program = do
                        _ <- TxBuild.spend (fst funding)
                        _ <- TxBuild.output requestOut
                        pure ()
                    draft =
                        TxBuild.draft
                            pp
                            ( program
                                :: TxBuild.TxBuild
                                    NoCtx
                                    String
                                    ()
                            )
                    expectedRaw =
                        let Coin locked = lockedCoin
                        in  locked - tip - measuredRejectFee
                when
                    (expectedRaw <= 0 || expectedRaw >= refundMin)
                    $ expectationFailure
                    $ "small-refund reject: constructed request \
                      \would not exercise top-up; expected raw refund "
                        <> show expectedRaw
                        <> ", refund min "
                        <> show refundMin
                case balanceTx pp [funding] [] genesisAddr draft of
                    Left err ->
                        expectationFailure
                            ( "small-refund reject: balanceTx failed: "
                                <> show err
                            )
                            *> error "unreachable"
                    Right BalanceResult{balancedTx} ->
                        pure (balancedTx, lockedCoin)

assertSmallRefundTopUp
    :: Context IO -> CageConfig -> Coin -> ConwayTx -> IO ()
assertSmallRefundTopUp ctx cfg (Coin locked) tx = do
    evalCtx <- decodedEvalContext ctx
    let pp = evalProtocolParameters evalCtx
        Coin tip = defaultTip cfg
        Coin fee = tx ^. bodyTxL . feeTxBodyL
        rawRefund = locked - tip - fee
        refundMinCoin@(Coin refundMin) =
            getMinCoinTxOut
                pp
                (mkBasicTxOut genesisAddr (inject (Coin 0)))
        refundOrChangeOutputs =
            [ ()
            | out <- toList (tx ^. bodyTxL . outputsTxBodyL)
            , out ^. coinTxOutL >= refundMinCoin
            , Nothing <- [extractCageDatum out]
            ]
    when (rawRefund <= 0 || rawRefund >= refundMin)
        $ expectationFailure
        $ "small-refund reject: raw refund "
            <> show rawRefund
            <> " is not below min-UTxO "
            <> show refundMin
    when (null refundOrChangeOutputs)
        $ expectationFailure
        $ "small-refund reject: no plain refund/change output at or above min-UTxO "
            <> show refundMin

-- | Strict-subset update row: submit three pending requests, ask
-- @/facts/update@ for exactly two refs, and prove only those two are
-- swept while the unselected request remains pending.
runUpdateSubsetRow
    :: CageConfig -> Context IO -> Application -> TokenId -> IO ()
runUpdateSubsetRow cfg ctx app tokenId = do
    reqIns <-
        mapM
            ( uncurry
                ( submitInsertRequest
                    "strict subset update"
                    cfg
                    ctx
                    app
                    tokenId
                )
            )
            matrixSubsetUpdateRequests
    (selectedRefs, leftoverRef) <-
        case reqIns of
            [first, leftover, third] ->
                pure
                    ( map txInToHashIx [first, third]
                    , txInToHashIx leftover
                    )
            _ ->
                expectationFailure
                    "strict subset update: expected three request refs"
                    *> error "unreachable"
    assertPendingRequestRefs app tokenId (map txInToHashIx reqIns)
    trusted <- waitForTrustedRoot app
    facts <- postUpdateFactsWithRefs app tokenId genesisAddr selectedRefs
    assertUtxoEntryRefs
        "strict subset update facts"
        selectedRefs
        (ufRequestUtxos facts)
    verified <-
        case verifyUpdateFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "strict subset update: verifyUpdateFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value -> pure value
    evalCtx <- decodedEvalContext ctx
    unsigned <-
        case updateCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( "strict subset update: updateCageTxWithEval \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    assertRedeemerCount
        "strict subset update"
        (1 + length selectedRefs)
        unsigned
    assertRealisticFee "strict subset update" unsigned
    expectedRoot <- expectedUpdateRoot unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "strict subset update" result
    awaitTx app (txIdTx signed)
    assertPendingRequestRefs app tokenId [leftoverRef]
    factIndexed app tokenId "subset-update-a"
    factIndexed app tokenId "subset-update-c"
    factAbsent app tokenId "subset-update-b"
    tokenRootIndexed app tokenId expectedRoot

-- | Strict-subset reject row: submit three pending requests, wait for
-- Phase 3, reject exactly two refs, and assert the unselected request
-- stays pending.
runRejectSubsetRow
    :: CageConfig -> Context IO -> Application -> TokenId -> IO ()
runRejectSubsetRow cfg ctx app tokenId = do
    reqIns <-
        mapM
            ( uncurry
                ( submitInsertRequest
                    "strict subset reject"
                    cfg
                    ctx
                    app
                    tokenId
                )
            )
            matrixSubsetRejectRequests
    (selectedRefs, leftoverRef) <-
        case reqIns of
            [first, leftover, third] ->
                pure
                    ( map txInToHashIx [first, third]
                    , txInToHashIx leftover
                    )
            _ ->
                expectationFailure
                    "strict subset reject: expected three request refs"
                    *> error "unreachable"
    assertPendingRequestRefs app tokenId (map txInToHashIx reqIns)
    threadDelay 11_000_000
    trusted <- waitForTrustedRoot app
    facts <- postRejectFactsWithRefs app tokenId genesisAddr selectedRefs
    assertUtxoEntryRefs
        "strict subset reject facts"
        selectedRefs
        (rfRequestUtxos facts)
    verified <-
        case verifyRejectFacts trusted facts of
            Left err ->
                expectationFailure
                    ( "strict subset reject: verifyRejectFacts \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right value -> pure value
    evalCtx <- decodedEvalContext ctx
    unsigned <-
        case rejectCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( "strict subset reject: rejectCageTxWithEval \
                      \failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    assertRedeemerCount
        "strict subset reject"
        (1 + length selectedRefs)
        unsigned
    assertRealisticFee "strict subset reject" unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "strict subset reject" result
    awaitTx app (txIdTx signed)
    assertPendingRequestRefs app tokenId [leftoverRef]

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
    evalCtx <- decodedEvalContext ctx
    unsigned <-
        case endCageTxWithEval
            evalCtx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ("end row: endCageTx failed: " <> show err)
                    *> error "unreachable"
            Right tx -> pure tx
    assertRealisticFee "end row" unsigned
    let signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted "end row" result
    awaitTx app (txIdTx signed)
    tokenRemoved app tokenId

submitInsertRequest
    :: String
    -> CageConfig
    -> Context IO
    -> Application
    -> TokenId
    -> ByteString
    -> ByteString
    -> IO TxIn
submitInsertRequest label cfg ctx app tokenId key value = do
    trusted <- waitForTrustedRoot app
    facts <- postRequestInsertFacts app tokenId key value genesisAddr
    verified <-
        case verifyRequestInsertFacts trusted facts of
            Left err ->
                expectationFailure
                    ( label
                        <> ": verifyRequestInsertFacts failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right v -> pure v
    unsigned <-
        case requestInsertCageTx
            (toClientCageConfig cfg)
            permissiveWalletPolicy
            verified of
            Left err ->
                expectationFailure
                    ( label
                        <> ": requestInsertCageTx failed: "
                        <> show err
                    )
                    *> error "unreachable"
            Right tx -> pure tx
    let signed = addKeyWitness genesisSignKey unsigned
        tid = txIdTx signed
    result <- submitTx (submitter ctx) signed
    assertSubmitted (label <> " insert") result
    awaitTx app tid
    pure (TxIn tid (TxIx 0))

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
        BootRequest{brAddr = [Hex (serialiseAddr addr)]}
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
    postUpdateFactsWithRefs app tokenId addr []

postUpdateFactsWithRefs
    :: Application -> TokenId -> Addr -> [Text] -> IO UpdateFacts
postUpdateFactsWithRefs app tokenId addr refs =
    postFactsRequest
        app
        "/facts/update"
        UpdateRequest
            { urToken = tokenIdJSON tokenId
            , urAddr = Hex (serialiseAddr addr)
            , urRequests = refs
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
    postRejectFactsWithRefs app tokenId addr []

postRejectFactsWithRefs
    :: Application -> TokenId -> Addr -> [Text] -> IO RejectFacts
postRejectFactsWithRefs app tokenId addr refs =
    postFactsRequest
        app
        "/facts/reject"
        RejectRequest
            { rejToken = tokenIdJSON tokenId
            , rejAddr = Hex (serialiseAddr addr)
            , rejRequests = refs
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
    when (simpleStatus resp /= status200)
        $ expectationFailure
        $ label
            <> ": "
            <> show path
            <> " returned "
            <> show (simpleStatus resp)
            <> " body="
            <> show (simpleBody resp)
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

assertPendingRequestRefs
    :: Application -> TokenId -> [Text] -> IO ()
assertPendingRequestRefs app tokenId expected = do
    mDone <-
        pollUntilJust 60 $ do
            refs <- requestRefs app tokenId
            if sort refs == sort expected
                then pure (Just ())
                else pure Nothing
    case mDone of
        Just () -> pure ()
        Nothing -> do
            actual <- requestRefs app tokenId
            expectationFailure
                ( "pending request refs mismatch: expected "
                    <> show (sort expected)
                    <> ", got "
                    <> show (sort actual)
                )

requestRefs :: Application -> TokenId -> IO [Text]
requestRefs app tokenId = do
    resp <-
        get app
            $ "/tokens/"
                <> tokenIdHex tokenId
                <> "/requests"
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("requests response decode failed: " <> err)
                *> error "unreachable"
        Right RequestsResponse{rrRequests} ->
            pure (map witnessedRequestRef rrRequests)

witnessedRequestRef :: WitnessedRequest -> Text
witnessedRequestRef
    WitnessedRequest
        { wrUtxo =
            WitnessedUtxo
                { wuTxIn =
                    TxInJSON
                        { tjTxId = Hex txId
                        , tjTxIx = ix
                        }
                }
        } =
        TE.decodeUtf8 (B16.encode txId)
            <> "#"
            <> T.pack (show ix)

assertUtxoEntryRefs :: String -> [Text] -> [UtxoEntry] -> IO ()
assertUtxoEntryRefs _label expected entries = do
    let actual = map utxoEntryRefText entries
    length actual `shouldBe` length expected
    sort actual `shouldBe` sort expected
  where
    utxoEntryRefText UtxoEntry{ueRef = UtxoRef{urTxId = Hex txId, urTxIx = ix}} =
        TE.decodeUtf8 (B16.encode txId)
            <> "#"
            <> T.pack (show ix)

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

extractTokenId :: CageConfig -> ConwayTx -> TokenId
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

decodedEvalContext :: Context IO -> IO DecodedEvalContext
decodedEvalContext ctx = do
    wire <- evalContext ctx
    case decodeEvalContext wire of
        Left err ->
            expectationFailure
                ("eval context decode failed: " <> show err)
                *> error "unreachable"
        Right decoded -> pure decoded

assertRealisticFee :: String -> ConwayTx -> IO ()
assertRealisticFee label tx = do
    let Coin fee = tx ^. bodyTxL . feeTxBodyL
    when (fee > realisticScriptFeeUpperBound)
        $ expectationFailure
        $ label
            <> ": fee "
            <> show fee
            <> " exceeds realistic hardening bound "
            <> show realisticScriptFeeUpperBound

realisticScriptFeeUpperBound :: Integer
realisticScriptFeeUpperBound = 5_000_000

requestFeeCapacity
    :: Context IO -> String -> ConwayTx -> IO Coin
requestFeeCapacity ctx label tx = do
    evalCtx <- decodedEvalContext ctx
    let pp = evalProtocolParameters evalCtx
        Coin refundMin =
            getMinCoinTxOut
                pp
                (mkBasicTxOut genesisAddr (inject (Coin 0)))
        requestOutputs =
            [ (out ^. coinTxOutL, requestFee req)
            | out <- toList (tx ^. bodyTxL . outputsTxBodyL)
            , Just (RequestDatum req) <- [extractCageDatum out]
            ]
    case requestOutputs of
        [(Coin locked, tip)]
            | locked >= tip + refundMin ->
                pure (Coin (locked - tip - refundMin))
            | otherwise ->
                expectationFailure
                    ( label
                        <> ": request output "
                        <> show locked
                        <> " below tip+refundMin "
                        <> show (tip + refundMin)
                    )
                    *> error "unreachable"
        outs ->
            expectationFailure
                ( label
                    <> ": expected one request output, got "
                    <> show (length outs)
                )
                *> error "unreachable"

assertRequestBufferCovers :: String -> Coin -> ConwayTx -> IO ()
assertRequestBufferCovers label (Coin feeCapacity) tx = do
    let Coin fee = tx ^. bodyTxL . feeTxBodyL
    when (fee > feeCapacity)
        $ expectationFailure
        $ label
            <> ": future tx fee "
            <> show fee
            <> " exceeds prepaid request buffer "
            <> show feeCapacity
    when (feeCapacity > realisticRequestBufferUpperBound)
        $ expectationFailure
        $ label
            <> ": prepaid request buffer "
            <> show feeCapacity
            <> " exceeds realistic hardening bound "
            <> show realisticRequestBufferUpperBound

realisticRequestBufferUpperBound :: Integer
realisticRequestBufferUpperBound = 5_000_000

assertRedeemerCount :: String -> Int -> ConwayTx -> IO ()
assertRedeemerCount label expected tx = do
    let Redeemers redeemers = tx ^. witsTxL . rdmrsTxWitsL
        actual = Map.size redeemers
    when (actual /= expected)
        $ expectationFailure
        $ label
            <> ": expected "
            <> show expected
            <> " redeemers, got "
            <> show actual

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
                awaitProofReadsReady ctx
                action cfg ctx

cageCfg :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes, mStakingBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash = computeScriptHash stateBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        , cfgStakeScript =
            fmap
                (\bs -> (bs, computeScriptHash bs))
                mStakingBytes
        }
