{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.WorkflowsIntegrationSpec
-- Description : Live-boundary integration for cardano-mpfs-workflows
-- License     : Apache-2.0
--
-- One @it@ per workflow exported by @cardano-mpfs-workflows@. Each row
-- proves the full live composition that #290 (CLI) and #291 (SPA) will
-- rely on:
--
--   1. Drive @Cardano.MPFS.Workflows.<name>@ with a real 'HttpClient'
--      backed by the in-process WAI application — the workflow POSTs
--      its facts request to the running app, verifies the
--      proof-bearing response against the live trusted root, and
--      builds an unsigned transaction.
--   2. Decode that 'UnsignedTx', sign it with the genesis key (the
--      same e2e signing the @\/submit@ test uses), and POST the signed
--      CBOR to @\/submit@ (merged in #288).
--   3. Await the returned txId against the indexer.
--   4. Assert the expected on-chain side-effect via the read
--      endpoints (token indexed / request queued / fact materialised /
--      request drained / token burned).
--
-- This is NOT a re-run of the unit suite against a server, and NOT a
-- smoke for @\/submit@ itself (#288 owns that). It is the proof that
-- the workflow layer and @\/submit@ compose end to end on a real
-- chain. Each row boots its own fresh token so per-token state never
-- bleeds across rows.
module Cardano.MPFS.E2E.WorkflowsIntegrationSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Control.Monad (forM)
import Data.Aeson (decode, eitherDecode, encode)
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Functor (($>))
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word64)
import Lens.Micro ((^.))
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    )
import Network.HTTP.Types.Status (statusCode)
import Network.Wai (Application, Request (..))
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
    , SpecWith
    , aroundAll
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    , shouldReturn
    )
import Text.Read (readMaybe)

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address (serialiseAddr)
import Cardano.Ledger.Api.Tx (bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..), TxIx (..))
import Cardano.Ledger.Binary
    ( Annotator
    , Decoder
    , decCBOR
    , decodeFull
    , decodeFullAnnotator
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes
    ( extractHash
    , unsafeMakeSafeHash
    )
import Cardano.Ledger.Mary.Value (AssetName (..), MultiAsset (..))
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
    , mkTxInPartial
    )
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (FactResponse (..), StatusResponse (..))
import Cardano.MPFS.API.Types.Common
    ( EvalContext (..)
    , TokenIdJSON (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( BootFacts
    , EndFacts
    , RejectFacts
    , RequestDeleteFacts
    , RequestInsertFacts
    , RequestUpdateFacts
    , RetractFacts
    , UpdateFacts
    )
import Cardano.MPFS.Application (AppConfig (..), withApplication)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Facts
    ( FactPresentFacts (..)
    , verifyFactPresentFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint (CageScripts, loadCageScripts)
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    )
import Cardano.MPFS.Core.OnChain qualified as OnChain
import Cardano.MPFS.Core.Types
    ( Addr
    , ConwayEra
    , LocatedRequest (LocatedRequest)
    , LocatedTokenState (LocatedTokenState)
    , Request (..)
    , SlotNo (..)
    , TokenId (..)
    , TxOut
    )
import Cardano.MPFS.HTTP.Server
    ( mkApp
    , mkBootFacts
    )
import Cardano.MPFS.HTTP.Types
    ( SubmitRequest (..)
    , SubmitResponse (..)
    , parseAddr
    , tokenIdFromJSON
    )
import Cardano.MPFS.HTTP.Types.Facts
    ( mkEndFacts
    , mkRejectFacts
    , mkRequestDeleteFacts
    , mkRequestInsertFacts
    , mkRequestUpdateFacts
    , mkRetractFacts
    , mkUpdateFacts
    )
import Cardano.MPFS.Indexer.Reads
    ( readSnapshot
    , readUtxoSetAt
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ResolvedWalletInput
    , TrieFact
    )
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cagePolicyIdFromCfg
    , computeScriptHash
    , currentPosixMs
    , extractCageDatum
    , requestAddrFromCfg
    )
import Cardano.MPFS.TxBuilder.Real.Update qualified as UpdateTx
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )

import Cardano.MPFS.Client.Cage.Eval
    ( DecodedEvalContext
    , decodeEvalContext
    )
import Cardano.MPFS.Workflows
    ( BootRequest (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , HttpClient (..)
    , HttpError (..)
    , InsertRequest (..)
    , RejectRequest (..)
    , RetractRequest (..)
    , UnsignedTx (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    , WorkflowError
    , WorkflowsConfig (..)
    , applyRequests
    , deleteFact
    , endCage
    , insertFact
    , registerToken
    , rejectExpired
    , retractRequest
    , updateFact
    )

-- | Shared, read-only environment for every workflow row: the cage
-- config, the in-process application, and the 'HttpClient' the
-- workflows post through. Each row boots its own token, so no mutable
-- state is shared.
data Env = Env
    { envCfg :: CageConfig
    , envApp :: Application
    , envHttp :: HttpClient
    , envEvalContext :: DecodedEvalContext
    }

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "WorkflowsIntegration: workflows through /submit (e2e)" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (MPFS_BLUEPRINT not set)" $ pure @IO ()
        Just path -> do
            eScripts <- runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right scripts ->
                    aroundAll (withSharedEnv scripts) workflowRows

-- | One row per exported workflow. Declaration order is execution
-- order; each row is self-contained.
workflowRows :: SpecWith Env
workflowRows = do
    it "registerToken boots a token" $ \env -> do
        signed <- runBoot env
        tokenVisible (envApp env) (extractTokenId (envCfg env) signed)
            `shouldReturn` True

    it "insertFact queues an insert request" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "insertFact"
                (insertFact (envHttp env))
                (insertReq tokenId factKey factValue)
        pendingRequestsNonEmpty (envApp env) tokenId

    it "updateFact queues an update request" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "updateFact"
                (updateFact (envHttp env))
                (updateReq tokenId factKey factValue updatedValue)
        pendingRequestsNonEmpty (envApp env) tokenId

    it "deleteFact queues a delete request" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "deleteFact"
                (deleteFact (envHttp env))
                (deleteReq tokenId factKey factValue)
        pendingRequestsNonEmpty (envApp env) tokenId

    it "applyRequests materialises a fact in the trie" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "insertFact (for apply)"
                (insertFact (envHttp env))
                (insertReq tokenId factKey factValue)
        pendingRequestsNonEmpty (envApp env) tokenId
        _ <-
            runWorkflow
                env
                "applyRequests"
                (applyRequests (envHttp env))
                (updateValueReq tokenId)
        pendingRequestsEmpty (envApp env) tokenId
        factIndexed (envApp env) tokenId factKey

    it "retractRequest drains a pending request" $ \env -> do
        tokenId <- bootToken env
        insertSigned <-
            runWorkflow
                env
                "insertFact (for retract)"
                (insertFact (envHttp env))
                (insertReq tokenId retractKey retractValue)
        pendingRequestsNonEmpty (envApp env) tokenId
        let reqTxIn = TxIn (txIdTx insertSigned) (TxIx 0)
        -- Phase 2 opens after process_time; the cage config below
        -- uses 5s, so a 7s wall-clock wait lands inside the window.
        threadDelay 7_000_000
        _ <-
            runWorkflow
                env
                "retractRequest"
                (retractRequest (envHttp env))
                (retractReq reqTxIn)
        pendingRequestsEmpty (envApp env) tokenId

    it "rejectExpired drains an expired request" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "insertFact (for reject)"
                (insertFact (envHttp env))
                (insertReq tokenId rejectKey rejectValue)
        pendingRequestsNonEmpty (envApp env) tokenId
        -- Phase 3 opens after process_time + retract_time (5s + 5s);
        -- an 11s wall-clock wait crosses the deadline.
        threadDelay 11_000_000
        _ <-
            runWorkflow
                env
                "rejectExpired"
                (rejectExpired (envHttp env))
                (rejectReq tokenId)
        pendingRequestsEmpty (envApp env) tokenId

    it "endCage burns the token" $ \env -> do
        tokenId <- bootToken env
        _ <-
            runWorkflow
                env
                "endCage"
                (endCage (envHttp env))
                (endReq tokenId)
        tokenRemoved (envApp env) tokenId

-- ---------------------------------------------------------
-- Workflow driver
-- ---------------------------------------------------------

-- | Run a workflow end to end: fetch the live trusted root, invoke
-- the workflow (which posts its facts request through the injected
-- 'HttpClient'), decode + sign the returned 'UnsignedTx', POST it to
-- @\/submit@, and await the txId. Returns the signed transaction so
-- callers can read its outputs (token id, request TxIn).
runWorkflow
    :: Env
    -> String
    -> (WorkflowsConfig -> req -> IO (Either WorkflowError UnsignedTx))
    -> req
    -> IO ConwayTx
runWorkflow env label run req = do
    trusted <- waitForTrustedRoot (envApp env)
    result <- run (mkWorkflowsConfig env trusted) req
    unsigned <-
        case result of
            Right u -> pure u
            Left err ->
                expectationFailure
                    (label <> ": workflow failed: " <> show err)
                    *> error "unreachable"
    tx <-
        case decodeFullAnnotator
            (natVersion @11)
            "Conway transaction"
            (decCBOR :: forall s. Decoder s (Annotator ConwayTx))
            (BSL.fromStrict (unsignedTxCbor unsigned)) of
            Right t -> pure (t :: ConwayTx)
            Left err ->
                expectationFailure
                    (label <> ": UnsignedTx CBOR did not decode: " <> show err)
                    *> error "unreachable"
    let signed = addKeyWitness genesisSignKey tx
    submitAndAwait env label signed
    pure signed

-- | The boot row keeps its signed tx so the token id can be read off
-- the mint.
runBoot :: Env -> IO ConwayTx
runBoot env =
    runWorkflow
        env
        "registerToken"
        (registerToken (envHttp env))
        bootReq

-- | Boot a fresh token and wait for it to be indexed; used by the
-- rows that need an existing cage.
bootToken :: Env -> IO TokenId
bootToken env = do
    signed <- runBoot env
    let tokenId = extractTokenId (envCfg env) signed
    visible <- tokenVisible (envApp env) tokenId
    visible `shouldBe` True
    pure tokenId

-- | Sign-already-done: serialise, POST to @\/submit@, assert the wire
-- contract, and await the txId against the indexer.
submitAndAwait :: Env -> String -> ConwayTx -> IO ()
submitAndAwait env label signed = do
    let rawCbor = serialize' (natVersion @11) signed
    resp <-
        postJson (envApp env) "/submit" (SubmitRequest (Hex rawCbor))
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just (SubmitResponse (Hex txIdBytes)) ->
            BS.length txIdBytes `shouldBe` 32
        Nothing ->
            expectationFailure
                (label <> ": /submit did not return a SubmitResponse")
    awaitTx (envApp env) (txIdTx signed)

mkWorkflowsConfig :: Env -> TrustedRoot -> WorkflowsConfig
mkWorkflowsConfig env trusted =
    WorkflowsConfig
        { wcCage = toClientCageConfig (envCfg env)
        , wcPolicy = permissiveWalletPolicy
        , wcTrustedRoot = trusted
        , wcEvalContext = envEvalContext env
        }

-- ---------------------------------------------------------
-- Request payloads
-- ---------------------------------------------------------

factKey :: ByteString
factKey = "wf-key"

factValue :: ByteString
factValue = "wf-value"

updatedValue :: ByteString
updatedValue = "wf-updated"

retractKey :: ByteString
retractKey = "wf-retract-key"

retractValue :: ByteString
retractValue = "wf-retract-value"

rejectKey :: ByteString
rejectKey = "wf-reject-key"

rejectValue :: ByteString
rejectValue = "wf-reject-value"

genesisHex :: Hex
genesisHex = Hex (serialiseAddr genesisAddr)

bootReq :: BootRequest
bootReq = BootRequest{brAddr = genesisHex}

insertReq :: TokenId -> ByteString -> ByteString -> InsertRequest
insertReq tokenId key value =
    InsertRequest
        { irToken = tokenIdJSON tokenId
        , irKey = Hex key
        , irValue = Hex value
        , irAddr = genesisHex
        }

updateReq
    :: TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> UpdateValueRequest
updateReq tokenId key oldValue newValue =
    UpdateValueRequest
        { uvrToken = tokenIdJSON tokenId
        , uvrKey = Hex key
        , uvrOldValue = Hex oldValue
        , uvrNewValue = Hex newValue
        , uvrAddr = genesisHex
        }

deleteReq :: TokenId -> ByteString -> ByteString -> DeleteRequest
deleteReq tokenId key value =
    DeleteRequest
        { drToken = tokenIdJSON tokenId
        , drKey = Hex key
        , drValue = Hex value
        , drAddr = genesisHex
        }

updateValueReq :: TokenId -> UpdateRequest
updateValueReq tokenId =
    UpdateRequest
        { urToken = tokenIdJSON tokenId
        , urAddr = genesisHex
        , urRequests = []
        }

retractReq :: TxIn -> RetractRequest
retractReq reqTxIn =
    RetractRequest
        { rrUtxo = txInToHashIx reqTxIn
        , rrAddr = genesisHex
        }

rejectReq :: TokenId -> RejectRequest
rejectReq tokenId =
    RejectRequest
        { rejToken = tokenIdJSON tokenId
        , rejAddr = genesisHex
        , rejRequests = []
        }

endReq :: TokenId -> EndRequest
endReq tokenId =
    EndRequest
        { erToken = tokenIdJSON tokenId
        , erAddr = genesisHex
        }

-- ---------------------------------------------------------
-- HTTP transport: a real 'HttpClient' over the in-process app
-- ---------------------------------------------------------

-- | Workflow rows exercise the WAI app for non-facts endpoints. Facts
-- responses use a devnet-wallet view here: after eval hardening, tx
-- construction needs full TxOut bytes for every input, while this shared
-- devnet's indexer address scans can retain historical leaves whose KV
-- TxOut bytes have already been deleted. Production browser flows get
-- current wallet inputs from CIP-30; this e2e transport mirrors that by
-- combining the devnet wallet's current LSQ UTxOs with exact CSMT proofs
-- for those inputs.
workflowHttpClient :: Context IO -> Application -> HttpClient
workflowHttpClient ctx app =
    HttpClient $ \path body ->
        case path of
            "/facts/boot" ->
                localFactsResponse body (workflowBootFacts ctx)
            "/facts/request/insert" ->
                localFactsResponse body (workflowRequestInsertFacts ctx)
            "/facts/request/update" ->
                localFactsResponse body (workflowRequestUpdateFacts ctx)
            "/facts/request/delete" ->
                localFactsResponse body (workflowRequestDeleteFacts ctx)
            "/facts/update" ->
                localFactsResponse body (workflowUpdateFacts ctx)
            "/facts/retract" ->
                localFactsResponse body (workflowRetractFacts ctx)
            "/facts/reject" ->
                localFactsResponse body (workflowRejectFacts ctx)
            "/facts/end" ->
                localFactsResponse body (workflowEndFacts ctx)
            _ -> runHttpPost (waiHttpClient app) path body

localFactsResponse
    :: (Aeson.FromJSON req, Aeson.ToJSON facts)
    => ByteString
    -> (req -> IO (Either HttpError facts))
    -> IO (Either HttpError ByteString)
localFactsResponse body build =
    case Aeson.eitherDecodeStrict' body of
        Left err ->
            pure
                $ statusError
                    400
                    ("workflow e2e facts request decode failed: " <> err)
        Right req ->
            fmap (fmap (BSL.toStrict . encode)) (build req)

workflowBootFacts
    :: Context IO
    -> BootRequest
    -> IO (Either HttpError BootFacts)
workflowBootFacts ctx BootRequest{brAddr = addrHex} =
    case parseAddr addrHex of
        Left err -> pure $ statusError 400 err
        Right addr -> do
            baseE <- snapshotAndWalletForWorkflow ctx addr
            pp <- queryProtocolParams (provider ctx)
            pure $ do
                (snap, funding) <- baseE
                Right $ mkBootFacts snap funding pp

workflowRequestInsertFacts
    :: Context IO
    -> InsertRequest
    -> IO (Either HttpError RequestInsertFacts)
workflowRequestInsertFacts
    ctx
    InsertRequest
        { irToken = tokenId
        , irKey = Hex k
        , irValue = Hex v
        , irAddr = addrHex@(Hex addrBytes)
        } =
        case parseAddr addrHex of
            Left err -> pure $ statusError 400 err
            Right addr -> do
                baseE <- snapshotAndWalletForWorkflow ctx addr
                pp <- queryProtocolParams (provider ctx)
                submittedAt <- currentPosixMs
                pure $ do
                    (snap, funding) <- baseE
                    Right
                        $ mkRequestInsertFacts
                            snap
                            (tokenIdFromJSON tokenId)
                            k
                            v
                            addrBytes
                            submittedAt
                            funding
                            pp

workflowRequestUpdateFacts
    :: Context IO
    -> UpdateValueRequest
    -> IO (Either HttpError RequestUpdateFacts)
workflowRequestUpdateFacts
    ctx
    UpdateValueRequest
        { uvrToken = tokenId
        , uvrKey = Hex k
        , uvrOldValue = Hex oldV
        , uvrNewValue = Hex newV
        , uvrAddr = addrHex@(Hex addrBytes)
        } =
        case parseAddr addrHex of
            Left err -> pure $ statusError 400 err
            Right addr -> do
                baseE <- snapshotAndWalletForWorkflow ctx addr
                pp <- queryProtocolParams (provider ctx)
                submittedAt <- currentPosixMs
                pure $ do
                    (snap, funding) <- baseE
                    Right
                        $ mkRequestUpdateFacts
                            snap
                            (tokenIdFromJSON tokenId)
                            k
                            oldV
                            newV
                            addrBytes
                            submittedAt
                            funding
                            pp

workflowRequestDeleteFacts
    :: Context IO
    -> DeleteRequest
    -> IO (Either HttpError RequestDeleteFacts)
workflowRequestDeleteFacts
    ctx
    DeleteRequest
        { drToken = tokenId
        , drKey = Hex k
        , drValue = Hex v
        , drAddr = addrHex@(Hex addrBytes)
        } =
        case parseAddr addrHex of
            Left err -> pure $ statusError 400 err
            Right addr -> do
                baseE <- snapshotAndWalletForWorkflow ctx addr
                pp <- queryProtocolParams (provider ctx)
                submittedAt <- currentPosixMs
                pure $ do
                    (snap, funding) <- baseE
                    Right
                        $ mkRequestDeleteFacts
                            snap
                            (tokenIdFromJSON tokenId)
                            k
                            v
                            addrBytes
                            submittedAt
                            funding
                            pp

workflowUpdateFacts
    :: Context IO
    -> UpdateRequest
    -> IO (Either HttpError UpdateFacts)
workflowUpdateFacts
    ctx
    UpdateRequest
        { urToken = tokenId
        , urAddr = addrHex
        , urRequests = requestRefs
        } =
        case parseAddr addrHex of
            Left err -> pure $ statusError 400 err
            Right addr
                | not (null requestRefs) ->
                    pure
                        $ statusError
                            400
                            "Workflow e2e update facts support all-pending requests"
                | otherwise -> do
                    let tid = tokenIdFromJSON tokenId
                    walletE <- walletInputsFromProvider ctx addr
                    case walletE of
                        Left err -> pure (Left err)
                        Right funding -> do
                            mSnap <- runIndexerTx ctx readSnapshot
                            stateE <-
                                stateInputFromStateForWorkflow ctx tid
                            requestUtxosE <-
                                requestInputsForTokenFromStateForWorkflow
                                    ctx
                                    tid
                            case (mSnap, stateE, requestUtxosE) of
                                (Nothing, _, _) ->
                                    pure
                                        $ statusError
                                            503
                                            "Indexer not ready: snapshot unavailable"
                                (_, Left err, _) -> pure (Left err)
                                (_, _, Left err) -> pure (Left err)
                                (Just snap, Right stateUtxo, Right requestUtxos) ->
                                    buildUpdateFacts
                                        ctx
                                        snap
                                        tid
                                        stateUtxo
                                        requestUtxos
                                        funding
      where
        buildUpdateFacts
            :: Context IO
            -> BundleSnapshot
            -> TokenId
            -> ResolvedWalletInput
            -> [ResolvedWalletInput]
            -> [ResolvedWalletInput]
            -> IO (Either HttpError UpdateFacts)
        buildUpdateFacts
            localCtx
            snap
            tid
            stateUtxo@(_, stateOutBytes, _)
            requestUtxos
            funding
                | null funding =
                    pure $ statusError 400 "No wallet UTxOs at address"
                | null requestUtxos =
                    pure
                        $ statusError
                            400
                            "No pending request UTxOs at request address"
                | otherwise = do
                    trieFactsE <-
                        computeRequestTrieFactsForWorkflow
                            localCtx
                            tid
                            requestUtxos
                    validityE <-
                        updateValidityUpperSlotForWorkflow
                            localCtx
                            stateOutBytes
                            requestUtxos
                    pp <- queryProtocolParams (provider localCtx)
                    pure $ do
                        trieFacts <- trieFactsE
                        trieRoot <-
                            stateTrieRootBytesForWorkflow stateOutBytes
                        validityUpperSlot <- validityE
                        Right
                            $ mkUpdateFacts
                                snap
                                tid
                                stateUtxo
                                requestUtxos
                                funding
                                trieRoot
                                trieFacts
                                validityUpperSlot
                                pp

workflowRetractFacts
    :: Context IO
    -> RetractRequest
    -> IO (Either HttpError RetractFacts)
workflowRetractFacts
    ctx
    RetractRequest
        { rrUtxo = utxoRef
        , rrAddr = addrHex
        } =
        case (parseAddr addrHex, parseUtxoRefForWorkflow utxoRef) of
            (Left err, _) -> pure $ statusError 400 err
            (_, Left err) -> pure (Left err)
            (Right addr, Right reqTxIn) -> do
                mLoc <-
                    St.getRequest
                        (St.requests (state ctx))
                        reqTxIn
                case mLoc of
                    Nothing ->
                        pure
                            $ statusError
                                404
                                "Unknown request: not in pending set"
                    Just (LocatedRequest _ r) -> do
                        let tid = requestToken r
                        walletE <- walletInputsFromProvider ctx addr
                        case walletE of
                            Left err -> pure (Left err)
                            Right walletInputs -> do
                                mSnap <- runIndexerTx ctx readSnapshot
                                requestE <-
                                    resolveInputForWorkflow
                                        ctx
                                        "workflow_e2e.retract.request_utxo"
                                        reqTxIn
                                stateE <-
                                    stateInputFromStateForWorkflow ctx tid
                                buildRetractFacts
                                    ctx
                                    tid
                                    mSnap
                                    requestE
                                    stateE
                                    walletInputs

buildRetractFacts
    :: Context IO
    -> TokenId
    -> Maybe BundleSnapshot
    -> Either HttpError ResolvedWalletInput
    -> Either HttpError ResolvedWalletInput
    -> [ResolvedWalletInput]
    -> IO (Either HttpError RetractFacts)
buildRetractFacts
    ctx
    tid
    mSnap
    requestE
    stateE
    walletInputs =
        case (mSnap, requestE, stateE) of
            (Nothing, _, _) ->
                pure
                    $ statusError
                        503
                        "Indexer not ready: snapshot unavailable"
            (_, Left err, _) -> pure (Left err)
            (_, _, Left err) -> pure (Left err)
            ( Just snap
                , Right requestUtxo@(_, reqOutBytes, _)
                , Right stateUtxo@(_, stateOutBytes, _)
                )
                    | null walletInputs ->
                        pure
                            $ statusError
                                400
                                "No wallet UTxOs at address"
                    | otherwise -> do
                        now <- currentPosixMs
                        validityE <-
                            retractValiditySlotsForWorkflow
                                ctx
                                snap
                                reqOutBytes
                                stateOutBytes
                                now
                        pp <- queryProtocolParams (provider ctx)
                        pure $ do
                            (startSlot, endSlot) <- validityE
                            Right
                                $ mkRetractFacts
                                    snap
                                    tid
                                    requestUtxo
                                    stateUtxo
                                    walletInputs
                                    startSlot
                                    endSlot
                                    pp

computeRequestTrieFactsForWorkflow
    :: Context IO
    -> TokenId
    -> [ResolvedWalletInput]
    -> IO (Either HttpError [TrieFact])
computeRequestTrieFactsForWorkflow ctx tid requestUtxos =
    case traverse decodeRequestRow requestUtxos of
        Left err -> pure (Left err)
        Right rows -> do
            trieReads <-
                fst
                    <$> UpdateTx.computeProofs
                        (trieManager ctx)
                        tid
                        rows
            pure (Right (map UpdateTx.readTrieFact trieReads))

updateValidityUpperSlotForWorkflow
    :: Context IO
    -> ByteString
    -> [ResolvedWalletInput]
    -> IO (Either HttpError Integer)
updateValidityUpperSlotForWorkflow ctx stateOutBytes requestUtxos =
    case ( decodeCageDatumForWorkflow
            "workflow_e2e.update.state_utxo"
            stateOutBytes
         , traverse decodeRequestRow requestUtxos
         ) of
        (Left err, _) -> pure (Left err)
        (_, Left err) -> pure (Left err)
        (Right (StateDatum oldState), Right rows) -> do
            SlotNo slot <-
                UpdateTx.computeUpperSlot
                    (provider ctx)
                    oldState
                    rows
            pure (Right (toInteger slot))
        (Right _, _) ->
            pure
                $ statusError
                    500
                    "workflow_e2e.update.state_utxo missing state datum"

stateTrieRootBytesForWorkflow
    :: ByteString -> Either HttpError ByteString
stateTrieRootBytesForWorkflow stateOutBytes =
    case decodeCageDatumForWorkflow
        "workflow_e2e.update.state_utxo"
        stateOutBytes of
        Right (StateDatum state') ->
            Right
                $ OnChain.unOnChainRoot
                $ OnChain.stateRoot state'
        Right _ ->
            statusError
                500
                "workflow_e2e.update.state_utxo missing state datum"
        Left err -> Left err

retractValiditySlotsForWorkflow
    :: Context IO
    -> BundleSnapshot
    -> ByteString
    -> ByteString
    -> Integer
    -> IO (Either HttpError (Integer, Integer))
retractValiditySlotsForWorkflow
    ctx
    snap
    requestOutBytes
    stateOutBytes
    now =
        case ( requestSubmittedAtForWorkflow requestOutBytes
             , stateRetractTimesForWorkflow stateOutBytes
             ) of
            (Right submittedAt, Right (procTime, retrTime)) -> do
                let phase2Start = submittedAt + procTime
                    phase2End =
                        submittedAt + procTime + retrTime
                if now < phase2Start
                    then
                        pure
                            $ statusError
                                400
                                "Request is not yet in the retract window"
                    else
                        if now >= phase2End
                            then
                                pure
                                    $ statusError
                                        400
                                        "Request is no longer retractable"
                            else do
                                SlotNo lowerRaw0 <-
                                    posixMsCeilSlot
                                        (provider ctx)
                                        now
                                evalCtxWire <- evalContext ctx
                                let ttl = workflowRejectTtlSlots
                                    safetyWindowMs =
                                        toInteger ttl
                                            * toInteger
                                                ( ecSlotLengthMs
                                                    evalCtxWire
                                                )
                                if phase2End <= now + safetyWindowMs
                                    then
                                        pure
                                            $ statusError
                                                400
                                                ( "Retract window is too "
                                                    <> "close to its end for "
                                                    <> "a safe validity interval"
                                                )
                                    else do
                                        let SlotNo snapSlot =
                                                snapshotSlot snap
                                            startSlot =
                                                toInteger
                                                    $ max
                                                        lowerRaw0
                                                        (snapSlot + 1)
                                            endSlot =
                                                startSlot
                                                    + toInteger ttl
                                        pure (Right (startSlot, endSlot))
            (Left err, _) -> pure (Left err)
            (_, Left err) -> pure (Left err)

requestSubmittedAtForWorkflow
    :: ByteString -> Either HttpError Integer
requestSubmittedAtForWorkflow requestOutBytes =
    case decodeCageDatumForWorkflow
        "workflow_e2e.retract.request_utxo"
        requestOutBytes of
        Right (RequestDatum reqDatum) ->
            Right (OnChain.requestSubmittedAt reqDatum)
        Right _ ->
            statusError
                500
                "workflow_e2e.retract.request_utxo missing request datum"
        Left err -> Left err

stateRetractTimesForWorkflow
    :: ByteString -> Either HttpError (Integer, Integer)
stateRetractTimesForWorkflow stateOutBytes =
    case decodeCageDatumForWorkflow
        "workflow_e2e.retract.state_utxo"
        stateOutBytes of
        Right (StateDatum state') ->
            Right
                ( OnChain.stateProcessTime state'
                , OnChain.stateRetractTime state'
                )
        Right _ ->
            statusError
                500
                "workflow_e2e.retract.state_utxo missing state datum"
        Left err -> Left err

decodeRequestRow
    :: ResolvedWalletInput
    -> Either HttpError (TxIn, TxOut ConwayEra)
decodeRequestRow (txIn, outBytes, _) = do
    out <-
        decodeTxOutForWorkflow
            "workflow_e2e.update.request_utxos[]"
            outBytes
    Right (txIn, out)

parseUtxoRefForWorkflow :: Text -> Either HttpError TxIn
parseUtxoRefForWorkflow ref =
    case T.splitOn "#" ref of
        [hashHex, ixText] -> do
            hashBytes <-
                case B16.decode (TE.encodeUtf8 hashHex) of
                    Right bs -> Right bs
                    Left _ ->
                        statusError 400 "Invalid tx hash hex"
            txId <- parseTxIdRawForWorkflow hashBytes
            case readMaybe (T.unpack ixText) of
                Just ix -> Right (mkTxInPartial txId ix)
                Nothing ->
                    statusError 400 "Invalid output index"
        _ ->
            statusError
                400
                "Invalid UTxO ref: expected txhash#ix"

parseTxIdRawForWorkflow :: ByteString -> Either HttpError TxId
parseTxIdRawForWorkflow bytes =
    case Crypto.hashFromBytes bytes of
        Just hash ->
            Right $ TxId $ unsafeMakeSafeHash hash
        Nothing ->
            statusError
                400
                "Invalid transaction ID: expected 32 bytes"

workflowEndFacts
    :: Context IO
    -> EndRequest
    -> IO (Either HttpError EndFacts)
workflowEndFacts ctx EndRequest{erToken, erAddr} =
    case parseAddr erAddr of
        Left err -> pure $ statusError 400 err
        Right addr -> do
            let tid = tokenIdFromJSON erToken
                cfg = cfgCage ctx
                requestAddr =
                    requestAddrFromCfg cfg tid (network cfg)
            walletE <- walletInputsFromProvider ctx addr
            case walletE of
                Left err -> pure (Left err)
                Right funding -> do
                    (mSnap, requestSet) <-
                        runIndexerTx ctx $ do
                            snap <- readSnapshot
                            reqSet <- readUtxoSetAt requestAddr
                            pure (snap, reqSet)
                    stateE <-
                        stateInputFromStateForWorkflow ctx tid
                    pp <- queryProtocolParams (provider ctx)
                    pure $ do
                        snap <- case mSnap of
                            Nothing ->
                                statusError
                                    503
                                    "Indexer not ready: snapshot unavailable"
                            Just value -> Right value
                        stateUtxo <- stateE
                        if null funding
                            then
                                statusError
                                    400
                                    "No wallet UTxOs at address"
                            else case requestSet of
                                ([], _) ->
                                    Right
                                        $ mkEndFacts
                                            snap
                                            tid
                                            stateUtxo
                                            funding
                                            requestSet
                                            pp
                                (entries, _) ->
                                    statusError
                                        409
                                        ( "Cannot end token: pending request "
                                            <> "UTxOs exist at the request "
                                            <> "address (count="
                                            <> show (length entries)
                                            <> ")"
                                        )

workflowRejectFacts
    :: Context IO
    -> RejectRequest
    -> IO (Either HttpError RejectFacts)
workflowRejectFacts ctx RejectRequest{rejToken, rejAddr, rejRequests} =
    case parseAddr rejAddr of
        Left err -> pure $ statusError 400 err
        Right addr
            | not (null rejRequests) ->
                pure
                    $ statusError
                        400
                        "Workflow e2e reject facts support all-pending requests"
            | otherwise -> do
                let tid = tokenIdFromJSON rejToken
                walletE <- walletInputsFromProvider ctx addr
                case walletE of
                    Left err -> pure (Left err)
                    Right funding -> do
                        mSnap <- runIndexerTx ctx readSnapshot
                        stateE <-
                            stateInputFromStateForWorkflow ctx tid
                        requestUtxosE <-
                            requestInputsForTokenFromStateForWorkflow
                                ctx
                                tid
                        case (mSnap, stateE, requestUtxosE) of
                            (Nothing, _, _) ->
                                pure
                                    $ statusError
                                        503
                                        "Indexer not ready: snapshot unavailable"
                            (_, Left err, _) -> pure (Left err)
                            (_, _, Left err) -> pure (Left err)
                            (Just snap, Right stateUtxo, Right requestUtxos) ->
                                buildRejectFacts
                                    ctx
                                    snap
                                    tid
                                    stateUtxo
                                    requestUtxos
                                    funding
  where
    buildRejectFacts
        :: Context IO
        -> BundleSnapshot
        -> TokenId
        -> ResolvedWalletInput
        -> [ResolvedWalletInput]
        -> [ResolvedWalletInput]
        -> IO (Either HttpError RejectFacts)
    buildRejectFacts
        localCtx
        snap
        tid
        stateUtxo@(_, stateOutBytes, _)
        requestUtxos
        funding
            | null funding =
                pure $ statusError 400 "No wallet UTxOs at address"
            | otherwise = do
                rejectableE <-
                    rejectableRequestUtxosForWorkflow
                        stateOutBytes
                        requestUtxos
                case rejectableE of
                    Left err -> pure (Left err)
                    Right [] ->
                        pure
                            $ statusError
                                400
                                "No rejectable request UTxOs at request address"
                    Right rejectable -> do
                        slotsE <-
                            rejectValiditySlotsForWorkflow
                                localCtx
                                snap
                                stateOutBytes
                                rejectable
                        pp <- queryProtocolParams (provider localCtx)
                        pure $ do
                            (lowerSlot, upperSlot) <- slotsE
                            Right
                                $ mkRejectFacts
                                    snap
                                    tid
                                    stateUtxo
                                    rejectable
                                    funding
                                    lowerSlot
                                    upperSlot
                                    pp

walletInputsFromProvider
    :: Context IO -> Addr -> IO (Either HttpError [ResolvedWalletInput])
walletInputsFromProvider ctx addr = do
    utxos <- queryUTxOs (provider ctx) addr
    rows <-
        forM utxos $ \(txIn, txOut) -> do
            mProof <- pollUntilJust 10 (utxoProof ctx txIn)
            pure $ case mProof of
                Nothing ->
                    statusError
                        503
                        ( "CSMT proof unavailable for current wallet input "
                            <> show txIn
                        )
                Just proof ->
                    Right
                        ( txIn
                        , serialize' (natVersion @11) txOut
                        , proof
                        )
    pure (sortResolvedInputs <$> sequence rows)

snapshotAndWalletForWorkflow
    :: Context IO
    -> Addr
    -> IO (Either HttpError (BundleSnapshot, [ResolvedWalletInput]))
snapshotAndWalletForWorkflow ctx addr = do
    mSnap <- runIndexerTx ctx readSnapshot
    walletE <- walletInputsFromProvider ctx addr
    pure $ do
        snap <- case mSnap of
            Nothing ->
                statusError
                    503
                    "Indexer not ready: snapshot unavailable"
            Just value -> Right value
        funding <- walletE
        if null funding
            then statusError 400 "No wallet UTxOs at address"
            else Right (snap, funding)

stateInputFromStateForWorkflow
    :: Context IO -> TokenId -> IO (Either HttpError ResolvedWalletInput)
stateInputFromStateForWorkflow ctx tid = do
    mToken <-
        St.getToken
            (St.tokens (state ctx))
            tid
    case mToken of
        Nothing ->
            pure
                $ statusError
                    404
                    "State UTxO not found for token"
        Just (LocatedTokenState stateTxIn _) ->
            resolveInputForWorkflow
                ctx
                "workflow_e2e.state_utxo"
                stateTxIn

requestInputsForTokenFromStateForWorkflow
    :: Context IO -> TokenId -> IO (Either HttpError [ResolvedWalletInput])
requestInputsForTokenFromStateForWorkflow ctx tid = do
    reqs <-
        St.requestsByToken
            (St.requests (state ctx))
            tid
    rows <-
        traverse
            ( \(LocatedRequest reqTxIn _) ->
                resolveInputForWorkflow
                    ctx
                    "workflow_e2e.request_utxos[]"
                    reqTxIn
            )
            reqs
    pure (sortResolvedInputs <$> sequence rows)

resolveInputForWorkflow
    :: Context IO
    -> String
    -> TxIn
    -> IO (Either HttpError ResolvedWalletInput)
resolveInputForWorkflow ctx label txIn = do
    mOut <- resolveUtxo ctx txIn
    mProof <- pollUntilJust 10 (utxoProof ctx txIn)
    pure $ case (mOut, mProof) of
        (Just out, Just proof) ->
            Right (txIn, out, proof)
        (Nothing, _) ->
            statusError
                404
                (label <> " not found in current UTxO KV: " <> show txIn)
        (_, Nothing) ->
            statusError
                503
                (label <> " CSMT proof unavailable: " <> show txIn)

rejectableRequestUtxosForWorkflow
    :: ByteString
    -> [ResolvedWalletInput]
    -> IO (Either HttpError [ResolvedWalletInput])
rejectableRequestUtxosForWorkflow stateOutBytes requestUtxos =
    case decodeCageDatumForWorkflow
        "workflow_e2e.reject.state_utxo"
        stateOutBytes of
        Left err -> pure (Left err)
        Right (StateDatum s) -> do
            now <- currentPosixMs
            let pt = OnChain.stateProcessTime s
                rt = OnChain.stateRetractTime s
            pure $ do
                flags <-
                    traverse
                        (isRejectable now pt rt)
                        requestUtxos
                Right
                    [ utxo
                    | (utxo, True) <- zip requestUtxos flags
                    ]
        Right _ ->
            pure
                $ statusError
                    500
                    "workflow_e2e.reject.state_utxo missing state datum"

rejectValiditySlotsForWorkflow
    :: Context IO
    -> BundleSnapshot
    -> ByteString
    -> [ResolvedWalletInput]
    -> IO (Either HttpError (Integer, Integer))
rejectValiditySlotsForWorkflow ctx snap stateOutBytes rejectableUtxos =
    case decodeCageDatumForWorkflow
        "workflow_e2e.reject.state_utxo"
        stateOutBytes of
        Left err -> pure (Left err)
        Right (StateDatum s) -> do
            let pt = OnChain.stateProcessTime s
                rt = OnChain.stateRetractTime s
            case traverse (requestDeadline pt rt) rejectableUtxos of
                Left err -> pure (Left err)
                Right deadlines -> do
                    let latest = maximum (0 : deadlines)
                        SlotNo snapSlot = snapshotSlot snap
                    SlotNo deadlineRaw <-
                        posixMsCeilSlot
                            (provider ctx)
                            latest
                    let lowerRaw =
                            max
                                (deadlineRaw + 1)
                                (snapSlot + 1)
                        upperRaw =
                            lowerRaw
                                + workflowRejectTtlSlots
                    pure
                        $ Right
                            ( toInteger lowerRaw
                            , toInteger upperRaw
                            )
        Right _ ->
            pure
                $ statusError
                    500
                    "workflow_e2e.reject.state_utxo missing state datum"

workflowRejectTtlSlots :: Word64
workflowRejectTtlSlots = 20

isRejectable
    :: Integer
    -> Integer
    -> Integer
    -> ResolvedWalletInput
    -> Either HttpError Bool
isRejectable now processTime retractTime (_, outBytes, _) =
    case decodeCageDatumForWorkflow
        "workflow_e2e.reject.request_utxos[]"
        outBytes of
        Left err -> Left err
        Right (RequestDatum r) ->
            let submittedAt = OnChain.requestSubmittedAt r
                deadline =
                    submittedAt
                        + processTime
                        + retractTime
            in  Right (now > deadline || submittedAt > now)
        Right _ -> Right False

requestDeadline
    :: Integer
    -> Integer
    -> ResolvedWalletInput
    -> Either HttpError Integer
requestDeadline processTime retractTime (_, outBytes, _) =
    case decodeCageDatumForWorkflow
        "workflow_e2e.reject.request_utxos[]"
        outBytes of
        Left err -> Left err
        Right (RequestDatum r) ->
            Right
                ( OnChain.requestSubmittedAt r
                    + processTime
                    + retractTime
                )
        Right _ ->
            statusError
                500
                "workflow_e2e.reject.request_utxos[] missing request datum"

decodeCageDatumForWorkflow
    :: String -> ByteString -> Either HttpError CageDatum
decodeCageDatumForWorkflow label bytes = do
    out <- decodeTxOutForWorkflow label bytes
    case extractCageDatum out of
        Just datum -> Right datum
        Nothing ->
            statusError
                500
                (label <> " missing inline cage datum")

decodeTxOutForWorkflow
    :: String -> ByteString -> Either HttpError (TxOut ConwayEra)
decodeTxOutForWorkflow label bytes =
    case decodeFull (natVersion @11) (BSL.fromStrict bytes) of
        Right out -> Right out
        Left err ->
            statusError
                500
                (label <> " TxOut CBOR decode failed: " <> show err)

sortResolvedInputs
    :: [ResolvedWalletInput] -> [ResolvedWalletInput]
sortResolvedInputs =
    sortOn (\(txIn, _, _) -> txIn)

statusError :: Int -> String -> Either HttpError a
statusError code msg =
    Left (HttpStatus code (TE.encodeUtf8 (T.pack msg)))

-- | Back the workflows' 'HttpClient' with the in-process WAI
-- application. POSTs the JSON body to the relative path and returns
-- the JSON response, mapping a non-200 to 'HttpStatus'.
waiHttpClient :: Application -> HttpClient
waiHttpClient app =
    HttpClient $ \path body -> do
        resp <-
            runSession
                ( srequest
                    SRequest
                        { simpleRequest =
                            (setPath defaultRequest (TE.encodeUtf8 path))
                                { requestMethod = methodPost
                                , requestHeaders =
                                    [(hContentType, "application/json")]
                                }
                        , simpleRequestBody = BSL.fromStrict body
                        }
                )
                app
        let code = statusCode (simpleStatus resp)
            respBody = BSL.toStrict (simpleBody resp)
        pure
            $ if code == 200
                then Right respBody
                else Left (HttpStatus code respBody)

-- ---------------------------------------------------------
-- Read-endpoint assertions (in-process WAI GET/POST helpers)
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
                        ("workflows e2e: decode /status: " <> err)
                        $> Nothing
                Right StatusResponse{currentUtxoRoot} ->
                    pure currentUtxoRoot
    case mRoot of
        Nothing ->
            expectationFailure
                "workflows e2e: /status never exposed utxo_root"
                *> error "unreachable"
        Just root -> pure (TrustedRoot root)

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
    bshow = BS.pack . map (fromIntegral . fromEnum) . show

tokenVisible :: Application -> TokenId -> IO Bool
tokenVisible app tokenId = do
    resp <- get app ("/tokens/" <> tokenIdHex tokenId)
    pure (simpleStatus resp == status200)

tokenRemoved :: Application -> TokenId -> IO ()
tokenRemoved app tokenId = do
    mGone <-
        pollUntilJust 60 $ do
            resp <- get app ("/tokens/" <> tokenIdHex tokenId)
            if simpleStatus resp == status200
                then pure Nothing
                else pure (Just ())
    case mGone of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "endCage row: token still visible after end"

pendingRequestsNonEmpty :: Application -> TokenId -> IO ()
pendingRequestsNonEmpty app tokenId = do
    mRes <-
        pollUntilJust 60 $ do
            resp <-
                get app ("/tokens/" <> tokenIdHex tokenId <> "/requests")
            if simpleStatus resp == status200
                then pure (requestsCount (simpleBody resp))
                else pure Nothing
    case mRes of
        Just n | n > 0 -> pure ()
        _ ->
            expectationFailure
                "request row: pending requests stayed empty after submit"

pendingRequestsEmpty :: Application -> TokenId -> IO ()
pendingRequestsEmpty app tokenId = do
    mDone <-
        pollUntilJust 60 $ do
            resp <-
                get app ("/tokens/" <> tokenIdHex tokenId <> "/requests")
            if simpleStatus resp == status200
                then pure (requestsEmpty (simpleBody resp))
                else pure Nothing
    case mDone of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "drain row: pending requests did not drain after submit"

factIndexed :: Application -> TokenId -> ByteString -> IO ()
factIndexed app tokenId key = do
    mOk <-
        pollUntilJust 60 $ do
            resp <-
                get
                    app
                    ( "/tokens/"
                        <> tokenIdHex tokenId
                        <> "/facts/"
                        <> B16.encode key
                    )
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
                "applyRequests row: fact never verified after apply"

-- | Count the @requests@ array, returning 'Nothing' when empty so the
-- poll keeps waiting.
requestsCount :: BSL.ByteString -> Maybe Int
requestsCount body =
    case eitherDecode body of
        Right (RequestsList rs) ->
            if null rs then Nothing else Just (length rs)
        Left _ -> Nothing

requestsEmpty :: BSL.ByteString -> Maybe ()
requestsEmpty body =
    case eitherDecode body of
        Right (RequestsList rs)
            | null rs -> Just ()
        _ -> Nothing

-- | Minimal decoder for the @requests@ field of the requests
-- response — avoids importing the full response type.
newtype RequestsList = RequestsList [Aeson.Value]

instance Aeson.FromJSON RequestsList where
    parseJSON =
        Aeson.withObject "requestsResponse" $ \o ->
            RequestsList <$> o Aeson..: "requests"

-- ---------------------------------------------------------
-- WAI plumbing
-- ---------------------------------------------------------

get :: Application -> ByteString -> IO SResponse
get app path =
    runSession (request (setPath defaultRequest path)) app

postJson
    :: (Aeson.ToJSON body)
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

pollUntilJust :: Int -> IO (Maybe a) -> IO (Maybe a)
pollUntilJust timeoutSec action = go (timeoutSec * 2)
  where
    go 0 = action
    go n = do
        result <- action
        case result of
            Just _ -> pure result
            Nothing -> threadDelay 500_000 >> go (n - 1)

-- ---------------------------------------------------------
-- Conversions / fixtures
-- ---------------------------------------------------------

extractTokenId :: CageConfig -> ConwayTx -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma = tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
    in  case Map.toList (Map.findWithDefault Map.empty pid ma) of
            [(an, _)] -> TokenId an
            _ -> error "workflows e2e: unexpected boot mint"

tokenIdJSON :: TokenId -> TokenIdJSON
tokenIdJSON (TokenId (AssetName sbs)) =
    TokenIdJSON (SBS.fromShort sbs)

tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

txIdHex :: TxId -> ByteString
txIdHex (TxId sh) =
    B16.encode (Crypto.hashToBytes (extractHash sh))

txInToHashIx :: TxIn -> Text
txInToHashIx (TxIn txId (TxIx ix)) =
    TE.decodeUtf8 (txIdHex txId) <> "#" <> T.pack (show ix)

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

-- ---------------------------------------------------------
-- Bracket
-- ---------------------------------------------------------

withSharedEnv :: CageScripts -> (Env -> IO ()) -> IO ()
withSharedEnv scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory "mpfs-workflows-e2e" $ \tmpDir -> do
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
                evalCtxWire <- evalContext ctx
                evalCtx <-
                    case decodeEvalContext evalCtxWire of
                        Right value -> pure value
                        Left err ->
                            fail
                                $ "WorkflowsIntegration: decodeEvalContext \
                                  \failed: "
                                    <> show err
                let app = mkApp ctx
                action
                    Env
                        { envCfg = cfg
                        , envApp = app
                        , envHttp = workflowHttpClient ctx app
                        , envEvalContext = evalCtx
                        }

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
