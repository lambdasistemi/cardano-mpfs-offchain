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
import Data.Aeson (decode, eitherDecode, encode)
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
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

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address (serialiseAddr)
import Cardano.Ledger.Api.Tx (bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..), TxIx (..))
import Cardano.Ledger.Binary
    ( Annotator
    , Decoder
    , decCBOR
    , decodeFullAnnotator
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value (AssetName (..), MultiAsset (..))
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (FactResponse (..), StatusResponse (..))
import Cardano.MPFS.API.Types.Common
    ( TokenIdJSON (..)
    , VerificationSnapshot (..)
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
import Cardano.MPFS.Core.Types
    ( SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( awaitProofReadsReady
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.Types
    ( SubmitRequest (..)
    , SubmitResponse (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
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
                awaitProofReadsReady ctx
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
                        , envHttp = waiHttpClient app
                        , envEvalContext = evalCtx
                        }

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
