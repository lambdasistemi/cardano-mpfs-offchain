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
import Data.Functor (($>))
import Data.Map.Strict qualified as Map
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
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn (TxId (..))

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootFacts
    , BootRequest (..)
    , DeleteRequest (..)
    , EndFacts
    , EndRequest (..)
    , InsertRequest (..)
    , RequestDeleteFacts
    , RequestInsertFacts
    , StatusResponse (..)
    )
import Cardano.MPFS.API.Types.Common (TokenIdJSON (..))
import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.End (endCageTx)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    )
import Cardano.MPFS.Client.Facts
    ( verifyBootFacts
    , verifyEndFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , ConwayEra
    , SlotNo (..)
    , TokenId (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..))
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
            -- Process the insert request via the existing
            -- internal txBuilder.updateToken path so the
            -- fact lands in the trie and the request set
            -- becomes empty. This is not a facts endpoint
            -- and is therefore out of scope of the matrix
            -- proper; we only use it to set up the
            -- preconditions for the delete row.
            processPendingRequests ctx app tokenId
            factIndexed app tokenId matrixInsertKey
            runRequestDeleteRow
                cfg
                ctx
                app
                tokenId
                matrixInsertKey
                matrixInsertValue
            processPendingRequests ctx app tokenId
            factAbsent app tokenId matrixInsertKey
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

-- | Drive the internal @updateToken@ tx builder to
-- materialise any pending requests for the token, then
-- wait until the requests list reports empty. Not a facts
-- endpoint; used by the matrix only to set up
-- preconditions for the delete and end rows.
processPendingRequests
    :: Context IO -> Application -> TokenId -> IO ()
processPendingRequests ctx app tokenId = do
    envelope <-
        updateToken
            (txBuilder ctx)
            emptyBundleSnapshot
            tokenId
            genesisAddr
    let signed = addKeyWitness genesisSignKey (envTx envelope)
    result <- submitTx (submitter ctx) signed
    assertSubmitted "process row" result
    awaitTx app (txIdTx signed)
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

-- | Poll @\/tokens\/:id\/facts\/:key@ until it returns 200.
factIndexed
    :: Application -> TokenId -> ByteString -> IO ()
factIndexed app tokenId key = do
    mOk <-
        pollUntilJust 60 $ do
            resp <- get app (factPath tokenId key)
            if simpleStatus resp == status200
                then pure (Just ())
                else pure Nothing
    case mOk of
        Just () -> pure ()
        Nothing ->
            expectationFailure
                "process row: fact never appeared at \
                \/tokens/:id/facts/:key after insert+process"

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
        Just () -> pure ()
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

-- | Placeholder snapshot used by the internal txBuilder
-- update path. The real CSMT root for processing is read
-- from the indexer inside the builder.
emptyBundleSnapshot :: BundleSnapshot
emptyBundleSnapshot =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

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
