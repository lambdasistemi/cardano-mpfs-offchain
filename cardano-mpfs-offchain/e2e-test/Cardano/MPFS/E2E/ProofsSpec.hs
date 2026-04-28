{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.ProofsSpec
-- Description : Verify proof-bearing envelopes end-to-end
-- License     : Apache-2.0
--
-- Boot a token, insert a fact, process it, then call the
-- proof-bearing read and write endpoints against a real devnet.
-- For each response we:
--
--   * parse the embedded verification snapshot;
--   * run 'verifyVerificationSnapshot' from
--     @cardano-mpfs-client@ (structural check on the
--     @utxo_root@ length and @chainpoint.block_id@);
--   * assert the fields that the bundled UTxO-CSMT and
--     MPF proofs live in are non-empty hex.
--
-- This gives us a minimum-viable end-to-end contract
-- check: the server emits proof-bearing envelopes that
-- the released verification client can parse and
-- structurally validate.
module Cardano.MPFS.E2E.ProofsSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Data.Aeson
    ( FromJSON (..)
    , Value
    , decode
    , encode
    , object
    , withObject
    , (.:)
    , (.=)
    )
import Data.Aeson.Key (Key)
import Data.Aeson.Key qualified as Key
import Data.Aeson.Types (parseEither)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
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
    , shouldSatisfy
    )

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address (Addr, serialiseAddr)
import Cardano.Ledger.Api.Tx (Tx, bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..), TxIx (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( extractCompiledCode
    , loadBlueprint
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , TokenId (..)
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider
    ( Provider (..)
    , SlotNo (..)
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

import Cardano.MPFS.Client
    ( BootTxResponse
    , EndTxResponse
    , Hex (..)
    , RejectTxResponse
    , RequestTxResponse
    , RetractTxResponse
    , UpdateTxResponse
    , VerificationSnapshot
    , csmtReplayFailedAt
    , flipProof
    , flipSnapshotRoot
    , flipTxOut
    , runForgeBoot
    , runForgeEnd
    , runForgeReject
    , runForgeRequest
    , runForgeRetract
    , runForgeUpdate
    , shouldAccept
    , shouldRejectWith
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyUpdateTxResponse
    , verifyVerificationSnapshot
    , withReason
    )

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Proof-bearing envelopes E2E" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (no MPFS_BLUEPRINT)"
                $ pure @IO ()
        Just path -> do
            ebp <- runIO $ loadBlueprint path
            case ebp of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right bp ->
                    case extractCompiledCode
                        "state."
                        bp of
                        Nothing ->
                            it "no compiled code"
                                $ expectationFailure
                                    "state script \
                                    \not found"
                        Just sb ->
                            proofsSpec sb

-- -------------------------------------------------
-- Scenario
-- -------------------------------------------------

-- | Await timeout in seconds. Devnet slots are 100ms.
awaitTimeout :: Int
awaitTimeout = 60

-- | Wait just past the devnet reject deadline. The
-- local 'CageConfig' uses millisecond windows; add a
-- small safety margin for wall-clock and indexing jitter.
rejectDeadlineDelay :: CageConfig -> Int
rejectDeadlineDelay cfg =
    fromIntegral
        ( defaultProcessTime cfg
            + defaultRetractTime cfg
            + 2_000
        )
        * 1_000

-- | Fact key and value used throughout the scenario.
factKey :: ByteString
factKey = "hello"

factValue :: ByteString
factValue = "world"

proofsSpec :: SBS.ShortByteString -> Spec
proofsSpec scriptBytes =
    it "read and write envelopes carry verifiable proofs"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx
                submit =
                    signSubmitAwait
                        awaitTimeout
                        app
                        ctx

            -- Boot → insert → update to land the fact
            bootTx <-
                submit $ bootToken tb emptySnap genesisAddr
            let tid = extractTokenId cfg bootTx
                tidHex = tokenIdHex tid
                keyHex = B16.encode factKey

            _ <-
                submit
                    $ requestInsert
                        tb
                        emptySnap
                        tid
                        factKey
                        factValue
                        genesisAddr
            _ <-
                submit
                    $ updateToken
                        tb
                        emptySnap
                        tid
                        genesisAddr

            -- Add a second pending request so the
            -- /requests endpoint returns a non-empty
            -- witnessed list.
            reqTx <-
                submit
                    $ requestInsert
                        tb
                        emptySnap
                        tid
                        "bye"
                        "moon"
                        genesisAddr
            let reqTxIn =
                    TxIn (txIdTx reqTx) (TxIx 0)

            -- Pull every proof-bearing read.
            tokenObj <- getJSON app ("/tokens/" <> tidHex)
            factObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/facts/"
                        <> keyHex
            proofObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/proofs/"
                        <> keyHex
            requestsObj <-
                getJSON app
                    $ "/tokens/"
                        <> tidHex
                        <> "/requests"

            -- Every response carries the same snapshot
            -- shape and that snapshot passes the client
            -- structural verifier.
            tokenSnap <- extractSnapshot tokenObj
            factSnap <- extractSnapshot factObj
            proofSnap <- extractSnapshot proofObj
            reqsSnap <- extractSnapshot requestsObj

            mapM_
                assertSnapshotValid
                [tokenSnap, factSnap, proofSnap, reqsSnap]

            -- Each response ships the proofs the client
            -- will later consume — assert they are
            -- present as non-empty hex.
            assertWitnessedUtxo
                =<< lookupObj tokenObj "state"
            assertFactEnvelope factObj
            assertProofEnvelope proofObj

            assertRequestsEnvelope requestsObj

            -- Drive every write endpoint over HTTP and
            -- verify its response with the offline
            -- Client.Verify DSL. We do not submit the
            -- returned unsigned txs — the goal is to
            -- confirm the server emits well-formed
            -- proof envelopes at the current state.
            let addrHex = hexAddr genesisAddr
                retractUtxoRef =
                    txInUrlRef reqTxIn

            -- Every endpoint pairs a positive `shouldAccept`
            -- against the honest server response with a
            -- negative `shouldRejectWith` driven by a
            -- `CsmtForge` or `TrieForge` program. The DSL is
            -- an operational free monad: forgeries are just
            -- sequenced instructions, and each endpoint has
            -- its own runner
            -- (`runForgeBoot`, `runForgeUpdate`, ...).
            -- One tampered field per program, explicit
            -- dotted field path + reason on every rejection.

            bootResp <-
                postJSON app "/tx/boot"
                    $ object ["address" .= addrHex]
            (bootResp :: BootTxResponse)
                `shouldAccept` verifyBootTxResponse
            runForgeBoot (flipProof "funding[0]") bootResp
                `shouldRejectWith` verifyBootTxResponse
                $ csmtReplayFailedAt
                    "boot.funding[0].utxo_proof"

            insertResp <-
                postJSON
                    app
                    "/tx/request/insert"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "key" .= Hex (TE.decodeUtf8 (B16.encode "baz"))
                        , "value"
                            .= Hex (TE.decodeUtf8 (B16.encode "qux"))
                        , "address" .= addrHex
                        ]
            (insertResp :: RequestTxResponse)
                `shouldAccept` verifyRequestTxResponse
            runForgeRequest
                (flipTxOut "funding[0]")
                insertResp
                `shouldRejectWith` verifyRequestTxResponse
                $ csmtReplayFailedAt
                    "request.funding[0].utxo_proof"
                    `withReason` "value binding mismatch"

            updateResp <-
                postJSON app "/tx/update"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            (updateResp :: UpdateTxResponse)
                `shouldAccept` verifyUpdateTxResponse
            -- CSMT forgery on the same update response:
            runForgeUpdate (flipTxOut "state") updateResp
                `shouldRejectWith` verifyUpdateTxResponse
                $ csmtReplayFailedAt
                    "update.state.utxo_proof"
                    `withReason` "value binding mismatch"
            -- MPF forgery is covered in the unit suite
            -- ("Cardano.MPFS.Client.VerifySpec") against a
            -- guaranteed-non-empty `trie_read` fixture. The
            -- devnet's `/tx/update` response may carry an
            -- empty `trie_read` if no pending request was
            -- observed in time, so `flipTrieRoot` is not a
            -- reliable forgery at this stage.

            retractResp <-
                postJSON app "/tx/retract"
                    $ object
                        [ "utxo" .= retractUtxoRef
                        , "address" .= addrHex
                        ]
            (retractResp :: RetractTxResponse)
                `shouldAccept` verifyRetractTxResponse
            runForgeRetract flipSnapshotRoot retractResp
                `shouldRejectWith` verifyRetractTxResponse
                $ csmtReplayFailedAt
                    "retract.request_in.utxo_proof"
                    `withReason` "root mismatch"

            threadDelay (rejectDeadlineDelay cfg)

            rejectResp <-
                postJSON app "/tx/reject"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            (rejectResp :: RejectTxResponse)
                `shouldAccept` verifyRejectTxResponse
            runForgeReject (flipProof "request_ins[0]") rejectResp
                `shouldRejectWith` verifyRejectTxResponse
                $ csmtReplayFailedAt
                    "reject.request_ins[0].utxo_proof"

            endResp <-
                postJSON app "/tx/end"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            (endResp :: EndTxResponse)
                `shouldAccept` verifyEndTxResponse
            runForgeEnd (flipProof "state") endResp
                `shouldRejectWith` verifyEndTxResponse
                $ csmtReplayFailedAt
                    "end.state.utxo_proof"

-- -------------------------------------------------
-- Assertions on response shape
-- -------------------------------------------------

-- | Verify a 'VerificationSnapshot' with the offline
-- verifier shipped by @cardano-mpfs-client@.
assertSnapshotValid :: VerificationSnapshot -> IO ()
assertSnapshotValid snap =
    case verifyVerificationSnapshot snap of
        Right () -> pure ()
        Left err ->
            expectationFailure
                $ "snapshot verification failed: "
                    <> show err

-- | Pull the @snapshot@ field out of a response object.
extractSnapshot :: Value -> IO VerificationSnapshot
extractSnapshot v =
    case parseEither snapshotField v of
        Right s -> pure s
        Left err ->
            do
                expectationFailure
                    $ "snapshot parse: " <> err
                error "unreachable"
  where
    snapshotField =
        withObject "response" $ \o ->
            o .: "snapshot"

-- | Every witnessed UTxO envelope must ship
-- @tx_in@, @tx_out@ and @utxo_proof@ as non-empty hex.
assertWitnessedUtxo :: Value -> IO ()
assertWitnessedUtxo v = do
    utxo <- lookupObj v "utxo"
    _ <- lookupObj utxo "tx_in"
    txOut <- lookupHex utxo "tx_out"
    txOut `shouldSatisfy` (not . T.null)
    proof <- lookupHex utxo "utxo_proof"
    proof `shouldSatisfy` (not . T.null)

-- | The @/facts/:key@ envelope carries the stored
-- MPF value, the state witness, and the MPF proof.
-- MPFS stores values as 32-byte content hashes, so we
-- only assert the @value@ field is non-empty hex.
assertFactEnvelope :: Value -> IO ()
assertFactEnvelope v = do
    val <- lookupHex v "value"
    val `shouldSatisfy` (not . T.null)
    fact <- lookupObj v "fact"
    state <- lookupObj fact "state"
    assertWitnessedUtxo state
    mpfProof <- lookupHex fact "mpf_proof"
    mpfProof `shouldSatisfy` (not . T.null)

-- | The @/proofs/:key@ envelope carries the state
-- witness and the MPF proof (no value).
assertProofEnvelope :: Value -> IO ()
assertProofEnvelope v = do
    fact <- lookupObj v "fact"
    state <- lookupObj fact "state"
    assertWitnessedUtxo state
    mpfProof <- lookupHex fact "mpf_proof"
    mpfProof `shouldSatisfy` (not . T.null)

-- | The @/requests@ envelope must contain at least one
-- witnessed request with both its UTxO witness and a
-- decoded request payload.
assertRequestsEnvelope :: Value -> IO ()
assertRequestsEnvelope v = do
    requests <- lookupArr v "requests"
    case requests of
        [] ->
            expectationFailure
                "expected at least one witnessed \
                \request"
        (wreq : _) -> do
            assertWitnessedUtxo wreq
            _ <- lookupObj wreq "request"
            pure ()

-- -------------------------------------------------
-- JSON plumbing
-- -------------------------------------------------

lookupObj :: Value -> Key -> IO Value
lookupObj = parseField

lookupHex :: Value -> Key -> IO Text
lookupHex v k = do
    Hex t <- parseField v k
    pure t

lookupArr :: Value -> Key -> IO [Value]
lookupArr = parseField

parseField :: (FromJSON a) => Value -> Key -> IO a
parseField v k =
    case parseEither
        (withObject "obj" (.: k))
        v of
        Right x -> pure x
        Left err ->
            do
                expectationFailure
                    $ "field "
                        <> Key.toString k
                        <> ": "
                        <> err
                error "unreachable"

-- -------------------------------------------------
-- DSL (mirrors HTTPLifecycleSpec)
-- -------------------------------------------------

signSubmitAwait
    :: Int
    -> Application
    -> Context IO
    -> IO (ProofEnvelope p)
    -> IO (Tx ConwayEra)
signSubmitAwait timeout app ctx buildBundle = do
    bundle <- buildBundle
    let unsigned = envTx bundle
        signed =
            addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx timeout app (txIdTx signed)
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

awaitTx :: Int -> Application -> TxId -> IO ()
awaitTx timeout app tid = do
    resp <- getRaw app path
    simpleStatus resp `shouldBe` status200
  where
    path =
        "/tx/"
            <> txIdHex tid
            <> "?timeout="
            <> bshow timeout
    bshow =
        BS.pack
            . map (fromIntegral . fromEnum)
            . show

getJSON :: Application -> ByteString -> IO Value
getJSON app path = do
    resp <- getRaw app path
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just v -> pure v
        Nothing ->
            do
                expectationFailure
                    $ "non-JSON response: "
                        <> show (simpleBody resp)
                error "unreachable"

-- | POST a JSON body and decode the response.
postJSON
    :: FromJSON a
    => Application
    -> ByteString
    -> Value
    -> IO a
postJSON app path body = do
    let req =
            (setPath defaultRequest path)
                { requestMethod = methodPost
                , requestHeaders =
                    [ (hContentType, "application/json")
                    ]
                }
    resp <-
        runSession
            (srequest (SRequest req (encode body)))
            app
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just v -> pure v
        Nothing ->
            do
                expectationFailure
                    $ "POST "
                        <> show path
                        <> " returned non-JSON: "
                        <> show (simpleBody resp)
                error "unreachable"

-- | Hex-encode a bech32-serialisable address for
-- JSON transport.
hexAddr :: Addr -> Hex
hexAddr =
    Hex . TE.decodeUtf8 . B16.encode . serialiseAddr

-- | Render a TxIn as the @txhash#ix@ string expected
-- by the /tx/retract request body.
txInUrlRef :: TxIn -> Text
txInUrlRef (TxIn txI (TxIx ix)) =
    TE.decodeUtf8 (txIdHex txI)
        <> "#"
        <> T.pack (show ix)

getRaw :: Application -> ByteString -> IO SResponse
getRaw app path =
    runSession
        (request (setPath defaultRequest path))
        app

assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

-- | Hex-encode a 'TokenId' for URL paths.
tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

-- | Raw 32-byte hash as hex for URL paths.
txIdHex :: TxId -> ByteString
txIdHex (TxId sh) =
    B16.encode
        $ Crypto.hashToBytes
        $ extractHash sh

-- | Extract the sole minted 'TokenId'.
extractTokenId
    :: CageConfig -> Tx ConwayEra -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "extractTokenId: \
                    \unexpected mint"

-- -------------------------------------------------
-- Bracket
-- -------------------------------------------------

withE2E
    :: SBS.ShortByteString
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scriptBytes action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-proofs-e2e"
            $ \tmpDir -> do
                let cfg = cageCfg scriptBytes
                    appCfg =
                        AppConfig
                            { epochSlots =
                                EpochSlots 4320
                            , shelleyGenesisPath =
                                gDir
                                    </> "shelley-genesis.json"
                            , socketPath = sock
                            , dbPath = tmpDir </> "db"
                            , channelCapacity = 16
                            , cageConfig = cfg
                            , byronGenesisPath =
                                Nothing
                            , followerEnabled = True
                            , appTracer = nullTracer
                            }
                withApplication appCfg $ \ctx -> do
                    _ <-
                        queryProtocolParams
                            (provider ctx)
                    threadDelay 10_000_000
                    action cfg ctx

cageCfg :: SBS.ShortByteString -> CageConfig
cageCfg scriptBytes =
    CageConfig
        { cageScriptBytes = scriptBytes
        , cfgScriptHash =
            computeScriptHash scriptBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
