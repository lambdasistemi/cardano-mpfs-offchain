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
import Control.Monad (void)
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
import Data.ByteString.Lazy qualified as BSL
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
import Cardano.Ledger.Api.Tx (bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body
    ( mintTxBodyL
    )
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Binary
    ( Annotator
    , Decoder
    , decCBOR
    , decodeFullAnnotator
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MultiAsset (..)
    )
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Ledger.TxIn (TxId (..))
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
    , TokenId (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( walletBootInputs
    , withBootFactsTxBuilder
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

import Cardano.MPFS.API.Encoding qualified as Api
import Cardano.MPFS.API.Types
    ( FactResponse (..)
    )
import Cardano.MPFS.API.Types.Common (UtxoEntry (..))
import Cardano.MPFS.API.Types.Common qualified as Common
import Cardano.MPFS.Client
    ( EndFacts (..)
    , FactPresentFacts (..)
    , Hex (..)
    , RejectFacts (..)
    , UpdateFacts (..)
    , VerificationSnapshot (..)
    , VerifiedEndFacts
    , VerifiedRejectFacts
    , VerifiedUpdateFacts
    , csmtReplayFailedAt
    , flipProof
    , runForgeRejectFacts
    , runForgeUpdateFacts
    , shouldAccept
    , shouldRejectWith
    , verifyEndFacts
    , verifyFactPresentFacts
    , verifyRejectFacts
    , verifyUpdateFacts
    , verifyVerificationSnapshot
    , withReason
    )
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.End (endCageTxWithEval)
import Cardano.MPFS.Client.Cage.Eval
    ( DecodedEvalContext
    , decodeEvalContext
    )
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Cage.Reject (rejectCageTxWithEval)
import Cardano.MPFS.Client.Cage.Update (updateCageTxWithEval)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Proof-bearing envelopes E2E" $ do
    mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mPath of
        Nothing ->
            it "skipped (no MPFS_BLUEPRINT)"
                $ pure @IO ()
        Just path -> do
            eScripts <- runIO $ loadCageScripts path
            case eScripts of
                Left err ->
                    it ("blueprint: " <> err)
                        $ expectationFailure err
                Right scripts ->
                    proofsSpec scripts

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

proofsSpec :: CageScripts -> Spec
proofsSpec scripts =
    it "read and write envelopes carry verifiable proofs"
        $ withE2E scripts
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx
                submit =
                    signSubmitAwait
                        awaitTimeout
                        app
                        ctx

            -- Boot → insert → update to land the fact
            bootInputs <-
                walletBootInputs
                    (provider ctx)
                    genesisAddr
            bootTx <-
                submit
                    $ bootToken
                        tb
                        emptySnap
                        bootInputs
                        genesisAddr
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
            _ <-
                submit
                    $ requestInsert
                        tb
                        emptySnap
                        tid
                        "bye"
                        "moon"
                        genesisAddr

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
            assertFactEnvelope (Api.Hex factKey) factObj
            assertProofEnvelope proofObj

            assertRequestsEnvelope requestsObj

            -- Drive the remaining write endpoints over HTTP
            -- and verify their responses with the offline
            -- Client.Verify DSL. The update and end paths are
            -- facts-only, so each response is verified and then
            -- built into a wallet-side tx.
            let addrHex = hexAddr genesisAddr

            updateFacts <-
                postJSON app "/facts/update"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            let updateTrusted =
                    updateFactsTrustedRoot updateFacts
                verifyUpdateFactsUnit =
                    void . verifyUpdateFacts updateTrusted
            (updateFacts :: UpdateFacts)
                `shouldAccept` verifyUpdateFactsUnit
            runForgeUpdateFacts
                (flipProof "state_utxo")
                updateFacts
                `shouldRejectWith` verifyUpdateFactsUnit
                $ csmtReplayFailedAt
                    "update.state_utxo.inclusion_proof"
            verifiedUpdateFacts <-
                expectUpdateFactsVerified
                    updateTrusted
                    updateFacts
            void (buildUpdateTx ctx cfg verifiedUpdateFacts)

            -- Reject (Addendum 001): wait past the Phase 3
            -- deadline, fetch facts via /facts/reject, verify
            -- the bundled witnesses, then build, sign, and
            -- submit the reject tx with rejectCageTx.

            threadDelay (rejectDeadlineDelay cfg)

            rejectFactsResp <-
                postJSON app "/facts/reject"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            let rejectTrusted =
                    rejectFactsTrustedRoot rejectFactsResp
                verifyRejectFactsUnit =
                    void . verifyRejectFacts rejectTrusted
            (rejectFactsResp :: RejectFacts)
                `shouldAccept` verifyRejectFactsUnit
            runForgeRejectFacts
                (flipProof "state_utxo")
                rejectFactsResp
                `shouldRejectWith` verifyRejectFactsUnit
                $ csmtReplayFailedAt
                    "reject.state_utxo.inclusion_proof"
            verifiedRejectFacts <-
                expectRejectFactsVerified
                    rejectTrusted
                    rejectFactsResp
            rejectTx <- buildRejectTx ctx cfg verifiedRejectFacts
            _ <-
                submitResponseTx
                    awaitTimeout
                    app
                    ctx
                    (encodeTxHex rejectTx)

            endFacts <-
                postJSON app "/facts/end"
                    $ object
                        [ "token" .= Hex (TE.decodeUtf8 tidHex)
                        , "address" .= addrHex
                        ]
            let trusted = endFactsTrustedRoot endFacts
                verifyEndFactsUnit =
                    void
                        . verifyEndFacts
                            (toClientCageConfig cfg)
                            trusted
            (endFacts :: EndFacts)
                `shouldAccept` verifyEndFactsUnit
            tamperEndStateProof endFacts
                `shouldRejectWith` verifyEndFactsUnit
                $ csmtReplayFailedAt
                    "end.state_utxo.inclusion_proof"
                    `withReason` "malformed proof CBOR"
            verifiedEndFacts <-
                expectEndFactsVerified cfg trusted endFacts
            void (buildEndTx ctx cfg verifiedEndFacts)

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

-- | The @/facts/:key@ envelope must verify against the
-- state witness's UTxO root and trie root, binding the
-- requested key to the returned raw value.
assertFactEnvelope :: Api.Hex -> Value -> IO ()
assertFactEnvelope factKey' v =
    case parseEither parseJSON v of
        Left err ->
            expectationFailure
                ("fact response parse: " <> err)
        Right resp@FactResponse{frSnapshot} -> do
            let trusted = TrustedRoot (Common.vsUtxoRoot frSnapshot)
                facts =
                    FactPresentFacts
                        { fpfKey = factKey'
                        , fpfResponse = resp
                        }
            case verifyFactPresentFacts trusted facts of
                Right _ -> pure ()
                Left err ->
                    expectationFailure
                        ("verifyFactPresentFacts failed: " <> show err)

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

-- | The trusted root for facts-only end verification is
-- supplied externally by the caller. In this e2e harness,
-- the app itself is that trusted source, so we pin it to
-- the snapshot returned by the same facts response.
endFactsTrustedRoot :: EndFacts -> TrustedRoot
endFactsTrustedRoot EndFacts{efSnapshot} =
    TrustedRoot (Common.vsUtxoRoot efSnapshot)

-- | The trusted root for facts-only update verification is
-- supplied externally by the caller. In this e2e harness, the
-- app itself is that trusted source, so we pin it to the
-- snapshot returned by the same facts response.
updateFactsTrustedRoot :: UpdateFacts -> TrustedRoot
updateFactsTrustedRoot UpdateFacts{ufSnapshot} =
    TrustedRoot (Common.vsUtxoRoot ufSnapshot)

tamperEndStateProof :: EndFacts -> EndFacts
tamperEndStateProof facts@EndFacts{efStateUtxo} =
    facts
        { efStateUtxo =
            efStateUtxo
                { ueInclusionProof = Api.Hex "\x00"
                }
        }

expectEndFactsVerified
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> IO VerifiedEndFacts
expectEndFactsVerified cfg trusted facts =
    case verifyEndFacts (toClientCageConfig cfg) trusted facts of
        Right verified -> pure verified
        Left err ->
            expectationFailure
                ("verifyEndFacts failed: " <> show err)
                *> error "unreachable"

expectUpdateFactsVerified
    :: TrustedRoot
    -> UpdateFacts
    -> IO VerifiedUpdateFacts
expectUpdateFactsVerified trusted facts =
    case verifyUpdateFacts trusted facts of
        Right verified -> pure verified
        Left err ->
            expectationFailure
                ("verifyUpdateFacts failed: " <> show err)
                *> error "unreachable"

buildUpdateTx
    :: Context IO
    -> CageConfig
    -> VerifiedUpdateFacts
    -> IO ConwayTx
buildUpdateTx ctx cfg verified = do
    evalCtx <- expectDecodedEvalContext ctx
    case updateCageTxWithEval
        evalCtx
        (toClientCageConfig cfg)
        permissiveWalletPolicy
        verified of
        Right tx -> pure tx
        Left err ->
            expectationFailure
                ("updateCageTxWithEval failed: " <> show err)
                *> error "unreachable"

-- | The trusted root for facts-only reject verification is
-- supplied externally by the caller; in this e2e harness the
-- app itself is that trusted source, so we pin it to the
-- snapshot returned by the same facts response.
rejectFactsTrustedRoot :: RejectFacts -> TrustedRoot
rejectFactsTrustedRoot RejectFacts{rfSnapshot} =
    TrustedRoot (Common.vsUtxoRoot rfSnapshot)

expectRejectFactsVerified
    :: TrustedRoot
    -> RejectFacts
    -> IO VerifiedRejectFacts
expectRejectFactsVerified trusted facts =
    case verifyRejectFacts trusted facts of
        Right verified -> pure verified
        Left err ->
            expectationFailure
                ("verifyRejectFacts failed: " <> show err)
                *> error "unreachable"

buildRejectTx
    :: Context IO
    -> CageConfig
    -> VerifiedRejectFacts
    -> IO ConwayTx
buildRejectTx ctx cfg verified = do
    evalCtx <- expectDecodedEvalContext ctx
    case rejectCageTxWithEval
        evalCtx
        (toClientCageConfig cfg)
        permissiveWalletPolicy
        verified of
        Right tx -> pure tx
        Left err ->
            expectationFailure
                ("rejectCageTxWithEval failed: " <> show err)
                *> error "unreachable"

encodeTxHex :: ConwayTx -> Hex
encodeTxHex tx =
    Hex
        ( TE.decodeUtf8
            $ B16.encode
            $ serialize' (natVersion @11) tx
        )

buildEndTx
    :: Context IO
    -> CageConfig
    -> VerifiedEndFacts
    -> IO ConwayTx
buildEndTx ctx cfg verified = do
    evalCtx <- expectDecodedEvalContext ctx
    case endCageTxWithEval
        evalCtx
        (toClientCageConfig cfg)
        permissiveWalletPolicy
        verified of
        Right tx -> pure tx
        Left err ->
            expectationFailure
                ("endCageTxWithEval failed: " <> show err)
                *> error "unreachable"

expectDecodedEvalContext :: Context IO -> IO DecodedEvalContext
expectDecodedEvalContext ctx = do
    wire <- evalContext ctx
    case decodeEvalContext wire of
        Right value -> pure value
        Left err ->
            expectationFailure
                ("decodeEvalContext failed: " <> show err)
                *> error "unreachable"

submitResponseTx
    :: Int
    -> Application
    -> Context IO
    -> Hex
    -> IO ConwayTx
submitResponseTx timeout app ctx txHex = do
    unsigned <- decodeResponseTx txHex
    let signed =
            addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx timeout app (txIdTx signed)
    pure signed

decodeResponseTx :: Hex -> IO ConwayTx
decodeResponseTx (Hex txText) =
    case B16.decode (TE.encodeUtf8 txText) of
        Left err ->
            expectationFailure
                ("response tx hex decode failed: " <> err)
                *> error "unreachable"
        Right txBytes ->
            case decodeFullAnnotator
                (natVersion @11)
                "Conway transaction"
                (decCBOR :: forall s. Decoder s (Annotator ConwayTx))
                (BSL.fromStrict txBytes) of
                Left err ->
                    expectationFailure
                        ("response tx CBOR decode failed: " <> show err)
                        *> error "unreachable"
                Right txValue -> pure txValue

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
    -> IO ConwayTx
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
    if simpleStatus resp == status200
        then pure ()
        else
            expectationFailure
                $ "GET "
                    <> show path
                    <> " returned "
                    <> show (simpleStatus resp)
                    <> " with body "
                    <> show (simpleBody resp)
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
    :: CageConfig -> ConwayTx -> TokenId
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

-- -------------------------------------------------
-- Bracket
-- -------------------------------------------------

withE2E
    :: CageScripts
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-proofs-e2e"
            $ \tmpDir -> do
                let cfg = cageCfg scripts
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
                    let ctx' =
                            withBootFactsTxBuilder cfg ctx
                    _ <-
                        queryProtocolParams
                            (provider ctx')
                    threadDelay 10_000_000
                    action cfg ctx'

cageCfg :: CageScripts -> CageConfig
cageCfg (stateBytes, requestBytes) =
    CageConfig
        { cageScriptBytes = stateBytes
        , requestScriptBytes = requestBytes
        , cfgScriptHash =
            computeScriptHash stateBytes
        , defaultProcessTime = 5_000
        , defaultRetractTime = 5_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        }
