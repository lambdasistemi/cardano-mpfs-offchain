{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.BootFactsSpec
-- Description : E2E proof for facts-only boot construction
-- License     : Apache-2.0
module Cardano.MPFS.E2E.BootFactsSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Data.Aeson
    ( eitherDecode
    , encode
    )
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
    ( bodyTxL
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
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootFacts
    , BootRequest (..)
    , StatusResponse (..)
    , TokenSetWitness (..)
    , TokensResponse (..)
    )
import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTxWithEval)
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Client.Cage.Eval (decodeEvalContext)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.Facts (verifyBootFacts)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
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
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "Boot facts E2E" $ do
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
                    bootFactsSpec scripts

bootFactsSpec :: CageScripts -> Spec
bootFactsSpec scripts =
    it
        "verifies facts, builds locally, submits, and indexes boot"
        $ withE2E scripts
        $ \cfg ctx -> do
            let app = mkApp ctx
            trusted <- waitForTrustedRoot app
            facts <- postBootFacts app genesisAddr
            verified <-
                case verifyBootFacts trusted facts of
                    Left err ->
                        expectationFailure
                            ( "verifyBootFacts failed: "
                                <> show err
                            )
                            *> error "unreachable"
                    Right value -> pure value
            evalCtxWire <- evalContext ctx
            evalCtx <-
                case decodeEvalContext evalCtxWire of
                    Left err ->
                        expectationFailure
                            ( "decodeEvalContext failed: "
                                <> show err
                            )
                            *> error "unreachable"
                    Right value -> pure value
            unsigned <-
                case bootCageTxWithEval
                    evalCtx
                    (toClientCageConfig cfg)
                    permissiveWalletPolicy
                    verified of
                    Left err ->
                        expectationFailure
                            ("bootCageTxWithEval failed: " <> show err)
                            *> error "unreachable"
                    Right tx -> pure tx
            let signed =
                    addKeyWitness genesisSignKey unsigned
                tokenId = extractTokenId cfg signed
            result <- submitTx (submitter ctx) signed
            assertSubmitted result
            awaitTx app (txIdTx signed)
            n <- tokenCount app
            n `shouldBe` 1
            visible <- tokenVisible app tokenId
            visible `shouldBe` True

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
                        ("Could not decode status: " <> err)
                        $> Nothing
                Right StatusResponse{currentUtxoRoot} ->
                    pure currentUtxoRoot
    case mRoot of
        Nothing ->
            expectationFailure
                "status did not expose utxo_root within timeout"
                *> error "unreachable"
        Just root ->
            pure (TrustedRoot root)

postBootFacts :: Application -> Addr -> IO BootFacts
postBootFacts app addr = do
    resp <-
        postJson
            app
            "/facts/boot"
            BootRequest
                { brAddr =
                    Hex (serialiseAddr addr)
                }
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("Could not decode BootFacts: " <> err)
                *> error "unreachable"
        Right facts -> pure facts

postJson
    :: Application
    -> ByteString
    -> BootRequest
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
        BS.pack
            . map (fromIntegral . fromEnum)
            . show

tokenCount :: Application -> IO Int
tokenCount app = do
    resp <- get app "/tokens"
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err ->
            expectationFailure
                ("Could not decode TokensResponse: " <> err)
                $> 0
        Right TokensResponse{trsTokens = TokenSetWitness{tswEntries}} ->
            pure (length tswEntries)

tokenVisible :: Application -> TokenId -> IO Bool
tokenVisible app tokenId = do
    resp <- get app ("/tokens/" <> tokenIdHex tokenId)
    pure (simpleStatus resp == status200)

get :: Application -> ByteString -> IO SResponse
get app path =
    runSession
        (request (setPath defaultRequest path))
        app

assertSubmitted :: SubmitResult -> IO ()
assertSubmitted (Submitted _) = pure ()
assertSubmitted (Rejected reason) =
    expectationFailure
        $ "Tx rejected: " <> show reason

pollUntilJust
    :: Int -> IO (Maybe a) -> IO (Maybe a)
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
    let MultiAsset ma =
            tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ ->
                error
                    "extractTokenId: unexpected mint"

tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName sbs)) =
    B16.encode (SBS.fromShort sbs)

txIdHex :: TxId -> ByteString
txIdHex (TxId sh) =
    B16.encode
        $ Crypto.hashToBytes
        $ extractHash sh

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
        withSystemTempDirectory "mpfs-boot-facts" $ \tmpDir -> do
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
