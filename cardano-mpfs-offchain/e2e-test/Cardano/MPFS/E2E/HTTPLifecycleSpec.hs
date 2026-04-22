{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.E2E.HTTPLifecycleSpec
-- Description : HTTP lifecycle E2E against devnet
-- License     : Apache-2.0
--
-- Boot a token, await on-chain confirmation via
-- the HTTP API, verify state, end the token, await
-- again, verify removal. Runs against a real
-- cardano-node devnet with CageFollower.
module Cardano.MPFS.E2E.HTTPLifecycleSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Data.Aeson (decode)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson.Types (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Vector qualified as V
import Lens.Micro ((^.))
import Network.HTTP.Types (status200)
import Network.Wai (Application)
import Network.Wai.Test
    ( SResponse (..)
    , defaultRequest
    , request
    , runSession
    , setPath
    )
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

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , txIdTx
    )
import Cardano.Ledger.Api.Tx.Body
    ( mintTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , TxIx (..)
    )
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
    ( applyVersion
    , extractCompiledCode
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
    )

-- | Skips when @MPFS_BLUEPRINT@ is not set.
spec :: Spec
spec = describe "HTTP lifecycle E2E" $ do
    mPath <-
        runIO $ lookupEnv "MPFS_BLUEPRINT"
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
                        "cage."
                        bp of
                        Nothing ->
                            it "no compiled code"
                                $ expectationFailure
                                    "cage script \
                                    \not found"
                        Just sb ->
                            lifecycleSpec
                                $ applyVersion 1 sb

-- -------------------------------------------------
-- Scenario
-- -------------------------------------------------

-- | Await timeout in seconds. Devnet slots are
-- 100ms so 60s is plenty.
awaitTimeout :: Int
awaitTimeout = 60

lifecycleSpec :: SBS.ShortByteString -> Spec
lifecycleSpec scriptBytes =
    it "boot → insert → update → insert → retract → end"
        $ withE2E scriptBytes
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx
                submit =
                    signSubmitAwait
                        awaitTimeout
                        app
                        ctx
                tokens = tokenCount app
                reqs tid =
                    requestCount
                        app
                        (tokenIdHex tid)

            -- Boot
            bootTx <-
                submit
                    $ bootToken tb emptySnap genesisAddr
            let tid =
                    extractTokenId cfg bootTx

            n <- tokens
            n `shouldBe` 1

            -- Insert a fact
            _ <-
                submit
                    $ requestInsert
                        tb
                        emptySnap
                        tid
                        "hello"
                        "world"
                        genesisAddr

            r <- reqs tid
            r `shouldBe` 1

            -- Update (processes the request)
            _ <-
                submit
                    $ updateToken
                        tb
                        emptySnap
                        tid
                        genesisAddr

            r' <- reqs tid
            r' `shouldBe` 0

            -- Second insert (to retract later)
            reqTx <-
                submit
                    $ requestInsert
                        tb
                        emptySnap
                        tid
                        "bye"
                        "moon"
                        genesisAddr

            r'' <- reqs tid
            r'' `shouldBe` 1

            -- Wait for Phase 2 so retract is valid
            threadDelay 7_000_000

            -- Retract
            let reqTxIn =
                    TxIn (txIdTx reqTx) (TxIx 0)
            _ <-
                submit
                    $ retractRequest
                        tb
                        emptySnap
                        reqTxIn
                        genesisAddr

            r''' <- reqs tid
            r''' `shouldBe` 0

            -- End
            _ <-
                submit
                    $ endToken
                        tb
                        emptySnap
                        tid
                        genesisAddr

            n' <- tokens
            n' `shouldBe` 0

-- -------------------------------------------------
-- DSL
-- -------------------------------------------------

-- | Sign, submit, and block until the indexer
-- has processed the transaction.
signSubmitAwait
    :: Int
    -- ^ Timeout in seconds
    -> Application
    -> Context IO
    -> IO (ProofEnvelope p)
    -> IO (Tx ConwayEra)
signSubmitAwait timeout app ctx buildBundle = do
    bundle <- buildBundle
    let unsigned = envTx bundle
        signed =
            addKeyWitness
                genesisSignKey
                unsigned
    result <-
        submitTx (submitter ctx) signed
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

-- | @GET \/tx\/:txId?timeout=N@ — block until
-- the indexer has seen this transaction.
awaitTx
    :: Int -> Application -> TxId -> IO ()
awaitTx timeout app tid = do
    resp <- get app path
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

-- | @GET \/tokens\/:id\/requests@ — count pending
-- requests for a token.
requestCount
    :: Application -> ByteString -> IO Int
requestCount app tidHex = do
    resp <-
        get app
            $ "/tokens/"
                <> tidHex
                <> "/requests"
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just (Object o)
            | Just (Array arr) <-
                KeyMap.lookup "requests" o ->
                pure (V.length arr)
        _ -> do
            expectationFailure
                "Expected { requests: [...] } envelope"
            pure 0

-- | @GET \/tokens@ — count indexed tokens.
tokenCount :: Application -> IO Int
tokenCount app = do
    resp <- get app "/tokens"
    simpleStatus resp `shouldBe` status200
    case decode (simpleBody resp) of
        Just (Array arr) ->
            pure (V.length arr)
        _ -> do
            expectationFailure
                "Expected JSON array"
            pure 0

-- | Low-level GET helper.
get
    :: Application
    -> ByteString
    -> IO SResponse
get app path =
    runSession
        ( request
            (setPath defaultRequest path)
        )
        app

-- -------------------------------------------------
-- Helpers
-- -------------------------------------------------

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

-- | Devnet node + full application + 10s warmup.
withE2E
    :: SBS.ShortByteString
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scriptBytes action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-http-lifecycle"
            $ \tmpDir -> do
                let cfg =
                        cageCfg
                            scriptBytes
                    appCfg =
                        AppConfig
                            { epochSlots =
                                EpochSlots 4320
                            , shelleyGenesisPath =
                                gDir
                                    </> "shelley-genesis.json"
                            , socketPath = sock
                            , dbPath =
                                tmpDir </> "db"
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
                    _ <-
                        queryProtocolParams
                            (provider ctx)
                    threadDelay 10_000_000
                    action cfg ctx

-- -------------------------------------------------
-- Config
-- -------------------------------------------------

cageCfg
    :: SBS.ShortByteString -> CageConfig
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
