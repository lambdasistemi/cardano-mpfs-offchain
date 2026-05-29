{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.SubmitEndpointSpec
-- Description : HTTP-level E2E for POST /submit
-- License     : Apache-2.0
--
-- Boots a devnet + full application, builds and
-- signs a genesis ADA-transfer tx, POSTs its CBOR to
-- @POST \/submit@ over HTTP, and awaits the returned
-- txId against the node. This is the live-boundary
-- proof that the locked wire contract works against a
-- real N2C submitter and a real chain.
module Cardano.MPFS.E2E.SubmitEndpointSpec
    ( spec
    ) where

import Control.Concurrent (threadDelay)
import Data.Aeson (decode, encode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Lens.Micro ((&), (.~))
import Network.HTTP.Types
    ( hContentType
    , methodPost
    , status200
    , status400
    )
import Network.Wai
    ( Application
    , requestHeaders
    , requestMethod
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

import Cardano.Chain.Slotting (EpochSlots (..))
import Control.Tracer (nullTracer)

import Cardano.Ledger.Allegra.Scripts
    ( ValidityInterval (..)
    )
import Cardano.Ledger.Api.Tx (mkBasicTx)
import Cardano.Ledger.Api.Tx.Body
    ( mkBasicTxBody
    , vldtTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( Network (..)
    , SlotNo (..)
    , StrictMaybe (SJust, SNothing)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize'
    )

import Cardano.MPFS.Application
    ( AppConfig (..)
    , withApplication
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint
    ( CageScripts
    , loadCageScripts
    )
import Cardano.MPFS.Core.Types (Coin (..))
import Cardano.MPFS.E2E.Helpers.Boot
    ( withBootFactsTxBuilder
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.HTTP.Types
    ( SubmitRequest (..)
    , SubmitResponse (..)
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( computeScriptHash
    )
import Cardano.Node.Client.Balance
    ( BalanceResult (..)
    , balanceTx
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
spec = describe "POST /submit (e2e)" $ do
    mPath <-
        runIO $ lookupEnv "MPFS_BLUEPRINT"
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
                    submitEndpointSpec scripts

-- | Await timeout in seconds. Devnet slots are
-- 100ms so 60s is plenty.
awaitTimeout :: Int
awaitTimeout = 60

submitEndpointSpec :: CageScripts -> Spec
submitEndpointSpec scripts =
    it "signs, submits, and awaits a real tx"
        $ withE2E scripts
        $ \_cfg ctx -> do
            let app = mkApp ctx
            utxos <-
                queryUTxOs (provider ctx) genesisAddr
            pp <- queryProtocolParams (provider ctx)
            case utxos of
                [] ->
                    expectationFailure
                        "no genesis UTxOs"
                (feeUtxo : _) -> do
                    let vldt =
                            ValidityInterval
                                SNothing
                                (SJust (SlotNo 100_000))
                        body =
                            mkBasicTxBody
                                & vldtTxBodyL .~ vldt
                        tx = mkBasicTx body
                    case balanceTx
                        pp
                        [feeUtxo]
                        genesisAddr
                        tx of
                        Left err ->
                            expectationFailure
                                $ "balanceTx failed: "
                                    <> show err
                        Right BalanceResult{balancedTx = balanced} -> do
                            let signed =
                                    addKeyWitness
                                        genesisSignKey
                                        balanced
                                rawCbor =
                                    serialize'
                                        (natVersion @11)
                                        signed
                                reqBody =
                                    encode
                                        ( SubmitRequest
                                            (Hex rawCbor)
                                        )
                            resp <- post app reqBody
                            simpleStatus resp
                                `shouldBe` status200
                            case decode (simpleBody resp) of
                                Nothing ->
                                    expectationFailure
                                        "expected a \
                                        \SubmitResponse \
                                        \JSON body"
                                Just (SubmitResponse (Hex txIdBytes)) -> do
                                    BS.length txIdBytes
                                        `shouldBe` 32
                                    awaitTx
                                        awaitTimeout
                                        app
                                        (B16.encode txIdBytes)
                            -- Bonus: undecodable CBOR is a
                            -- 400 (reuses the same booted
                            -- app; never reaches the node).
                            garbage <-
                                post
                                    app
                                    ( encode
                                        ( SubmitRequest
                                            (Hex "\NUL")
                                        )
                                    )
                            simpleStatus garbage
                                `shouldBe` status400

-- | @POST \/submit@ with a JSON body.
post :: Application -> BSL.ByteString -> IO SResponse
post app body =
    runSession (srequest (SRequest req body)) app
  where
    req =
        (setPath defaultRequest "/submit")
            { requestMethod = methodPost
            , requestHeaders =
                [(hContentType, "application/json")]
            }

-- | @GET \/tx\/:txId?timeout=N@ — block until the
-- indexer has seen this transaction.
awaitTx
    :: Int -> Application -> ByteString -> IO ()
awaitTx timeout app txIdHex = do
    resp <-
        runSession
            (request (setPath defaultRequest path))
            app
    simpleStatus resp `shouldBe` status200
  where
    path =
        "/tx/"
            <> txIdHex
            <> "?timeout="
            <> bshow timeout
    bshow =
        BS.pack
            . map (fromIntegral . fromEnum)
            . show

-- -------------------------------------------------
-- Bracket
-- -------------------------------------------------

-- | Devnet node + full application + 10s warmup.
withE2E
    :: CageScripts
    -> (CageConfig -> Context IO -> IO a)
    -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory
            "mpfs-submit-endpoint"
            $ \tmpDir -> do
                let cfg =
                        cageCfg
                            scripts
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
                    let ctx' =
                            withBootFactsTxBuilder cfg ctx
                    _ <-
                        queryProtocolParams
                            (provider ctx')
                    threadDelay 10_000_000
                    action cfg ctx'

-- -------------------------------------------------
-- Config
-- -------------------------------------------------

cageCfg
    :: CageScripts -> CageConfig
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
