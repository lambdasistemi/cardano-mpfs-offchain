{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.MPFS.E2E.TokenFactsCompletenessSpec
    ( spec
    , tokenFactsCompletenessMatch
    )
where

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Api.Tx (bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body (mintTxBodyL)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Mary.Value (AssetName (..), MultiAsset (..))
import Cardano.Ledger.TxIn (TxId (..))
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( FactEntry (..)
    , FactsResponse (..)
    , TokenStateJSON (..)
    , WitnessedTokenState (..)
    )
import Cardano.MPFS.Application (AppConfig (..), withApplication)
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint (CageScripts, loadCageScripts)
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , Root (..)
    , TokenId (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( awaitProofReadsReady
    , walletBootInputs
    , withBootFactsTxBuilder
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..), SlotNo (..))
import Cardano.MPFS.Submitter (SubmitResult (..), Submitter (..))
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.Trie.Pure (mkPureTrie)
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
import Cardano.Tx.Ledger (ConwayTx)
import Control.Monad (forM_)
import Control.Tracer (nullTracer)
import Data.Aeson (Value (..), eitherDecode)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short qualified as SBS
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Vector qualified as Vector
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
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , runIO
    , shouldBe
    )

tokenFactsCompletenessMatch :: String
tokenFactsCompletenessMatch =
    "GET /tokens/:id/facts proves complete fact set against token root"

spec :: Spec
spec = describe "GET /tokens/:id/facts completeness E2E" $ do
    mBlueprint <- runIO $ lookupEnv "MPFS_BLUEPRINT"
    case mBlueprint of
        Nothing ->
            it "skips without MPFS_BLUEPRINT"
                $ pure @IO ()
        Just blueprintPath -> do
            eScripts <- runIO (loadCageScripts blueprintPath)
            case eScripts of
                Left err ->
                    it "loads cage scripts"
                        $ expectationFailure ("failed to load blueprint: " <> err)
                Right scripts ->
                    completenessSpec scripts

completenessSpec :: CageScripts -> Spec
completenessSpec scripts =
    it tokenFactsCompletenessMatch
        $ withE2E scripts
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx
                submit = signSubmitAwait awaitTimeout app ctx
                expectedFacts =
                    [ ("alpha", "one")
                    , ("bravo", "two")
                    ]

            bootInputs <- walletBootInputs (provider ctx) genesisAddr
            bootTx <- submit $ bootToken tb emptySnap bootInputs genesisAddr
            let tid = extractTokenId cfg bootTx

            forM_ expectedFacts $ \(key, value) ->
                submit (requestInsert tb emptySnap tid key value genesisAddr)

            pending <- requestCount app tid
            pending `shouldBe` length expectedFacts

            _updateTx <- submit $ updateToken tb emptySnap tid genesisAddr

            remaining <- requestCount app tid
            remaining `shouldBe` 0

            response <- getTokenFacts app tid
            let returnedFacts = responseFacts response
            sort returnedFacts `shouldBe` sort expectedFacts

            rebuiltRoot <- reconstructRoot returnedFacts
            let tokenRoot = tokenRootFromState response
            rebuiltRoot `shouldBe` tokenRoot

            httpRoot <- getTokenRoot app tid
            httpRoot `shouldBe` tokenRoot

reconstructRoot :: [(ByteString, ByteString)] -> IO ByteString
reconstructRoot facts = do
    trie <- mkPureTrie
    forM_ facts $ uncurry (Trie.insert trie)
    Root rootBytes <- Trie.getRoot trie
    pure rootBytes

responseFacts :: FactsResponse -> [(ByteString, ByteString)]
responseFacts FactsResponse{frsFacts} =
    [ (key, value)
    | FactEntry{feKey = Hex key, feValue = Hex value} <- frsFacts
    ]

tokenRootFromState :: FactsResponse -> ByteString
tokenRootFromState
    FactsResponse
        { frsState =
            WitnessedTokenState
                { wtsState = TokenStateJSON{root = Hex tokenRoot}
                }
        } = tokenRoot

getTokenFacts :: Application -> TokenId -> IO FactsResponse
getTokenFacts app tid = do
    resp <- get app ("/tokens/" <> tokenIdHex tid <> "/facts")
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err -> failExpectation ("invalid facts response: " <> err)
        Right body -> pure body

getTokenRoot :: Application -> TokenId -> IO ByteString
getTokenRoot app tid = do
    resp <- get app ("/tokens/" <> tokenIdHex tid <> "/root")
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err -> failExpectation ("invalid root response: " <> err)
        Right (Hex rootBytes) -> pure rootBytes

requestCount :: Application -> TokenId -> IO Int
requestCount app tid = do
    resp <- get app ("/tokens/" <> tokenIdHex tid <> "/requests")
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Right (Object obj)
            | Just (Array items) <- KeyMap.lookup "requests" obj ->
                pure (Vector.length items)
        _ -> failExpectation "expected requests array"

signSubmitAwait
    :: Int
    -> Application
    -> Context IO
    -> IO (ProofEnvelope p)
    -> IO ConwayTx
signSubmitAwait timeoutSeconds app ctx buildBundle = do
    bundle <- buildBundle
    let unsigned = envTx bundle
        signed = addKeyWitness genesisSignKey unsigned
    result <- submitTx (submitter ctx) signed
    assertSubmitted result
    awaitTx timeoutSeconds app (txIdTx signed)
    pure signed

assertSubmitted :: SubmitResult -> IO ()
assertSubmitted result =
    case result of
        Submitted _ -> pure ()
        Rejected msg -> expectationFailure ("transaction rejected: " <> show msg)

awaitTx :: Int -> Application -> TxId -> IO ()
awaitTx timeoutSeconds app txid = do
    resp <-
        get
            app
            ("/tx/" <> txIdHex txid <> "?timeout=" <> bsShow timeoutSeconds)
    simpleStatus resp `shouldBe` status200

get :: Application -> ByteString -> IO SResponse
get app path =
    runSession (request (setPath defaultRequest path)) app

withE2E :: CageScripts -> (CageConfig -> Context IO -> IO a) -> IO a
withE2E scripts action = do
    gDir <- genesisDir
    withCardanoNode gDir $ \sock _startMs ->
        withSystemTempDirectory "mpfs-token-facts-completeness" $ \tmpDir -> do
            let cfg = cageCfg scripts
                appCfg =
                    AppConfig
                        { epochSlots = EpochSlots 4320
                        , shelleyGenesisPath = gDir </> "shelley-genesis.json"
                        , socketPath = sock
                        , dbPath = tmpDir </> "db"
                        , channelCapacity = 16
                        , cageConfig = cfg
                        , byronGenesisPath = Nothing
                        , followerEnabled = True
                        , appTracer = nullTracer
                        }
            withApplication appCfg $ \ctx -> do
                let ctx' = withBootFactsTxBuilder cfg ctx
                _ <- queryProtocolParams (provider ctx')
                awaitProofReadsReady ctx'
                action cfg ctx'

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

emptySnap :: BundleSnapshot
emptySnap =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

awaitTimeout :: Int
awaitTimeout = 60

tokenIdHex :: TokenId -> ByteString
tokenIdHex (TokenId (AssetName tid)) = B16.encode (SBS.fromShort tid)

txIdHex :: TxId -> ByteString
txIdHex (TxId h) = B16.encode (Crypto.hashToBytes (extractHash h))

bsShow :: Show a => a -> ByteString
bsShow = BS.pack . fmap (fromIntegral . fromEnum) . show

extractTokenId :: CageConfig -> ConwayTx -> TokenId
extractTokenId cfg tx =
    let MultiAsset ma = tx ^. bodyTxL . mintTxBodyL
        pid = cagePolicyIdFromCfg cfg
        assets = Map.toList (ma Map.! pid)
    in  case assets of
            [(an, _)] -> TokenId an
            _ -> error "extractTokenId: unexpected mint"

failExpectation :: String -> IO a
failExpectation msg = do
    expectationFailure msg
    pure (error msg)
