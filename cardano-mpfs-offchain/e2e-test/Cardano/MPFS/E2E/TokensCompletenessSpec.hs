{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.MPFS.E2E.TokensCompletenessSpec
    ( spec
    , tokensCompletenessMatch
    )
where

import CSMT.Core.Hash (byteStringToKey)
import CSMT.Core.Types (Key)
import CSMT.Verify.Blake2b (blake2b256)

import Cardano.Chain.Slotting (EpochSlots (..))
import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address (serialiseAddr)
import Cardano.Ledger.Api.Tx (bodyTxL, txIdTx)
import Cardano.Ledger.Api.Tx.Body
    ( outputsTxBodyL
    )
import Cardano.Ledger.BaseTypes
    ( TxIx (..)
    )
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( TokenSetWitness (..)
    , TokenUtxoEntry (..)
    , TokensResponse (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Application (AppConfig (..), withApplication)
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
    )
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Blueprint (CageScripts, loadCageScripts)
import Cardano.MPFS.Core.OnChain (CageDatum (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    )
import Cardano.MPFS.E2E.Helpers.Boot
    ( awaitProofReadsReady
    , ensureBootFunding
    , genesisCageConfig
    , registerStakeCredIfNeeded
    , walletBootInputs
    , withBootFactsTxBuilder
    )
import Cardano.MPFS.HTTP.Server (mkApp)
import Cardano.MPFS.Provider (Provider (..), SlotNo (..))
import Cardano.MPFS.Submitter (SubmitResult (..), Submitter (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , ProofEnvelope (..)
    , TxBuilder (..)
    )
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , extractCageDatum
    )
import Cardano.Node.Client.E2E.Devnet (withCardanoNode)
import Cardano.Node.Client.E2E.Setup
    ( addKeyWitness
    , genesisAddr
    , genesisDir
    , genesisSignKey
    )
import Cardano.Tx.Ledger (ConwayTx)
import Control.Tracer (nullTracer)
import Data.Aeson (eitherDecode)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.Foldable (toList)
import Data.List (sort)
import Data.Word (Word64)
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

tokensCompletenessMatch :: String
tokensCompletenessMatch =
    "GET /tokens proves complete token UTxO set against UTxO root"

spec :: Spec
spec = describe "GET /tokens completeness E2E" $ do
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
    it tokensCompletenessMatch
        $ withE2E scripts
        $ \cfg ctx -> do
            let app = mkApp ctx
                tb = txBuilder ctx

            firstTokenRef <- bootOneToken app ctx tb
            secondTokenRef <- bootOneToken app ctx tb
            firstTokenRef == secondTokenRef
                `shouldBe` False

            response <- getTokens app
            let tokenWitness@TokenSetWitness{tswEntries} =
                    trsTokens response
                witness = tokenSetAsUtxoSet tokenWitness
                returnedRefs = sort (map entryRef tswEntries)
                bootedRefs =
                    sort
                        [ txInRef firstTokenRef
                        , txInRef secondTokenRef
                        ]
            returnedRefs `shouldBe` bootedRefs

            case verifyUtxoSetCompleteness
                "tokens"
                (snapshotRoot response)
                (cageSetPrefix cfg)
                witness of
                Right () -> pure ()
                Left err ->
                    expectationFailure
                        ("invalid token-set completeness proof: " <> show err)

bootOneToken
    :: Application
    -> Context IO
    -> TxBuilder IO
    -> IO TxIn
bootOneToken app ctx tb = do
    bootInputs <- walletBootInputs (provider ctx) genesisAddr
    bootTx <-
        signSubmitAwait awaitTimeout app ctx
            $ bootToken tb emptySnap bootInputs genesisAddr
    stateRefFromTx bootTx

getTokens :: Application -> IO TokensResponse
getTokens app = do
    resp <- get app "/tokens"
    simpleStatus resp `shouldBe` status200
    case eitherDecode (simpleBody resp) of
        Left err -> failExpectation ("invalid tokens response: " <> err)
        Right body -> pure body

snapshotRoot :: TokensResponse -> ByteString
snapshotRoot
    TokensResponse
        { trsSnapshot = VerificationSnapshot{vsUtxoRoot = Hex root}
        } =
        root

entryRef :: TokenUtxoEntry -> (ByteString, Word64)
entryRef TokenUtxoEntry{tueRef = UtxoRef{urTxId = Hex txId, urTxIx}} =
    (txId, urTxIx)

tokenSetAsUtxoSet :: TokenSetWitness -> UtxoSetWitness
tokenSetAsUtxoSet
    TokenSetWitness
        { tswEntries
        , tswCompletenessProof
        } =
        UtxoSetWitness
            { uswEntries =
                map
                    ( \TokenUtxoEntry
                        { tueRef
                        , tueTxOutCbor
                        } ->
                            UtxoEntryRefOnly
                                { uerRef = tueRef
                                , uerTxOutCbor = tueTxOutCbor
                                }
                    )
                    tswEntries
            , uswCompletenessProof = tswCompletenessProof
            }

txInRef :: TxIn -> (ByteString, Word64)
txInRef (TxIn (TxId h) (TxIx ix)) =
    (Crypto.hashToBytes (extractHash h), fromIntegral ix)

cageSetPrefix :: CageConfig -> Key
cageSetPrefix cfg =
    byteStringToKey
        $ blake2b256
        $ serialiseAddr
        $ cageAddrFromCfg cfg (network cfg)

stateRefFromTx :: ConwayTx -> IO TxIn
stateRefFromTx tx =
    case stateRefs of
        [ref] -> pure ref
        [] -> failExpectation "boot transaction had no state output"
        refs ->
            failExpectation
                ("boot transaction had multiple state outputs: " <> show refs)
  where
    stateRefs =
        [ TxIn (txIdTx tx) (TxIx (fromIntegral ix))
        | (ix, out) <-
            zip
                [(0 :: Int) ..]
                (toList (tx ^. bodyTxL . outputsTxBodyL))
        , Just StateDatum{} <- [extractCageDatum out]
        ]

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
        withSystemTempDirectory "mpfs-tokens-completeness" $ \tmpDir -> do
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
                ensureBootFunding ctx'
                awaitProofReadsReady ctx'
                registerStakeCredIfNeeded cfg ctx'
                action cfg ctx'

cageCfg :: CageScripts -> CageConfig
cageCfg = genesisCageConfig

emptySnap :: BundleSnapshot
emptySnap =
    BundleSnapshot
        { snapshotUtxoRoot = BS.replicate 32 0
        , snapshotSlot = SlotNo 0
        , snapshotBlockId = BlockId (BS.replicate 32 0)
        }

awaitTimeout :: Int
awaitTimeout = 60

txIdHex :: TxId -> ByteString
txIdHex (TxId h) = B16.encode (Crypto.hashToBytes (extractHash h))

bsShow :: Show a => a -> ByteString
bsShow = BS.pack . fmap (fromIntegral . fromEnum) . show

failExpectation :: String -> IO a
failExpectation msg = do
    expectationFailure msg
    pure (error msg)
