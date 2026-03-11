{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.E2E.BootstrapSpec
-- Description : E2E tests for bootstrap file generation
-- License     : Apache-2.0
--
-- Verifies that a bootstrap CBOR file generated from
-- @shelley-genesis.json@ produces TxIn/TxOut entries
-- matching the actual on-chain genesis UTxOs.
module Cardano.MPFS.E2E.BootstrapSpec
    ( spec
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.IORef
    ( modifyIORef'
    , newIORef
    , readIORef
    )
import System.Exit (ExitCode (..))
import System.IO.Temp (withSystemTempDirectory)
import System.Process
    ( CreateProcess (..)
    , StdStream (..)
    , proc
    , waitForProcess
    , withCreateProcess
    )
import Test.Hspec
    ( Spec
    , around
    , describe
    , it
    , shouldBe
    )

import Cardano.Ledger.Binary
    ( natVersion
    , serialize
    )
import Cardano.Ledger.Shelley.Genesis
    ( initialFundsPseudoTxIn
    )

import Cardano.MPFS.Core.Bootstrap
    ( BootstrapHeader (..)
    , foldBootstrapEntries
    )
import Cardano.MPFS.Provider (Provider (..))
import Cardano.MPFS.Provider.NodeClient
    ( mkNodeClientProvider
    )
import Cardano.Node.Client.E2E.Setup
    ( genesisAddr
    , genesisDir
    , withDevnet
    )
import Cardano.Node.Client.N2C.Types
    ( LSQChannel
    , LTxSChannel
    )

-- | Expected serialised TxIn for the genesis address.
expectedTxInBytes :: ByteString
expectedTxInBytes =
    BSL.toStrict
        $ serialize
            (natVersion @11)
            (initialFundsPseudoTxIn genesisAddr)

spec :: Spec
spec = do
    describe "Bootstrap genesis file" $ do
        it "produces entries with correct TxIn"
            $ do
                entries <- generateAndRead
                case entries of
                    [] ->
                        fail "No bootstrap entries"
                    (txInBS, _) : _ ->
                        txInBS
                            `shouldBe` expectedTxInBytes

    around withDevnet'
        $ describe "Bootstrap vs live devnet"
        $ do
            it "bootstrap TxIn matches on-chain UTxO"
                $ \(lsq, _) -> do
                    entries <- generateAndRead
                    let prov =
                            mkNodeClientProvider lsq
                    utxos <-
                        queryUTxOs prov genesisAddr
                    case (entries, utxos) of
                        ( (bsKey, _) : _
                            , (onChainTxIn, _) : _
                            ) ->
                                let onChainBytes =
                                        BSL.toStrict
                                            $ serialize
                                                ( natVersion
                                                    @11
                                                )
                                                onChainTxIn
                                in  bsKey
                                        `shouldBe` onChainBytes
                        ([], _) ->
                            fail
                                "No bootstrap entries"
                        (_, []) ->
                            fail
                                "No on-chain UTxOs"

-- | Run @mpfs-bootstrap-genesis@ on the test genesis
-- file and read back the CBOR entries.
generateAndRead :: IO [(ByteString, ByteString)]
generateAndRead = do
    gDir <- genesisDir
    withSystemTempDirectory "e2e-bootstrap"
        $ \tmpDir -> do
            let genesisFile =
                    gDir <> "/shelley-genesis.json"
                outFile = tmpDir <> "/bootstrap.cbor"
            runBootstrapExe
                genesisFile
                outFile
            pairsRef <- newIORef []
            foldBootstrapEntries
                outFile
                ( \hdr -> do
                    bootstrapSlot hdr `shouldBe` 0
                    bootstrapBlockHash hdr
                        `shouldBe` Nothing
                )
                ( \k v ->
                    modifyIORef'
                        pairsRef
                        (<> [(k, v)])
                )
            readIORef pairsRef

-- | Run @mpfs-bootstrap-genesis@ directly (must be on PATH).
runBootstrapExe
    :: FilePath -> FilePath -> IO ()
runBootstrapExe genesisFile outFile = do
    let cp =
            (proc "mpfs-bootstrap-genesis" args)
                { std_out = CreatePipe
                , std_err = CreatePipe
                }
        args =
            [ "--genesis"
            , genesisFile
            , "--output"
            , outFile
            ]
    withCreateProcess cp $ \_ _ _ ph -> do
        ec <- waitForProcess ph
        case ec of
            ExitSuccess -> pure ()
            _ ->
                error
                    $ "mpfs-bootstrap-genesis "
                        <> "exited with "
                        <> show ec

-- | Wrap 'withDevnet' for Hspec 'around'.
withDevnet'
    :: ((LSQChannel, LTxSChannel) -> IO a)
    -> IO a
withDevnet' action =
    withDevnet $ curry action
