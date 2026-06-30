{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.HTTP.AtomicReadFixture
-- Description : Shared fixtures for atomic proof-bearing read tests
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.AtomicReadFixture
    ( cafeTid
    , cafeTokenJSON
    , insertFacts
    , sampleRequestOutBytes
    , sampleStateOutBytes
    , staleRoot
    , stateSetPrefix
    , stateTxOutBytesWithRoot
    , testCageConfig
    , toClientCageConfig
    , withProofIndexer
    ) where

import Control.Monad (forM, forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)

import CSMT.Core.Types (Key)
import CSMT.Hashes (generateInclusionProof)
import CSMT.Hashes.Types (Hash)
import CSMT.Interface (FromKV)
import Cardano.Ledger.Address (serialiseAddr)
import Cardano.Ledger.Api.Tx.Out
    ( TxOut
    , datumTxOutL
    , mkBasicTxOut
    )
import Cardano.Ledger.BaseTypes
    ( Inject (..)
    , Network (..)
    )
import Cardano.Ledger.Binary
    ( natVersion
    , serialize
    , serialize'
    )
import Cardano.Ledger.Mary.Value (AssetName (..))
import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Columns (..)
    )
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    , CSMTOps (..)
    , mkCSMTOps
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( context
    , hashAddressKey
    )
import Cardano.UTxOCSMT.Ouroboros.Types (Point)
import ChainFollower.Rollbacks.Store qualified as Store
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( RunTransaction (..)
    , Transaction
    , mapColumns
    , newRunTransaction
    )
import Database.RocksDB
    ( DB (..)
    , withDBCF
    )
import System.Directory (doesFileExist)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.Unsafe (unsafePerformIO)

import Cardano.MPFS.Application
    ( allColumnFamilies
    , dbConfig
    , unifiedCodecs
    )
import Cardano.MPFS.Client.Cage.Config qualified as Client
import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainRoot (..)
    , OnChainTokenState (..)
    )
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Coin (..)
    , ConwayEra
    , Root
    , SlotNo (..)
    , TokenId (..)
    , TxIn
    )
import Cardano.MPFS.HTTP.Types (TokenIdJSON (..))
import Cardano.MPFS.Indexer.Columns (UnifiedColumns (..))
import Cardano.MPFS.Indexer.Reads
    ( IndexerTx (..)
    , readMerkleRoot
    )
import Cardano.MPFS.Indexer.TxFixtures (testScriptHash)
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Trie qualified as Trie
import Cardano.MPFS.Trie.Persistent
    ( mkPersistentTrieManager
    )
import Cardano.MPFS.TxBuilder (ResolvedWalletInput)
import Cardano.MPFS.TxBuilder.Config (CageConfig (..))
import Cardano.MPFS.TxBuilder.Real.Internal
    ( cageAddrFromCfg
    , mkInlineDatum
    , requestAddrFromCfg
    , toPlcData
    )
import Control.Lens ((&), (.~))
import PlutusTx.Builtins.Internal (BuiltinByteString (..))

-- | "cafe" token — hex "63616665".
cafeTid :: TokenId
cafeTid = TokenId (AssetName (SBS.toShort "cafe"))

cafeTokenJSON :: TokenIdJSON
cafeTokenJSON = TokenIdJSON "cafe"

staleRoot :: ByteString
staleRoot = BS.replicate 32 0x99

testCageConfig :: CageConfig
testCageConfig =
    CageConfig
        { cageScriptBytes = SBS.toShort "dummy"
        , requestScriptBytes = testRequestScriptBytes
        , cfgScriptHash = testScriptHash
        , defaultProcessTime = 60_000
        , defaultRetractTime = 30_000
        , defaultTip = Coin 1_000_000
        , network = Testnet
        , cfgStakeScript = Nothing
        }

testRequestScriptBytes :: SBS.ShortByteString
testRequestScriptBytes =
    unsafePerformIO loadTestRequestScriptBytes
{-# NOINLINE testRequestScriptBytes #-}

loadTestRequestScriptBytes :: IO SBS.ShortByteString
loadTestRequestScriptBytes = do
    hex <- tryRead candidatePaths
    let trimmed =
            BS.takeWhile
                (\b -> b /= 10 && b /= 13)
                hex
    case B16.decode trimmed of
        Right bs -> pure (SBS.toShort bs)
        Left err ->
            error
                $ "loadTestRequestScriptBytes: "
                    <> err
  where
    candidatePaths =
        [ "test-data/request.uplc.hex"
        , "cardano-mpfs-offchain/test-data/request.uplc.hex"
        ]
    tryRead [] =
        error
            "loadTestRequestScriptBytes: \
            \test-data/request.uplc.hex not found \
            \in any of the candidate paths"
    tryRead (p : ps) = do
        exists <- doesFileExist p
        if exists
            then BS.readFile p
            else tryRead ps

sampleStateOutBytes :: Integer -> ByteString
sampleStateOutBytes _ =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    txOut =
        mkBasicTxOut
            (cageAddrFromCfg testCageConfig Testnet)
            (inject (Coin 2_000_000))

-- | A state @TxOut@ whose inline datum carries the given trie root.
-- Unlike 'sampleStateOutBytes', this produces a decodable state datum,
-- so a witnessed UTxO built from it lets a client decode the on-chain
-- trie root from provable bytes - exactly what the read verifiers now
-- require instead of a server-side projection.
stateTxOutBytesWithRoot :: ByteString -> ByteString
stateTxOutBytesWithRoot root =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    datum =
        StateDatum
            OnChainTokenState
                { stateOwner =
                    BuiltinByteString (BS.replicate 28 0)
                , stateRoot = OnChainRoot root
                , stateMaxFee = 1_000_000
                , stateProcessTime = 60_000
                , stateRetractTime = 30_000
                , stateStakeScript = Nothing
                }
    txOut =
        mkBasicTxOut
            (cageAddrFromCfg testCageConfig Testnet)
            (inject (Coin 2_000_000))
            & datumTxOutL
                .~ mkInlineDatum (toPlcData datum)

sampleRequestOutBytes
    :: TokenId
    -> Integer
    -> ByteString
sampleRequestOutBytes tid _submittedAt =
    serialize'
        (natVersion @11)
        (txOut :: TxOut ConwayEra)
  where
    txOut =
        mkBasicTxOut
            (requestAddrFromCfg testCageConfig tid Testnet)
            (inject (Coin 2_000_000))

stateSetPrefix :: CageConfig -> Key
stateSetPrefix cfg =
    hashAddressKey
        $ serialiseAddr
        $ cageAddrFromCfg cfg (network cfg)

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

insertFacts
    :: Context IO
    -> TokenId
    -> [(ByteString, ByteString)]
    -> IO Root
insertFacts ctx tid facts = do
    Trie.createTrie (trieManager ctx) tid
    Trie.withTrie (trieManager ctx) tid $ \trie -> do
        forM_ facts $ uncurry (Trie.insert trie)
        Trie.getRoot trie

withProofIndexer
    :: Maybe ByteString
    -> [(TxIn, ByteString)]
    -> Context IO
    -> (ByteString -> Context IO -> IO a)
    -> IO a
withProofIndexer mOutOfTxRoot entries ctx action =
    withSystemTempDirectory "atomic-read-indexer-test"
        $ \dir ->
            withDBCF
                dir
                dbConfig
                allColumnFamilies
                $ \db@DB{columnFamilies} -> do
                    let database =
                            mkRocksDBDatabase
                                db
                                (mkColumns columnFamilies unifiedCodecs)
                        CSMTContext{fromKV, hashing} = context
                        csmtOps = mkCSMTOps fromKV hashing
                    RunTransaction{runTransaction} <-
                        newRunTransaction database
                    let seededEntries =
                            [ ( serialize
                                    (natVersion @11)
                                    txIn
                              , BSL.fromStrict txOutBytes
                              )
                            | (txIn, txOutBytes) <- entries
                            ]
                    runTransaction
                        $ mapColumns InUtxo
                        $ forM_ seededEntries
                        $ uncurry
                            (csmtInsert csmtOps)
                    runTransaction
                        $ Store.armageddonSetup
                            InRollbacks
                            (SlotNo 42)
                            (Just (BlockId "block-id-bytes"))
                    let IndexerTx readRoot = readMerkleRoot
                    mRoot <- runTransaction readRoot
                    txRoot <- case mRoot of
                        Nothing ->
                            fail
                                "proof-indexer helper did not seed \
                                \a UTxO root"
                        Just root -> pure root
                    witnesses <-
                        Map.fromList
                            <$> runTransaction
                                ( mapColumns InUtxo
                                    $ forM entries
                                    $ buildWitness fromKV
                                )
                    tm <- case drop 9 columnFamilies of
                        nodesCF : kvCF : metaCF : _ ->
                            mkPersistentTrieManager
                                db
                                nodesCF
                                kvCF
                                metaCF
                        _ ->
                            fail
                                "proof-indexer helper did not open \
                                \trie column families"
                    St.putCheckpoint
                        (St.checkpoints (state ctx))
                        (SlotNo 42)
                        (BlockId "block-id-bytes")
                    let ctxWithIndexer =
                            ctx
                                { cfgCage = testCageConfig
                                , trieManager = tm
                                , runIndexerTx =
                                    \(IndexerTx body) ->
                                        runTransaction body
                                , utxoRoot =
                                    pure
                                        $ Just
                                        $ fromMaybe
                                            txRoot
                                            mOutOfTxRoot
                                , resolveUtxo =
                                    \txIn ->
                                        pure
                                            $ (\(_, out, _) -> out)
                                                <$> Map.lookup
                                                    txIn
                                                    witnesses
                                , utxoProof =
                                    \txIn ->
                                        pure
                                            $ (\(_, _, proof) -> proof)
                                                <$> Map.lookup
                                                    txIn
                                                    witnesses
                                }
                    action txRoot ctxWithIndexer

buildWitness
    :: FromKV BSL.ByteString BSL.ByteString Hash
    -> (TxIn, ByteString)
    -> Transaction
        IO
        cf
        ( Columns
            Point
            Hash
            BSL.ByteString
            BSL.ByteString
        )
        op
        (TxIn, ResolvedWalletInput)
buildWitness fromKV (txIn, txOutBytes) = do
    let key = serialize (natVersion @11) txIn
    mProof <-
        generateInclusionProof
            fromKV
            KVCol
            CSMTCol
            key
    proof <- case mProof of
        Just (_, proofBytes) -> pure proofBytes
        Nothing ->
            error
                "proof-indexer helper could not generate \
                \an inclusion proof for a seeded UTxO"
    pure (txIn, (txIn, txOutBytes, proof))
