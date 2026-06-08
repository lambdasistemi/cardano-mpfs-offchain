{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Indexer.ReadsSpec
-- Description : Tests for indexer read primitives
-- License     : Apache-2.0
module Cardano.MPFS.Indexer.ReadsSpec (spec) where

import Control.Lens (view)
import Control.Lens qualified as Lens
import Control.Monad (forM, forM_)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.IORef
    ( newIORef
    , readIORef
    , writeIORef
    )
import Data.List (nub, sort)

import Test.Hspec
    ( Spec
    , describe
    , expectationFailure
    , it
    , shouldBe
    , shouldContain
    )
import Test.QuickCheck (generate, vectorOf)

import CSMT
    ( Standalone (..)
    , StandaloneCodecs (..)
    , deleting
    , inserting
    )
import CSMT.Backend.Pure
    ( Pure
    , emptyInMemoryDB
    , pureDatabase
    , runPure
    , runPureTransaction
    )
import CSMT.Backend.Standalone
    ( StandaloneCF
    , StandaloneOp
    )
import CSMT.Core.Types (Indirect (..))
import CSMT.Hashes
    ( Hash
    , fromKVHashes
    , hashHashing
    , isoHash
    , mkHash
    )
import CSMT.Interface (FromKV (..), Key, root)
import CSMT.MTS
    ( CommonOps (..)
    , fullCommon
    , kvCommon
    , mkKVOnlyOps
    , toFull
    )
import CSMT.Proof.Completeness (collectValues)
import CSMT.Test.Lib (evalPureFromEmptyDB)
import Cardano.Ledger.Address (Addr (..), serialiseAddr)
import Cardano.Ledger.Api.Tx.Out (TxOut, mkBasicTxOut)
import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Binary
    ( decodeFull
    , natVersion
    , serialize
    , serialize'
    )
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Credential
    ( Credential (..)
    , StakeReference (..)
    )
import Cardano.Ledger.Val (inject)
import Cardano.UTxOCSMT.Application.Database.Implementation.Transaction
    ( CSMTContext (..)
    )
import Cardano.UTxOCSMT.Application.Run.Config
    ( context
    , hashAddressKey
    )

import Database.KV.Transaction
    ( Transaction
    , query
    , runTransactionUnguarded
    )

import Cardano.MPFS.Core.Types
    ( ConwayEra
    , TxIn
    )
import Cardano.MPFS.Generators (genTxIn)
import Cardano.MPFS.Indexer.Reads (addressScopedLeafKey)
import Cardano.MPFS.Indexer.TxFixtures
    ( testCageAddr
    , wrongScriptHash
    )

spec :: Spec
spec = describe "address-prefixed UTxO leaves" $ do
    it "strips the queried address prefix before decoding TxIn" $ do
        txIn <- generate genTxIn
        let CSMTContext{fromKV = fkv} = context
            txInBytes = encodeTxIn txIn
            addressKey = hashAddressKey "queried-address"
            leaf =
                Indirect
                    { jump =
                        addressKey
                            <> view (isoK fkv) txInBytes
                    , value = mkHash "unused"
                    }
        case addressScopedLeafKey fkv addressKey leaf of
            Right keyBytes ->
                decodeTxIn keyBytes `shouldBe` Right txIn
            Left err ->
                expectationFailure err

    it "reports a mismatch when a leaf is outside the queried prefix" $ do
        txIn <- generate genTxIn
        let CSMTContext{fromKV = fkv} = context
            txInBytes = encodeTxIn txIn
            queriedKey = hashAddressKey "queried-address"
            otherKey = hashAddressKey "other-address"
            leaf =
                Indirect
                    { jump =
                        otherKey
                            <> view (isoK fkv) txInBytes
                    , value = mkHash "unused"
                    }
        case addressScopedLeafKey fkv queriedKey leaf of
            Left err ->
                err
                    `shouldContain` "did not start with queried \
                                    \address prefix"
            Right _ ->
                expectationFailure
                    "Expected address prefix mismatch"

    it
        "removes spent address leaves and restores them on rollback"
        $ do
            txIns <- generate $ vectorOf 4 genTxIn
            let CSMTContext{fromKV = fkv, hashing} = context
                keys = encodeTxIn <$> txIns
                spentKey =
                    case keys of
                        k : _ -> k
                        [] -> error "vectorOf 4 genTxIn returned no keys"
                liveKeys = filter (/= spentKey) keys
                txOutBytes = encodeWalletTxOut
                addressKey =
                    hashAddressKey (serialiseAddr testCageAddr)
                (before, afterDelete, afterRollback) =
                    evalPureFromEmptyDB $ do
                        runPureTransaction addressCodecs $ do
                            forM_ keys $ \key ->
                                inserting
                                    []
                                    fkv
                                    hashing
                                    StandaloneKVCol
                                    StandaloneCSMTCol
                                    key
                                    txOutBytes
                            keysBefore <-
                                loadAddressKeys fkv addressKey
                            deleting
                                []
                                fkv
                                hashing
                                StandaloneKVCol
                                StandaloneCSMTCol
                                spentKey
                            keysAfterDelete <-
                                loadAddressKeys fkv addressKey
                            inserting
                                []
                                fkv
                                hashing
                                StandaloneKVCol
                                StandaloneCSMTCol
                                spentKey
                                txOutBytes
                            keysAfterRollback <-
                                loadAddressKeys fkv addressKey
                            pure
                                ( keysBefore
                                , keysAfterDelete
                                , keysAfterRollback
                                )
            before `shouldBe` Right (sort keys)
            afterDelete `shouldBe` Right (sort liveKeys)
            afterRollback `shouldBe` Right (sort keys)

    it
        "removes spent leaves from the queried address in a mixed tree"
        $ do
            generated <- generate $ vectorOf 96 genTxIn
            let keys = take 48 $ nub $ encodeTxIn <$> generated
            case splitAt 24 keys of
                (walletKeys, otherKeys)
                    | length walletKeys == 24
                        && length otherKeys == 24 -> do
                        let CSMTContext{fromKV = fkv, hashing} = context
                            walletOut = encodeTxOutAt testCageAddr
                            otherOut = encodeTxOutAt otherAddr
                            addressKey =
                                hashAddressKey
                                    (serialiseAddr testCageAddr)
                            spentWallet = take 9 walletKeys
                            liveWallet =
                                filter (`notElem` spentWallet) walletKeys
                            (before, afterDelete, afterRollback) =
                                evalPureFromEmptyDB $ do
                                    runPureTransaction addressCodecs $ do
                                        forM_
                                            (zip walletKeys otherKeys)
                                            $ \(walletKey, otherKey) -> do
                                                inserting
                                                    []
                                                    fkv
                                                    hashing
                                                    StandaloneKVCol
                                                    StandaloneCSMTCol
                                                    walletKey
                                                    walletOut
                                                inserting
                                                    []
                                                    fkv
                                                    hashing
                                                    StandaloneKVCol
                                                    StandaloneCSMTCol
                                                    otherKey
                                                    otherOut
                                        keysBefore <-
                                            loadAddressKeys
                                                fkv
                                                addressKey
                                        forM_ spentWallet $ \key ->
                                            deleting
                                                []
                                                fkv
                                                hashing
                                                StandaloneKVCol
                                                StandaloneCSMTCol
                                                key
                                        keysAfterDelete <-
                                            loadAddressKeys
                                                fkv
                                                addressKey
                                        forM_ spentWallet $ \key ->
                                            inserting
                                                []
                                                fkv
                                                hashing
                                                StandaloneKVCol
                                                StandaloneCSMTCol
                                                key
                                                walletOut
                                        keysAfterRollback <-
                                            loadAddressKeys
                                                fkv
                                                addressKey
                                        pure
                                            ( keysBefore
                                            , keysAfterDelete
                                            , keysAfterRollback
                                            )
                        before `shouldBe` Right (sort walletKeys)
                        afterDelete `shouldBe` Right (sort liveWallet)
                        afterRollback `shouldBe` Right (sort walletKeys)
                _ ->
                    expectationFailure
                        "genTxIn did not produce enough unique keys"

    it
        "KVOnly toFull replay removes spent address-prefixed leaves"
        $ do
            let initialWallet = strictKeys "wallet-old-" 48
                initialOther = strictKeys "other-old-" 48
                spentWallet = take 17 initialWallet
                liveWallet =
                    filter (`notElem` spentWallet) initialWallet
                createdWallet = strictKeys "wallet-new-" 23
                followingSpent =
                    case liveWallet of
                        key : _ -> key
                        [] -> error "liveWallet unexpectedly empty"
                expectedWallet =
                    sort
                        $ filter
                            (/= followingSpent)
                            (liveWallet <> createdWallet)
                seedFull = do
                    forM_ initialWallet $ \key ->
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictWalletValue
                    forM_ initialOther $ \key ->
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictOtherValue
            expectedRoot <-
                withStrictPureDb $ \rtx -> do
                    rtx $ do
                        seedFull
                        forM_ spentWallet
                            $ deleting
                                []
                                strictAddressFromKV
                                hashHashing
                                StandaloneKVCol
                                StandaloneCSMTCol
                        forM_ createdWallet $ \key ->
                            inserting
                                []
                                strictAddressFromKV
                                hashHashing
                                StandaloneKVCol
                                StandaloneCSMTCol
                                key
                                strictWalletValue
                        deleting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            followingSpent
                        root hashHashing StandaloneCSMTCol []
            (actualRoot, actualWallet) <-
                withStrictPureDb $ \rtx -> do
                    rtx seedFull
                    let kvOps =
                            mkKVOnlyOps
                                []
                                4
                                100
                                StandaloneKVCol
                                StandaloneCSMTCol
                                StandaloneJournalCol
                                StandaloneMetricsCol
                                (Lens.iso id id)
                                strictAddressFromKV
                                hashHashing
                                rtx
                                rtx
                                (const $ pure ())
                    forM_ spentWallet $ \key ->
                        rtx $ opsDelete (kvCommon kvOps) key
                    forM_ createdWallet $ \key ->
                        rtx
                            $ opsInsert
                                (kvCommon kvOps)
                                key
                                strictWalletValue
                    mFull <- toFull kvOps
                    fullOps <- case mFull of
                        Nothing ->
                            expectationFailure
                                "KVOnly toFull returned Nothing"
                                *> error "unreachable"
                        Just full -> pure full
                    rtx
                        $ opsDelete
                            (fullCommon fullOps)
                            followingSpent
                    actualRoot <-
                        rtx $ root hashHashing StandaloneCSMTCol []
                    actualWallet <-
                        rtx
                            $ loadAddressKeys
                                strictAddressFromKV
                                strictWalletPrefix
                    pure (actualRoot, actualWallet)
            actualRoot `shouldBe` expectedRoot
            actualWallet `shouldBe` Right expectedWallet

    it
        "Full updates remove the old value-derived address leaf"
        $ do
            let key = "same-key"
            (walletAfterDelete, otherAfterDelete) <-
                withStrictPureDb $ \rtx -> do
                    rtx $ do
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictWalletValue
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictOtherValue
                        deleting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                        walletAfterDelete <-
                            loadAddressKeys
                                strictAddressFromKV
                                strictWalletPrefix
                        otherAfterDelete <-
                            loadAddressKeys
                                strictAddressFromKV
                                strictOtherPrefix
                        pure (walletAfterDelete, otherAfterDelete)
            walletAfterDelete `shouldBe` Right []
            otherAfterDelete `shouldBe` Right []

    it
        "KVOnly toFull updates remove the old value-derived address leaf"
        $ do
            let key = "same-key"
            expectedRoot <-
                withStrictPureDb $ \rtx -> do
                    rtx $ do
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictWalletValue
                        deleting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                        inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictOtherValue
                        root hashHashing StandaloneCSMTCol []
            (actualRoot, walletAfterReplay, otherAfterReplay) <-
                withStrictPureDb $ \rtx -> do
                    rtx
                        $ inserting
                            []
                            strictAddressFromKV
                            hashHashing
                            StandaloneKVCol
                            StandaloneCSMTCol
                            key
                            strictWalletValue
                    let kvOps =
                            mkKVOnlyOps
                                []
                                4
                                100
                                StandaloneKVCol
                                StandaloneCSMTCol
                                StandaloneJournalCol
                                StandaloneMetricsCol
                                (Lens.iso id id)
                                strictAddressFromKV
                                hashHashing
                                rtx
                                rtx
                                (const $ pure ())
                    rtx
                        $ opsInsert
                            (kvCommon kvOps)
                            key
                            strictOtherValue
                    mFull <- toFull kvOps
                    case mFull of
                        Nothing ->
                            expectationFailure
                                "KVOnly toFull returned Nothing"
                        Just _ ->
                            pure ()
                    actualRoot <-
                        rtx $ root hashHashing StandaloneCSMTCol []
                    walletAfterReplay <-
                        rtx
                            $ loadAddressKeys
                                strictAddressFromKV
                                strictWalletPrefix
                    otherAfterReplay <-
                        rtx
                            $ loadAddressKeys
                                strictAddressFromKV
                                strictOtherPrefix
                    pure
                        ( actualRoot
                        , walletAfterReplay
                        , otherAfterReplay
                        )
            actualRoot `shouldBe` expectedRoot
            walletAfterReplay `shouldBe` Right []
            otherAfterReplay `shouldBe` Right [key]

encodeTxIn :: TxIn -> BSL.ByteString
encodeTxIn =
    serialize (natVersion @11)

decodeTxIn :: BSL.ByteString -> Either String TxIn
decodeTxIn bs =
    case decodeFull (natVersion @11) bs of
        Left err -> Left (show err)
        Right txIn -> Right txIn

addressCodecs
    :: StandaloneCodecs BSL.ByteString BSL.ByteString Hash
addressCodecs =
    StandaloneCodecs
        { keyCodec = Lens.lazy
        , valueCodec = Lens.lazy
        , nodeCodec = isoHash
        }

loadAddressKeys
    :: (Ord key, Show key)
    => FromKV key value Hash
    -> Key
    -> Transaction
        Pure
        cf
        (Standalone key value Hash)
        op
        (Either String [key])
loadAddressKeys fkv addressKey = do
    leaves <- collectValues StandaloneCSMTCol [] addressKey
    keys <-
        forM leaves $ \leaf ->
            case addressScopedLeafKey fkv addressKey leaf of
                Left err ->
                    pure $ Left err
                Right key -> do
                    mTxOut <- query StandaloneKVCol key
                    pure $ case mTxOut of
                        Nothing ->
                            Left
                                $ "stale address leaf without KV \
                                  \bytes: "
                                    <> show key
                        Just _ ->
                            Right key
    pure $ sequence keys

encodeWalletTxOut :: BSL.ByteString
encodeWalletTxOut =
    encodeTxOutAt testCageAddr

encodeTxOutAt :: Addr -> BSL.ByteString
encodeTxOutAt addr =
    BSL.fromStrict
        $ serialize'
            (natVersion @11)
            ( mkBasicTxOut
                addr
                (inject (Coin 2_000_000))
                :: TxOut ConwayEra
            )

otherAddr :: Addr
otherAddr =
    Addr
        Testnet
        (ScriptHashObj wrongScriptHash)
        StakeRefNull

strictCodecs
    :: StandaloneCodecs BS.ByteString BS.ByteString Hash
strictCodecs =
    StandaloneCodecs
        { keyCodec = Lens.iso id id
        , valueCodec = Lens.iso id id
        , nodeCodec = isoHash
        }

withStrictPureDb
    :: ( ( forall a
            . Transaction
                Pure
                StandaloneCF
                (Standalone BS.ByteString BS.ByteString Hash)
                StandaloneOp
                a
           -> IO a
         )
         -> IO r
       )
    -> IO r
withStrictPureDb action = do
    ref <- newIORef emptyInMemoryDB
    let database = pureDatabase strictCodecs
        rtx tx = do
            db <- readIORef ref
            let (result, db') =
                    runPure
                        db
                        (runTransactionUnguarded database tx)
            writeIORef ref db'
            pure result
    action rtx

strictAddressFromKV
    :: FromKV BS.ByteString BS.ByteString Hash
strictAddressFromKV =
    fromKVHashes
        { treePrefix =
            \value ->
                if value == strictWalletValue
                    then strictWalletPrefix
                    else strictOtherPrefix
        }

strictWalletPrefix :: Key
strictWalletPrefix = hashAddressKey "strict-wallet"

strictOtherPrefix :: Key
strictOtherPrefix = hashAddressKey "strict-other"

strictWalletValue :: BS.ByteString
strictWalletValue = "wallet"

strictOtherValue :: BS.ByteString
strictOtherValue = "other"

strictKeys :: BS.ByteString -> Int -> [BS.ByteString]
strictKeys prefix count =
    [ prefix <> BS.pack [fromIntegral n]
    | n <- [1 .. count]
    ]
