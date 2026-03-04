{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Codecs
-- Description : Serialization codecs for indexer columns
-- License     : Apache-2.0
--
-- 'Prism''-based codecs for encoding and decoding
-- indexer column keys and values to\/from
-- 'ByteString'. Uses CBOR via @cborg@ for structured
-- types. Ledger types ('TxIn', 'Coin', 'KeyHash')
-- are serialized via @cardano-ledger-binary@ (version
-- 2) and embedded as CBOR byte strings within the
-- outer encoding. Trie columns use identity codecs
-- (raw 'ByteString' passthrough) since the MPF
-- library handles its own serialization.
--
-- 'allUnifiedCodecs' builds codecs for
-- 'UnifiedColumns' — the combined 11-column-family
-- selector used by the block processing transaction.
module Cardano.MPFS.Indexer.Codecs
    ( -- * Column codecs
      allCodecs

      -- * Unified codecs
    , allUnifiedCodecs

      -- * Individual prisms
    , tokenIdPrism
    , tokenStatePrism
    , txInPrism
    , requestPrism
    , unitPrism
    , checkpointPrism
    , slotNoPrism
    , rollbackPointPrism
    , trieStatusPrism
    ) where

import Cardano.Ledger.Binary
    ( DecCBOR
    , EncCBOR
    , Version
    , decodeFull
    , natVersion
    , serialize
    )
import Cardano.Ledger.Mary.Value (AssetName (..))
import Codec.CBOR.Decoding
    ( Decoder
    , decodeBytes
    , decodeInteger
    , decodeListLen
    , decodeListLenOf
    , decodeWord8
    )
import Codec.CBOR.Encoding
    ( Encoding
    , encodeBytes
    , encodeInteger
    , encodeListLen
    , encodeWord8
    )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Control.Lens (Prism', prism')
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Short qualified as SBS
import Data.Dependent.Map qualified as DMap
import Database.KV.Transaction
    ( Codecs (..)
    , DMap
    , DSum ((:=>))
    , fromPairList
    )

import MPF.Backend.Standalone
    ( MPFStandaloneCodecs (mpfValueCodec)
    )
import MPF.Hashes (MPFHash)
import MPF.Interface (hexKeyPrism, mpfCodecs)
import MPF.Test.Lib (mpfHashCodecs)

import Cardano.UTxOCSMT.Application.Database.Implementation.Columns
    ( Prisms
    , codecs
    )

import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo
    , TokenId (..)
    , TokenState (..)
    , TxIn
    )
import MTS.Rollbacks.Types (RollbackPoint (..))

import Cardano.MPFS.Indexer.Columns
    ( AllColumns (..)
    , CageCheckpoint (..)
    , TrieStatus (..)
    , UnifiedColumns (..)
    )
import Cardano.MPFS.Indexer.Event
    ( CageInverseOp (..)
    )

-- | Codecs for all indexer column families.
allCodecs :: DMap AllColumns Codecs
allCodecs =
    fromPairList
        [ CageTokens
            :=> Codecs
                { keyCodec = tokenIdPrism
                , valueCodec = tokenStatePrism
                }
        , CageRequests
            :=> Codecs
                { keyCodec = txInPrism
                , valueCodec = requestPrism
                }
        , CageCfg
            :=> Codecs
                { keyCodec = unitPrism
                , valueCodec = checkpointPrism
                }
        , CageRollbacks
            :=> Codecs
                { keyCodec = slotNoPrism
                , valueCodec = rollbackPointPrism
                }
        , TrieNodes
            :=> mpfCodecs isoMPFHash
        , TrieKV
            :=> Codecs
                { keyCodec = hexKeyPrism
                , valueCodec = isoMPFHash
                }
        , TrieMeta
            :=> Codecs
                { keyCodec = tokenIdPrism
                , valueCodec = trieStatusPrism
                }
        ]

-- | Codecs for the unified column set (4 UTxO + 6
-- cage). Combines the UTxO codecs (from
-- cardano-utxo-csmt) with the cage codecs, re-keyed
-- via 'InUtxo' and 'InCage'.
allUnifiedCodecs
    :: Prisms slot hash key value
    -> DMap
        (UnifiedColumns slot hash key value)
        Codecs
allUnifiedCodecs ps =
    DMap.mapKeysMonotonic InUtxo (codecs ps)
        `DMap.union` DMap.mapKeysMonotonic
            InCage
            allCodecs

-- | Encode/decode 'TokenId' as raw asset name
-- bytes. No CBOR wrapping — the 'AssetName'
-- 'ShortByteString' is used directly.
tokenIdPrism :: Prism' ByteString TokenId
tokenIdPrism =
    prism'
        ( \(TokenId (AssetName sbs)) ->
            SBS.fromShort sbs
        )
        ( Just
            . TokenId
            . AssetName
            . SBS.toShort
        )

-- | Encode/decode 'TokenState' as a 5-element CBOR
-- list: @[owner, root, maxFee, processTime,
-- retractTime]@. Ledger types are embedded as
-- sub-encoded byte strings.
tokenStatePrism :: Prism' ByteString TokenState
tokenStatePrism = prism' enc dec
  where
    enc TokenState{..} =
        toStrictByteString
            $ encodeListLen 5
                <> encodeBytes
                    (ledgerEnc owner)
                <> encodeBytes (unRoot root)
                <> encodeBytes
                    (ledgerEnc maxFee)
                <> encodeInteger processTime
                <> encodeInteger retractTime
    dec = decodeCBOR $ do
        decodeListLenOf 5
        owner' <- ledgerDec =<< decodeBytes
        root' <- Root <$> decodeBytes
        maxFee' <- ledgerDec =<< decodeBytes
        processTime' <- decodeInteger
        retractTime' <- decodeInteger
        pure
            TokenState
                { owner = owner'
                , root = root'
                , maxFee = maxFee'
                , processTime = processTime'
                , retractTime = retractTime'
                }

-- | Encode/decode 'TxIn' via its ledger CBOR
-- instances.
txInPrism :: Prism' ByteString TxIn
txInPrism = prism' ledgerEnc ledgerDecMaybe

-- | Encode/decode 'Request' as a 6-element CBOR
-- list: @[token, owner, key, op, fee,
-- submittedAt]@.
requestPrism :: Prism' ByteString Request
requestPrism = prism' enc dec
  where
    enc Request{..} =
        toStrictByteString
            $ encodeListLen 6
                <> encodeBytes
                    (encTokenId requestToken)
                <> encodeBytes
                    (ledgerEnc requestOwner)
                <> encodeBytes requestKey
                <> encodeOperation
                    requestValue
                <> encodeBytes
                    (ledgerEnc requestFee)
                <> encodeInteger
                    requestSubmittedAt
    dec = decodeCBOR $ do
        decodeListLenOf 6
        requestToken <-
            decTokenId =<< decodeBytes
        requestOwner <-
            ledgerDec =<< decodeBytes
        requestKey <- decodeBytes
        requestValue <- decodeOperation
        requestFee <-
            ledgerDec =<< decodeBytes
        requestSubmittedAt <-
            decodeInteger
        pure Request{..}

-- | Encode/decode unit key as empty bytes.
unitPrism :: Prism' ByteString ()
unitPrism =
    prism' (const mempty) (const (Just ()))

-- | Encode/decode 'CageCheckpoint' as a 2-element
-- CBOR list: @[slot, blockId]@.
checkpointPrism :: Prism' ByteString CageCheckpoint
checkpointPrism = prism' enc dec
  where
    enc CageCheckpoint{..} =
        toStrictByteString
            $ encodeListLen 2
                <> encodeBytes
                    (ledgerEnc checkpointSlot)
                <> encodeBytes
                    (unBlockId checkpointBlockId)
    dec = decodeCBOR $ do
        decodeListLenOf 2
        s <- ledgerDec =<< decodeBytes
        b <- BlockId <$> decodeBytes
        pure
            CageCheckpoint
                { checkpointSlot = s
                , checkpointBlockId = b
                }

-- | Encode/decode 'SlotNo' via ledger CBOR.
slotNoPrism :: Prism' ByteString SlotNo
slotNoPrism = prism' ledgerEnc ledgerDecMaybe

-- | Encode/decode 'RollbackPoint CageInverseOp
-- BlockId' as a 2-element CBOR list:
-- @[inverses, meta]@.
rollbackPointPrism
    :: Prism'
        ByteString
        (RollbackPoint CageInverseOp BlockId)
rollbackPointPrism = prism' enc dec
  where
    enc RollbackPoint{..} =
        toStrictByteString
            $ encodeListLen 2
                <> encodeInvOps rpInverses
                <> encodeMaybeMeta rpMeta
    dec = decodeCBOR $ do
        decodeListLenOf 2
        invs <- decodeInvOps
        meta <- decodeMaybeMeta
        pure
            RollbackPoint
                { rpInverses = invs
                , rpMeta = meta
                }
    encodeMaybeMeta Nothing = encodeListLen 0
    encodeMaybeMeta (Just (BlockId bs)) =
        encodeListLen 1 <> encodeBytes bs
    decodeMaybeMeta = do
        n <- decodeListLen
        case n of
            0 -> pure Nothing
            1 ->
                Just . BlockId <$> decodeBytes
            _ ->
                fail "decodeMaybeMeta: bad len"

-- | Prism for 'MPFHash' values in trie columns.
-- Reuses the codec from @haskell-mts@ test lib.
isoMPFHash :: Prism' ByteString MPFHash
isoMPFHash = mpfValueCodec mpfHashCodecs

-- | Encode/decode 'TrieStatus' as a single byte.
-- @0x01@ = 'Visible', @0x02@ = 'Hidden'.
trieStatusPrism :: Prism' ByteString TrieStatus
trieStatusPrism = prism' enc dec
  where
    enc Visible = BS.singleton 0x01
    enc Hidden = BS.singleton 0x02
    dec bs = case BS.uncons bs of
        Just (0x01, rest)
            | BS.null rest -> Just Visible
        Just (0x02, rest)
            | BS.null rest -> Just Hidden
        _ -> Nothing

-- --------------------------------------------------------
-- Ledger serialization bridge
-- --------------------------------------------------------

-- | Protocol version for ledger CBOR encoding.
ledgerVer :: Version
ledgerVer = natVersion @2

-- | Serialize a ledger type to strict bytes.
ledgerEnc :: (EncCBOR a) => a -> ByteString
ledgerEnc =
    BSL.toStrict . serialize ledgerVer

-- | Deserialize a ledger type from strict bytes,
-- failing in the 'Decoder' monad on error.
ledgerDec
    :: (DecCBOR a) => ByteString -> Decoder s a
ledgerDec bs =
    case decodeFull ledgerVer (BSL.fromStrict bs) of
        Left err -> fail (show err)
        Right a -> pure a

-- | Deserialize a ledger type, returning 'Maybe'.
ledgerDecMaybe
    :: (DecCBOR a) => ByteString -> Maybe a
ledgerDecMaybe bs =
    case decodeFull ledgerVer (BSL.fromStrict bs) of
        Left _ -> Nothing
        Right a -> Just a

-- --------------------------------------------------------
-- TokenId helpers
-- --------------------------------------------------------

encTokenId :: TokenId -> ByteString
encTokenId (TokenId (AssetName sbs)) =
    SBS.fromShort sbs

decTokenId :: ByteString -> Decoder s TokenId
decTokenId bs =
    pure $ TokenId $ AssetName $ SBS.toShort bs

-- --------------------------------------------------------
-- Operation CBOR helpers
-- --------------------------------------------------------

-- | Tag: 0 = Insert, 1 = Delete, 2 = Update.
encodeOperation :: Operation -> Encoding
encodeOperation = \case
    Insert v ->
        encodeListLen 2
            <> encodeWord8 0
            <> encodeBytes v
    Delete v ->
        encodeListLen 2
            <> encodeWord8 1
            <> encodeBytes v
    Update v1 v2 ->
        encodeListLen 3
            <> encodeWord8 2
            <> encodeBytes v1
            <> encodeBytes v2

decodeOperation :: Decoder s Operation
decodeOperation = do
    len <- decodeListLen
    tag <- decodeWord8
    case (tag, len) of
        (0, 2) -> Insert <$> decodeBytes
        (1, 2) -> Delete <$> decodeBytes
        (2, 3) ->
            Update
                <$> decodeBytes
                <*> decodeBytes
        _ -> fail "Unknown Operation tag"

-- --------------------------------------------------------
-- CageInverseOp CBOR encoding
-- --------------------------------------------------------

-- Tags: 0=RestoreToken 1=RemoveToken
--       2=RestoreRequest 3=RemoveRequest
--       4=RestoreRoot 5=TrieInsert 6=TrieDelete

encodeInvOps :: [CageInverseOp] -> Encoding
encodeInvOps ops =
    encodeListLen (fromIntegral (length ops))
        <> foldMap encodeInvOp ops

encodeInvOp :: CageInverseOp -> Encoding
encodeInvOp = \case
    InvRestoreToken tid ts ->
        encodeListLen 3
            <> encodeWord8 0
            <> encodeBytes (encTokenId tid)
            <> encodeBytes
                ( toStrictByteString
                    $ encodeTokenState ts
                )
    InvRemoveToken tid ->
        encodeListLen 2
            <> encodeWord8 1
            <> encodeBytes (encTokenId tid)
    InvRestoreRequest txIn req ->
        encodeListLen 3
            <> encodeWord8 2
            <> encodeBytes (ledgerEnc txIn)
            <> encodeBytes (encRequest req)
    InvRemoveRequest txIn ->
        encodeListLen 2
            <> encodeWord8 3
            <> encodeBytes (ledgerEnc txIn)
    InvRestoreRoot tid root ->
        encodeListLen 3
            <> encodeWord8 4
            <> encodeBytes (encTokenId tid)
            <> encodeBytes (unRoot root)
    InvTrieInsert tid k v ->
        encodeListLen 4
            <> encodeWord8 5
            <> encodeBytes (encTokenId tid)
            <> encodeBytes k
            <> encodeBytes v
    InvTrieDelete tid k ->
        encodeListLen 3
            <> encodeWord8 6
            <> encodeBytes (encTokenId tid)
            <> encodeBytes k

encodeTokenState :: TokenState -> Encoding
encodeTokenState TokenState{..} =
    encodeListLen 5
        <> encodeBytes (ledgerEnc owner)
        <> encodeBytes (unRoot root)
        <> encodeBytes (ledgerEnc maxFee)
        <> encodeInteger processTime
        <> encodeInteger retractTime

encRequest :: Request -> ByteString
encRequest Request{..} =
    toStrictByteString
        $ encodeListLen 6
            <> encodeBytes
                (encTokenId requestToken)
            <> encodeBytes
                (ledgerEnc requestOwner)
            <> encodeBytes requestKey
            <> encodeOperation requestValue
            <> encodeBytes
                (ledgerEnc requestFee)
            <> encodeInteger requestSubmittedAt

decodeInvOps :: Decoder s [CageInverseOp]
decodeInvOps = do
    n <- decodeListLen
    mapM (const decodeInvOp) [1 .. n]

decodeInvOp :: Decoder s CageInverseOp
decodeInvOp = do
    len <- decodeListLen
    tag <- decodeWord8
    case (tag, len) of
        (0, 3) -> do
            tid <- decTokenId =<< decodeBytes
            tsBytes <- decodeBytes
            ts <- decTokenState tsBytes
            pure $ InvRestoreToken tid ts
        (1, 2) ->
            InvRemoveToken
                <$> (decTokenId =<< decodeBytes)
        (2, 3) -> do
            txIn <- ledgerDec =<< decodeBytes
            reqBytes <- decodeBytes
            req <- decReq reqBytes
            pure $ InvRestoreRequest txIn req
        (3, 2) ->
            InvRemoveRequest
                <$> (ledgerDec =<< decodeBytes)
        (4, 3) -> do
            tid <- decTokenId =<< decodeBytes
            r <- Root <$> decodeBytes
            pure $ InvRestoreRoot tid r
        (5, 4) -> do
            tid <- decTokenId =<< decodeBytes
            k <- decodeBytes
            InvTrieInsert tid k <$> decodeBytes
        (6, 3) -> do
            tid <- decTokenId =<< decodeBytes
            InvTrieDelete tid <$> decodeBytes
        _ -> fail "Unknown CageInverseOp tag"

decTokenState
    :: ByteString -> Decoder s TokenState
decTokenState bs = case decodeCBOR dec bs of
    Just ts -> pure ts
    Nothing -> fail "decTokenState: bad CBOR"
  where
    dec = do
        decodeListLenOf 5
        owner' <- ledgerDec =<< decodeBytes
        root' <- Root <$> decodeBytes
        maxFee' <- ledgerDec =<< decodeBytes
        processTime' <- decodeInteger
        retractTime' <- decodeInteger
        pure
            TokenState
                { owner = owner'
                , root = root'
                , maxFee = maxFee'
                , processTime = processTime'
                , retractTime = retractTime'
                }

decReq :: ByteString -> Decoder s Request
decReq bs = case decodeCBOR dec bs of
    Just req -> pure req
    Nothing -> fail "decReq: bad CBOR"
  where
    dec = do
        decodeListLenOf 6
        requestToken <-
            decTokenId =<< decodeBytes
        requestOwner <-
            ledgerDec =<< decodeBytes
        requestKey <- decodeBytes
        requestValue <- decodeOperation
        requestFee <-
            ledgerDec =<< decodeBytes
        requestSubmittedAt <- decodeInteger
        pure Request{..}

-- --------------------------------------------------------
-- CBOR decode helper
-- --------------------------------------------------------

-- | Run a CBOR decoder on a strict 'ByteString'.
decodeCBOR
    :: (forall s. Decoder s a)
    -> ByteString
    -> Maybe a
decodeCBOR decoder bs =
    case deserialiseFromBytes
        decoder
        (BSL.fromStrict bs) of
        Right (rest, a)
            | BSL.null rest -> Just a
        _ -> Nothing
